package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Status string

const (
	Queued    Status = "queued"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Cancelled Status = "cancelled"
)

type Job struct {
	ID        string            `json:"id"`
	Kind      string            `json:"kind"`
	Payload   map[string]string `json:"payload"`
	Status    Status            `json:"status"`
	Attempts  int               `json:"attempts"`
	Error     string            `json:"error,omitempty"`
	CreatedAt time.Time         `json:"createdAt"`
	UpdatedAt time.Time         `json:"updatedAt"`
}
type Handler func(context.Context, Job) error
type Metrics struct {
	Queued      int64 `json:"queued"`
	Running     int64 `json:"running"`
	Succeeded   int64 `json:"succeeded"`
	Failed      int64 `json:"failed"`
	Retried     int64 `json:"retried"`
	StoreErrors int64 `json:"storeErrors"`
}
type Config struct {
	Workers        int
	QueueSize      int
	MaxAttempts    int
	AttemptTimeout time.Duration
	Backoff        time.Duration
}
type Runner struct {
	cfg         Config
	handler     Handler
	store       Store
	queue       chan string
	mu          sync.RWMutex
	cancels     map[string]context.CancelFunc
	stopped     chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
	queued      atomic.Int64
	running     atomic.Int64
	succeeded   atomic.Int64
	failed      atomic.Int64
	retried     atomic.Int64
	storeErrors atomic.Int64
}

func New(cfg Config, handler Handler) *Runner {
	return NewWithStore(cfg, handler, NewMemoryStore())
}

func NewWithStore(cfg Config, handler Handler, store Store) *Runner {
	if cfg.Workers < 1 {
		cfg.Workers = 4
	}
	if cfg.QueueSize < 1 {
		cfg.QueueSize = 128
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 3
	}
	if cfg.AttemptTimeout <= 0 {
		cfg.AttemptTimeout = 10 * time.Second
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = 100 * time.Millisecond
	}
	if store == nil {
		store = NewMemoryStore()
	}
	return &Runner{cfg: cfg, handler: handler, store: store, queue: make(chan string, cfg.QueueSize), cancels: map[string]context.CancelFunc{}, stopped: make(chan struct{})}
}
func (r *Runner) Start() {
	if err := r.StartContext(context.Background()); err != nil {
		r.storeErrors.Add(1)
	}
}
func (r *Runner) StartContext(ctx context.Context) error {
	jobs, err := r.store.List(ctx)
	if err != nil {
		return fmt.Errorf("recover jobs: %w", err)
	}
	for _, job := range jobs {
		if job.Status != Queued && job.Status != Running {
			continue
		}
		job.Status = Queued
		job.Error = ""
		job.UpdatedAt = time.Now().UTC()
		if err := r.store.Update(ctx, job); err != nil {
			return fmt.Errorf("recover job %q: %w", job.ID, err)
		}
		select {
		case r.queue <- job.ID:
			r.queued.Add(1)
		default:
			return fmt.Errorf("recover jobs: queue capacity exceeded")
		}
	}
	for i := 0; i < r.cfg.Workers; i++ {
		r.wg.Add(1)
		go r.worker()
	}
	return nil
}
func (r *Runner) Stop(ctx context.Context) error {
	r.stopOnce.Do(func() { close(r.stopped) })
	done := make(chan struct{})
	go func() { r.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (r *Runner) Submit(job Job) (Job, error) {
	if job.ID == "" || job.Kind == "" {
		return Job{}, errors.New("id and kind are required")
	}
	now := time.Now().UTC()
	job.Status = Queued
	job.CreatedAt = now
	job.UpdatedAt = now
	stored, created, err := r.store.Create(context.Background(), job)
	if err != nil {
		r.storeErrors.Add(1)
		return Job{}, fmt.Errorf("persist job: %w", err)
	}
	if !created {
		return stored, nil
	}
	select {
	case r.queue <- job.ID:
		r.queued.Add(1)
		return job, nil
	case <-r.stopped:
		return Job{}, errors.New("runner stopped")
	default:
		if err := r.store.Delete(context.Background(), job.ID); err != nil {
			r.storeErrors.Add(1)
			return Job{}, fmt.Errorf("queue full; rollback persistence: %w", err)
		}
		return Job{}, errors.New("queue full")
	}
}
func (r *Runner) Get(id string) (Job, bool) {
	job, ok, err := r.GetContext(context.Background(), id)
	if err != nil {
		return Job{}, false
	}
	return job, ok
}
func (r *Runner) GetContext(ctx context.Context, id string) (Job, bool, error) {
	job, ok, err := r.store.Get(ctx, id)
	if err != nil {
		r.storeErrors.Add(1)
		return Job{}, false, err
	}
	return job, ok, nil
}
func (r *Runner) List() []Job {
	jobs, err := r.ListContext(context.Background())
	if err != nil {
		return []Job{}
	}
	return jobs
}
func (r *Runner) ListContext(ctx context.Context) ([]Job, error) {
	jobs, err := r.store.List(ctx)
	if err != nil {
		r.storeErrors.Add(1)
		return nil, err
	}
	return jobs, nil
}
func (r *Runner) Cancel(id string) bool {
	ok, err := r.CancelContext(context.Background(), id)
	return err == nil && ok
}
func (r *Runner) CancelContext(ctx context.Context, id string) (bool, error) {
	job, ok, err := r.store.Get(ctx, id)
	if err != nil {
		r.storeErrors.Add(1)
		return false, err
	}
	if !ok || job.Status == Succeeded || job.Status == Failed {
		return false, nil
	}
	r.mu.Lock()
	if cancel := r.cancels[id]; cancel != nil {
		cancel()
	}
	r.mu.Unlock()
	job.Status = Cancelled
	job.UpdatedAt = time.Now().UTC()
	if err := r.store.Update(ctx, job); err != nil {
		r.storeErrors.Add(1)
		return false, err
	}
	return true, nil
}
func (r *Runner) Metrics() Metrics {
	return Metrics{r.queued.Load(), r.running.Load(), r.succeeded.Load(), r.failed.Load(), r.retried.Load(), r.storeErrors.Load()}
}
func (r *Runner) worker() {
	defer r.wg.Done()
	for {
		select {
		case <-r.stopped:
			return
		case id := <-r.queue:
			r.execute(id)
		}
	}
}
func (r *Runner) execute(id string) {
	job, ok, err := r.store.Get(context.Background(), id)
	if err != nil {
		r.storeErrors.Add(1)
		return
	}
	if !ok || job.Status == Cancelled {
		if ok && job.Status == Cancelled {
			r.queued.Add(-1)
		}
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.mu.Lock()
	r.cancels[id] = cancel
	r.mu.Unlock()
	job.Status = Running
	job.UpdatedAt = time.Now().UTC()
	if err := r.store.Update(context.Background(), job); err != nil {
		r.storeErrors.Add(1)
		cancel()
		r.mu.Lock()
		delete(r.cancels, id)
		r.mu.Unlock()
		return
	}
	r.queued.Add(-1)
	r.running.Add(1)
	defer func() { cancel(); r.mu.Lock(); delete(r.cancels, id); r.mu.Unlock(); r.running.Add(-1) }()
	var last error
	for attempt := 1; attempt <= r.cfg.MaxAttempts; attempt++ {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
		job.Attempts = attempt
		job.UpdatedAt = time.Now().UTC()
		if err := r.store.Update(context.Background(), job); err != nil {
			r.storeErrors.Add(1)
			attemptCancel()
			return
		}
		if err := r.store.StartAttempt(context.Background(), id, attempt, job.UpdatedAt); err != nil {
			r.storeErrors.Add(1)
			attemptCancel()
			return
		}
		last = r.handler(attemptCtx, job)
		attemptErr := attemptCtx.Err()
		attemptCancel()
		outcome := "failed"
		if last == nil {
			outcome = "succeeded"
		} else if errors.Is(attemptErr, context.DeadlineExceeded) {
			outcome = "timed_out"
		} else if ctx.Err() != nil {
			outcome = "cancelled"
		}
		message := ""
		if last != nil {
			message = last.Error()
		}
		if err := r.store.FinishAttempt(context.Background(), id, attempt, outcome, message, time.Now().UTC()); err != nil {
			r.storeErrors.Add(1)
			return
		}
		if last == nil {
			r.finish(id, Succeeded, "")
			r.succeeded.Add(1)
			return
		}
		if ctx.Err() != nil {
			r.finish(id, Cancelled, ctx.Err().Error())
			return
		}
		if attempt < r.cfg.MaxAttempts {
			r.retried.Add(1)
			select {
			case <-time.After(time.Duration(attempt) * r.cfg.Backoff):
			case <-ctx.Done():
				r.finish(id, Cancelled, ctx.Err().Error())
				return
			}
		}
	}
	r.finish(id, Failed, fmt.Sprintf("after %d attempts: %v", r.cfg.MaxAttempts, last))
	r.failed.Add(1)
}
func (r *Runner) finish(id string, status Status, message string) {
	job, ok, err := r.store.Get(context.Background(), id)
	if err != nil || !ok {
		r.storeErrors.Add(1)
		return
	}
	if job.Status == Cancelled && status != Cancelled {
		return
	}
	job.Status = status
	job.Error = message
	job.UpdatedAt = time.Now().UTC()
	if err := r.store.Update(context.Background(), job); err != nil {
		r.storeErrors.Add(1)
	}
}
