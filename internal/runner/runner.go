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
	Queued    int64 `json:"queued"`
	Running   int64 `json:"running"`
	Succeeded int64 `json:"succeeded"`
	Failed    int64 `json:"failed"`
	Retried   int64 `json:"retried"`
}
type Config struct {
	Workers        int
	QueueSize      int
	MaxAttempts    int
	AttemptTimeout time.Duration
	Backoff        time.Duration
}
type Runner struct {
	cfg       Config
	handler   Handler
	queue     chan string
	mu        sync.RWMutex
	jobs      map[string]*Job
	cancels   map[string]context.CancelFunc
	stopped   chan struct{}
	stopOnce  sync.Once
	wg        sync.WaitGroup
	queued    atomic.Int64
	running   atomic.Int64
	succeeded atomic.Int64
	failed    atomic.Int64
	retried   atomic.Int64
}

func New(cfg Config, handler Handler) *Runner {
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
	return &Runner{cfg: cfg, handler: handler, queue: make(chan string, cfg.QueueSize), jobs: map[string]*Job{}, cancels: map[string]context.CancelFunc{}, stopped: make(chan struct{})}
}
func (r *Runner) Start() {
	for i := 0; i < r.cfg.Workers; i++ {
		r.wg.Add(1)
		go r.worker()
	}
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
	r.mu.Lock()
	if existing, ok := r.jobs[job.ID]; ok {
		copy := *existing
		r.mu.Unlock()
		return copy, nil
	}
	now := time.Now().UTC()
	job.Status = Queued
	job.CreatedAt = now
	job.UpdatedAt = now
	copy := job
	r.jobs[job.ID] = &copy
	r.mu.Unlock()
	select {
	case r.queue <- job.ID:
		r.queued.Add(1)
		return job, nil
	case <-r.stopped:
		return Job{}, errors.New("runner stopped")
	default:
		r.mu.Lock()
		delete(r.jobs, job.ID)
		r.mu.Unlock()
		return Job{}, errors.New("queue full")
	}
}
func (r *Runner) Get(id string) (Job, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	job, ok := r.jobs[id]
	if !ok {
		return Job{}, false
	}
	return *job, true
}
func (r *Runner) List() []Job {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Job, 0, len(r.jobs))
	for _, job := range r.jobs {
		out = append(out, *job)
	}
	return out
}
func (r *Runner) Cancel(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	job, ok := r.jobs[id]
	if !ok || job.Status == Succeeded || job.Status == Failed {
		return false
	}
	if cancel := r.cancels[id]; cancel != nil {
		cancel()
	}
	job.Status = Cancelled
	job.UpdatedAt = time.Now().UTC()
	return true
}
func (r *Runner) Metrics() Metrics {
	return Metrics{r.queued.Load(), r.running.Load(), r.succeeded.Load(), r.failed.Load(), r.retried.Load()}
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
	r.mu.Lock()
	job := r.jobs[id]
	if job == nil || job.Status == Cancelled {
		r.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancels[id] = cancel
	job.Status = Running
	job.UpdatedAt = time.Now().UTC()
	r.queued.Add(-1)
	r.running.Add(1)
	r.mu.Unlock()
	defer func() { cancel(); r.mu.Lock(); delete(r.cancels, id); r.mu.Unlock(); r.running.Add(-1) }()
	var last error
	for attempt := 1; attempt <= r.cfg.MaxAttempts; attempt++ {
		attemptCtx, attemptCancel := context.WithTimeout(ctx, r.cfg.AttemptTimeout)
		r.mu.Lock()
		job.Attempts = attempt
		r.mu.Unlock()
		last = r.handler(attemptCtx, *job)
		attemptCancel()
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
	r.mu.Lock()
	defer r.mu.Unlock()
	if job := r.jobs[id]; job != nil {
		job.Status = status
		job.Error = message
		job.UpdatedAt = time.Now().UTC()
	}
}
