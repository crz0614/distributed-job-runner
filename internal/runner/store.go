package runner

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Store is the durable state boundary used by Runner. Create must be
// idempotent: when a job already exists it returns the original record with
// created=false.
type Store interface {
	Create(context.Context, Job) (job Job, created bool, err error)
	Get(context.Context, string) (Job, bool, error)
	List(context.Context) ([]Job, error)
	Update(context.Context, Job) error
	Delete(context.Context, string) error
	StartAttempt(context.Context, string, int, time.Time) error
	FinishAttempt(context.Context, string, int, string, string, time.Time) error
}

type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]Job
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: make(map[string]Job)}
}

func (s *MemoryStore) Create(_ context.Context, job Job) (Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.jobs[job.ID]; ok {
		return cloneJob(existing), false, nil
	}
	s.jobs[job.ID] = cloneJob(job)
	return cloneJob(job), true, nil
}

func (s *MemoryStore) Get(_ context.Context, id string) (Job, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, ok := s.jobs[id]
	return cloneJob(job), ok, nil
}

func (s *MemoryStore) List(_ context.Context) ([]Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Job, 0, len(s.jobs))
	for _, job := range s.jobs {
		out = append(out, cloneJob(job))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *MemoryStore) Update(_ context.Context, job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = cloneJob(job)
	return nil
}

func (s *MemoryStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.jobs, id)
	return nil
}

func (s *MemoryStore) StartAttempt(context.Context, string, int, time.Time) error { return nil }

func (s *MemoryStore) FinishAttempt(context.Context, string, int, string, string, time.Time) error {
	return nil
}

func cloneJob(job Job) Job {
	if job.Payload != nil {
		job.Payload = map[string]string{}
		for key, value := range job.Payload {
			job.Payload[key] = value
		}
	}
	return job
}
