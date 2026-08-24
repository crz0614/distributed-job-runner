package runner

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func wait(t *testing.T, r *Runner, id string, want Status) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if job, ok := r.Get(id); ok && job.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	job, _ := r.Get(id)
	t.Fatalf("status=%s want=%s", job.Status, want)
}
func TestIdempotency(t *testing.T) {
	r := New(Config{Workers: 1}, func(context.Context, Job) error { return nil })
	r.Start()
	defer r.Stop(context.Background())
	first, _ := r.Submit(Job{ID: "same", Kind: "sync"})
	second, _ := r.Submit(Job{ID: "same", Kind: "other"})
	if first.Kind != second.Kind {
		t.Fatal("duplicate submission replaced original job")
	}
}
func TestRetryThenSuccess(t *testing.T) {
	var calls atomic.Int32
	r := New(Config{Workers: 1, MaxAttempts: 3, Backoff: time.Millisecond}, func(context.Context, Job) error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		return nil
	})
	r.Start()
	defer r.Stop(context.Background())
	r.Submit(Job{ID: "retry", Kind: "fetch"})
	wait(t, r, "retry", Succeeded)
	if r.Metrics().Retried != 2 {
		t.Fatalf("retries=%d", r.Metrics().Retried)
	}
}
func TestPermanentFailure(t *testing.T) {
	r := New(Config{Workers: 1, MaxAttempts: 2, Backoff: time.Millisecond}, func(context.Context, Job) error { return errors.New("down") })
	r.Start()
	defer r.Stop(context.Background())
	r.Submit(Job{ID: "fail", Kind: "fetch"})
	wait(t, r, "fail", Failed)
	job, _ := r.Get("fail")
	if job.Attempts != 2 {
		t.Fatalf("attempts=%d", job.Attempts)
	}
}

func TestRecoversQueuedAndRunningJobs(t *testing.T) {
	store := NewMemoryStore()
	now := time.Now().UTC()
	for _, job := range []Job{
		{ID: "queued-before-restart", Kind: "fetch", Status: Queued, CreatedAt: now, UpdatedAt: now},
		{ID: "running-before-restart", Kind: "fetch", Status: Running, CreatedAt: now.Add(time.Millisecond), UpdatedAt: now},
		{ID: "already-done", Kind: "fetch", Status: Succeeded, CreatedAt: now.Add(2 * time.Millisecond), UpdatedAt: now},
	} {
		if _, _, err := store.Create(context.Background(), job); err != nil {
			t.Fatal(err)
		}
	}
	r := NewWithStore(Config{Workers: 1}, func(context.Context, Job) error { return nil }, store)
	if err := r.StartContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer r.Stop(context.Background())
	wait(t, r, "queued-before-restart", Succeeded)
	wait(t, r, "running-before-restart", Succeeded)
	job, _ := r.Get("already-done")
	if job.Attempts != 0 || job.Status != Succeeded {
		t.Fatalf("completed job was replayed: %+v", job)
	}
}

func TestCancelledJobCannotBecomeSucceeded(t *testing.T) {
	started := make(chan struct{})
	r := New(Config{Workers: 1}, func(ctx context.Context, _ Job) error {
		close(started)
		<-ctx.Done()
		return nil
	})
	r.Start()
	defer r.Stop(context.Background())
	if _, err := r.Submit(Job{ID: "cancel-race", Kind: "fetch"}); err != nil {
		t.Fatal(err)
	}
	<-started
	if !r.Cancel("cancel-race") {
		t.Fatal("cancel returned false")
	}
	wait(t, r, "cancel-race", Cancelled)
	time.Sleep(20 * time.Millisecond)
	job, _ := r.Get("cancel-race")
	if job.Status != Cancelled {
		t.Fatalf("cancelled job changed status: %s", job.Status)
	}
}
