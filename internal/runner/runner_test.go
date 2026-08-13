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
