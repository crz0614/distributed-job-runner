package runner

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestPostgresStoreLifecycleAndIdempotency(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := OpenPostgres(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, `TRUNCATE job_attempts, jobs`); err != nil {
		t.Fatal(err)
	}
	store := NewPostgresStore(db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	original := Job{ID: "persistent", Kind: "collect", Payload: map[string]string{"url": "https://example.com"}, Status: Queued, CreatedAt: now, UpdatedAt: now}
	created, inserted, err := store.Create(ctx, original)
	if err != nil || !inserted || created.ID != original.ID {
		t.Fatalf("first create: job=%+v inserted=%v err=%v", created, inserted, err)
	}
	duplicate := original
	duplicate.Kind = "must-not-replace"
	existing, inserted, err := store.Create(ctx, duplicate)
	if err != nil || inserted || existing.Kind != original.Kind {
		t.Fatalf("duplicate create: job=%+v inserted=%v err=%v", existing, inserted, err)
	}
	existing.Status = Running
	existing.Attempts = 1
	existing.UpdatedAt = now.Add(time.Second)
	if err := store.Update(ctx, existing); err != nil {
		t.Fatal(err)
	}
	loaded, ok, err := store.Get(ctx, original.ID)
	if err != nil || !ok || loaded.Status != Running || loaded.Attempts != 1 || loaded.Payload["url"] != original.Payload["url"] {
		t.Fatalf("loaded job=%+v ok=%v err=%v", loaded, ok, err)
	}
	if err := store.StartAttempt(ctx, original.ID, 1, now); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishAttempt(ctx, original.ID, 1, "succeeded", "", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var outcome string
	if err := db.QueryRowContext(ctx, `SELECT outcome FROM job_attempts WHERE job_id = $1 AND attempt = 1`, original.ID).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != "succeeded" {
		t.Fatalf("attempt outcome=%q", outcome)
	}
	jobs, err := store.List(ctx)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("list jobs=%+v err=%v", jobs, err)
	}
	if err := store.Delete(ctx, original.ID); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := store.Get(ctx, original.ID); err != nil || ok {
		t.Fatalf("deleted job still exists: ok=%v err=%v", ok, err)
	}
}
