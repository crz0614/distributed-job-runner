package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/crz0614/distributed-job-runner/internal/runner"
)

type APIAuth struct {
	tokenHash [sha256.Size]byte
	enabled   bool
}

func NewAPIAuth(token string) APIAuth {
	if token == "" {
		return APIAuth{}
	}
	return APIAuth{tokenHash: sha256.Sum256([]byte(token)), enabled: true}
}

func (a APIAuth) Protect(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if !a.enabled {
			write(w, http.StatusServiceUnavailable, map[string]string{"error": "writes_disabled"})
			return
		}
		const prefix = "Bearer "
		header := req.Header.Get("Authorization")
		if !strings.HasPrefix(header, prefix) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			write(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		candidate := sha256.Sum256([]byte(strings.TrimPrefix(header, prefix)))
		if subtle.ConstantTimeCompare(candidate[:], a.tokenHash[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			write(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next(w, req)
	}
}

func main() {
	cfg := runner.Config{Workers: 6, QueueSize: 256, MaxAttempts: 3, AttemptTimeout: 5 * time.Second, Backoff: 150 * time.Millisecond}
	handler := func(ctx context.Context, j runner.Job) error {
		select {
		case <-time.After(250 * time.Millisecond):
			if j.Payload["simulate"] == "failure" {
				return errors.New("simulated upstream failure")
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var r *runner.Runner
	var database *sql.DB
	if databaseURL := os.Getenv("DATABASE_URL"); databaseURL != "" {
		startupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var err error
		database, err = runner.OpenPostgres(startupCtx, databaseURL)
		if err != nil {
			log.Fatalf("database startup failed: %v", err)
		}
		defer database.Close()
		if err := runner.MigratePostgres(startupCtx, database); err != nil {
			log.Fatalf("database migration failed: %v", err)
		}
		r = runner.NewWithStore(cfg, handler, runner.NewPostgresStore(database))
	} else {
		log.Printf("DATABASE_URL is not set; using non-durable in-memory storage")
		r = runner.New(cfg, handler)
	}
	if err := r.StartContext(context.Background()); err != nil {
		log.Fatalf("runner startup failed: %v", err)
	}
	auth := NewAPIAuth(os.Getenv("API_TOKEN"))
	if !auth.enabled {
		log.Printf("API_TOKEN is not set; mutating endpoints are disabled")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, req *http.Request) {
		storage := "memory"
		if database != nil {
			storage = "postgres"
			if err := database.PingContext(req.Context()); err != nil {
				write(w, 503, map[string]any{"ok": false, "service": "distributed-job-runner", "storage": storage})
				return
			}
		}
		write(w, 200, map[string]any{"ok": true, "service": "distributed-job-runner", "storage": storage, "writesEnabled": auth.enabled})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) { write(w, 200, r.Metrics()) })
	mux.HandleFunc("GET /jobs", func(w http.ResponseWriter, req *http.Request) {
		jobs, err := r.ListContext(req.Context())
		if err != nil {
			write(w, 503, map[string]string{"error": "storage_unavailable"})
			return
		}
		write(w, 200, jobs)
	})
	mux.HandleFunc("POST /jobs", auth.Protect(func(w http.ResponseWriter, req *http.Request) {
		var job runner.Job
		if json.NewDecoder(http.MaxBytesReader(w, req.Body, 1<<20)).Decode(&job) != nil {
			write(w, 400, map[string]string{"error": "invalid_json"})
			return
		}
		created, err := r.Submit(job)
		if err != nil {
			write(w, 422, map[string]string{"error": err.Error()})
			return
		}
		write(w, 202, created)
	}))
	mux.HandleFunc("GET /jobs/", func(w http.ResponseWriter, req *http.Request) {
		job, ok, err := r.GetContext(req.Context(), strings.TrimPrefix(req.URL.Path, "/jobs/"))
		if err != nil {
			write(w, 503, map[string]string{"error": "storage_unavailable"})
			return
		}
		if !ok {
			write(w, 404, map[string]string{"error": "not_found"})
			return
		}
		write(w, 200, job)
	})
	mux.HandleFunc("DELETE /jobs/", auth.Protect(func(w http.ResponseWriter, req *http.Request) {
		cancelled, err := r.CancelContext(req.Context(), strings.TrimPrefix(req.URL.Path, "/jobs/"))
		if err != nil {
			write(w, 503, map[string]string{"error": "storage_unavailable"})
			return
		}
		if !cancelled {
			write(w, 409, map[string]string{"error": "not_cancellable"})
			return
		}
		write(w, 202, map[string]bool{"cancelled": true})
	}))
	server := &http.Server{Addr: ":8080", Handler: requestLog(mux), ReadHeaderTimeout: 3 * time.Second}
	go func() {
		log.Printf("runner listening on %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(ctx)
	_ = r.Stop(ctx)
}
func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("method=%s path=%s duration=%s", r.Method, r.URL.Path, time.Since(start))
	})
}
