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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/crz0614/distributed-job-runner/internal/runner"
)

type APIAuth struct {
	tokenHash [sha256.Size]byte
	enabled   bool
}

type WriteRateLimiter struct {
	mu          sync.Mutex
	limit       int
	window      time.Duration
	windowStart time.Time
	used        int
	rejected    int64
	now         func() time.Time
}

func NewWriteRateLimiter(limit int, window time.Duration) *WriteRateLimiter {
	return &WriteRateLimiter{limit: limit, window: window, now: time.Now}
}

func (l *WriteRateLimiter) Protect(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		allowed, retryAfter := l.allow()
		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(int((retryAfter+time.Second-1)/time.Second)))
			write(w, http.StatusTooManyRequests, map[string]string{"error": "rate_limited"})
			return
		}
		next(w, req)
	}
}

func (l *WriteRateLimiter) allow() (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if l.windowStart.IsZero() || now.Sub(l.windowStart) >= l.window {
		l.windowStart = now
		l.used = 0
	}
	if l.used >= l.limit {
		l.rejected++
		return false, l.window - now.Sub(l.windowStart)
	}
	l.used++
	return true, 0
}

func (l *WriteRateLimiter) RejectedTotal() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rejected
}

func writeRateLimit() int {
	const defaultLimit = 60
	raw := os.Getenv("WRITE_RATE_LIMIT_PER_MINUTE")
	if raw == "" {
		return defaultLimit
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 1 {
		log.Printf("invalid WRITE_RATE_LIMIT_PER_MINUTE=%q; using %d", raw, defaultLimit)
		return defaultLimit
	}
	return limit
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
	writeLimit := writeRateLimit()
	limiter := NewWriteRateLimiter(writeLimit, time.Minute)
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
		write(w, 200, map[string]any{"ok": true, "service": "distributed-job-runner", "storage": storage, "writesEnabled": auth.enabled, "writeRateLimitPerMinute": writeLimit})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(prometheusMetrics(r.Metrics(), limiter.RejectedTotal())))
	})
	mux.HandleFunc("GET /jobs", func(w http.ResponseWriter, req *http.Request) {
		jobs, err := r.ListContext(req.Context())
		if err != nil {
			write(w, 503, map[string]string{"error": "storage_unavailable"})
			return
		}
		write(w, 200, jobs)
	})
	mux.HandleFunc("POST /jobs", auth.Protect(limiter.Protect(func(w http.ResponseWriter, req *http.Request) {
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
	})))
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
	mux.HandleFunc("DELETE /jobs/", auth.Protect(limiter.Protect(func(w http.ResponseWriter, req *http.Request) {
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
	})))
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

func prometheusMetrics(metrics runner.Metrics, rateLimited int64) string {
	values := []struct {
		name       string
		help       string
		metricType string
		value      int64
	}{
		{"job_runner_queued_jobs", "Current number of queued jobs.", "gauge", metrics.Queued},
		{"job_runner_running_jobs", "Current number of running jobs.", "gauge", metrics.Running},
		{"job_runner_succeeded_jobs_total", "Total number of successful job executions.", "counter", metrics.Succeeded},
		{"job_runner_failed_jobs_total", "Total number of failed job executions.", "counter", metrics.Failed},
		{"job_runner_retries_total", "Total number of retried job attempts.", "counter", metrics.Retried},
		{"job_runner_store_errors_total", "Total number of persistence errors.", "counter", metrics.StoreErrors},
		{"job_runner_rate_limited_requests_total", "Total number of rejected mutating API requests.", "counter", rateLimited},
	}
	var output strings.Builder
	for _, metric := range values {
		output.WriteString("# HELP " + metric.name + " " + metric.help + "\n")
		output.WriteString("# TYPE " + metric.name + " " + metric.metricType + "\n")
		output.WriteString(metric.name + " " + strconv.FormatInt(metric.value, 10) + "\n")
	}
	return output.String()
}
