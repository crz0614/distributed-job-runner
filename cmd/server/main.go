package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/crz0614/distributed-job-runner/internal/runner"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	r := runner.New(runner.Config{Workers: 6, QueueSize: 256, MaxAttempts: 3, AttemptTimeout: 5 * time.Second, Backoff: 150 * time.Millisecond}, func(ctx context.Context, j runner.Job) error {
		select {
		case <-time.After(250 * time.Millisecond):
			if j.Payload["simulate"] == "failure" {
				return errors.New("simulated upstream failure")
			}
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	r.Start()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		write(w, 200, map[string]any{"ok": true, "service": "distributed-job-runner"})
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) { write(w, 200, r.Metrics()) })
	mux.HandleFunc("GET /jobs", func(w http.ResponseWriter, _ *http.Request) { write(w, 200, r.List()) })
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, req *http.Request) {
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
	})
	mux.HandleFunc("GET /jobs/", func(w http.ResponseWriter, req *http.Request) {
		job, ok := r.Get(strings.TrimPrefix(req.URL.Path, "/jobs/"))
		if !ok {
			write(w, 404, map[string]string{"error": "not_found"})
			return
		}
		write(w, 200, job)
	})
	mux.HandleFunc("DELETE /jobs/", func(w http.ResponseWriter, req *http.Request) {
		if !r.Cancel(strings.TrimPrefix(req.URL.Path, "/jobs/")) {
			write(w, 409, map[string]string{"error": "not_cancellable"})
			return
		}
		write(w, 202, map[string]bool{"cancelled": true})
	})
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
