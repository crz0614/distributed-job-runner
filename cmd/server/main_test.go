package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/crz0614/distributed-job-runner/internal/runner"
)

func TestAPIAuthFailsClosedWithoutToken(t *testing.T) {
	called := false
	handler := NewAPIAuth("").Protect(func(http.ResponseWriter, *http.Request) { called = true })
	recorder := httptest.NewRecorder()
	handler(recorder, httptest.NewRequest(http.MethodPost, "/jobs", nil))
	if recorder.Code != http.StatusServiceUnavailable || called {
		t.Fatalf("status=%d called=%v", recorder.Code, called)
	}
}

func TestPrometheusMetricsExposeStableNamesAndTypes(t *testing.T) {
	output := prometheusMetrics(runner.Metrics{Queued: 2, Running: 1, Succeeded: 8, Failed: 3, Retried: 4, StoreErrors: 1}, 2)
	want := []string{
		"# TYPE job_runner_queued_jobs gauge\njob_runner_queued_jobs 2\n",
		"# TYPE job_runner_running_jobs gauge\njob_runner_running_jobs 1\n",
		"# TYPE job_runner_succeeded_jobs_total counter\njob_runner_succeeded_jobs_total 8\n",
		"# TYPE job_runner_failed_jobs_total counter\njob_runner_failed_jobs_total 3\n",
		"# TYPE job_runner_retries_total counter\njob_runner_retries_total 4\n",
		"# TYPE job_runner_store_errors_total counter\njob_runner_store_errors_total 1\n",
		"# TYPE job_runner_rate_limited_requests_total counter\njob_runner_rate_limited_requests_total 2\n",
	}
	for _, fragment := range want {
		if !strings.Contains(output, fragment) {
			t.Fatalf("metrics output missing %q:\n%s", fragment, output)
		}
	}
}

func TestWriteRateLimiterRejectsAndResets(t *testing.T) {
	limiter := NewWriteRateLimiter(2, time.Minute)
	now := time.Date(2026, time.August, 25, 0, 0, 0, 0, time.UTC)
	limiter.now = func() time.Time { return now }
	called := 0
	handler := limiter.Protect(func(http.ResponseWriter, *http.Request) { called++ })

	for i := 0; i < 2; i++ {
		recorder := httptest.NewRecorder()
		handler(recorder, httptest.NewRequest(http.MethodPost, "/jobs", nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("allowed request %d status=%d", i, recorder.Code)
		}
	}

	rejected := httptest.NewRecorder()
	handler(rejected, httptest.NewRequest(http.MethodPost, "/jobs", nil))
	if rejected.Code != http.StatusTooManyRequests || rejected.Header().Get("Retry-After") != "60" {
		t.Fatalf("rejected status=%d retry-after=%q", rejected.Code, rejected.Header().Get("Retry-After"))
	}
	if called != 2 || limiter.RejectedTotal() != 1 {
		t.Fatalf("called=%d rejected=%d", called, limiter.RejectedTotal())
	}

	now = now.Add(time.Minute)
	reset := httptest.NewRecorder()
	handler(reset, httptest.NewRequest(http.MethodPost, "/jobs", nil))
	if reset.Code != http.StatusOK || called != 3 {
		t.Fatalf("reset status=%d called=%d", reset.Code, called)
	}
}

func TestAPIAuthRejectsInvalidAndAcceptsValidBearerToken(t *testing.T) {
	called := false
	handler := NewAPIAuth("correct-token").Protect(func(http.ResponseWriter, *http.Request) { called = true })

	invalid := httptest.NewRecorder()
	invalidRequest := httptest.NewRequest(http.MethodPost, "/jobs", nil)
	invalidRequest.Header.Set("Authorization", "Bearer wrong-token")
	handler(invalid, invalidRequest)
	if invalid.Code != http.StatusUnauthorized || called {
		t.Fatalf("invalid status=%d called=%v", invalid.Code, called)
	}

	valid := httptest.NewRecorder()
	validRequest := httptest.NewRequest(http.MethodPost, "/jobs", nil)
	validRequest.Header.Set("Authorization", "Bearer correct-token")
	handler(valid, validRequest)
	if valid.Code != http.StatusOK || !called {
		t.Fatalf("valid status=%d called=%v", valid.Code, called)
	}
}
