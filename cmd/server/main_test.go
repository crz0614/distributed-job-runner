package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
	output := prometheusMetrics(runner.Metrics{Queued: 2, Running: 1, Succeeded: 8, Failed: 3, Retried: 4, StoreErrors: 1})
	want := []string{
		"# TYPE job_runner_queued_jobs gauge\njob_runner_queued_jobs 2\n",
		"# TYPE job_runner_running_jobs gauge\njob_runner_running_jobs 1\n",
		"# TYPE job_runner_succeeded_jobs_total counter\njob_runner_succeeded_jobs_total 8\n",
		"# TYPE job_runner_failed_jobs_total counter\njob_runner_failed_jobs_total 3\n",
		"# TYPE job_runner_retries_total counter\njob_runner_retries_total 4\n",
		"# TYPE job_runner_store_errors_total counter\njob_runner_store_errors_total 1\n",
	}
	for _, fragment := range want {
		if !strings.Contains(output, fragment) {
			t.Fatalf("metrics output missing %q:\n%s", fragment, output)
		}
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
