package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
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
