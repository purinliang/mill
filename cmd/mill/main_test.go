package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealth(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()

	newHandler(ready, nil).ServeHTTP(response, request)

	assertResponse(t, response, http.StatusOK, "{\"status\":\"ok\"}\n")
}

func TestLiveness(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/livez", nil)
	response := httptest.NewRecorder()

	check := func(context.Context) error {
		t.Fatal("liveness probe checked database readiness")
		return nil
	}
	newHandler(check, nil).ServeHTTP(response, request)

	assertResponse(t, response, http.StatusOK, "{\"status\":\"ok\"}\n")
}

func TestReadinessWhenDatabaseIsAvailable(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()

	newHandler(ready, nil).ServeHTTP(response, request)

	assertResponse(t, response, http.StatusOK, "{\"status\":\"ready\"}\n")
}

func TestReadinessWhenDatabaseIsUnavailable(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	response := httptest.NewRecorder()

	check := func(context.Context) error {
		return errors.New("database unavailable")
	}
	newHandler(check, nil).ServeHTTP(response, request)

	assertResponse(t, response, http.StatusServiceUnavailable, "{\"status\":\"not_ready\"}\n")
}

func TestUnknownRoute(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	response := httptest.NewRecorder()

	newHandler(ready, nil).ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("status code = %d, want %d", response.Code, http.StatusNotFound)
	}
}

func TestParseParallelism(t *testing.T) {
	for _, value := range []string{"", "zero", "0", "10001"} {
		if _, err := parseParallelism(value); err == nil {
			t.Errorf("parseParallelism(%q) succeeded, want an error", value)
		}
	}
	parallelism, err := parseParallelism("3")
	if err != nil {
		t.Fatalf("parse parallelism: %v", err)
	}
	if parallelism != 3 {
		t.Errorf("parallelism = %d, want 3", parallelism)
	}
}

func ready(context.Context) error {
	return nil
}

func assertResponse(t *testing.T, response *httptest.ResponseRecorder, statusCode int, body string) {
	t.Helper()

	if response.Code != statusCode {
		t.Fatalf("status code = %d, want %d", response.Code, statusCode)
	}
	if contentType := response.Header().Get("Content-Type"); contentType != "application/json" {
		t.Errorf("Content-Type = %q, want %q", contentType, "application/json")
	}
	if got := response.Body.String(); got != body {
		t.Errorf("body = %q, want %q", got, body)
	}
}
