// This file tests job submission through the public HTTP API.
package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/job"
)

const testMaxRequestBodyBytes = 64 << 10

func TestCreateJob(t *testing.T) {
	store := fakeStore{
		create: func(
			_ context.Context,
			key string,
			submission job.Submission,
		) (job.Job, bool, error) {
			if key != "request-001" {
				t.Errorf("idempotency key = %q, want request-001", key)
			}
			if submission.Executable.Image != "mill/example:dev" {
				t.Errorf(
					"image = %q, want mill/example:dev",
					submission.Executable.Image,
				)
			}
			if submission.Executable.Args == nil {
				t.Error("args are nil, want an empty array")
			}
			return exampleJob(), true, nil
		},
	}

	response := serveRequest(
		t,
		store,
		http.MethodPost,
		"/jobs",
		`{
			"executable":{"image":"mill/example:dev"},
			"input":{"uri":"file:///data/records.jsonl"}
		}`,
		map[string]string{
			"Content-Type":    "application/json; charset=utf-8",
			"Idempotency-Key": "request-001",
		},
	)

	if response.Code != http.StatusCreated {
		t.Fatalf(
			"status = %d, want %d",
			response.Code,
			http.StatusCreated,
		)
	}
	wantLocation := "/jobs/" + testJobID
	if location := response.Header().Get("Location"); location != wantLocation {
		t.Errorf("Location = %q, want %q", location, wantLocation)
	}

	var got job.Job
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ID != testJobID {
		t.Errorf("job ID = %q, want %q", got.ID, testJobID)
	}
	if got.Executable.Args == nil {
		t.Error("response args are null, want an empty array")
	}
}

func TestCreateJobReplay(t *testing.T) {
	store := fakeStore{
		create: func(
			context.Context,
			string,
			job.Submission,
		) (job.Job, bool, error) {
			return exampleJob(), false, nil
		},
	}

	response := serveValidCreate(t, store)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"status = %d, want %d",
			response.Code,
			http.StatusOK,
		)
	}
}

func TestCreateJobConflicts(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code string
	}{
		{
			name: "idempotency key",
			err:  job.ErrIdempotencyConflict,
			code: "idempotency_conflict",
		},
		{
			name: "partitioned input",
			err:  job.ErrInputConflict,
			code: "input_conflict",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := fakeStore{
				create: func(
					context.Context,
					string,
					job.Submission,
				) (job.Job, bool, error) {
					return job.Job{}, false, test.err
				},
			}
			response := serveValidCreate(t, store)
			assertAPIError(
				t,
				response,
				http.StatusConflict,
				test.code,
			)
		})
	}
}

func TestCreateJobReportsInvalidInput(t *testing.T) {
	store := fakeStore{
		create: func(
			context.Context,
			string,
			job.Submission,
		) (job.Job, bool, error) {
			return job.Job{}, false, &job.ValidationError{
				Field:   "input record 0",
				Problem: "must be valid JSON",
			}
		},
	}
	response := serveValidCreate(t, store)
	assertAPIError(t, response, http.StatusBadRequest, "invalid_input")
}

func TestCreateJobValidation(t *testing.T) {
	unusedStore := fakeStore{
		create: func(
			context.Context,
			string,
			job.Submission,
		) (job.Job, bool, error) {
			t.Fatal("store was called for an invalid request")
			return job.Job{}, false, nil
		},
	}

	tests := []struct {
		name    string
		body    string
		headers map[string]string
		status  int
		code    string
	}{
		{
			name:   "missing content type",
			body:   `{}`,
			status: http.StatusUnsupportedMediaType,
			code:   "unsupported_media_type",
		},
		{
			name: "missing idempotency key",
			body: `{}`,
			headers: map[string]string{
				"Content-Type": "application/json",
			},
			status: http.StatusBadRequest,
			code:   "missing_idempotency_key",
		},
		{
			name: "invalid idempotency key",
			body: `{}`,
			headers: map[string]string{
				"Content-Type":    "application/json",
				"Idempotency-Key": strings.Repeat("x", 256),
			},
			status: http.StatusBadRequest,
			code:   "invalid_idempotency_key",
		},
		{
			name: "unknown field",
			body: `{
				"executable":{"image":"mill/example:dev"},
				"input":{"uri":"file:///data/records.jsonl"},
				"unknown":true
			}`,
			headers: validCreateHeaders(),
			status:  http.StatusBadRequest,
			code:    "invalid_request",
		},
		{
			name:    "multiple JSON values",
			body:    `{} {}`,
			headers: validCreateHeaders(),
			status:  http.StatusBadRequest,
			code:    "invalid_request",
		},
		{
			name: "unsupported input scheme",
			body: `{
				"executable":{"image":"mill/example:dev"},
				"input":{"uri":"https://example.com/records.jsonl"}
			}`,
			headers: validCreateHeaders(),
			status:  http.StatusBadRequest,
			code:    "invalid_request",
		},
		{
			name:    "body too large",
			body:    strings.Repeat(" ", testMaxRequestBodyBytes+1),
			headers: validCreateHeaders(),
			status:  http.StatusBadRequest,
			code:    "invalid_request",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := serveRequest(
				t,
				unusedStore,
				http.MethodPost,
				"/jobs",
				test.body,
				test.headers,
			)
			assertAPIError(
				t,
				response,
				test.status,
				test.code,
			)
		})
	}
}

func TestCreateRejectsDuplicateIdempotencyHeaders(t *testing.T) {
	store := fakeStore{
		create: func(
			context.Context,
			string,
			job.Submission,
		) (job.Job, bool, error) {
			t.Fatal("Create was called for duplicate headers")
			return job.Job{}, false, nil
		},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		strings.NewReader(`{
			"executable":{"image":"mill/word-count:dev"},
			"input":{"uri":"s3://mill-input/records.jsonl"}
		}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Add("Idempotency-Key", "request-one")
	request.Header.Add("Idempotency-Key", "request-two")

	response := serveHTTP(t, store, request)
	assertAPIError(
		t,
		response,
		http.StatusBadRequest,
		"missing_idempotency_key",
	)
}

func TestCreateHidesUnexpectedStoreErrors(t *testing.T) {
	backendFailure := errors.New(
		"postgresql://admin:secret@database/mill",
	)
	store := fakeStore{
		create: func(
			context.Context,
			string,
			job.Submission,
		) (job.Job, bool, error) {
			return job.Job{}, false, backendFailure
		},
	}
	response := serveValidCreate(t, store)
	assertAPIError(
		t,
		response,
		http.StatusInternalServerError,
		"internal_error",
	)
	if strings.Contains(response.Body.String(), "secret") {
		t.Fatalf("response exposed backend details: %s", response.Body)
	}
}

func serveValidCreate(
	t *testing.T,
	store fakeStore,
) *httptest.ResponseRecorder {
	t.Helper()
	return serveRequest(
		t,
		store,
		http.MethodPost,
		"/jobs",
		`{
			"executable":{"image":"mill/example:dev","args":[]},
			"input":{"uri":"file:///data/records.jsonl"}
		}`,
		validCreateHeaders(),
	)
}

func validCreateHeaders() map[string]string {
	return map[string]string{
		"Content-Type":    "application/json",
		"Idempotency-Key": "request-001",
	}
}
