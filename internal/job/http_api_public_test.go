package job_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/job"
)

const publicTestJobID = "0199c123-4567-7000-8000-000000000001"

type publicJobStore struct {
	create func(context.Context, string, job.Submission) (job.Job, bool, error)
	get    func(context.Context, string) (job.Job, error)
}

func (s publicJobStore) Create(ctx context.Context, key string, submission job.Submission) (job.Job, bool, error) {
	return s.create(ctx, key, submission)
}

func (s publicJobStore) Get(ctx context.Context, id string) (job.Job, error) {
	return s.get(ctx, id)
}

func TestGetCompletedJobPreservesThePublicJSONContract(t *testing.T) {
	now := time.Date(2026, 9, 8, 3, 4, 5, 0, time.UTC)
	want := job.Job{
		ID:    publicTestJobID,
		State: job.StateCompleted,
		Executable: job.Executable{
			Image: "mill/word-count:dev",
			Args:  []string{"--minimum-length", "3"},
		},
		Input:         job.Input{URI: "s3://mill-input/records.jsonl", SHA256: strings.Repeat("a", 64), RecordCount: 20},
		Output:        job.Output{URI: "s3://mill-output/jobs/" + publicTestJobID + "/"},
		Parallelism:   3,
		ResourceClass: job.ResourceClassMedium,
		Resources: execution.Resources{
			CPURequestMillis: 100, CPULimitMillis: 1000,
			MemoryRequestBytes: 512 << 20, MemoryLimitBytes: 512 << 20,
		},
		Progress: job.Progress{Total: 2, Completed: 2},
		Results: []job.Result{
			{TaskID: "task-1", ShardIndex: 0, AttemptID: "attempt-1", URI: "s3://mill-output/task-1.jsonl"},
			{TaskID: "task-2", ShardIndex: 1, AttemptID: "attempt-2", URI: "s3://mill-output/task-2.jsonl"},
		},
		CreatedAt: now.Add(-time.Minute),
		UpdatedAt: now,
	}
	store := publicJobStore{
		get: func(_ context.Context, id string) (job.Job, error) {
			if id != publicTestJobID {
				t.Fatalf("Get id = %q", id)
			}
			return want, nil
		},
	}
	response := servePublicJobRequest(t, store, http.MethodGet, "/jobs/"+publicTestJobID, "", nil)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("Content-Type = %q", response.Header().Get("Content-Type"))
	}
	var got job.Job
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response job = %+v, want %+v", got, want)
	}
}

func TestCreateRejectsDuplicateIdempotencyHeaders(t *testing.T) {
	store := publicJobStore{
		create: func(context.Context, string, job.Submission) (job.Job, bool, error) {
			t.Fatal("Create was called for duplicate Idempotency-Key headers")
			return job.Job{}, false, nil
		},
	}
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{
		"executable":{"image":"mill/word-count:dev"},
		"input":{"uri":"s3://mill-input/records.jsonl"}
	}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Add("Idempotency-Key", "request-one")
	request.Header.Add("Idempotency-Key", "request-two")

	response := servePublicJobHTTP(t, store, request)
	assertPublicAPIError(t, response, http.StatusBadRequest, "missing_idempotency_key")
}

func TestHTTPAPIHidesUnexpectedStoreErrors(t *testing.T) {
	backendFailure := errors.New("postgresql://admin:secret@database/mill")
	tests := []struct {
		name    string
		method  string
		target  string
		body    string
		headers map[string]string
		store   publicJobStore
	}{
		{
			name:   "create",
			method: http.MethodPost,
			target: "/jobs",
			body: `{
				"executable":{"image":"mill/word-count:dev"},
				"input":{"uri":"s3://mill-input/records.jsonl"}
			}`,
			headers: map[string]string{"Content-Type": "application/json", "Idempotency-Key": "request-1"},
			store: publicJobStore{create: func(context.Context, string, job.Submission) (job.Job, bool, error) {
				return job.Job{}, false, backendFailure
			}},
		},
		{
			name:   "get",
			method: http.MethodGet,
			target: "/jobs/" + publicTestJobID,
			store: publicJobStore{get: func(context.Context, string) (job.Job, error) {
				return job.Job{}, backendFailure
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := servePublicJobRequest(t, test.store, test.method, test.target, test.body, test.headers)
			assertPublicAPIError(t, response, http.StatusInternalServerError, "internal_error")
			if strings.Contains(response.Body.String(), "secret") || strings.Contains(response.Body.String(), "postgresql") {
				t.Fatalf("response exposed backend details: %s", response.Body.String())
			}
		})
	}
}

func servePublicJobRequest(t *testing.T, store job.Store, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	return servePublicJobHTTP(t, store, request)
}

func servePublicJobHTTP(t *testing.T, store job.Store, request *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	job.NewHandler(store, log.New(io.Discard, "", 0)).RegisterRoutes(mux)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	return response
}

func assertPublicAPIError(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status = %d, want %d; body = %s", response.Code, status, response.Body.String())
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if body.Error.Code != code {
		t.Fatalf("error code = %q, want %q", body.Error.Code, code)
	}
}
