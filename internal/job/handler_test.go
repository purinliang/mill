package job

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const testJobID = "0198b7c9-1d24-7000-8000-000000000001"

type fakeStore struct {
	create func(context.Context, string, Submission) (Job, bool, error)
	get    func(context.Context, string) (Job, error)
}

func (s fakeStore) Create(ctx context.Context, key string, submission Submission) (Job, bool, error) {
	return s.create(ctx, key, submission)
}

func (s fakeStore) Get(ctx context.Context, id string) (Job, error) {
	return s.get(ctx, id)
}

func TestCreateJob(t *testing.T) {
	store := fakeStore{
		create: func(_ context.Context, key string, submission Submission) (Job, bool, error) {
			if key != "request-001" {
				t.Errorf("idempotency key = %q, want %q", key, "request-001")
			}
			if submission.Workload.Image != "mill/example:dev" {
				t.Errorf("image = %q, want %q", submission.Workload.Image, "mill/example:dev")
			}
			if submission.Workload.Args == nil {
				t.Error("args are nil, want an empty array")
			}
			return exampleJob(), true, nil
		},
	}

	response := serveRequest(store, http.MethodPost, "/jobs", `{
		"workload":{"image":"mill/example:dev"},
		"dataset":{"manifest_uri":"file:///data/manifest.json"}
	}`, map[string]string{
		"Content-Type":    "application/json; charset=utf-8",
		"Idempotency-Key": "request-001",
	})

	if response.Code != http.StatusCreated {
		t.Fatalf("status code = %d, want %d", response.Code, http.StatusCreated)
	}
	if location := response.Header().Get("Location"); location != "/jobs/"+testJobID {
		t.Errorf("Location = %q, want %q", location, "/jobs/"+testJobID)
	}

	var got Job
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got.ID != testJobID {
		t.Errorf("job ID = %q, want %q", got.ID, testJobID)
	}
	if got.Workload.Args == nil {
		t.Error("response args are null, want an empty array")
	}
}

func TestCreateJobReplay(t *testing.T) {
	store := fakeStore{
		create: func(context.Context, string, Submission) (Job, bool, error) {
			return exampleJob(), false, nil
		},
	}

	response := serveValidCreate(store)

	if response.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", response.Code, http.StatusOK)
	}
	if location := response.Header().Get("Location"); location != "/jobs/"+testJobID {
		t.Errorf("Location = %q, want %q", location, "/jobs/"+testJobID)
	}
}

func TestCreateJobConflict(t *testing.T) {
	store := fakeStore{
		create: func(context.Context, string, Submission) (Job, bool, error) {
			return Job{}, false, ErrIdempotencyConflict
		},
	}

	response := serveValidCreate(store)

	assertErrorResponse(t, response, http.StatusConflict, "idempotency_conflict")
}

func TestCreateJobValidation(t *testing.T) {
	unusedStore := fakeStore{
		create: func(context.Context, string, Submission) (Job, bool, error) {
			t.Fatal("store was called for an invalid request")
			return Job{}, false, nil
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
			name: "unknown field",
			body: `{"workload":{"image":"mill/example:dev"},"dataset":{"manifest_uri":"file:///data/manifest.json"},"unknown":true}`,
			headers: map[string]string{
				"Content-Type":    "application/json",
				"Idempotency-Key": "request-001",
			},
			status: http.StatusBadRequest,
			code:   "invalid_request",
		},
		{
			name: "multiple JSON values",
			body: `{} {}`,
			headers: map[string]string{
				"Content-Type":    "application/json",
				"Idempotency-Key": "request-001",
			},
			status: http.StatusBadRequest,
			code:   "invalid_request",
		},
		{
			name: "unsupported manifest scheme",
			body: `{"workload":{"image":"mill/example:dev"},"dataset":{"manifest_uri":"s3://bucket/manifest.json"}}`,
			headers: map[string]string{
				"Content-Type":    "application/json",
				"Idempotency-Key": "request-001",
			},
			status: http.StatusBadRequest,
			code:   "invalid_request",
		},
		{
			name: "body too large",
			body: strings.Repeat(" ", maxRequestBodyBytes+1),
			headers: map[string]string{
				"Content-Type":    "application/json",
				"Idempotency-Key": "request-001",
			},
			status: http.StatusBadRequest,
			code:   "invalid_request",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := serveRequest(unusedStore, http.MethodPost, "/jobs", test.body, test.headers)
			assertErrorResponse(t, response, test.status, test.code)
		})
	}
}

func TestGetJob(t *testing.T) {
	store := fakeStore{
		get: func(_ context.Context, id string) (Job, error) {
			if id != testJobID {
				t.Errorf("job ID = %q, want %q", id, testJobID)
			}
			return exampleJob(), nil
		},
	}

	response := serveRequest(store, http.MethodGet, "/jobs/"+testJobID, "", nil)

	if response.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", response.Code, http.StatusOK)
	}
}

func TestGetJobErrors(t *testing.T) {
	t.Run("invalid ID", func(t *testing.T) {
		store := fakeStore{get: func(context.Context, string) (Job, error) {
			t.Fatal("store was called for an invalid ID")
			return Job{}, nil
		}}
		response := serveRequest(store, http.MethodGet, "/jobs/not-a-uuid", "", nil)
		assertErrorResponse(t, response, http.StatusBadRequest, "invalid_job_id")
	})

	t.Run("not found", func(t *testing.T) {
		store := fakeStore{get: func(context.Context, string) (Job, error) {
			return Job{}, ErrNotFound
		}}
		response := serveRequest(store, http.MethodGet, "/jobs/"+testJobID, "", nil)
		assertErrorResponse(t, response, http.StatusNotFound, "job_not_found")
	})
}

func TestJobMethodNotAllowed(t *testing.T) {
	store := fakeStore{}
	response := serveRequest(store, http.MethodGet, "/jobs", "", nil)

	assertErrorResponse(t, response, http.StatusMethodNotAllowed, "method_not_allowed")
	if allow := response.Header().Get("Allow"); allow != http.MethodPost {
		t.Errorf("Allow = %q, want %q", allow, http.MethodPost)
	}
}

func serveValidCreate(store Store) *httptest.ResponseRecorder {
	return serveRequest(store, http.MethodPost, "/jobs", `{
		"workload":{"image":"mill/example:dev","args":[]},
		"dataset":{"manifest_uri":"file:///data/manifest.json"}
	}`, map[string]string{
		"Content-Type":    "application/json",
		"Idempotency-Key": "request-001",
	})
}

func serveRequest(store Store, method, target, body string, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response := httptest.NewRecorder()
	mux := http.NewServeMux()
	NewHandler(store, log.New(io.Discard, "", 0)).RegisterRoutes(mux)
	mux.ServeHTTP(response, request)
	return response
}

func assertErrorResponse(t *testing.T, response *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if response.Code != status {
		t.Fatalf("status code = %d, want %d; body = %s", response.Code, status, response.Body.String())
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

func exampleJob() Job {
	timestamp := time.Date(2026, time.September, 4, 2, 0, 0, 0, time.UTC)
	return Job{
		ID:    testJobID,
		State: StatePreparing,
		Workload: Workload{
			Image: "mill/example:dev",
			Args:  []string{},
		},
		Dataset:   Dataset{ManifestURI: "file:///data/manifest.json"},
		Output:    Output{URI: "file:///var/lib/mill/output/jobs/" + testJobID + "/"},
		CreatedAt: timestamp,
		UpdatedAt: timestamp,
	}
}
