// This file tests public routing and provides shared HTTP fixtures.
package httpapi_test

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

	"github.com/purinliang/mill/internal/job"
	"github.com/purinliang/mill/internal/job/httpapi"
)

const testJobID = "0198b7c9-1d24-7000-8000-000000000001"

type fakeStore struct {
	create func(
		context.Context,
		string,
		job.Submission,
	) (job.Job, bool, error)
	get func(context.Context, string) (job.Job, error)
}

func (s fakeStore) Create(
	ctx context.Context,
	key string,
	submission job.Submission,
) (job.Job, bool, error) {
	return s.create(ctx, key, submission)
}

func (s fakeStore) Get(
	ctx context.Context,
	id string,
) (job.Job, error) {
	return s.get(ctx, id)
}

func TestHandlerUsesDefaultLoggerWhenNoneIsProvided(t *testing.T) {
	store := fakeStore{
		get: func(context.Context, string) (job.Job, error) {
			return job.Job{ID: testJobID, State: job.StateRunning}, nil
		},
	}
	mux := http.NewServeMux()
	httpapi.NewHandler(store, nil).RegisterRoutes(mux)
	response := httptest.NewRecorder()
	request := httptest.NewRequest(
		http.MethodGet,
		"/jobs/"+testJobID,
		nil,
	)
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
}

func TestJobRoutesRejectUnsupportedMethods(t *testing.T) {
	tests := []struct {
		name    string
		method  string
		target  string
		allowed string
	}{
		{
			name:    "collection",
			method:  http.MethodGet,
			target:  "/jobs",
			allowed: http.MethodPost,
		},
		{
			name:    "resource",
			method:  http.MethodPost,
			target:  "/jobs/" + testJobID,
			allowed: http.MethodGet,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := serveRequest(
				t,
				fakeStore{},
				test.method,
				test.target,
				"",
				nil,
			)
			assertAPIError(
				t,
				response,
				http.StatusMethodNotAllowed,
				"method_not_allowed",
			)
			if allow := response.Header().Get("Allow"); allow != test.allowed {
				t.Errorf("Allow = %q, want %q", allow, test.allowed)
			}
		})
	}
}

func serveRequest(
	t *testing.T,
	store httpapi.Store,
	method string,
	target string,
	body string,
	headers map[string]string,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(method, target, strings.NewReader(body))
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	return serveHTTP(t, store, request)
}

func serveHTTP(
	t *testing.T,
	store httpapi.Store,
	request *http.Request,
) *httptest.ResponseRecorder {
	t.Helper()
	mux := http.NewServeMux()
	logger := log.New(io.Discard, "", 0)
	httpapi.NewHandler(store, logger).RegisterRoutes(mux)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	return response
}

func assertAPIError(
	t *testing.T,
	response *httptest.ResponseRecorder,
	status int,
	code string,
) {
	t.Helper()
	if response.Code != status {
		t.Fatalf(
			"status = %d, want %d; body = %s",
			response.Code,
			status,
			response.Body.String(),
		)
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

func exampleJob() job.Job {
	timestamp := time.Date(
		2026,
		time.September,
		4,
		2,
		0,
		0,
		0,
		time.UTC,
	)
	return job.Job{
		ID:    testJobID,
		State: job.StatePreparing,
		Executable: job.Executable{
			Image: "mill/example:dev",
			Args:  []string{},
		},
		Input: job.Input{
			URI: "file:///data/records.jsonl",
		},
		Output: job.Output{
			URI: "file:///var/lib/mill/output/jobs/" + testJobID + "/",
		},
		Parallelism: 3,
		CreatedAt:   timestamp,
		UpdatedAt:   timestamp,
	}
}
