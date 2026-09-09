// This file tests job retrieval through the public HTTP API.
package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/job"
)

func TestGetJob(t *testing.T) {
	store := fakeStore{
		get: func(_ context.Context, id string) (job.Job, error) {
			if id != testJobID {
				t.Errorf("job ID = %q, want %q", id, testJobID)
			}
			return exampleJob(), nil
		},
	}

	response := serveRequest(
		t,
		store,
		http.MethodGet,
		"/jobs/"+testJobID,
		"",
		nil,
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"status = %d, want %d",
			response.Code,
			http.StatusOK,
		)
	}
}

func TestGetCompletedJobPreservesPublicJSONContract(t *testing.T) {
	now := time.Date(2026, 9, 8, 3, 4, 5, 0, time.UTC)
	want := job.Job{
		ID:    testJobID,
		State: job.StateCompleted,
		Executable: job.Executable{
			Image: "mill/word-count:dev",
			Args:  []string{"--minimum-length", "3"},
		},
		Input: job.Input{
			URI:         "s3://mill-input/records.jsonl",
			SHA256:      strings.Repeat("a", 64),
			RecordCount: 20,
		},
		Output: job.Output{
			URI: "s3://mill-output/jobs/" + testJobID + "/",
		},
		Parallelism:   3,
		ResourceClass: job.ResourceClassMedium,
		Resources: execution.Resources{
			CPURequestMillis:   100,
			CPULimitMillis:     1000,
			MemoryRequestBytes: 512 << 20,
			MemoryLimitBytes:   512 << 20,
		},
		Progress: job.Progress{Total: 2, Completed: 2},
		Results: []job.Result{
			{
				TaskID:     "task-1",
				ShardIndex: 0,
				AttemptID:  "attempt-1",
				URI:        "s3://mill-output/task-1.jsonl",
			},
			{
				TaskID:     "task-2",
				ShardIndex: 1,
				AttemptID:  "attempt-2",
				URI:        "s3://mill-output/task-2.jsonl",
			},
		},
		CreatedAt: now.Add(-time.Minute),
		UpdatedAt: now,
	}
	store := fakeStore{
		get: func(_ context.Context, id string) (job.Job, error) {
			if id != testJobID {
				t.Fatalf("Get id = %q", id)
			}
			return want, nil
		},
	}
	response := serveRequest(
		t,
		store,
		http.MethodGet,
		"/jobs/"+testJobID,
		"",
		nil,
	)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	if response.Header().Get("Content-Type") != "application/json" {
		t.Fatalf(
			"Content-Type = %q",
			response.Header().Get("Content-Type"),
		)
	}
	var got job.Job
	if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("response job = %+v, want %+v", got, want)
	}
}

func TestGetJobErrors(t *testing.T) {
	t.Run("invalid ID", func(t *testing.T) {
		store := fakeStore{
			get: func(context.Context, string) (job.Job, error) {
				t.Fatal("store was called for an invalid ID")
				return job.Job{}, nil
			},
		}
		response := serveRequest(
			t,
			store,
			http.MethodGet,
			"/jobs/not-a-uuid",
			"",
			nil,
		)
		assertAPIError(
			t,
			response,
			http.StatusBadRequest,
			"invalid_job_id",
		)
	})

	t.Run("not found", func(t *testing.T) {
		store := fakeStore{
			get: func(context.Context, string) (job.Job, error) {
				return job.Job{}, job.ErrNotFound
			},
		}
		response := serveRequest(
			t,
			store,
			http.MethodGet,
			"/jobs/"+testJobID,
			"",
			nil,
		)
		assertAPIError(
			t,
			response,
			http.StatusNotFound,
			"job_not_found",
		)
	})
}

func TestGetHidesUnexpectedStoreErrors(t *testing.T) {
	backendFailure := errors.New(
		"postgresql://admin:secret@database/mill",
	)
	store := fakeStore{
		get: func(context.Context, string) (job.Job, error) {
			return job.Job{}, backendFailure
		},
	}
	response := serveRequest(
		t,
		store,
		http.MethodGet,
		"/jobs/"+testJobID,
		"",
		nil,
	)
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
