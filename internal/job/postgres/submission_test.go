// This file tests submission validation at the PostgreSQL boundary.
package postgres_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/job"
	jobpostgres "github.com/purinliang/mill/internal/job/postgres"
)

func TestSubmissionMethodsRejectInvalidArguments(t *testing.T) {
	repository, err := jobpostgres.NewRepository(
		&pgxpool.Pool{},
		"file:///tmp/mill-output",
	)
	if err != nil {
		t.Fatal(err)
	}
	valid := job.Submission{
		Executable: job.Executable{Image: "mill/example:dev"},
		Input:      job.InputSpec{URI: "file:///tmp/records.jsonl"},
	}
	digest := sha256.Sum256([]byte("{\"text\":\"test record\"}\n"))
	validDigest := fmt.Sprintf("%x", digest)
	resources := execution.Resources{
		CPURequestMillis:   100,
		CPULimitMillis:     1000,
		MemoryRequestBytes: 128 << 20,
		MemoryLimitBytes:   128 << 20,
	}

	tests := map[string]func() error{
		"find without idempotency key": func() error {
			_, _, err := repository.FindSubmission(
				context.Background(), "", valid,
			)
			return err
		},
		"find invalid submission": func() error {
			_, _, err := repository.FindSubmission(
				context.Background(), "request-1", job.Submission{},
			)
			return err
		},
		"create without idempotency key": func() error {
			_, _, err := repository.Create(
				context.Background(), "", valid,
				validDigest, 1, 1, resources,
			)
			return err
		},
		"create invalid submission": func() error {
			_, _, err := repository.Create(
				context.Background(), "request-1", job.Submission{},
				validDigest, 1, 1, resources,
			)
			return err
		},
		"create invalid digest": func() error {
			_, _, err := repository.Create(
				context.Background(), "request-1", valid,
				"not-a-digest", 1, 1, resources,
			)
			return err
		},
		"create empty input": func() error {
			_, _, err := repository.Create(
				context.Background(), "request-1", valid,
				validDigest, 0, 1, resources,
			)
			return err
		},
		"create invalid parallelism": func() error {
			_, _, err := repository.Create(
				context.Background(), "request-1", valid,
				validDigest, 1, 0, resources,
			)
			return err
		},
	}

	for name, operation := range tests {
		t.Run(name, func(t *testing.T) {
			requireInvalidArgument(t, operation())
		})
	}
}

func requireInvalidArgument(t *testing.T, err error) {
	t.Helper()
	var invalidArgument interface {
		InvalidArgument() bool
	}
	if !errors.As(err, &invalidArgument) ||
		!invalidArgument.InvalidArgument() {
		t.Fatalf("error = %v, want invalid argument", err)
	}
}
