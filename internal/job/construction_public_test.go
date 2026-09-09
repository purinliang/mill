// This file tests public constructors and dependency validation.
package job_test

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	executionpostgres "github.com/purinliang/mill/internal/execution/postgres"
	"github.com/purinliang/mill/internal/job"
	"github.com/purinliang/mill/internal/job/jsonl"
	jobpostgres "github.com/purinliang/mill/internal/job/postgres"
)

func TestPublicConstructorsRejectInvalidDependenciesAndPolicy(t *testing.T) {
	if _, err := jobpostgres.NewRepository(nil, "file:///tmp/mill-output"); err == nil || !strings.Contains(err.Error(), "PostgreSQL") {
		t.Fatalf("NewRepository nil-pool error = %v", err)
	}
	if _, err := executionpostgres.NewRepository(nil); err == nil ||
		!strings.Contains(err.Error(), "PostgreSQL") {
		t.Fatalf("execution postgres New nil-pool error = %v", err)
	}

	pool := &pgxpool.Pool{}
	if _, err := jobpostgres.NewRepository(pool, "relative/output"); err == nil || !strings.Contains(err.Error(), "MILL_OUTPUT_ROOT_URI") {
		t.Fatalf("NewRepository invalid-output error = %v", err)
	}
	repository, err := jobpostgres.NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}

	if _, err := job.NewService(nil, jsonl.Planner{}, 3); err == nil ||
		!strings.Contains(err.Error(), "store") {
		t.Fatalf("NewService nil-repository error = %v", err)
	}
	if _, err := job.NewService(repository, nil, 3); err == nil ||
		!strings.Contains(err.Error(), "planner") {
		t.Fatalf("NewService nil-planner error = %v", err)
	}
	for _, parallelism := range []int{0, job.MaxParallelism + 1} {
		_, err := job.NewService(repository, jsonl.Planner{}, parallelism)
		if err == nil || !strings.Contains(err.Error(), "MILL_PARALLELISM") {
			t.Fatalf("NewService parallelism %d error = %v", parallelism, err)
		}
	}
	if _, err := job.NewService(repository, jsonl.Planner{}, 3); err != nil {
		t.Fatalf("NewService valid configuration: %v", err)
	}
}

func TestValidateParallelismUsesDocumentedBounds(t *testing.T) {
	for _, parallelism := range []int{1, job.MaxParallelism} {
		if err := job.ValidateParallelism(parallelism); err != nil {
			t.Fatalf("ValidateParallelism(%d): %v", parallelism, err)
		}
	}

	for _, parallelism := range []int{0, job.MaxParallelism + 1} {
		err := job.ValidateParallelism(parallelism)
		var validationError *job.ValidationError
		if !errors.As(err, &validationError) {
			t.Fatalf(
				"ValidateParallelism(%d) error = %T, want ValidationError",
				parallelism,
				err,
			)
		}
		if validationError.Field != "parallelism" || !strings.Contains(
			validationError.Problem,
			strconv.Itoa(job.MaxParallelism),
		) {
			t.Fatalf("validation error = %+v", validationError)
		}
	}
}
