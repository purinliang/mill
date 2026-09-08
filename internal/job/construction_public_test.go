package job_test

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/purinliang/mill/internal/job"
)

func TestPublicConstructorsRejectInvalidDependenciesAndPolicy(t *testing.T) {
	if _, err := job.NewRepository(nil, "file:///tmp/mill-output"); err == nil || !strings.Contains(err.Error(), "PostgreSQL") {
		t.Fatalf("NewRepository nil-pool error = %v", err)
	}

	pool := &pgxpool.Pool{}
	if _, err := job.NewRepository(pool, "relative/output"); err == nil || !strings.Contains(err.Error(), "MILL_OUTPUT_ROOT_URI") {
		t.Fatalf("NewRepository invalid-output error = %v", err)
	}
	repository, err := job.NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("NewRepository: %v", err)
	}

	if _, err := job.NewService(nil, job.JSONLPartitioner{}, 3); err == nil || !strings.Contains(err.Error(), "repository") {
		t.Fatalf("NewService nil-repository error = %v", err)
	}
	for _, parallelism := range []int{0, 10_001} {
		if _, err := job.NewService(repository, job.JSONLPartitioner{}, parallelism); err == nil || !strings.Contains(err.Error(), "MILL_PARALLELISM") {
			t.Fatalf("NewService parallelism %d error = %v", parallelism, err)
		}
	}
	if _, err := job.NewService(repository, job.JSONLPartitioner{}, 3); err != nil {
		t.Fatalf("NewService valid configuration: %v", err)
	}
}
