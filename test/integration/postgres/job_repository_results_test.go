// This file tests successful-result lookup against PostgreSQL.
package postgres_test

import (
	"context"
	"testing"

	. "github.com/purinliang/mill/internal/job/postgres"
)

func TestCompletedResultsForMissingJobAreEmpty(t *testing.T) {
	pool := openIntegrationDatabase(t)
	defer pool.Close()
	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatal(err)
	}

	results, err := repository.CompletedResults(
		context.Background(),
		"00000000-0000-7000-8000-000000000001",
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Fatalf("results = %+v, want none", results)
	}
}
