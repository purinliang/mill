// This file tests construction of the PostgreSQL Job repository.
package postgres_test

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	jobpostgres "github.com/purinliang/mill/internal/job/postgres"
)

func TestNewRepositoryValidatesConfiguration(t *testing.T) {
	if _, err := jobpostgres.NewRepository(
		nil,
		"file:///tmp/mill-output",
	); err == nil || !strings.Contains(err.Error(), "PostgreSQL") {
		t.Fatalf("nil database error = %v", err)
	}

	pool := &pgxpool.Pool{}
	if _, err := jobpostgres.NewRepository(
		pool,
		"relative/output",
	); err == nil || !strings.Contains(err.Error(), "MILL_OUTPUT_ROOT_URI") {
		t.Fatalf("invalid output root error = %v", err)
	}
	if _, err := jobpostgres.NewRepository(
		pool,
		"file:///tmp/mill-output",
	); err != nil {
		t.Fatalf("valid configuration: %v", err)
	}
	if _, err := jobpostgres.NewRepository(
		pool,
		"s3://mill-output",
	); err != nil {
		t.Fatalf("valid S3 output root: %v", err)
	}
}
