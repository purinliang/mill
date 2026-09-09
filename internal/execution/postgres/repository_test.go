// This file tests construction of the PostgreSQL execution repository.
package postgres_test

import (
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	executionpostgres "github.com/purinliang/mill/internal/execution/postgres"
)

func TestNewRepositoryRequiresDatabase(t *testing.T) {
	if _, err := executionpostgres.NewRepository(nil); err == nil ||
		!strings.Contains(err.Error(), "PostgreSQL") {
		t.Fatalf("nil database error = %v", err)
	}
	if _, err := executionpostgres.NewRepository(
		&pgxpool.Pool{},
	); err != nil {
		t.Fatalf("valid configuration: %v", err)
	}
}
