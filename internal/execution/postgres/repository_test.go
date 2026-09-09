// This file tests construction of the PostgreSQL execution repository.
package postgres_test

import (
	"context"
	"strings"
	"testing"
	"time"

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

func TestRepositoryReportsClosedDatabase(t *testing.T) {
	pool, err := pgxpool.New(
		context.Background(),
		"postgresql://mill:mill@127.0.0.1:1/mill",
	)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := executionpostgres.NewRepository(pool)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()

	attemptID := "00000000-0000-7000-8000-000000000001"
	leaseToken := "00000000-0000-7000-8000-000000000002"
	operations := map[string]func() error{
		"claim": func() error {
			_, err := repository.ClaimNextAttempt(
				context.Background(),
				"kubernetes",
				"executor-1",
				15*time.Second,
			)
			return err
		},
		"get attempt": func() error {
			_, err := repository.GetAttempt(
				context.Background(), attemptID,
			)
			return err
		},
		"start attempt": func() error {
			_, err := repository.MarkAttemptRunning(
				context.Background(), attemptID, leaseToken, "job-uid",
			)
			return err
		},
		"complete attempt": func() error {
			_, err := repository.CompleteAttempt(
				context.Background(), attemptID, leaseToken,
			)
			return err
		},
		"fail attempt": func() error {
			_, err := repository.FailAttempt(
				context.Background(), attemptID, leaseToken, "failed",
			)
			return err
		},
		"lease active attempts": func() error {
			_, err := repository.LeaseActiveAttempts(
				context.Background(),
				"kubernetes",
				"executor-1",
				15*time.Second,
			)
			return err
		},
	}

	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if err := operation(); err == nil {
				t.Fatal("operation succeeded with a closed database")
			}
		})
	}
}
