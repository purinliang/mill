// This file provides shared PostgreSQL integration-test fixtures.
package postgres

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/purinliang/mill/internal/execution"
)

var testInputSHA256 = strings.Repeat("a", 64)

var testResources = execution.Resources{
	CPURequestMillis:   100,
	CPULimitMillis:     1000,
	MemoryRequestBytes: 128 << 20,
	MemoryLimitBytes:   128 << 20,
}

func integrationDatabaseURL(t *testing.T) string {
	t.Helper()
	databaseURL := os.Getenv("MILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("MILL_TEST_DATABASE_URL is not set")
	}
	return databaseURL
}

func openIntegrationDatabase(
	t *testing.T,
	databaseURL string,
) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create integration database pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping integration database: %v", err)
	}

	assertIntegrationSchema(t, pool, ctx)
	return pool
}

func assertIntegrationSchema(
	t *testing.T,
	pool *pgxpool.Pool,
	ctx context.Context,
) {
	t.Helper()
	var jobsTable, tasksTable, attemptsTable *string
	var hasLogicalRanges, hasAttemptLeases bool
	err := pool.QueryRow(ctx, `
		SELECT
			to_regclass('public.jobs')::text,
			to_regclass('public.tasks')::text,
			to_regclass('public.attempts')::text,
			EXISTS (
				SELECT 1
				FROM information_schema.columns
				WHERE table_schema = 'public'
					AND table_name = 'tasks'
					AND column_name = 'input_start_byte'
			),
			EXISTS (
				SELECT 1
				FROM information_schema.columns
				WHERE table_schema = 'public'
					AND table_name = 'attempts'
					AND column_name = 'lease_token'
			)
	`).Scan(
		&jobsTable,
		&tasksTable,
		&attemptsTable,
		&hasLogicalRanges,
		&hasAttemptLeases,
	)
	if err != nil {
		pool.Close()
		t.Fatalf("check database migrations: %v", err)
	}
	if jobsTable == nil || tasksTable == nil || attemptsTable == nil ||
		!hasLogicalRanges || !hasAttemptLeases {
		pool.Close()
		t.Fatal(
			"required schema does not exist; apply all numbered migrations",
		)
	}
}

func deleteJobByKey(t *testing.T, pool *pgxpool.Pool, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()
	if _, err := pool.Exec(
		ctx,
		"DELETE FROM public.jobs WHERE idempotency_key = $1",
		key,
	); err != nil {
		t.Fatalf("delete integration test job: %v", err)
	}
}
