// This file provides shared cross-package integration-test fixtures.
package integration_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/purinliang/mill/internal/execution"
)

var integrationResources = execution.Resources{
	CPURequestMillis:   100,
	CPULimitMillis:     1000,
	MemoryRequestBytes: 128 << 20,
	MemoryLimitBytes:   128 << 20,
}

var integrationInputSHA256 = fmt.Sprintf(
	"%x",
	sha256.Sum256([]byte("{\"record\":1}\n")),
)

func openIntegrationDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("MILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("MILL_TEST_DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pool
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
		t.Fatalf("delete integration job: %v", err)
	}
}

func writeTestJSONL(t *testing.T, filename string, records int) {
	t.Helper()
	var contents strings.Builder
	for index := range records {
		fmt.Fprintf(&contents, `{"record":%d}`+"\n", index)
	}
	if err := os.WriteFile(
		filename,
		[]byte(contents.String()),
		0o600,
	); err != nil {
		t.Fatalf("write test JSONL: %v", err)
	}
}

func fileURI(filename string) string {
	return (&url.URL{Scheme: "file", Path: filename}).String()
}
