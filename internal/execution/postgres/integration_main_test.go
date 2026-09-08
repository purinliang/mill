// This file serializes execution-repository fixtures across test packages.
package postgres

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
)

func TestMain(m *testing.M) {
	os.Exit(runWithDatabaseLock(m))
}

func runWithDatabaseLock(m *testing.M) int {
	databaseURL := os.Getenv("MILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		return m.Run()
	}
	connection, err := pgx.Connect(context.Background(), databaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "acquire integration lock connection: %v\n", err)
		return 1
	}
	defer connection.Close(context.Background())
	if _, err := connection.Exec(
		context.Background(),
		"SELECT pg_advisory_lock(724655108)",
	); err != nil {
		fmt.Fprintf(os.Stderr, "acquire integration lock: %v\n", err)
		return 1
	}
	return m.Run()
}
