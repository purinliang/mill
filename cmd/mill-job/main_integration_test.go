package main

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestPostgreSQLConnectionIntegration(t *testing.T) {
	databaseURL := os.Getenv("MILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("MILL_TEST_DATABASE_URL is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	database, err := openDatabase(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	defer database.Close()

	var answer int
	if err := database.QueryRow(ctx, "select 1").Scan(&answer); err != nil {
		t.Fatalf("query database: %v", err)
	}
	if answer != 1 {
		t.Fatalf("answer = %d, want 1", answer)
	}
}
