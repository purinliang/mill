package main

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestCoordinatorOwnershipIntegration(t *testing.T) {
	databaseURL := os.Getenv("MILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("MILL_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	first, err := acquireCoordinatorLock(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(ctx)
	second, err := acquireCoordinatorLock(ctx, databaseURL)
	if err == nil {
		second.Close(ctx)
		t.Fatal("second coordinator acquired an owned database")
	}
	if err := first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	restarted, err := acquireCoordinatorLock(ctx, databaseURL)
	if err != nil {
		t.Fatalf("ownership did not release on close: %v", err)
	}
	defer restarted.Close(ctx)
}
