// This file tests the gRPC client and server together over an in-memory link.
package integration_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/purinliang/mill/internal/execution"
	executionrpc "github.com/purinliang/mill/internal/execution/rpc"
	executionv1 "github.com/purinliang/mill/internal/execution/rpc/v1"
)

type testBackend struct {
	claimed       execution.ClaimedAttempt
	active        []execution.ClaimedAttempt
	claimError    error
	mutationError error
	leaseDuration time.Duration
}

func (b *testBackend) LeaseActiveAttempts(_ context.Context, _, _ string, duration time.Duration) ([]execution.ClaimedAttempt, error) {
	b.leaseDuration = duration
	return b.active, nil
}

func (b *testBackend) ClaimNextAttempt(_ context.Context, _, _ string, duration time.Duration) (execution.ClaimedAttempt, error) {
	b.leaseDuration = duration
	return b.claimed, b.claimError
}

func (b *testBackend) MarkAttemptRunning(_ context.Context, _, _, externalID string) (execution.Attempt, error) {
	if b.mutationError != nil {
		return execution.Attempt{}, b.mutationError
	}
	attempt := b.claimed.Attempt
	attempt.State = execution.AttemptStateRunning
	attempt.ExternalID = externalID
	return attempt, nil
}

func (b *testBackend) CompleteAttempt(context.Context, string, string) (execution.Attempt, error) {
	if b.mutationError != nil {
		return execution.Attempt{}, b.mutationError
	}
	attempt := b.claimed.Attempt
	attempt.State = execution.AttemptStateCompleted
	return attempt, nil
}

func (b *testBackend) FailAttempt(context.Context, string, string, string) (execution.Attempt, error) {
	if b.mutationError != nil {
		return execution.Attempt{}, b.mutationError
	}
	attempt := b.claimed.Attempt
	attempt.State = execution.AttemptStateFailed
	return attempt, nil
}

func TestClientServerRoundTripAndServerOwnedLeasePolicy(t *testing.T) {
	now := time.Date(2026, 9, 6, 1, 2, 3, 0, time.UTC)
	expires := now.Add(15 * time.Second)
	backend := &testBackend{claimed: execution.ClaimedAttempt{
		Attempt: execution.Attempt{
			ID: "attempt-1", JobID: "job-1", TaskID: "task-1", Number: 2,
			Executor: "kubernetes", State: execution.AttemptStateStarting,
			CreatedAt: now, UpdatedAt: now, LeaseOwner: "executor-a",
			LeaseToken: "token-1", LeaseExpiresAt: &expires,
		},
		Executable: execution.Executable{Image: "mill/word-count:dev", Args: []string{"--demo"}},
		ShardIndex: 3, InputURI: "s3://input/data.jsonl", InputStartByte: 10,
		InputEndByte: 20, OutputURI: "s3://output/result.jsonl",
		Resources: execution.Resources{CPURequestMillis: 100, CPULimitMillis: 1000,
			MemoryRequestBytes: 512 << 20, MemoryLimitBytes: 512 << 20},
	}}
	backend.active = []execution.ClaimedAttempt{backend.claimed}
	client := newTestClient(t, backend, 15*time.Second)

	active, err := client.LeaseActiveAttempts(context.Background(), "kubernetes", "executor-a")
	if err != nil || len(active) != 1 || active[0].Attempt.LeaseToken != "token-1" {
		t.Fatalf("active attempts = %+v, err = %v", active, err)
	}
	claimed, err := client.ClaimNextAttempt(context.Background(), "kubernetes", "executor-a")
	if err != nil {
		t.Fatal(err)
	}
	if backend.leaseDuration != 15*time.Second {
		t.Fatalf("backend lease duration = %s", backend.leaseDuration)
	}
	if claimed.Attempt.ID != backend.claimed.Attempt.ID || claimed.Executable.Image != backend.claimed.Executable.Image ||
		claimed.ShardIndex != 3 || claimed.InputStartByte != 10 || claimed.InputEndByte != 20 ||
		claimed.Resources != backend.claimed.Resources {
		t.Fatalf("claimed attempt changed across RPC: %+v", claimed)
	}
	running, err := client.MarkAttemptRunning(context.Background(), claimed.Attempt.ID, claimed.Attempt.LeaseToken, "uid-1")
	if err != nil || running.State != execution.AttemptStateRunning || running.ExternalID != "uid-1" {
		t.Fatalf("running attempt = %+v, err = %v", running, err)
	}
}

func newTestClient(
	t *testing.T,
	backend executionrpc.Backend,
	leaseDuration time.Duration,
) *executionrpc.Client {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	executionServer, err := executionrpc.NewServer(backend, leaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	executionv1.RegisterExecutionServiceServer(server, executionServer)
	go func() {
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(
		"passthrough:///bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	client, err := executionrpc.NewClient(
		executionv1.NewExecutionServiceClient(connection),
		time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
