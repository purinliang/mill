// This file tests the gRPC client and server together over an in-memory link.
package executionrpc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/purinliang/mill/internal/execution"
	executionv1 "github.com/purinliang/mill/internal/execution/rpc/v1"
)

type testBackend struct {
	claimed       execution.ClaimedAttempt
	active        []execution.ClaimedAttempt
	claimError    error
	mutationError error
	leaseDuration time.Duration
}

type deadlineClient struct {
	executionv1.ExecutionServiceClient
	hasDeadline bool
}

func (c *deadlineClient) ClaimNextAttempt(ctx context.Context, _ *executionv1.ClaimNextAttemptRequest, _ ...grpc.CallOption) (*executionv1.ClaimNextAttemptResponse, error) {
	_, c.hasDeadline = ctx.Deadline()
	return nil, status.Error(codes.Unavailable, "test transport unavailable")
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

func TestDomainErrorsSurviveTransport(t *testing.T) {
	backend := &testBackend{claimError: execution.ErrNoTaskAvailable}
	client := newTestClient(t, backend, 15*time.Second)
	if _, err := client.ClaimNextAttempt(context.Background(), "kubernetes", "executor-a"); !errors.Is(err, execution.ErrNoTaskAvailable) {
		t.Fatalf("claim error = %v", err)
	}

	backend.mutationError = execution.ErrAttemptLeaseLost
	if _, err := client.CompleteAttempt(context.Background(), "attempt-1", "stale-token"); !errors.Is(err, execution.ErrAttemptLeaseLost) {
		t.Fatalf("lease error = %v", err)
	}
	backend.mutationError = execution.ErrInvalidAttemptTransition
	if _, err := client.CompleteAttempt(context.Background(), "attempt-1", "token-1"); !errors.Is(err, execution.ErrInvalidAttemptTransition) {
		t.Fatalf("transition error = %v", err)
	}
}

func TestServerHidesUnexpectedBackendErrors(t *testing.T) {
	backend := &testBackend{claimError: errors.New("password=secret database detail")}
	client := newTestClient(t, backend, 15*time.Second)
	_, err := client.ClaimNextAttempt(context.Background(), "kubernetes", "executor-a")
	if status.Code(err) != codes.Internal || status.Convert(err).Message() != "execution state operation failed" {
		t.Fatalf("unexpected error = %v", err)
	}
}

func TestClientAddsPerCallDeadline(t *testing.T) {
	transport := &deadlineClient{}
	client, err := NewClient(transport, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ClaimNextAttempt(context.Background(), "kubernetes", "executor-a")
	if !transport.hasDeadline || status.Code(err) != codes.Unavailable {
		t.Fatalf("has deadline = %t, error = %v", transport.hasDeadline, err)
	}
}

func newTestClient(t *testing.T, backend Backend, leaseDuration time.Duration) *Client {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	executionServer, err := NewServer(backend, leaseDuration)
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
	client, err := NewClient(executionv1.NewExecutionServiceClient(connection), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
