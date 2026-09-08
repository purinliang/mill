package executionrpc_test

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/executionrpc"
	executionv1 "github.com/purinliang/mill/internal/executionrpc/v1"
)

type recordingBackend struct {
	attempt        execution.Attempt
	failedID       string
	failedToken    string
	failureMessage string
}

func (b *recordingBackend) LeaseActiveAttempts(context.Context, string, string, time.Duration) ([]execution.ClaimedAttempt, error) {
	return nil, nil
}

func (b *recordingBackend) ClaimNextAttempt(context.Context, string, string, time.Duration) (execution.ClaimedAttempt, error) {
	return execution.ClaimedAttempt{}, execution.ErrNoTaskAvailable
}

func (b *recordingBackend) MarkAttemptRunning(context.Context, string, string, string) (execution.Attempt, error) {
	return execution.Attempt{}, execution.ErrInvalidAttemptTransition
}

func (b *recordingBackend) CompleteAttempt(context.Context, string, string) (execution.Attempt, error) {
	return execution.Attempt{}, execution.ErrInvalidAttemptTransition
}

func (b *recordingBackend) FailAttempt(_ context.Context, id, token, message string) (execution.Attempt, error) {
	b.failedID = id
	b.failedToken = token
	b.failureMessage = message
	result := b.attempt
	result.State = execution.AttemptStateFailed
	result.FailureMessage = message
	return result, nil
}

func TestFailAttemptRoundTripPreservesFailureDetails(t *testing.T) {
	backend := &recordingBackend{attempt: execution.Attempt{
		ID:         "attempt-1",
		JobID:      "job-1",
		TaskID:     "task-1",
		Number:     1,
		Executor:   "kubernetes",
		State:      execution.AttemptStateRunning,
		LeaseOwner: "executor-a",
		LeaseToken: "token-1",
		CreatedAt:  time.Date(2026, 9, 8, 1, 2, 3, 0, time.UTC),
		UpdatedAt:  time.Date(2026, 9, 8, 1, 2, 4, 0, time.UTC),
	}}
	client := newPublicClient(t, backend)

	failed, err := client.FailAttempt(context.Background(), "attempt-1", "token-1", "workload exited with status 1")
	if err != nil {
		t.Fatalf("fail attempt: %v", err)
	}
	if backend.failedID != "attempt-1" || backend.failedToken != "token-1" || backend.failureMessage != "workload exited with status 1" {
		t.Fatalf("backend received id=%q token=%q message=%q", backend.failedID, backend.failedToken, backend.failureMessage)
	}
	if failed.ID != "attempt-1" || failed.State != execution.AttemptStateFailed || failed.FailureMessage != "workload exited with status 1" {
		t.Fatalf("failed attempt = %+v", failed)
	}
}

func newPublicClient(t *testing.T, backend executionrpc.Backend) *executionrpc.Client {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	executionServer, err := executionrpc.NewServer(backend, 15*time.Second)
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
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })

	client, err := executionrpc.NewClient(executionv1.NewExecutionServiceClient(connection), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return client
}
