package main

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/executionrpc"
	executionv1 "github.com/purinliang/mill/internal/executionrpc/v1"
)

type executionRPCBackend struct {
	claimed   execution.ClaimedAttempt
	failCalls int
}

func (b *executionRPCBackend) LeaseActiveAttempts(context.Context, string, string, time.Duration) ([]execution.ClaimedAttempt, error) {
	return []execution.ClaimedAttempt{b.claimed}, nil
}

func (b *executionRPCBackend) ClaimNextAttempt(context.Context, string, string, time.Duration) (execution.ClaimedAttempt, error) {
	return b.claimed, nil
}

func (b *executionRPCBackend) MarkAttemptRunning(context.Context, string, string, string) (execution.Attempt, error) {
	return b.claimed.Attempt, nil
}

func (b *executionRPCBackend) CompleteAttempt(context.Context, string, string) (execution.Attempt, error) {
	return b.claimed.Attempt, nil
}

func (b *executionRPCBackend) FailAttempt(context.Context, string, string, string) (execution.Attempt, error) {
	b.failCalls++
	return b.claimed.Attempt, nil
}

func TestExecutionRPCServerRegistersLeaseService(t *testing.T) {
	backend := testExecutionRPCBackend()
	client := serveExecutionRPCForTest(t, backend)
	response, err := client.ClaimNextAttempt(context.Background(), &executionv1.ClaimNextAttemptRequest{
		Executor: "kubernetes", LeaseOwner: "executor-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.Attempt.Attempt.Id != "attempt-1" || response.Attempt.Attempt.LeaseToken != "token-1" {
		t.Fatalf("claimed attempt = %+v", response.Attempt)
	}
}

func TestExecutionRPCServerRejectsOversizedRequest(t *testing.T) {
	backend := testExecutionRPCBackend()
	client := serveExecutionRPCForTest(t, backend)
	_, err := client.FailAttempt(context.Background(), &executionv1.FailAttemptRequest{
		AttemptId: "attempt-1", LeaseToken: "token-1",
		FailureMessage: strings.Repeat("x", executionrpc.MaxMessageBytes+1),
	})
	if status.Code(err) != codes.ResourceExhausted || backend.failCalls != 0 {
		t.Fatalf("error = %v, backend calls = %d", err, backend.failCalls)
	}
}

func TestExecutionRPCServerShutsDownGracefully(t *testing.T) {
	listener := bufconn.Listen(executionrpc.MaxMessageBytes)
	server, err := newExecutionRPCServer(testExecutionRPCBackend())
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	service := &executionRPCService{server: server, errors: serveErrors}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := executionRPCServeError(<-serveErrors); err != nil {
		t.Fatal(err)
	}
}

func testExecutionRPCBackend() *executionRPCBackend {
	now := time.Date(2026, 9, 6, 1, 2, 3, 0, time.UTC)
	expires := now.Add(attemptLeaseDuration)
	return &executionRPCBackend{claimed: execution.ClaimedAttempt{
		Attempt: execution.Attempt{
			ID: "attempt-1", JobID: "job-1", TaskID: "task-1", Number: 1,
			Executor: "kubernetes", State: execution.AttemptStateStarting,
			CreatedAt: now, UpdatedAt: now, LeaseOwner: "executor-a",
			LeaseToken: "token-1", LeaseExpiresAt: &expires,
		},
		Executable: execution.Executable{Image: "mill/word-count:dev"},
		InputURI:   "s3://input/data.jsonl", InputEndByte: 10,
		OutputURI: "s3://output/result.jsonl",
	}}
}

func serveExecutionRPCForTest(t *testing.T, backend *executionRPCBackend) executionv1.ExecutionServiceClient {
	t.Helper()
	listener := bufconn.Listen(executionrpc.MaxMessageBytes * 2)
	server, err := newExecutionRPCServer(backend)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
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
	return executionv1.NewExecutionServiceClient(connection)
}
