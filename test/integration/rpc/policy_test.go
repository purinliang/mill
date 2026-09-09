// This file tests error mapping, information hiding, and client deadlines.
package rpc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/purinliang/mill/internal/execution"
	executionrpc "github.com/purinliang/mill/internal/execution/rpc"
	executionv1 "github.com/purinliang/mill/internal/execution/rpc/v1"
)

type deadlineClient struct {
	executionv1.ExecutionServiceClient
	hasDeadline bool
}

func (c *deadlineClient) ClaimNextAttempt(
	ctx context.Context,
	_ *executionv1.ClaimNextAttemptRequest,
	_ ...grpc.CallOption,
) (*executionv1.ClaimNextAttemptResponse, error) {
	_, c.hasDeadline = ctx.Deadline()
	return nil, status.Error(
		codes.Unavailable,
		"test transport unavailable",
	)
}

func TestDomainErrorsSurviveTransport(t *testing.T) {
	backend := &testBackend{claimError: execution.ErrNoTaskAvailable}
	client := newTestClient(t, backend, 15*time.Second)
	if _, err := client.ClaimNextAttempt(
		context.Background(), "kubernetes", "executor-a",
	); !errors.Is(err, execution.ErrNoTaskAvailable) {
		t.Fatalf("claim error = %v", err)
	}

	backend.mutationError = execution.ErrAttemptLeaseLost
	if _, err := client.CompleteAttempt(
		context.Background(), "attempt-1", "stale-token",
	); !errors.Is(err, execution.ErrAttemptLeaseLost) {
		t.Fatalf("lease error = %v", err)
	}
	backend.mutationError = execution.ErrInvalidAttemptTransition
	if _, err := client.CompleteAttempt(
		context.Background(), "attempt-1", "token-1",
	); !errors.Is(err, execution.ErrInvalidAttemptTransition) {
		t.Fatalf("transition error = %v", err)
	}
}

func TestServerHidesUnexpectedBackendErrors(t *testing.T) {
	backend := &testBackend{
		claimError: errors.New("password=secret database detail"),
	}
	client := newTestClient(t, backend, 15*time.Second)
	_, err := client.ClaimNextAttempt(
		context.Background(), "kubernetes", "executor-a",
	)
	if status.Code(err) != codes.Internal ||
		status.Convert(err).Message() != "execution state operation failed" {
		t.Fatalf("unexpected error = %v", err)
	}
}

func TestClientAddsPerCallDeadline(t *testing.T) {
	transport := &deadlineClient{}
	client, err := executionrpc.NewClient(transport, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ClaimNextAttempt(
		context.Background(), "kubernetes", "executor-a",
	)
	if !transport.hasDeadline || status.Code(err) != codes.Unavailable {
		t.Fatalf(
			"has deadline = %t, error = %v",
			transport.hasDeadline,
			err,
		)
	}
}
