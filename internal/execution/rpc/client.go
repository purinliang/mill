// This file adapts the execution Store interface to a bounded gRPC client.
package executionrpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/purinliang/mill/internal/execution"
	executionv1 "github.com/purinliang/mill/internal/execution/rpc/v1"
)

type Client struct {
	client  executionv1.ExecutionServiceClient
	timeout time.Duration
}

func NewClient(client executionv1.ExecutionServiceClient, timeout time.Duration) (*Client, error) {
	if client == nil {
		return nil, errors.New("execution RPC client is required")
	}
	if timeout <= 0 {
		return nil, errors.New("execution RPC timeout must be positive")
	}
	return &Client{client: client, timeout: timeout}, nil
}

func (c *Client) LeaseActiveAttempts(ctx context.Context, executor, leaseOwner string) ([]execution.ClaimedAttempt, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.client.LeaseActiveAttempts(ctx, &executionv1.LeaseActiveAttemptsRequest{Executor: executor, LeaseOwner: leaseOwner})
	if err != nil {
		return nil, clientError(err, false)
	}
	attempts := make([]execution.ClaimedAttempt, 0, len(response.Attempts))
	for _, value := range response.Attempts {
		attempt, err := claimedAttemptFromProto(value)
		if err != nil {
			return nil, err
		}
		attempts = append(attempts, attempt)
	}
	return attempts, nil
}

func (c *Client) ClaimNextAttempt(ctx context.Context, executor, leaseOwner string) (execution.ClaimedAttempt, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.client.ClaimNextAttempt(ctx, &executionv1.ClaimNextAttemptRequest{Executor: executor, LeaseOwner: leaseOwner})
	if err != nil {
		return execution.ClaimedAttempt{}, clientError(err, true)
	}
	return claimedAttemptFromProto(response.Attempt)
}

func (c *Client) MarkAttemptRunning(ctx context.Context, id, leaseToken, externalID string) (execution.Attempt, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.client.MarkAttemptRunning(ctx, &executionv1.MarkAttemptRunningRequest{
		AttemptId: id, LeaseToken: leaseToken, ExternalId: externalID,
	})
	return decodeAttemptResponse(response, err)
}

func (c *Client) CompleteAttempt(ctx context.Context, id, leaseToken string) (execution.Attempt, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.client.CompleteAttempt(ctx, &executionv1.CompleteAttemptRequest{AttemptId: id, LeaseToken: leaseToken})
	return decodeAttemptResponse(response, err)
}

func (c *Client) FailAttempt(ctx context.Context, id, leaseToken, failureMessage string) (execution.Attempt, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	response, err := c.client.FailAttempt(ctx, &executionv1.FailAttemptRequest{
		AttemptId: id, LeaseToken: leaseToken, FailureMessage: failureMessage,
	})
	return decodeAttemptResponse(response, err)
}

func decodeAttemptResponse(response *executionv1.AttemptResponse, err error) (execution.Attempt, error) {
	if err != nil {
		return execution.Attempt{}, clientError(err, false)
	}
	if response == nil {
		return execution.Attempt{}, fmt.Errorf("execution RPC returned no response")
	}
	return attemptFromProto(response.Attempt)
}

func clientError(err error, noWorkOperation bool) error {
	switch status.Code(err) {
	case codes.NotFound:
		if noWorkOperation {
			return fmt.Errorf("%w: %s", execution.ErrNoTaskAvailable, status.Convert(err).Message())
		}
		return fmt.Errorf("%w: %s", execution.ErrAttemptNotFound, status.Convert(err).Message())
	case codes.Aborted:
		return fmt.Errorf("%w: %s", execution.ErrAttemptLeaseLost, status.Convert(err).Message())
	case codes.FailedPrecondition:
		return fmt.Errorf("%w: %s", execution.ErrInvalidAttemptTransition, status.Convert(err).Message())
	default:
		return err
	}
}
