// This file defines the durable state required by the coordinator.
package coordinator

import (
	"context"

	"github.com/purinliang/mill/internal/execution"
)

// Store is the coordinator's durable control-plane boundary. Lease policy
// belongs to the implementation behind this interface, not to the coordinator.
type Store interface {
	LeaseActiveAttempts(
		ctx context.Context,
		executor, leaseOwner string,
	) ([]execution.ClaimedAttempt, error)
	ClaimNextAttempt(
		ctx context.Context,
		executor, leaseOwner string,
	) (execution.ClaimedAttempt, error)
	MarkAttemptRunning(
		ctx context.Context,
		id, leaseToken, externalID string,
	) (execution.Attempt, error)
	CompleteAttempt(
		ctx context.Context,
		id, leaseToken string,
	) (execution.Attempt, error)
	FailAttempt(
		ctx context.Context,
		id, leaseToken, failureMessage string,
	) (execution.Attempt, error)
}
