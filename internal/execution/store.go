// This file defines the durable state boundary used by execution replicas.
package execution

import "context"

// Store is the execution service's durable control-plane boundary. Lease policy
// belongs to the implementation behind this interface, not to an execution replica.
type Store interface {
	LeaseActiveAttempts(context.Context, string, string) ([]ClaimedAttempt, error)
	ClaimNextAttempt(context.Context, string, string) (ClaimedAttempt, error)
	MarkAttemptRunning(context.Context, string, string, string) (Attempt, error)
	CompleteAttempt(context.Context, string, string) (Attempt, error)
	FailAttempt(context.Context, string, string, string) (Attempt, error)
}
