package execution

import "context"

// Store is the executor's durable control-plane boundary. Lease policy belongs
// to the implementation behind this interface, not to an executor replica.
type Store interface {
	LeaseActiveAttempts(context.Context, string, string) ([]ClaimedAttempt, error)
	ClaimNextAttempt(context.Context, string, string) (ClaimedAttempt, error)
	MarkAttemptRunning(context.Context, string, string, string) (Attempt, error)
	CompleteAttempt(context.Context, string, string) (Attempt, error)
	FailAttempt(context.Context, string, string, string) (Attempt, error)
}
