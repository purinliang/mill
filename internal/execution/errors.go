// This file defines errors shared by execution stores and coordinators.
package execution

import "errors"

var (
	// ErrNoTaskAvailable means no eligible task can currently be claimed.
	ErrNoTaskAvailable = errors.New("no task is available for execution")

	// ErrAttemptNotFound means the requested attempt does not exist.
	ErrAttemptNotFound = errors.New("attempt not found")

	// ErrAttemptLeaseLost rejects a mutation from a stale lease owner.
	ErrAttemptLeaseLost = errors.New("attempt lease is not active")

	// ErrInvalidAttemptTransition rejects an invalid durable state change.
	ErrInvalidAttemptTransition = errors.New(
		"invalid attempt state transition",
	)
)
