// Package execution defines the backend-independent contract between Mill's
// durable job state and the service that realizes workload attempts.
package execution

import "time"

// AttemptState is the durable lifecycle state of one execution attempt.
type AttemptState string

const (
	// AttemptStateStarting means the attempt exists before runtime creation.
	AttemptStateStarting AttemptState = "starting"

	// AttemptStateRunning means the runtime accepted the workload.
	AttemptStateRunning AttemptState = "running"

	// AttemptStateCompleted means the workload finished successfully.
	AttemptStateCompleted AttemptState = "completed"

	// AttemptStateFailed means the attempt ended unsuccessfully.
	AttemptStateFailed AttemptState = "failed"
)

// Attempt records one durable try to execute a logical task.
type Attempt struct {
	ID             string
	JobID          string
	TaskID         string
	Number         int
	Executor       string
	State          AttemptState
	ExternalID     string
	FailureMessage string
	CreatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
	UpdatedAt      time.Time
	LeaseOwner     string
	LeaseToken     string
	LeaseExpiresAt *time.Time
}

// ClaimedAttempt combines durable attempt identity with its workload input.
type ClaimedAttempt struct {
	Attempt        Attempt
	Executable     Executable
	ShardIndex     int
	InputURI       string
	InputStartByte int64
	InputEndByte   int64
	OutputURI      string
	Resources      Resources
}
