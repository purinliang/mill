// Package execution defines the backend-independent contract between Mill's
// durable job state and an execution service that realizes attempts in a runtime.
// This file defines runtime-neutral attempts, resources, and workload inputs.
package execution

import (
	"errors"
	"time"
)

var (
	ErrNoTaskAvailable          = errors.New("no task is available for execution")
	ErrAttemptNotFound          = errors.New("attempt not found")
	ErrAttemptLeaseLost         = errors.New("attempt lease is not active")
	ErrInvalidAttemptTransition = errors.New("invalid attempt state transition")
)

type AttemptState string

const (
	AttemptStateStarting  AttemptState = "starting"
	AttemptStateRunning   AttemptState = "running"
	AttemptStateCompleted AttemptState = "completed"
	AttemptStateFailed    AttemptState = "failed"
)

type Executable struct {
	Image string   `json:"image"`
	Args  []string `json:"args"`
}

type Resources struct {
	CPURequestMillis   int64 `json:"cpu_request_millis"`
	CPULimitMillis     int64 `json:"cpu_limit_millis"`
	MemoryRequestBytes int64 `json:"memory_request_bytes"`
	MemoryLimitBytes   int64 `json:"memory_limit_bytes"`
}

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
