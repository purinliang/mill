package executionrpc

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/purinliang/mill/internal/execution"
	executionv1 "github.com/purinliang/mill/internal/execution/rpc/v1"
)

func claimedAttemptToProto(claimed execution.ClaimedAttempt) *executionv1.ClaimedAttempt {
	return &executionv1.ClaimedAttempt{
		Attempt: attemptToProto(claimed.Attempt),
		Executable: &executionv1.Executable{
			Image: claimed.Executable.Image,
			Args:  append([]string(nil), claimed.Executable.Args...),
		},
		ShardIndex:     int32(claimed.ShardIndex),
		InputUri:       claimed.InputURI,
		InputStartByte: claimed.InputStartByte,
		InputEndByte:   claimed.InputEndByte,
		OutputUri:      claimed.OutputURI,
		Resources: &executionv1.Resources{
			CpuRequestMillis:   claimed.Resources.CPURequestMillis,
			CpuLimitMillis:     claimed.Resources.CPULimitMillis,
			MemoryRequestBytes: claimed.Resources.MemoryRequestBytes,
			MemoryLimitBytes:   claimed.Resources.MemoryLimitBytes,
		},
	}
}

func claimedAttemptFromProto(value *executionv1.ClaimedAttempt) (execution.ClaimedAttempt, error) {
	if value == nil || value.Attempt == nil || value.Executable == nil || value.Resources == nil {
		return execution.ClaimedAttempt{}, fmt.Errorf("execution RPC returned an incomplete claimed attempt")
	}
	attempt, err := attemptFromProto(value.Attempt)
	if err != nil {
		return execution.ClaimedAttempt{}, err
	}
	return execution.ClaimedAttempt{
		Attempt: attempt,
		Executable: execution.Executable{
			Image: value.Executable.Image,
			Args:  append([]string(nil), value.Executable.Args...),
		},
		ShardIndex:     int(value.ShardIndex),
		InputURI:       value.InputUri,
		InputStartByte: value.InputStartByte,
		InputEndByte:   value.InputEndByte,
		OutputURI:      value.OutputUri,
		Resources: execution.Resources{
			CPURequestMillis:   value.Resources.CpuRequestMillis,
			CPULimitMillis:     value.Resources.CpuLimitMillis,
			MemoryRequestBytes: value.Resources.MemoryRequestBytes,
			MemoryLimitBytes:   value.Resources.MemoryLimitBytes,
		},
	}, nil
}

func attemptToProto(attempt execution.Attempt) *executionv1.Attempt {
	return &executionv1.Attempt{
		Id:             attempt.ID,
		JobId:          attempt.JobID,
		TaskId:         attempt.TaskID,
		Number:         int32(attempt.Number),
		Executor:       attempt.Executor,
		State:          stateToProto(attempt.State),
		ExternalId:     attempt.ExternalID,
		FailureMessage: attempt.FailureMessage,
		CreatedAt:      timestamp(attempt.CreatedAt),
		StartedAt:      optionalTimestamp(attempt.StartedAt),
		FinishedAt:     optionalTimestamp(attempt.FinishedAt),
		UpdatedAt:      timestamp(attempt.UpdatedAt),
		LeaseOwner:     attempt.LeaseOwner,
		LeaseToken:     attempt.LeaseToken,
		LeaseExpiresAt: optionalTimestamp(attempt.LeaseExpiresAt),
	}
}

func attemptFromProto(value *executionv1.Attempt) (execution.Attempt, error) {
	if value == nil {
		return execution.Attempt{}, fmt.Errorf("execution RPC returned no attempt")
	}
	state, err := stateFromProto(value.State)
	if err != nil {
		return execution.Attempt{}, err
	}
	createdAt, err := requiredTime(value.CreatedAt, "created_at")
	if err != nil {
		return execution.Attempt{}, err
	}
	updatedAt, err := requiredTime(value.UpdatedAt, "updated_at")
	if err != nil {
		return execution.Attempt{}, err
	}
	startedAt, err := optionalTime(value.StartedAt, "started_at")
	if err != nil {
		return execution.Attempt{}, err
	}
	finishedAt, err := optionalTime(value.FinishedAt, "finished_at")
	if err != nil {
		return execution.Attempt{}, err
	}
	leaseExpiresAt, err := optionalTime(value.LeaseExpiresAt, "lease_expires_at")
	if err != nil {
		return execution.Attempt{}, err
	}
	return execution.Attempt{
		ID: value.Id, JobID: value.JobId, TaskID: value.TaskId,
		Number: int(value.Number), Executor: value.Executor, State: state,
		ExternalID: value.ExternalId, FailureMessage: value.FailureMessage,
		CreatedAt: createdAt, StartedAt: startedAt, FinishedAt: finishedAt,
		UpdatedAt: updatedAt, LeaseOwner: value.LeaseOwner,
		LeaseToken: value.LeaseToken, LeaseExpiresAt: leaseExpiresAt,
	}, nil
}

func stateToProto(state execution.AttemptState) executionv1.AttemptState {
	switch state {
	case execution.AttemptStateStarting:
		return executionv1.AttemptState_ATTEMPT_STATE_STARTING
	case execution.AttemptStateRunning:
		return executionv1.AttemptState_ATTEMPT_STATE_RUNNING
	case execution.AttemptStateCompleted:
		return executionv1.AttemptState_ATTEMPT_STATE_COMPLETED
	case execution.AttemptStateFailed:
		return executionv1.AttemptState_ATTEMPT_STATE_FAILED
	default:
		return executionv1.AttemptState_ATTEMPT_STATE_UNSPECIFIED
	}
}

func stateFromProto(state executionv1.AttemptState) (execution.AttemptState, error) {
	switch state {
	case executionv1.AttemptState_ATTEMPT_STATE_STARTING:
		return execution.AttemptStateStarting, nil
	case executionv1.AttemptState_ATTEMPT_STATE_RUNNING:
		return execution.AttemptStateRunning, nil
	case executionv1.AttemptState_ATTEMPT_STATE_COMPLETED:
		return execution.AttemptStateCompleted, nil
	case executionv1.AttemptState_ATTEMPT_STATE_FAILED:
		return execution.AttemptStateFailed, nil
	default:
		return "", fmt.Errorf("execution RPC returned unknown attempt state %s", state)
	}
}

func timestamp(value time.Time) *timestamppb.Timestamp {
	return timestamppb.New(value)
}

func optionalTimestamp(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestamp(*value)
}

func requiredTime(value *timestamppb.Timestamp, field string) (time.Time, error) {
	if value == nil {
		return time.Time{}, fmt.Errorf("execution RPC returned no %s", field)
	}
	if err := value.CheckValid(); err != nil {
		return time.Time{}, fmt.Errorf("execution RPC returned invalid %s: %w", field, err)
	}
	return value.AsTime(), nil
}

func optionalTime(value *timestamppb.Timestamp, field string) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	parsed, err := requiredTime(value, field)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}
