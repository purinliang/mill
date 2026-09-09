// This file provides fakes shared by coordinator unit tests.
package coordinator_test

import (
	"context"
	"io"
	"log"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/execution/coordinator"
)

type fakeStore struct {
	active           []execution.ClaimedAttempt
	transitions      []string
	leaseError       error
	claimError       error
	markRunningError error
	claimsRemaining  int
	claimCalls       int
}

func (s *fakeStore) LeaseActiveAttempts(
	context.Context,
	string,
	string,
) ([]execution.ClaimedAttempt, error) {
	return s.active, s.leaseError
}

func (s *fakeStore) ClaimNextAttempt(
	context.Context,
	string,
	string,
) (execution.ClaimedAttempt, error) {
	s.claimCalls++
	if s.claimError != nil {
		return execution.ClaimedAttempt{}, s.claimError
	}
	if s.claimsRemaining > 0 {
		s.claimsRemaining--
		return execution.ClaimedAttempt{Attempt: execution.Attempt{
			ID:    "claimed-attempt",
			JobID: "job-1",
			State: execution.AttemptStateStarting,
		}}, nil
	}
	return execution.ClaimedAttempt{}, execution.ErrNoTaskAvailable
}

func (s *fakeStore) MarkAttemptRunning(
	_ context.Context,
	id, token, externalID string,
) (execution.Attempt, error) {
	if s.markRunningError != nil {
		return execution.Attempt{}, s.markRunningError
	}
	s.transitions = append(
		s.transitions,
		"running:"+id+":"+token+":"+externalID,
	)
	return execution.Attempt{
		ID: id, State: execution.AttemptStateRunning,
	}, nil
}

func (s *fakeStore) CompleteAttempt(
	_ context.Context,
	id, token string,
) (execution.Attempt, error) {
	s.transitions = append(s.transitions, "completed:"+id+":"+token)
	return execution.Attempt{
		ID: id, State: execution.AttemptStateCompleted,
	}, nil
}

func (s *fakeStore) FailAttempt(
	_ context.Context,
	id, token, message string,
) (execution.Attempt, error) {
	s.transitions = append(
		s.transitions,
		"failed:"+id+":"+token+":"+message,
	)
	return execution.Attempt{
		ID: id, State: execution.AttemptStateFailed,
	}, nil
}

type fakeRuntime func(
	context.Context,
	execution.ClaimedAttempt,
) (coordinator.Observation, error)

func (run fakeRuntime) Reconcile(
	ctx context.Context,
	attempt execution.ClaimedAttempt,
) (coordinator.Observation, error) {
	return run(ctx, attempt)
}

func newTestCoordinator(
	store coordinator.Store,
	run fakeRuntime,
) *coordinator.Coordinator {
	return &coordinator.Coordinator{
		Store:      store,
		Runtime:    run,
		LeaseOwner: "executor-a",
		Logger:     log.New(io.Discard, "", 0),
	}
}
