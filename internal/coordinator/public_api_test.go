package coordinator_test

import (
	"context"
	"errors"
	"io"
	"log"
	"reflect"
	"testing"

	"github.com/purinliang/mill/internal/coordinator"
	"github.com/purinliang/mill/internal/execution"
)

type publicStore struct {
	active      []execution.ClaimedAttempt
	transitions []string
	claimCalls  int
}

func (s *publicStore) LeaseActiveAttempts(context.Context, string, string) ([]execution.ClaimedAttempt, error) {
	return s.active, nil
}

func (s *publicStore) ClaimNextAttempt(context.Context, string, string) (execution.ClaimedAttempt, error) {
	s.claimCalls++
	return execution.ClaimedAttempt{}, execution.ErrNoTaskAvailable
}

func (s *publicStore) MarkAttemptRunning(_ context.Context, id, token, externalID string) (execution.Attempt, error) {
	s.transitions = append(s.transitions, "running:"+id+":"+token+":"+externalID)
	return execution.Attempt{ID: id, State: execution.AttemptStateRunning}, nil
}

func (s *publicStore) CompleteAttempt(_ context.Context, id, token string) (execution.Attempt, error) {
	s.transitions = append(s.transitions, "completed:"+id+":"+token)
	return execution.Attempt{ID: id, State: execution.AttemptStateCompleted}, nil
}

func (s *publicStore) FailAttempt(_ context.Context, id, token, message string) (execution.Attempt, error) {
	s.transitions = append(s.transitions, "failed:"+id+":"+token+":"+message)
	return execution.Attempt{ID: id, State: execution.AttemptStateFailed}, nil
}

type publicExecutor func(context.Context, execution.ClaimedAttempt) (coordinator.Observation, error)

func (execute publicExecutor) Reconcile(ctx context.Context, attempt execution.ClaimedAttempt) (coordinator.Observation, error) {
	return execute(ctx, attempt)
}

func TestTickRecordsFastCompletionInValidTransitionOrder(t *testing.T) {
	store := &publicStore{active: []execution.ClaimedAttempt{{Attempt: execution.Attempt{
		ID: "attempt-1", JobID: "job-1", TaskID: "task-1",
		State: execution.AttemptStateStarting, LeaseToken: "token-1",
	}}}}
	runner := &coordinator.Coordinator{
		Store:      store,
		LeaseOwner: "executor-a",
		Logger:     log.New(io.Discard, "", 0),
		Executor: publicExecutor(func(context.Context, execution.ClaimedAttempt) (coordinator.Observation, error) {
			return coordinator.Observation{ExternalID: "kubernetes-job-uid", Completed: true}, nil
		}),
	}

	if err := runner.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	want := []string{
		"running:attempt-1:token-1:kubernetes-job-uid",
		"completed:attempt-1:token-1",
	}
	if !reflect.DeepEqual(store.transitions, want) {
		t.Fatalf("transitions = %v, want %v", store.transitions, want)
	}
	if store.claimCalls != 1 {
		t.Fatalf("new-work claim calls = %d, want 1", store.claimCalls)
	}
}

func TestTickObservesEveryActiveAttemptBeforeReturningErrors(t *testing.T) {
	store := &publicStore{active: []execution.ClaimedAttempt{
		{Attempt: execution.Attempt{ID: "attempt-1", State: execution.AttemptStateRunning}},
		{Attempt: execution.Attempt{ID: "attempt-2", State: execution.AttemptStateRunning}},
	}}
	firstFailure := errors.New("first observation failed")
	secondFailure := errors.New("second observation failed")
	observed := make([]string, 0, 2)
	runner := &coordinator.Coordinator{
		Store:      store,
		LeaseOwner: "executor-a",
		Logger:     log.New(io.Discard, "", 0),
		Executor: publicExecutor(func(_ context.Context, attempt execution.ClaimedAttempt) (coordinator.Observation, error) {
			observed = append(observed, attempt.Attempt.ID)
			if attempt.Attempt.ID == "attempt-1" {
				return coordinator.Observation{}, firstFailure
			}
			return coordinator.Observation{}, secondFailure
		}),
	}

	err := runner.Tick(context.Background())
	if !errors.Is(err, firstFailure) || !errors.Is(err, secondFailure) {
		t.Fatalf("Tick error = %v, want both observation failures", err)
	}
	if !reflect.DeepEqual(observed, []string{"attempt-1", "attempt-2"}) {
		t.Fatalf("observed attempts = %v", observed)
	}
	if store.claimCalls != 0 {
		t.Fatalf("claimed new work after observation failure %d times", store.claimCalls)
	}
}
