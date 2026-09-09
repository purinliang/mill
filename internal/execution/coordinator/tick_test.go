// This file tests successful coordinator ticks through the public API.
package coordinator_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/execution/coordinator"
)

func TestTickRecordsFastCompletionInValidTransitionOrder(t *testing.T) {
	store := &fakeStore{active: []execution.ClaimedAttempt{{
		Attempt: execution.Attempt{
			ID:         "attempt-1",
			JobID:      "job-1",
			TaskID:     "task-1",
			State:      execution.AttemptStateStarting,
			LeaseToken: "token-1",
		},
	}}}
	runner := newTestCoordinator(store, func(
		context.Context,
		execution.ClaimedAttempt,
	) (coordinator.Observation, error) {
		return coordinator.Observation{
			ExternalID: "kubernetes-job-uid",
			Completed:  true,
		}, nil
	})

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

func TestTickBoundsNewClaimsToOneHundredPerPass(t *testing.T) {
	store := &fakeStore{claimsRemaining: 101}
	runtimeCalls := 0
	runner := newTestCoordinator(store, func(
		context.Context,
		execution.ClaimedAttempt,
	) (coordinator.Observation, error) {
		runtimeCalls++
		return coordinator.Observation{}, nil
	})

	if err := runner.Tick(context.Background()); err != nil {
		t.Fatalf("Tick: %v", err)
	}
	if store.claimCalls != 100 || runtimeCalls != 100 ||
		store.claimsRemaining != 1 {
		t.Fatalf(
			"claim calls = %d, runtime calls = %d, remaining = %d",
			store.claimCalls,
			runtimeCalls,
			store.claimsRemaining,
		)
	}
}
