// This file tests coordinator error handling through the public API.
package coordinator_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/execution/coordinator"
)

func TestTickObservesEveryActiveAttemptBeforeReturningErrors(t *testing.T) {
	store := &fakeStore{active: []execution.ClaimedAttempt{
		{Attempt: execution.Attempt{
			ID: "attempt-1", State: execution.AttemptStateRunning,
		}},
		{Attempt: execution.Attempt{
			ID: "attempt-2", State: execution.AttemptStateRunning,
		}},
	}}
	firstFailure := errors.New("first observation failed")
	secondFailure := errors.New("second observation failed")
	observed := make([]string, 0, 2)
	runner := newTestCoordinator(store, func(
		_ context.Context,
		attempt execution.ClaimedAttempt,
	) (coordinator.Observation, error) {
		observed = append(observed, attempt.Attempt.ID)
		if attempt.Attempt.ID == "attempt-1" {
			return coordinator.Observation{}, firstFailure
		}
		return coordinator.Observation{}, secondFailure
	})

	err := runner.Tick(context.Background())
	if !errors.Is(err, firstFailure) || !errors.Is(err, secondFailure) {
		t.Fatalf("Tick error = %v, want both observation failures", err)
	}
	if !reflect.DeepEqual(observed, []string{"attempt-1", "attempt-2"}) {
		t.Fatalf("observed attempts = %v", observed)
	}
	if store.claimCalls != 0 {
		t.Fatalf(
			"claimed new work after observation failure %d times",
			store.claimCalls,
		)
	}
}

func TestTickReturnsLeaseFailureWithoutClaimingOrExecuting(t *testing.T) {
	leaseFailure := errors.New("lease active attempts failed")
	store := &fakeStore{leaseError: leaseFailure}
	runtimeCalls := 0
	runner := newTestCoordinator(store, func(
		context.Context,
		execution.ClaimedAttempt,
	) (coordinator.Observation, error) {
		runtimeCalls++
		return coordinator.Observation{}, nil
	})

	if err := runner.Tick(
		context.Background(),
	); !errors.Is(err, leaseFailure) {
		t.Fatalf("Tick error = %v, want lease failure", err)
	}
	if store.claimCalls != 0 || runtimeCalls != 0 {
		t.Fatalf(
			"claim calls = %d, runtime calls = %d",
			store.claimCalls,
			runtimeCalls,
		)
	}
}

func TestTickReturnsUnexpectedClaimFailure(t *testing.T) {
	claimFailure := errors.New("claim failed")
	store := &fakeStore{claimError: claimFailure}
	runner := newTestCoordinator(store, func(
		context.Context,
		execution.ClaimedAttempt,
	) (coordinator.Observation, error) {
		t.Fatal("runtime called without a claimed attempt")
		return coordinator.Observation{}, nil
	})

	if err := runner.Tick(
		context.Background(),
	); !errors.Is(err, claimFailure) {
		t.Fatalf("Tick error = %v, want claim failure", err)
	}
	if store.claimCalls != 1 {
		t.Fatalf("claim calls = %d, want 1", store.claimCalls)
	}
}

func TestTickReturnsRunningTransitionFailure(t *testing.T) {
	transitionFailure := errors.New("mark running failed")
	store := &fakeStore{
		active: []execution.ClaimedAttempt{{Attempt: execution.Attempt{
			ID:         "attempt-1",
			State:      execution.AttemptStateStarting,
			LeaseToken: "token-1",
		}}},
		markRunningError: transitionFailure,
	}
	runner := newTestCoordinator(store, func(
		context.Context,
		execution.ClaimedAttempt,
	) (coordinator.Observation, error) {
		return coordinator.Observation{ExternalID: "job-uid-1"}, nil
	})

	if err := runner.Tick(
		context.Background(),
	); !errors.Is(err, transitionFailure) {
		t.Fatalf("Tick error = %v, want transition failure", err)
	}
	if store.claimCalls != 0 {
		t.Fatalf(
			"claimed new work after transition failure %d times",
			store.claimCalls,
		)
	}
}
