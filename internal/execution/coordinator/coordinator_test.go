// This file tests coordinator reconciliation with in-memory collaborators.
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"testing"
	"time"

	"github.com/purinliang/mill/internal/execution"
)

const testLeaseDuration = 15 * time.Second

type memoryStore struct {
	attempts                 []execution.ClaimedAttempt
	next, limit, total, peak int
	failed                   bool
}

func (s *memoryStore) LeaseActiveAttempts(_ context.Context, _, owner string) ([]execution.ClaimedAttempt, error) {
	var active []execution.ClaimedAttempt
	now := time.Now()
	for index := range s.attempts {
		a := &s.attempts[index]
		if a.Attempt.State != execution.AttemptStateStarting && a.Attempt.State != execution.AttemptStateRunning {
			continue
		}
		if a.Attempt.LeaseOwner != owner && a.Attempt.LeaseExpiresAt != nil && a.Attempt.LeaseExpiresAt.After(now) {
			continue
		}
		if a.Attempt.LeaseOwner != owner {
			a.Attempt.LeaseToken = "lease-" + owner + "-" + a.Attempt.ID
		}
		a.Attempt.LeaseOwner = owner
		expires := now.Add(testLeaseDuration)
		a.Attempt.LeaseExpiresAt = &expires
		active = append(active, *a)
	}
	return active, nil
}

func (s *memoryStore) ClaimNextAttempt(_ context.Context, _, owner string) (execution.ClaimedAttempt, error) {
	active := 0
	for _, attempt := range s.attempts {
		if attempt.Attempt.State == execution.AttemptStateStarting || attempt.Attempt.State == execution.AttemptStateRunning {
			active++
		}
	}
	if s.failed || s.next == s.total || active == s.limit {
		return execution.ClaimedAttempt{}, execution.ErrNoTaskAvailable
	}
	expires := time.Now().Add(testLeaseDuration)
	a := execution.ClaimedAttempt{Attempt: execution.Attempt{
		ID: fmt.Sprint(s.next), JobID: "job", State: execution.AttemptStateStarting,
		LeaseOwner: owner, LeaseToken: "lease-" + fmt.Sprint(s.next), LeaseExpiresAt: &expires,
	}, ShardIndex: s.next}
	s.next++
	s.attempts = append(s.attempts, a)
	if active+1 > s.peak {
		s.peak = active + 1
	}
	return a, nil
}

func (s *memoryStore) transition(id string, state execution.AttemptState) (execution.Attempt, error) {
	for i := range s.attempts {
		if s.attempts[i].Attempt.ID == id {
			s.attempts[i].Attempt.State = state
			return s.attempts[i].Attempt, nil
		}
	}
	return execution.Attempt{}, errors.New("missing attempt")
}

func (s *memoryStore) MarkAttemptRunning(_ context.Context, id, _, external string) (execution.Attempt, error) {
	for i := range s.attempts {
		if s.attempts[i].Attempt.ID == id {
			s.attempts[i].Attempt.ExternalID = external
		}
	}
	return s.transition(id, execution.AttemptStateRunning)
}
func (s *memoryStore) CompleteAttempt(_ context.Context, id, _ string) (execution.Attempt, error) {
	return s.transition(id, execution.AttemptStateCompleted)
}
func (s *memoryStore) FailAttempt(_ context.Context, id, _, _ string) (execution.Attempt, error) {
	s.failed = true
	return s.transition(id, execution.AttemptStateFailed)
}

type runtimeFunc func(context.Context, execution.ClaimedAttempt) (Observation, error)

func (f runtimeFunc) Reconcile(ctx context.Context, a execution.ClaimedAttempt) (Observation, error) {
	return f(ctx, a)
}

func TestTwelveTasksWithThreeSlotsAndIndependentCompletion(t *testing.T) {
	store := &memoryStore{limit: 3, total: 12}
	finished := map[string]bool{}
	c := &Coordinator{Store: store, Logger: log.New(io.Discard, "", 0), LeaseOwner: "executor-a", Runtime: runtimeFunc(func(_ context.Context, a execution.ClaimedAttempt) (Observation, error) {
		return Observation{ExternalID: "pod-" + a.Attempt.ID, Completed: finished[a.Attempt.ID]}, nil
	})}
	if err := c.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.next != 3 {
		t.Fatalf("claimed %d, want 3", store.next)
	}
	// Task 1 finishes ahead of task 0: its freed slot must start task 3.
	finished["1"] = true
	if err := c.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.next != 4 || store.attempts[0].Attempt.State != execution.AttemptStateRunning {
		t.Fatal("did not replenish the independently freed slot")
	}
	for range 12 {
		for _, a := range store.attempts {
			finished[a.Attempt.ID] = true
		}
		if err := c.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if store.peak != 3 || len(store.attempts) != 12 {
		t.Fatalf("peak=%d attempts=%d", store.peak, len(store.attempts))
	}
	for _, a := range store.attempts {
		if a.Attempt.State != execution.AttemptStateCompleted {
			t.Fatalf("unfinished: %+v", a)
		}
	}
}

func TestAmbiguousDispatchKeepsAttemptForRestart(t *testing.T) {
	store := &memoryStore{limit: 1, total: 2}
	c := &Coordinator{Store: store, Logger: log.New(io.Discard, "", 0), LeaseOwner: "executor-a", Runtime: runtimeFunc(func(context.Context, execution.ClaimedAttempt) (Observation, error) {
		return Observation{}, errors.New("lost create response")
	})}
	if err := c.Tick(context.Background()); err == nil {
		t.Fatal("expected transient error")
	}
	if store.next != 1 || store.attempts[0].Attempt.State != execution.AttemptStateStarting {
		t.Fatal("lost durable intent")
	}
	expired := time.Now().Add(-time.Second)
	store.attempts[0].Attempt.LeaseExpiresAt = &expired
	// A new coordinator instance uses persisted active attempts, no in-memory queue.
	restarted := &Coordinator{Store: store, Logger: c.Logger, LeaseOwner: "executor-b", Runtime: runtimeFunc(func(_ context.Context, a execution.ClaimedAttempt) (Observation, error) {
		return Observation{ExternalID: "existing-job"}, nil
	})}
	if err := restarted.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.next != 1 || store.attempts[0].Attempt.State != execution.AttemptStateRunning {
		t.Fatal("restart did not resume the same attempt")
	}
}

func TestCoordinatorTakesOverOnlyAfterLeaseExpiry(t *testing.T) {
	store := &memoryStore{limit: 1, total: 1}
	observedByB := 0
	coordinatorA := &Coordinator{
		Store: store, Logger: log.New(io.Discard, "", 0),
		LeaseOwner: "executor-a",
		Runtime: runtimeFunc(func(_ context.Context, a execution.ClaimedAttempt) (Observation, error) {
			return Observation{ExternalID: "job-" + a.Attempt.ID}, nil
		}),
	}
	coordinatorB := &Coordinator{
		Store: store, Logger: coordinatorA.Logger,
		LeaseOwner: "executor-b",
		Runtime: runtimeFunc(func(context.Context, execution.ClaimedAttempt) (Observation, error) {
			observedByB++
			return Observation{}, nil
		}),
	}
	if err := coordinatorA.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	firstToken := store.attempts[0].Attempt.LeaseToken
	if err := coordinatorB.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if observedByB != 0 || store.attempts[0].Attempt.LeaseToken != firstToken {
		t.Fatal("second coordinator stole an unexpired attempt")
	}
	expired := time.Now().Add(-time.Second)
	store.attempts[0].Attempt.LeaseExpiresAt = &expired
	if err := coordinatorB.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if observedByB != 1 || store.attempts[0].Attempt.LeaseToken == firstToken {
		t.Fatal("second coordinator did not take over the expired attempt")
	}
}

func TestExhaustedFailureStopsClaimsButObservesOtherActiveTasks(t *testing.T) {
	// This fake models the store deciding the retry budget is exhausted. The
	// coordinator follows durable store decisions rather than owning a budget.
	store := &memoryStore{limit: 2, total: 4}
	c := &Coordinator{Store: store, Logger: log.New(io.Discard, "", 0), LeaseOwner: "executor-a"}
	c.Runtime = runtimeFunc(func(context.Context, execution.ClaimedAttempt) (Observation, error) {
		return Observation{ExternalID: "external"}, nil
	})
	if err := c.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.Runtime = runtimeFunc(func(_ context.Context, a execution.ClaimedAttempt) (Observation, error) {
		if a.ShardIndex == 0 {
			return Observation{ExternalID: "external", Failure: "exit 1"}, nil
		}
		return Observation{ExternalID: "external", Completed: true}, nil
	})
	if err := c.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.next != 2 || store.attempts[1].Attempt.State != execution.AttemptStateCompleted {
		t.Fatal("failure handling did not drain active work")
	}
}
