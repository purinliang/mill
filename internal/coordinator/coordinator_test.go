package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"testing"

	"github.com/purinliang/mill/internal/job"
)

type memoryStore struct {
	attempts                 []job.ClaimedAttempt
	next, limit, total, peak int
	failed                   bool
}

func (s *memoryStore) ActiveAttempts(context.Context, string) ([]job.ClaimedAttempt, error) {
	var active []job.ClaimedAttempt
	for _, a := range s.attempts {
		if a.Attempt.State == job.AttemptStateStarting || a.Attempt.State == job.AttemptStateRunning {
			active = append(active, a)
		}
	}
	return active, nil
}

func (s *memoryStore) ClaimNextAttempt(ctx context.Context, _ string) (job.ClaimedAttempt, error) {
	active, _ := s.ActiveAttempts(ctx, "")
	if s.failed || s.next == s.total || len(active) == s.limit {
		return job.ClaimedAttempt{}, job.ErrNoTaskAvailable
	}
	a := job.ClaimedAttempt{Attempt: job.Attempt{ID: fmt.Sprint(s.next), JobID: "job", State: job.AttemptStateStarting}, ShardIndex: s.next}
	s.next++
	s.attempts = append(s.attempts, a)
	if len(active)+1 > s.peak {
		s.peak = len(active) + 1
	}
	return a, nil
}

func (s *memoryStore) transition(id string, state job.AttemptState) (job.Attempt, error) {
	for i := range s.attempts {
		if s.attempts[i].Attempt.ID == id {
			s.attempts[i].Attempt.State = state
			return s.attempts[i].Attempt, nil
		}
	}
	return job.Attempt{}, errors.New("missing attempt")
}

func (s *memoryStore) MarkAttemptRunning(_ context.Context, id, external string) (job.Attempt, error) {
	for i := range s.attempts {
		if s.attempts[i].Attempt.ID == id {
			s.attempts[i].Attempt.ExternalID = external
		}
	}
	return s.transition(id, job.AttemptStateRunning)
}
func (s *memoryStore) CompleteAttempt(_ context.Context, id string) (job.Attempt, error) {
	return s.transition(id, job.AttemptStateCompleted)
}
func (s *memoryStore) FailAttempt(_ context.Context, id, _ string) (job.Attempt, error) {
	s.failed = true
	return s.transition(id, job.AttemptStateFailed)
}

type executorFunc func(context.Context, job.ClaimedAttempt) (Observation, error)

func (f executorFunc) Reconcile(ctx context.Context, a job.ClaimedAttempt) (Observation, error) {
	return f(ctx, a)
}

func TestTwelveTasksWithThreeSlotsAndIndependentCompletion(t *testing.T) {
	store := &memoryStore{limit: 3, total: 12}
	finished := map[string]bool{}
	c := &Coordinator{Store: store, Logger: log.New(io.Discard, "", 0), Executor: executorFunc(func(_ context.Context, a job.ClaimedAttempt) (Observation, error) {
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
	if store.next != 4 || store.attempts[0].Attempt.State != job.AttemptStateRunning {
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
		if a.Attempt.State != job.AttemptStateCompleted {
			t.Fatalf("unfinished: %+v", a)
		}
	}
}

func TestAmbiguousDispatchKeepsAttemptForRestart(t *testing.T) {
	store := &memoryStore{limit: 1, total: 2}
	c := &Coordinator{Store: store, Logger: log.New(io.Discard, "", 0), Executor: executorFunc(func(context.Context, job.ClaimedAttempt) (Observation, error) {
		return Observation{}, errors.New("lost create response")
	})}
	if err := c.Tick(context.Background()); err == nil {
		t.Fatal("expected transient error")
	}
	if store.next != 1 || store.attempts[0].Attempt.State != job.AttemptStateStarting {
		t.Fatal("lost durable intent")
	}
	// A new coordinator instance uses persisted active attempts, no in-memory queue.
	restarted := &Coordinator{Store: store, Logger: c.Logger, Executor: executorFunc(func(_ context.Context, a job.ClaimedAttempt) (Observation, error) {
		return Observation{ExternalID: "existing-job"}, nil
	})}
	if err := restarted.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.next != 1 || store.attempts[0].Attempt.State != job.AttemptStateRunning {
		t.Fatal("restart did not resume the same attempt")
	}
}

func TestExhaustedFailureStopsClaimsButObservesOtherActiveTasks(t *testing.T) {
	// This fake models the store deciding the retry budget is exhausted. The
	// coordinator follows durable store decisions rather than owning a budget.
	store := &memoryStore{limit: 2, total: 4}
	c := &Coordinator{Store: store, Logger: log.New(io.Discard, "", 0)}
	c.Executor = executorFunc(func(context.Context, job.ClaimedAttempt) (Observation, error) {
		return Observation{ExternalID: "external"}, nil
	})
	if err := c.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.Executor = executorFunc(func(_ context.Context, a job.ClaimedAttempt) (Observation, error) {
		if a.ShardIndex == 0 {
			return Observation{ExternalID: "external", Failure: "exit 1"}, nil
		}
		return Observation{ExternalID: "external", Completed: true}, nil
	})
	if err := c.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.next != 2 || store.attempts[1].Attempt.State != job.AttemptStateCompleted {
		t.Fatal("failure handling did not drain active work")
	}
}
