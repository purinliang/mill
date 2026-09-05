// Package coordinator connects durable task intent to an execution backend.
package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/purinliang/mill/internal/job"
)

const ExecutorName = "kubernetes"

type Store interface {
	ActiveAttempts(context.Context, string) ([]job.ClaimedAttempt, error)
	ClaimNextAttempt(context.Context, string) (job.ClaimedAttempt, error)
	MarkAttemptRunning(context.Context, string, string) (job.Attempt, error)
	CompleteAttempt(context.Context, string) (job.Attempt, error)
	FailAttempt(context.Context, string, string) (job.Attempt, error)
}

type Observation struct {
	ExternalID string
	Completed  bool
	Failure    string
}

type Executor interface {
	Reconcile(context.Context, job.ClaimedAttempt) (Observation, error)
}

type Coordinator struct {
	Store    Store
	Executor Executor
	Logger   *log.Logger
}

// Tick observes every active attempt before filling newly available slots.
// An API error is ambiguous: retain durable intent and retry observation on
// the next tick, instead of declaring failure and potentially duplicating work.
func (c *Coordinator) Tick(ctx context.Context) error {
	active, err := c.Store.ActiveAttempts(ctx, ExecutorName)
	if err != nil {
		return err
	}
	var failures []error
	for _, attempt := range active {
		if err := c.reconcile(ctx, attempt); err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) != 0 {
		return errors.Join(failures...)
	}
	// Bound each tick so a backlog across many jobs cannot starve observation.
	for range 100 {
		attempt, err := c.Store.ClaimNextAttempt(ctx, ExecutorName)
		if errors.Is(err, job.ErrNoTaskAvailable) {
			return nil
		}
		if err != nil {
			return err
		}
		c.Logger.Printf("claimed job=%s shard=%d attempt=%s number=%d", attempt.Attempt.JobID, attempt.ShardIndex, attempt.Attempt.ID, attempt.Attempt.Number)
		if err := c.reconcile(ctx, attempt); err != nil {
			return err
		}
	}
	return nil
}

func (c *Coordinator) reconcile(ctx context.Context, claimed job.ClaimedAttempt) error {
	observed, err := c.Executor.Reconcile(ctx, claimed)
	if err != nil {
		return fmt.Errorf("reconcile attempt %s: %w", claimed.Attempt.ID, err)
	}
	id := claimed.Attempt.ID
	if observed.ExternalID != "" && claimed.Attempt.State == job.AttemptStateStarting {
		if _, err := c.Store.MarkAttemptRunning(ctx, id, observed.ExternalID); err != nil {
			return err
		}
		c.Logger.Printf("dispatched job=%s shard=%d attempt=%s external=%s", claimed.Attempt.JobID, claimed.ShardIndex, id, observed.ExternalID)
	}
	if observed.Failure != "" {
		_, err = c.Store.FailAttempt(ctx, id, observed.Failure)
		if err == nil {
			c.Logger.Printf("attempt_failed job=%s shard=%d attempt=%s number=%d reason=%s", claimed.Attempt.JobID, claimed.ShardIndex, id, claimed.Attempt.Number, observed.Failure)
		}
		return err
	}
	if observed.Completed {
		_, err = c.Store.CompleteAttempt(ctx, id)
		if err == nil {
			c.Logger.Printf("completed job=%s shard=%d attempt=%s", claimed.Attempt.JobID, claimed.ShardIndex, id)
		}
	}
	return err
}
