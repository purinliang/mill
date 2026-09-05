package job

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRetryKeepsHistoryDelayAndSuccessfulOutput(t *testing.T) {
	r, created := createAttemptTestJob(t, "integration:retry-success", 1, 1)
	ctx := context.Background()
	first := claimRetryTestAttempt(t, r)
	if _, err := r.MarkAttemptRunning(ctx, first.Attempt.ID, "first-uid"); err != nil {
		t.Fatal(err)
	}
	failed, err := r.FailAttempt(ctx, first.Attempt.ID, "injected exit 1")
	if err != nil {
		t.Fatal(err)
	}
	var available time.Time
	if err := r.database.QueryRow(ctx, "SELECT available_at FROM tasks WHERE id = $1::uuid", first.Attempt.TaskID).Scan(&available); err != nil {
		t.Fatal(err)
	}
	if failed.FinishedAt == nil || !available.Equal(failed.FinishedAt.Add(TaskRetryDelay)) {
		t.Fatalf("retry time=%s finished=%v", available, failed.FinishedAt)
	}
	status := getAttemptTestJob(t, r, created.ID)
	if status.State != StateRunning || status.Progress != (Progress{Total: 1, Pending: 1}) {
		t.Fatalf("retrying task counted as terminal failure: %+v", status)
	}
	if _, err := r.ClaimNextAttempt(ctx, "kubernetes"); !errors.Is(err, ErrNoTaskAvailable) {
		t.Fatalf("claimed before delay elapsed: %v", err)
	}
	if _, err := r.FailAttempt(ctx, first.Attempt.ID, "duplicate observation"); err != nil {
		t.Fatal(err)
	}
	var replayAvailable time.Time
	if err := r.database.QueryRow(ctx, "SELECT available_at FROM tasks WHERE id = $1::uuid", first.Attempt.TaskID).Scan(&replayAvailable); err != nil {
		t.Fatal(err)
	}
	if !available.Equal(replayAvailable) {
		t.Fatal("duplicate failure postponed retry")
	}
	// Advance the durable deadline instead of sleeping in an integration test.
	makeRetryAvailable(t, r, first.Attempt.TaskID)
	restarted, err := NewRepository(r.database, "file:///tmp/mill-attempt-output")
	if err != nil {
		t.Fatal(err)
	}
	second := claimRetryTestAttempt(t, restarted)
	if second.Attempt.Number != 2 || second.Attempt.TaskID != first.Attempt.TaskID || second.OutputURI == first.OutputURI || second.Attempt.ID == first.Attempt.ID {
		t.Fatalf("retry did not preserve task and replace attempt/output: %+v", second)
	}
	active, err := restarted.ActiveAttempts(ctx, "kubernetes")
	if err != nil || len(active) != 1 || active[0].Attempt.Number != 2 {
		t.Fatalf("reconstructed retry=%+v err=%v", active, err)
	}
	// A stale failure must not move the new running attempt back to pending.
	if _, err := r.FailAttempt(ctx, first.Attempt.ID, "stale observation"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ClaimNextAttempt(ctx, "kubernetes"); !errors.Is(err, ErrNoTaskAvailable) {
		t.Fatalf("duplicate retry claim: %v", err)
	}
	if _, err := restarted.MarkAttemptRunning(ctx, second.Attempt.ID, "second-uid"); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.CompleteAttempt(ctx, second.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	status = getAttemptTestJob(t, r, created.ID)
	if status.State != StateCompleted || status.Progress.Completed != 1 {
		t.Fatalf("retry did not complete job: %+v", status)
	}
	results, err := r.CompletedResults(ctx, created.ID)
	if err != nil || len(results) != 1 || results[0].URI != second.OutputURI {
		t.Fatalf("results included failed output: %+v %v", results, err)
	}
	history, err := r.GetAttempt(ctx, first.Attempt.ID)
	if err != nil || history.State != AttemptStateFailed || history.FailureMessage != "injected exit 1" {
		t.Fatalf("failed attempt history lost: %+v %v", history, err)
	}
}

func TestRetryLimitFailsJobAndDrainsActiveWork(t *testing.T) {
	r, created := createAttemptTestJob(t, "integration:retry-limit", 3, 2)
	ctx := context.Background()
	first := claimRetryTestAttempt(t, r)
	sibling := claimRetryTestAttempt(t, r)
	current := first
	for number := 1; number <= MaxTaskAttempts; number++ {
		if current.Attempt.Number != number || current.Attempt.TaskID != first.Attempt.TaskID {
			t.Fatalf("wrong retry: %+v", current)
		}
		if _, err := r.MarkAttemptRunning(ctx, current.Attempt.ID, fmt.Sprintf("uid-%d", number)); err != nil {
			t.Fatal(err)
		}
		if _, err := r.FailAttempt(ctx, current.Attempt.ID, "permanent failure"); err != nil {
			t.Fatal(err)
		}
		if number < MaxTaskAttempts {
			makeRetryAvailable(t, r, first.Attempt.TaskID)
			current = claimRetryTestAttempt(t, r)
			if _, err := r.ClaimNextAttempt(ctx, "kubernetes"); !errors.Is(err, ErrNoTaskAvailable) {
				t.Fatalf("retry exceeded parallelism: %v", err)
			}
		}
	}
	status := getAttemptTestJob(t, r, created.ID)
	if status.State != StateFailed || status.Progress != (Progress{Total: 3, Failed: 1, Running: 1, Pending: 1}) {
		t.Fatalf("exhausted job=%+v", status)
	}
	if _, err := r.ClaimNextAttempt(ctx, "kubernetes"); !errors.Is(err, ErrNoTaskAvailable) {
		t.Fatalf("failed job dispatched work: %v", err)
	}
	active, err := r.ActiveAttempts(ctx, "kubernetes")
	if err != nil || len(active) != 1 || active[0].Attempt.ID != sibling.Attempt.ID {
		t.Fatalf("lost active sibling: %+v %v", active, err)
	}
	// Even a first attempt must not queue a retry after another task fails the job.
	if _, err := r.FailAttempt(ctx, sibling.Attempt.ID, "sibling failed too"); err != nil {
		t.Fatal(err)
	}
	status = getAttemptTestJob(t, r, created.ID)
	if status.State != StateFailed || status.Progress != (Progress{Total: 3, Failed: 2, Pending: 1}) {
		t.Fatalf("failed job restarted sibling: %+v", status)
	}
	var count int
	if err := r.database.QueryRow(ctx, "SELECT count(*) FROM attempts WHERE task_id = $1::uuid", first.Attempt.TaskID).Scan(&count); err != nil || count != MaxTaskAttempts {
		t.Fatalf("attempt count=%d err=%v", count, err)
	}
}

func TestWaitingRetryAllowsOtherTasksAndConcurrentClaimsStayUnique(t *testing.T) {
	r, _ := createAttemptTestJob(t, "integration:retry-capacity", 2, 1)
	ctx := context.Background()
	first := claimRetryTestAttempt(t, r)
	if _, err := r.FailAttempt(ctx, first.Attempt.ID, "failure"); err != nil {
		t.Fatal(err)
	}
	next := claimRetryTestAttempt(t, r)
	if next.ShardIndex != 1 {
		t.Fatal("waiting retry blocked an untouched task")
	}
	makeRetryAvailable(t, r, first.Attempt.TaskID)
	if _, err := r.ClaimNextAttempt(ctx, "kubernetes"); !errors.Is(err, ErrNoTaskAvailable) {
		t.Fatalf("due retry ignored occupied slot: %v", err)
	}
	if _, err := r.MarkAttemptRunning(ctx, next.Attempt.ID, "uid"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.CompleteAttempt(ctx, next.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Go(func() {
			_, err := r.ClaimNextAttempt(ctx, "kubernetes")
			results <- err
		})
	}
	wg.Wait()
	close(results)
	claims := 0
	for err := range results {
		if err == nil {
			claims++
		} else if !errors.Is(err, ErrNoTaskAvailable) {
			t.Fatal(err)
		}
	}
	if claims != 1 {
		t.Fatalf("concurrent retry claims=%d, want 1", claims)
	}
}

func claimRetryTestAttempt(t *testing.T, r *Repository) ClaimedAttempt {
	t.Helper()
	a, err := r.ClaimNextAttempt(context.Background(), "kubernetes")
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func makeRetryAvailable(t *testing.T, r *Repository, taskID string) {
	t.Helper()
	if _, err := r.database.Exec(context.Background(), "UPDATE tasks SET available_at = now() - interval '1 second' WHERE id = $1::uuid", taskID); err != nil {
		t.Fatal(err)
	}
}
