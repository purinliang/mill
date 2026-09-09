// This file tests lease renewal and takeover against real PostgreSQL.
package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	jobmodel "github.com/purinliang/mill/internal/job"
	"github.com/purinliang/mill/internal/job/partition"
	"github.com/purinliang/mill/internal/objectstore"
)

func TestAttemptLeaseRenewalAndFencedTakeover(t *testing.T) {
	repository, _ := createAttemptTestJob(t, "integration:attempt-lease", 1, 1)
	ctx := context.Background()
	first, err := repository.ClaimNextAttempt(ctx, "kubernetes", "executor-a", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Attempt.LeaseToken == "" || first.Attempt.LeaseExpiresAt == nil {
		t.Fatalf("claim has no lease: %+v", first.Attempt)
	}

	contended, err := repository.LeaseActiveAttempts(ctx, "kubernetes", "executor-b", 30*time.Second)
	if err != nil || len(contended) != 0 {
		t.Fatalf("unexpired lease was stolen: %+v %v", contended, err)
	}
	renewed, err := repository.LeaseActiveAttempts(ctx, "kubernetes", "executor-a", 30*time.Second)
	if err != nil || len(renewed) != 1 {
		t.Fatalf("renewed attempts = %+v, %v", renewed, err)
	}
	if renewed[0].Attempt.LeaseToken != first.Attempt.LeaseToken ||
		!renewed[0].Attempt.LeaseExpiresAt.After(*first.Attempt.LeaseExpiresAt) {
		t.Fatalf("renewal replaced identity or did not extend expiry: %+v", renewed[0].Attempt)
	}

	if _, err := repository.database.Exec(ctx, `
		UPDATE public.attempts
		SET lease_expires_at = now() - interval '1 second'
		WHERE id = $1::uuid
	`, first.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	takenOver, err := repository.LeaseActiveAttempts(ctx, "kubernetes", "executor-b", 30*time.Second)
	if err != nil || len(takenOver) != 1 {
		t.Fatalf("takeover attempts = %+v, %v", takenOver, err)
	}
	second := takenOver[0]
	if second.Attempt.ID != first.Attempt.ID || second.Attempt.LeaseToken == first.Attempt.LeaseToken {
		t.Fatalf("takeover did not preserve attempt and replace fence: %+v", second.Attempt)
	}
	if _, err := repository.MarkAttemptRunning(ctx, first.Attempt.ID, first.Attempt.LeaseToken, "stale-uid"); !errors.Is(err, ErrAttemptLeaseLost) {
		t.Fatalf("stale transition error = %v, want %v", err, ErrAttemptLeaseLost)
	}
	if _, err := repository.MarkAttemptRunning(ctx, second.Attempt.ID, second.Attempt.LeaseToken, "current-uid"); err != nil {
		t.Fatal(err)
	}
}

func TestActiveAttemptsSurviveSiblingFailureAndListSuccessfulOutputs(t *testing.T) {
	repository, created := createAttemptTestJob(t, "integration:execution-recovery", 2, 2)
	ctx := context.Background()
	first, err := repository.ClaimNextAttempt(ctx, "kubernetes", testLeaseOwner, testLeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	second, err := repository.ClaimNextAttempt(ctx, "kubernetes", testLeaseOwner, testLeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.MarkAttemptRunning(ctx, second.Attempt.ID, second.Attempt.LeaseToken, "uid-second"); err != nil {
		t.Fatal(err)
	}
	active, err := repository.LeaseActiveAttempts(ctx, "kubernetes", testLeaseOwner, testLeaseDuration)
	if err != nil || len(active) != 2 {
		t.Fatalf("active=%+v err=%v", active, err)
	}
	if active[0].Attempt.ID != first.Attempt.ID || active[0].OutputURI != first.OutputURI || active[0].InputEndByte != first.InputEndByte {
		t.Fatalf("starting attempt did not reconstruct its invocation: %+v", active[0])
	}
	if active[1].Attempt.ExternalID != "uid-second" || active[1].Attempt.State != AttemptStateRunning {
		t.Fatalf("running attempt=%+v", active[1])
	}
	other, err := repository.LeaseActiveAttempts(ctx, "docker", testLeaseOwner, testLeaseDuration)
	if err != nil || len(other) != 0 {
		t.Fatalf("executor filter=%+v %v", other, err)
	}
	if _, err := repository.FailAttempt(ctx, first.Attempt.ID, first.Attempt.LeaseToken, "workload failed"); err != nil {
		t.Fatal(err)
	}
	active, err = repository.LeaseActiveAttempts(ctx, "kubernetes", testLeaseOwner, testLeaseDuration)
	if err != nil || len(active) != 1 || active[0].Attempt.ID != second.Attempt.ID {
		t.Fatalf("failed job lost other active attempt: %+v %v", active, err)
	}
	if _, err := repository.CompleteAttempt(ctx, second.Attempt.ID, second.Attempt.LeaseToken); err != nil {
		t.Fatal(err)
	}
	results, err := repository.CompletedResults(ctx, created.ID)
	if err != nil || len(results) != 1 {
		t.Fatalf("results=%+v %v", results, err)
	}
	if results[0].URI != second.OutputURI || results[0].TaskID != second.Attempt.TaskID || results[0].ShardIndex != 1 {
		t.Fatalf("wrong result=%+v", results[0])
	}
}

func TestCompletedJobStatusIncludesResults(t *testing.T) {
	repository, created := createAttemptTestJob(t, "integration:execution-results", 1, 1)
	ctx := context.Background()
	a, err := repository.ClaimNextAttempt(ctx, "kubernetes", testLeaseOwner, testLeaseDuration)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.MarkAttemptRunning(ctx, a.Attempt.ID, a.Attempt.LeaseToken, "uid"); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.CompleteAttempt(ctx, a.Attempt.ID, a.Attempt.LeaseToken); err != nil {
		t.Fatal(err)
	}
	service, err := jobmodel.NewService(
		repository.jobs,
		partition.New(&objectstore.Store{}),
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	status, err := service.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != jobmodel.StateCompleted || len(status.Results) != 1 || !strings.Contains(status.Results[0].URI, a.Attempt.ID) {
		t.Fatalf("status=%+v", status)
	}
}
