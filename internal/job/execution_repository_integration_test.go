package job

import (
	"context"
	"strings"
	"testing"
)

func TestActiveAttemptsSurviveSiblingFailureAndListSuccessfulOutputs(t *testing.T) {
	repository, created := createAttemptTestJob(t, "integration:execution-recovery", 2, 2)
	ctx := context.Background()
	first, err := repository.ClaimNextAttempt(ctx, "kubernetes")
	if err != nil {
		t.Fatal(err)
	}
	second, err := repository.ClaimNextAttempt(ctx, "kubernetes")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.MarkAttemptRunning(ctx, second.Attempt.ID, "uid-second"); err != nil {
		t.Fatal(err)
	}
	active, err := repository.ActiveAttempts(ctx, "kubernetes")
	if err != nil || len(active) != 2 {
		t.Fatalf("active=%+v err=%v", active, err)
	}
	if active[0].Attempt.ID != first.Attempt.ID || active[0].OutputURI != first.OutputURI || active[0].InputEndByte != first.InputEndByte {
		t.Fatalf("starting attempt did not reconstruct its invocation: %+v", active[0])
	}
	if active[1].Attempt.ExternalID != "uid-second" || active[1].Attempt.State != AttemptStateRunning {
		t.Fatalf("running attempt=%+v", active[1])
	}
	other, err := repository.ActiveAttempts(ctx, "docker")
	if err != nil || len(other) != 0 {
		t.Fatalf("executor filter=%+v %v", other, err)
	}
	if _, err := repository.FailAttempt(ctx, first.Attempt.ID, "workload failed"); err != nil {
		t.Fatal(err)
	}
	active, err = repository.ActiveAttempts(ctx, "kubernetes")
	if err != nil || len(active) != 1 || active[0].Attempt.ID != second.Attempt.ID {
		t.Fatalf("failed job lost other active attempt: %+v %v", active, err)
	}
	if _, err := repository.CompleteAttempt(ctx, second.Attempt.ID); err != nil {
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
	a, err := repository.ClaimNextAttempt(ctx, "kubernetes")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.MarkAttemptRunning(ctx, a.Attempt.ID, "uid"); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.CompleteAttempt(ctx, a.Attempt.ID); err != nil {
		t.Fatal(err)
	}
	service, err := NewService(repository, JSONLPartitioner{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	status, err := service.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != StateCompleted || len(status.Results) != 1 || !strings.Contains(status.Results[0].URI, a.Attempt.ID) {
		t.Fatalf("status=%+v", status)
	}
}
