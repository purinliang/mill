// This file tests completed status across Job and execution persistence.
package job_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/purinliang/mill/internal/job"
)

func TestCompletedJobStatusIncludesResults(t *testing.T) {
	repository, _, key := newPublicRepository(t)
	service, err := job.NewService(
		repository,
		fixedPartitioner{shards: publicShardSet()},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	created, wasCreated, err := service.Create(
		ctx,
		key,
		publicSubmission(),
	)
	if err != nil || !wasCreated {
		t.Fatalf("submit job = %+v, created %t, error %v", created, wasCreated, err)
	}
	attempt, err := repository.ClaimNextAttempt(
		ctx,
		"kubernetes",
		"status-test",
		30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.MarkAttemptRunning(
		ctx,
		attempt.Attempt.ID,
		attempt.Attempt.LeaseToken,
		"job-uid",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.CompleteAttempt(
		ctx,
		attempt.Attempt.ID,
		attempt.Attempt.LeaseToken,
	); err != nil {
		t.Fatal(err)
	}

	status, err := service.Get(ctx, created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != job.StateCompleted || len(status.Results) != 1 {
		t.Fatalf("completed status = %+v", status)
	}
	if !strings.Contains(status.Results[0].URI, attempt.Attempt.ID) {
		t.Fatalf("result does not identify attempt: %+v", status.Results[0])
	}
}

type fixedPartitioner struct {
	shards job.ShardSet
}

func (p fixedPartitioner) Partition(
	context.Context,
	string,
	int,
) (job.ShardSet, error) {
	return p.shards, nil
}
