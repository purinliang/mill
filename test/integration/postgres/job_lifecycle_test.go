// This file tests one completed Job across Job and execution persistence.
package postgres_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"
	"time"

	executionpostgres "github.com/purinliang/mill/internal/execution/postgres"
	"github.com/purinliang/mill/internal/job"
	jobpostgres "github.com/purinliang/mill/internal/job/postgres"
)

func TestCompletedJobStatusIncludesResults(t *testing.T) {
	pool := openIntegrationDatabase(t)
	key := fmt.Sprintf("lifecycle:%d", time.Now().UnixNano())
	deleteJobByKey(t, pool, key)
	t.Cleanup(func() { deleteJobByKey(t, pool, key) })

	jobStore, err := jobpostgres.NewRepository(
		pool,
		"file:///tmp/mill-integration-output",
	)
	if err != nil {
		t.Fatal(err)
	}
	executionStore, err := executionpostgres.NewRepository(pool)
	if err != nil {
		t.Fatal(err)
	}
	input := []byte("{\"text\":\"test record\"}\n")
	digest := sha256.Sum256(input)
	service, err := job.NewService(
		jobStore,
		fixedPartitioner{shards: job.ShardSet{
			InputSHA256: fmt.Sprintf("%x", digest),
			RecordCount: 1,
			Shards: []job.LogicalShard{
				{StartByte: 0, EndByte: int64(len(input))},
			},
		}},
		1,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	created, wasCreated, err := service.Create(
		ctx,
		key,
		job.Submission{
			Executable: job.Executable{Image: "mill/example:dev"},
			Input: job.InputSpec{
				URI: "file:///tmp/records.jsonl",
			},
		},
	)
	if err != nil || !wasCreated {
		t.Fatalf(
			"submit job = %+v, created %t, error %v",
			created,
			wasCreated,
			err,
		)
	}
	attempt, err := executionStore.ClaimNextAttempt(
		ctx,
		"kubernetes",
		"lifecycle-test",
		30*time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executionStore.MarkAttemptRunning(
		ctx,
		attempt.Attempt.ID,
		attempt.Attempt.LeaseToken,
		"job-uid",
	); err != nil {
		t.Fatal(err)
	}
	if _, err := executionStore.CompleteAttempt(
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
