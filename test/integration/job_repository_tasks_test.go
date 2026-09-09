// This file tests durable task materialization against PostgreSQL.
package integration_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	. "github.com/purinliang/mill/internal/job"
	. "github.com/purinliang/mill/internal/job/postgres"
)

func TestRepositoryMaterializeLogicalShardsAndReportProgress(t *testing.T) {
	pool := openIntegrationDatabase(t)
	defer pool.Close()

	key := "integration:materialize-progress"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)
	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	createdJob, _, err := repository.Create(
		context.Background(),
		key,
		Submission{
			Executable: Executable{Image: "mill/example:dev"},
			Input: InputSpec{
				URI: "file:///data/records.jsonl",
			},
		},
		integrationInputSHA256,
		30,
		3,
		integrationResources,
	)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	shards := ShardSet{
		InputSHA256: integrationInputSHA256,
		RecordCount: 30,
		Shards: []LogicalShard{
			{StartByte: 0, EndByte: 100},
			{StartByte: 100, EndByte: 220},
			{StartByte: 220, EndByte: 360},
		},
	}

	materializedJob, err := repository.Materialize(
		context.Background(),
		createdJob.ID,
		shards,
	)
	if err != nil {
		t.Fatalf("materialize tasks: %v", err)
	}
	wantProgress := Progress{Total: 3, Pending: 3}
	if materializedJob.State != StateRunning ||
		materializedJob.Progress != wantProgress {
		t.Errorf(
			"materialized job = state %q progress %+v",
			materializedJob.State,
			materializedJob.Progress,
		)
	}

	assertMaterializedTasks(t, pool, createdJob.ID, shards)

	if _, err := repository.Materialize(
		context.Background(),
		createdJob.ID,
		shards,
	); err != nil {
		t.Fatalf("replay materialization: %v", err)
	}
	changedShards := shards
	changedShards.InputSHA256 = strings.Repeat("b", 64)
	if _, err := repository.Materialize(
		context.Background(),
		createdJob.ID,
		changedShards,
	); !errors.Is(err, ErrInputConflict) {
		t.Fatalf(
			"changed input error = %v, want %v",
			err,
			ErrInputConflict,
		)
	}
	changedShards = shards
	changedShards.Shards = append(
		changedShards.Shards,
		LogicalShard{StartByte: 360, EndByte: 400},
	)
	if _, err := repository.Materialize(
		context.Background(),
		createdJob.ID,
		changedShards,
	); !errors.Is(err, ErrInputConflict) {
		t.Fatalf(
			"changed shard count error = %v, want %v",
			err,
			ErrInputConflict,
		)
	}
	if _, err := repository.Materialize(
		context.Background(),
		"00000000-0000-7000-8000-000000000001",
		shards,
	); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing job error = %v, want %v", err, ErrNotFound)
	}

	if _, err := pool.Exec(context.Background(), `
		UPDATE public.tasks
		SET state = 'completed', updated_at = now()
		WHERE job_id = $1::uuid AND shard_index = 0
	`, createdJob.ID); err != nil {
		t.Fatalf("complete task: %v", err)
	}
	progressJob, err := repository.Get(
		context.Background(),
		createdJob.ID,
	)
	if err != nil {
		t.Fatalf("get job progress: %v", err)
	}
	wantProgress = Progress{Total: 3, Pending: 2, Completed: 1}
	if progressJob.Progress != wantProgress {
		t.Errorf("progress = %+v, want %+v", progressJob.Progress, wantProgress)
	}
}

func assertMaterializedTasks(
	t *testing.T,
	pool *pgxpool.Pool,
	jobID string,
	shards ShardSet,
) {
	t.Helper()
	rows, err := pool.Query(context.Background(), `
		SELECT id::text, shard_index, input_start_byte, input_end_byte, state
		FROM public.tasks
		WHERE job_id = $1::uuid
		ORDER BY shard_index
	`, jobID)
	if err != nil {
		t.Fatalf("query tasks: %v", err)
	}
	defer rows.Close()

	index := 0
	for rows.Next() {
		var id, state string
		var shardIndex int
		var startByte, endByte int64
		if err := rows.Scan(
			&id,
			&shardIndex,
			&startByte,
			&endByte,
			&state,
		); err != nil {
			t.Fatalf("scan task: %v", err)
		}
		if len(id) != 36 || id[14] != '7' {
			t.Errorf("task ID = %q, want a UUIDv7", id)
		}
		if shardIndex != index ||
			startByte != shards.Shards[index].StartByte ||
			endByte != shards.Shards[index].EndByte {
			t.Errorf(
				"task %d = shard %d range [%d,%d); want shard %d range %+v",
				index,
				shardIndex,
				startByte,
				endByte,
				index,
				shards.Shards[index],
			)
		}
		if state != "pending" {
			t.Errorf("task state = %q, want pending", state)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tasks: %v", err)
	}
	if index != len(shards.Shards) {
		t.Fatalf("task count = %d, want %d", index, len(shards.Shards))
	}
}
