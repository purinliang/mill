package job

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const testInputSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func TestRepositoryCreateReplayGetAndPersist(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	pool := openIntegrationDatabase(t, databaseURL)
	key := "integration:create-replay-get"
	deleteJobByKey(t, pool, key)

	repository, err := NewRepository(pool, "file:///tmp/mill-output-a")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	submission := Submission{
		Executable: Executable{Image: "mill/example:dev"},
		Input:      InputSpec{URI: "file:///definitely/not/present/records.jsonl"},
	}

	createdJob, created, err := repository.Create(context.Background(), key, submission, testInputSHA256, 100, 3)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}
	if createdJob.State != StatePreparing {
		t.Errorf("state = %q, want %q", createdJob.State, StatePreparing)
	}
	if len(createdJob.ID) != 36 || createdJob.ID[14] != '7' {
		t.Errorf("job ID = %q, want a UUIDv7", createdJob.ID)
	}
	wantOutputURI := "file:///tmp/mill-output-a/jobs/" + createdJob.ID + "/"
	if createdJob.Output.URI != wantOutputURI {
		t.Errorf("output URI = %q, want %q", createdJob.Output.URI, wantOutputURI)
	}
	if createdJob.Input.SHA256 != testInputSHA256 || createdJob.Input.RecordCount != 100 {
		t.Errorf("input identity = SHA %q records %d, want SHA %q records 100", createdJob.Input.SHA256, createdJob.Input.RecordCount, testInputSHA256)
	}
	if createdJob.Parallelism != 3 {
		t.Errorf("parallelism = %d, want 3", createdJob.Parallelism)
	}
	if createdJob.Executable.Args == nil {
		t.Error("executable args are nil, want an empty array")
	}

	pool.Close()
	pool = openIntegrationDatabase(t, databaseURL)
	defer pool.Close()
	defer deleteJobByKey(t, pool, key)

	restartedRepository, err := NewRepository(pool, "file:///tmp/mill-output-b")
	if err != nil {
		t.Fatalf("create restarted repository: %v", err)
	}
	persistedJob, err := restartedRepository.Get(context.Background(), createdJob.ID)
	if err != nil {
		t.Fatalf("get persisted job: %v", err)
	}
	if persistedJob.ID != createdJob.ID {
		t.Errorf("persisted ID = %q, want %q", persistedJob.ID, createdJob.ID)
	}

	replayedJob, replayCreated, err := restartedRepository.Create(
		context.Background(), key, submission, strings.Repeat("b", 64), 200, 9,
	)
	if err != nil {
		t.Fatalf("replay job: %v", err)
	}
	if replayCreated {
		t.Fatal("created = true on replay, want false")
	}
	if replayedJob.ID != createdJob.ID || replayedJob.Output.URI != wantOutputURI || replayedJob.Parallelism != 3 {
		t.Errorf("replayed job = ID %q output %q parallelism %d, want original values", replayedJob.ID, replayedJob.Output.URI, replayedJob.Parallelism)
	}

	conflictingSubmissions := map[string]Submission{
		"image": {
			Executable: Executable{Image: "mill/other:dev"},
			Input:      submission.Input,
		},
		"arguments": {
			Executable: Executable{Image: submission.Executable.Image, Args: []string{"--changed"}},
			Input:      submission.Input,
		},
		"input": {
			Executable: submission.Executable,
			Input:      InputSpec{URI: "file:///data/other.jsonl"},
		},
	}
	for name, conflictingSubmission := range conflictingSubmissions {
		t.Run("conflicting "+name, func(t *testing.T) {
			if _, _, err := restartedRepository.Create(
				context.Background(), key, conflictingSubmission, testInputSHA256, 100, 3,
			); !errors.Is(err, ErrIdempotencyConflict) {
				t.Fatalf("conflicting create error = %v, want %v", err, ErrIdempotencyConflict)
			}
		})
	}

	if _, err := pool.Exec(context.Background(), "UPDATE public.jobs SET state = 'invalid' WHERE id = $1::uuid", createdJob.ID); err == nil {
		t.Fatal("invalid state update succeeded, want database constraint error")
	}
}

func TestRepositoryConcurrentIdempotentCreate(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	pool := openIntegrationDatabase(t, databaseURL)
	defer pool.Close()

	key := "integration:concurrent-create"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)
	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	submission := Submission{
		Executable: Executable{Image: "mill/example:dev", Args: []string{"--mode", "fast"}},
		Input:      InputSpec{URI: "file:///data/records.jsonl"},
	}

	const callers = 8
	start := make(chan struct{})
	results := make(chan createResult, callers)
	var callersDone sync.WaitGroup
	callersDone.Add(callers)
	for range callers {
		go func() {
			defer callersDone.Done()
			<-start
			job, created, err := repository.Create(context.Background(), key, submission, testInputSHA256, 100, 3)
			results <- createResult{job: job, created: created, err: err}
		}()
	}
	close(start)
	callersDone.Wait()
	close(results)

	createdCount := 0
	jobID := ""
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent create: %v", result.err)
		}
		if result.created {
			createdCount++
		}
		if jobID == "" {
			jobID = result.job.ID
		}
		if result.job.ID != jobID {
			t.Errorf("job ID = %q, want shared ID %q", result.job.ID, jobID)
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want 1", createdCount)
	}
}

func TestRepositoryMaterializeLogicalShardsAndReportProgress(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	pool := openIntegrationDatabase(t, databaseURL)
	defer pool.Close()

	key := "integration:materialize-progress"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)
	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	createdJob, _, err := repository.Create(context.Background(), key, Submission{
		Executable: Executable{Image: "mill/example:dev"},
		Input:      InputSpec{URI: "file:///data/records.jsonl"},
	}, testInputSHA256, 30, 3)
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	plan := PartitionPlan{
		InputSHA256: testInputSHA256,
		RecordCount: 30,
		Shards: []LogicalShard{
			{StartByte: 0, EndByte: 100},
			{StartByte: 100, EndByte: 220},
			{StartByte: 220, EndByte: 360},
		},
	}

	materializedJob, err := repository.Materialize(context.Background(), createdJob.ID, plan)
	if err != nil {
		t.Fatalf("materialize tasks: %v", err)
	}
	if materializedJob.State != StateRunning || materializedJob.Progress != (Progress{Total: 3, Pending: 3}) {
		t.Errorf("materialized job = state %q progress %+v, want running with 3 pending", materializedJob.State, materializedJob.Progress)
	}

	rows, err := pool.Query(context.Background(), `
		SELECT id::text, shard_index, input_start_byte, input_end_byte, state
		FROM public.tasks
		WHERE job_id = $1::uuid
		ORDER BY shard_index
	`, createdJob.ID)
	if err != nil {
		t.Fatalf("query tasks: %v", err)
	}
	defer rows.Close()
	index := 0
	for rows.Next() {
		var id, state string
		var shardIndex int
		var startByte, endByte int64
		if err := rows.Scan(&id, &shardIndex, &startByte, &endByte, &state); err != nil {
			t.Fatalf("scan task: %v", err)
		}
		if len(id) != 36 || id[14] != '7' {
			t.Errorf("task ID = %q, want a UUIDv7", id)
		}
		if shardIndex != index || startByte != plan.Shards[index].StartByte || endByte != plan.Shards[index].EndByte {
			t.Errorf("task %d = shard %d range [%d,%d), want shard %d range %+v", index, shardIndex, startByte, endByte, index, plan.Shards[index])
		}
		if state != "pending" {
			t.Errorf("task state = %q, want pending", state)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tasks: %v", err)
	}
	if index != len(plan.Shards) {
		t.Fatalf("task count = %d, want %d", index, len(plan.Shards))
	}

	if _, err := repository.Materialize(context.Background(), createdJob.ID, plan); err != nil {
		t.Fatalf("replay materialization: %v", err)
	}
	changedPlan := plan
	changedPlan.InputSHA256 = strings.Repeat("b", 64)
	if _, err := repository.Materialize(context.Background(), createdJob.ID, changedPlan); !errors.Is(err, ErrInputConflict) {
		t.Fatalf("changed input error = %v, want %v", err, ErrInputConflict)
	}

	if _, err := pool.Exec(context.Background(), `
		UPDATE public.tasks
		SET state = 'completed', updated_at = now()
		WHERE job_id = $1::uuid AND shard_index = 0
	`, createdJob.ID); err != nil {
		t.Fatalf("complete task: %v", err)
	}
	progressJob, err := repository.Get(context.Background(), createdJob.ID)
	if err != nil {
		t.Fatalf("get job progress: %v", err)
	}
	if progressJob.Progress != (Progress{Total: 3, Pending: 2, Completed: 1}) {
		t.Errorf("progress = %+v, want total=3 pending=2 completed=1", progressJob.Progress)
	}
}

type createResult struct {
	job     Job
	created bool
	err     error
}

func integrationDatabaseURL(t *testing.T) string {
	t.Helper()
	databaseURL := os.Getenv("MILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("MILL_TEST_DATABASE_URL is not set")
	}
	return databaseURL
}

func openIntegrationDatabase(t *testing.T, databaseURL string) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("create integration database pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping integration database: %v", err)
	}

	var jobsTable, tasksTable, attemptsTable *string
	var hasLogicalRanges bool
	if err := pool.QueryRow(ctx, `
		SELECT
			to_regclass('public.jobs')::text,
			to_regclass('public.tasks')::text,
			to_regclass('public.attempts')::text,
			EXISTS (
				SELECT 1
				FROM information_schema.columns
				WHERE table_schema = 'public'
					AND table_name = 'tasks'
					AND column_name = 'input_start_byte'
			)
	`).Scan(&jobsTable, &tasksTable, &attemptsTable, &hasLogicalRanges); err != nil {
		pool.Close()
		t.Fatalf("check database migrations: %v", err)
	}
	if jobsTable == nil || tasksTable == nil || attemptsTable == nil || !hasLogicalRanges {
		pool.Close()
		t.Fatal("required schema does not exist; apply all numbered migrations to the test database")
	}
	return pool
}

func deleteJobByKey(t *testing.T, pool *pgxpool.Pool, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, "DELETE FROM public.jobs WHERE idempotency_key = $1", key); err != nil {
		t.Fatalf("delete integration test job: %v", err)
	}
}
