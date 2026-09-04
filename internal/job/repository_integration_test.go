package job

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

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
		Workload: Workload{Image: "mill/example:dev"},
		Dataset:  Dataset{ManifestURI: "file:///definitely/not/present/manifest.json"},
	}

	createdJob, created, err := repository.Create(context.Background(), key, submission)
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
	if createdJob.Workload.Args == nil {
		t.Error("workload args are nil, want an empty array")
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

	replayedJob, replayCreated, err := restartedRepository.Create(context.Background(), key, submission)
	if err != nil {
		t.Fatalf("replay job: %v", err)
	}
	if replayCreated {
		t.Fatal("created = true on replay, want false")
	}
	if replayedJob.ID != createdJob.ID {
		t.Errorf("replayed ID = %q, want %q", replayedJob.ID, createdJob.ID)
	}
	if replayedJob.Output.URI != wantOutputURI {
		t.Errorf("replayed output URI = %q, want original %q", replayedJob.Output.URI, wantOutputURI)
	}

	conflictingSubmissions := map[string]Submission{
		"image": {
			Workload: Workload{Image: "mill/other:dev"},
			Dataset:  submission.Dataset,
		},
		"arguments": {
			Workload: Workload{Image: submission.Workload.Image, Args: []string{"--changed"}},
			Dataset:  submission.Dataset,
		},
		"manifest": {
			Workload: submission.Workload,
			Dataset:  Dataset{ManifestURI: "file:///data/other-manifest.json"},
		},
	}
	for name, conflictingSubmission := range conflictingSubmissions {
		t.Run("conflicting "+name, func(t *testing.T) {
			if _, _, err := restartedRepository.Create(context.Background(), key, conflictingSubmission); !errors.Is(err, ErrIdempotencyConflict) {
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
		Workload: Workload{Image: "mill/example:dev", Args: []string{"--mode", "fast"}},
		Dataset:  Dataset{ManifestURI: "file:///data/manifest.json"},
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
			job, created, err := repository.Create(context.Background(), key, submission)
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

func TestRepositoryMaterializeAndReportProgress(t *testing.T) {
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
		Workload: Workload{Image: "mill/example:dev"},
		Dataset:  Dataset{ManifestURI: "file:///data/manifest.json"},
	})
	if err != nil {
		t.Fatalf("create job: %v", err)
	}
	manifest := Manifest{
		Version: manifestVersion,
		SHA256:  strings.Repeat("a", 64),
		Shards: []ManifestShard{
			{URI: "file:///data/shard-000.json"},
			{URI: "file:///data/shard-001.json"},
			{URI: "file:///data/shard-002.json"},
		},
	}

	materializedJob, err := repository.Materialize(context.Background(), createdJob.ID, manifest)
	if err != nil {
		t.Fatalf("materialize tasks: %v", err)
	}
	if materializedJob.State != StateRunning {
		t.Errorf("state = %q, want %q", materializedJob.State, StateRunning)
	}
	if materializedJob.Dataset.ManifestSHA256 != manifest.SHA256 {
		t.Errorf("manifest SHA-256 = %q, want %q", materializedJob.Dataset.ManifestSHA256, manifest.SHA256)
	}
	if materializedJob.Progress != (Progress{Total: 3, Pending: 3}) {
		t.Errorf("progress = %+v, want total=3 pending=3", materializedJob.Progress)
	}

	rows, err := pool.Query(context.Background(), `
		SELECT id::text, shard_index, input_uri, output_uri, state
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
		var id, inputURI, outputURI, state string
		var shardIndex int
		if err := rows.Scan(&id, &shardIndex, &inputURI, &outputURI, &state); err != nil {
			t.Fatalf("scan task: %v", err)
		}
		if len(id) != 36 || id[14] != '7' {
			t.Errorf("task ID = %q, want a UUIDv7", id)
		}
		if shardIndex != index {
			t.Errorf("shard index = %d, want %d", shardIndex, index)
		}
		if inputURI != manifest.Shards[index].URI {
			t.Errorf("input URI = %q, want %q", inputURI, manifest.Shards[index].URI)
		}
		wantOutputURI := materializedJob.Output.URI + "tasks/" + strconv.Itoa(index) + "/"
		if outputURI != wantOutputURI {
			t.Errorf("output URI = %q, want %q", outputURI, wantOutputURI)
		}
		if state != "pending" {
			t.Errorf("task state = %q, want pending", state)
		}
		index++
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate tasks: %v", err)
	}
	if index != len(manifest.Shards) {
		t.Fatalf("task count = %d, want %d", index, len(manifest.Shards))
	}

	replayedJob, err := repository.Materialize(context.Background(), createdJob.ID, manifest)
	if err != nil {
		t.Fatalf("replay materialization: %v", err)
	}
	if replayedJob.Progress != materializedJob.Progress {
		t.Errorf("replayed progress = %+v, want %+v", replayedJob.Progress, materializedJob.Progress)
	}

	changedManifest := manifest
	changedManifest.SHA256 = strings.Repeat("b", 64)
	if _, err := repository.Materialize(context.Background(), createdJob.ID, changedManifest); !errors.Is(err, ErrManifestConflict) {
		t.Fatalf("changed manifest error = %v, want %v", err, ErrManifestConflict)
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

	var jobsTable, tasksTable *string
	if err := pool.QueryRow(ctx, `
		SELECT to_regclass('public.jobs')::text, to_regclass('public.tasks')::text
	`).Scan(&jobsTable, &tasksTable); err != nil {
		pool.Close()
		t.Fatalf("check database migrations: %v", err)
	}
	if jobsTable == nil || tasksTable == nil {
		pool.Close()
		t.Fatal("required tables do not exist; apply all numbered migrations to the test database")
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
