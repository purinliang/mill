// This file tests submission across Job, partition, store, and PostgreSQL.
package postgres_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/purinliang/mill/internal/job"
	"github.com/purinliang/mill/internal/job/partition"
	jobpostgres "github.com/purinliang/mill/internal/job/postgres"
	"github.com/purinliang/mill/internal/objectstore"
)

func TestServicePartitionsInputAndReplaysSubmission(t *testing.T) {
	pool := openIntegrationDatabase(t)
	key := "integration:service-partition"
	deleteJobByKey(t, pool, key)
	t.Cleanup(func() { deleteJobByKey(t, pool, key) })

	inputFilename := filepath.Join(t.TempDir(), "records.jsonl")
	writeTestJSONL(t, inputFilename, 100)
	submission := job.Submission{
		Executable: job.Executable{Image: "mill/example:dev"},
		Input:      job.InputSpec{URI: fileURI(inputFilename)},
	}
	service := newSubmissionService(t, pool, 3)

	createdJob, created, err := service.Create(
		context.Background(), key, submission,
	)
	if err != nil || !created {
		t.Fatalf("create = %+v, created %t, error %v", createdJob, created, err)
	}
	wantProgress := job.Progress{Total: 12, Pending: 12}
	if createdJob.State != job.StateRunning ||
		createdJob.Progress != wantProgress {
		t.Fatalf("created job = %+v, want running with 12 pending", createdJob)
	}
	if createdJob.Input.RecordCount != 100 ||
		createdJob.Input.SHA256 == "" ||
		createdJob.Parallelism != 3 {
		t.Errorf("created input or parallelism = %+v", createdJob)
	}

	if err := os.Remove(inputFilename); err != nil {
		t.Fatal(err)
	}
	replayedJob, replayCreated, err := service.Create(
		context.Background(), key, submission,
	)
	if err != nil || replayCreated || replayedJob.ID != createdJob.ID {
		t.Fatalf(
			"replay = %+v, created %t, error %v",
			replayedJob,
			replayCreated,
			err,
		)
	}
}

func TestServiceConcurrentSubmissionCreatesOneTaskSet(t *testing.T) {
	pool := openIntegrationDatabase(t)
	key := "integration:service-concurrent-partition"
	deleteJobByKey(t, pool, key)
	t.Cleanup(func() { deleteJobByKey(t, pool, key) })
	inputFilename := filepath.Join(t.TempDir(), "records.jsonl")
	writeTestJSONL(t, inputFilename, 100)
	submission := job.Submission{
		Executable: job.Executable{Image: "mill/example:dev"},
		Input:      job.InputSpec{URI: fileURI(inputFilename)},
	}
	service := newSubmissionService(t, pool, 3)

	const callers = 8
	start := make(chan struct{})
	results := make(chan serviceCreateResult, callers)
	var callersDone sync.WaitGroup
	callersDone.Add(callers)
	for range callers {
		go func() {
			defer callersDone.Done()
			<-start
			value, created, err := service.Create(
				context.Background(), key, submission,
			)
			results <- serviceCreateResult{
				job: value, created: created, err: err,
			}
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
		wantProgress := job.Progress{Total: 12, Pending: 12}
		if result.job.ID != jobID || result.job.Progress != wantProgress {
			t.Errorf("concurrent job = %+v, shared ID = %q", result.job, jobID)
		}
	}
	if createdCount != 1 {
		t.Errorf("created count = %d, want 1", createdCount)
	}

	var taskCount int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM public.tasks WHERE job_id = $1::uuid
	`, jobID).Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if taskCount != 12 {
		t.Errorf("task count = %d, want 12", taskCount)
	}
}

func TestServiceRejectsInvalidInputBeforePersistence(t *testing.T) {
	pool := openIntegrationDatabase(t)
	key := "integration:service-invalid-input"
	deleteJobByKey(t, pool, key)
	t.Cleanup(func() { deleteJobByKey(t, pool, key) })
	inputFilename := filepath.Join(t.TempDir(), "invalid.jsonl")
	if err := os.WriteFile(inputFilename, []byte("{\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	submission := job.Submission{
		Executable: job.Executable{Image: "mill/example:dev"},
		Input:      job.InputSpec{URI: fileURI(inputFilename)},
	}
	service := newSubmissionService(t, pool, 3)

	_, _, err := service.Create(context.Background(), key, submission)
	var validationError *job.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("invalid input error = %v, want ValidationError", err)
	}

	var jobCount int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM public.jobs WHERE idempotency_key = $1
	`, key).Scan(&jobCount); err != nil {
		t.Fatal(err)
	}
	if jobCount != 0 {
		t.Fatalf("job count = %d, want 0", jobCount)
	}
}

func TestServiceResumesPreparingJobWithStoredPolicy(t *testing.T) {
	pool := openIntegrationDatabase(t)
	key := "integration:service-resume-preparing"
	deleteJobByKey(t, pool, key)
	t.Cleanup(func() { deleteJobByKey(t, pool, key) })
	inputFilename := filepath.Join(t.TempDir(), "records.jsonl")
	writeTestJSONL(t, inputFilename, 100)
	submission := job.Submission{
		Executable: job.Executable{Image: "mill/example:dev"},
		Input:      job.InputSpec{URI: fileURI(inputFilename)},
	}
	partitioner := partition.New(&objectstore.Store{})
	shards, err := partitioner.Partition(
		context.Background(), submission.Input.URI, 3,
	)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := jobpostgres.NewRepository(
		pool,
		"file:///tmp/mill-output",
	)
	if err != nil {
		t.Fatal(err)
	}
	preparingJob, created, err := repository.Create(
		context.Background(),
		key,
		submission,
		shards.InputSHA256,
		shards.RecordCount,
		3,
		integrationResources,
	)
	if err != nil || !created || preparingJob.State != job.StatePreparing {
		t.Fatalf(
			"preparing job = %+v, created %t, error %v",
			preparingJob,
			created,
			err,
		)
	}

	restartedRepository, err := jobpostgres.NewRepository(
		pool,
		"file:///tmp/changed-output-root",
	)
	if err != nil {
		t.Fatal(err)
	}
	restartedService, err := job.NewService(
		restartedRepository,
		partitioner,
		9,
	)
	if err != nil {
		t.Fatal(err)
	}
	recoveredJob, retryCreated, err := restartedService.Create(
		context.Background(), key, submission,
	)
	if err != nil || retryCreated {
		t.Fatalf(
			"recovered job = %+v, created %t, error %v",
			recoveredJob,
			retryCreated,
			err,
		)
	}
	if recoveredJob.ID != preparingJob.ID ||
		recoveredJob.State != job.StateRunning ||
		recoveredJob.Parallelism != 3 ||
		recoveredJob.Progress.Total != 12 {
		t.Fatalf("recovered job = %+v", recoveredJob)
	}
	wantOutputURI := "file:///tmp/mill-output/jobs/" + preparingJob.ID + "/"
	if recoveredJob.Output.URI != wantOutputURI {
		t.Errorf("output URI = %q, want %q", recoveredJob.Output.URI, wantOutputURI)
	}
}

func newSubmissionService(
	t *testing.T,
	pool *pgxpool.Pool,
	parallelism int,
) *job.Service {
	t.Helper()
	repository, err := jobpostgres.NewRepository(
		pool,
		"file:///tmp/mill-output",
	)
	if err != nil {
		t.Fatal(err)
	}
	service, err := job.NewService(
		repository,
		partition.New(&objectstore.Store{}),
		parallelism,
	)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type serviceCreateResult struct {
	job     job.Job
	created bool
	err     error
}
