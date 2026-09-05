package job

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func TestAttemptSuccessfulLifecycle(t *testing.T) {
	repository, job := createAttemptTestJob(t, "integration:attempt-success", 1, 1)

	claimed, err := repository.ClaimNextAttempt(context.Background(), "docker")
	if err != nil {
		t.Fatalf("claim attempt: %v", err)
	}
	if claimed.Attempt.JobID != job.ID || claimed.Attempt.TaskID == "" {
		t.Errorf("claimed identity = job %q task %q, want job %q and a task", claimed.Attempt.JobID, claimed.Attempt.TaskID, job.ID)
	}
	if claimed.Attempt.State != AttemptStateStarting || claimed.Attempt.Number != 1 || claimed.Attempt.Executor != "docker" {
		t.Errorf("attempt = state %q number %d executor %q, want starting attempt 1 on Docker", claimed.Attempt.State, claimed.Attempt.Number, claimed.Attempt.Executor)
	}
	if claimed.Attempt.StartedAt != nil || claimed.Attempt.FinishedAt != nil {
		t.Errorf("starting timestamps = started %v finished %v, want nil", claimed.Attempt.StartedAt, claimed.Attempt.FinishedAt)
	}
	if claimed.ShardIndex != 0 || claimed.InputStartByte != 0 || claimed.InputEndByte <= 0 {
		t.Errorf("claimed shard = index %d range [%d,%d), want shard 0 with a non-empty range", claimed.ShardIndex, claimed.InputStartByte, claimed.InputEndByte)
	}
	wantOutputURI := job.Output.URI + "tasks/0/attempts/" + claimed.Attempt.ID + "/result.jsonl"
	if claimed.OutputURI != wantOutputURI {
		t.Errorf("output URI = %q, want %q", claimed.OutputURI, wantOutputURI)
	}

	progress := getAttemptTestJob(t, repository, job.ID)
	if progress.Progress != (Progress{Total: 1, Running: 1}) {
		t.Errorf("claimed progress = %+v, want one running task", progress.Progress)
	}
	if _, err := repository.ClaimNextAttempt(context.Background(), "docker"); !errors.Is(err, ErrNoTaskAvailable) {
		t.Fatalf("second claim error = %v, want %v", err, ErrNoTaskAvailable)
	}

	running, err := repository.MarkAttemptRunning(context.Background(), claimed.Attempt.ID, "container-001")
	if err != nil {
		t.Fatalf("mark attempt running: %v", err)
	}
	if running.State != AttemptStateRunning || running.ExternalID != "container-001" || running.StartedAt == nil || running.FinishedAt != nil {
		t.Errorf("running attempt = %+v", running)
	}
	if _, err := repository.MarkAttemptRunning(context.Background(), running.ID, "container-001"); err != nil {
		t.Fatalf("replay running transition: %v", err)
	}
	if _, err := repository.MarkAttemptRunning(context.Background(), running.ID, "different-container"); !errors.Is(err, ErrInvalidAttemptTransition) {
		t.Fatalf("changed external ID error = %v, want %v", err, ErrInvalidAttemptTransition)
	}

	completed, err := repository.CompleteAttempt(context.Background(), running.ID)
	if err != nil {
		t.Fatalf("complete attempt: %v", err)
	}
	if completed.State != AttemptStateCompleted || completed.StartedAt == nil || completed.FinishedAt == nil {
		t.Errorf("completed attempt = %+v", completed)
	}
	if _, err := repository.CompleteAttempt(context.Background(), completed.ID); err != nil {
		t.Fatalf("replay completed transition: %v", err)
	}
	if _, err := repository.FailAttempt(context.Background(), completed.ID, "too late"); !errors.Is(err, ErrInvalidAttemptTransition) {
		t.Fatalf("completed-to-failed error = %v, want %v", err, ErrInvalidAttemptTransition)
	}

	finishedJob := getAttemptTestJob(t, repository, job.ID)
	if finishedJob.State != StateCompleted || finishedJob.Progress != (Progress{Total: 1, Completed: 1}) {
		t.Errorf("finished job = state %q progress %+v, want completed", finishedJob.State, finishedJob.Progress)
	}
}

func TestAttemptCanFailBeforeExternalExecutionStarts(t *testing.T) {
	repository, job := createAttemptTestJob(t, "integration:attempt-start-failure", 1, 1)
	claimed, err := repository.ClaimNextAttempt(context.Background(), "docker")
	if err != nil {
		t.Fatalf("claim attempt: %v", err)
	}

	failed, err := repository.FailAttempt(context.Background(), claimed.Attempt.ID, "Docker create failed")
	if err != nil {
		t.Fatalf("fail starting attempt: %v", err)
	}
	if failed.State != AttemptStateFailed || failed.ExternalID != "" || failed.StartedAt != nil || failed.FinishedAt == nil {
		t.Errorf("failed attempt = %+v", failed)
	}
	if failed.FailureMessage != "Docker create failed" {
		t.Errorf("failure message = %q, want Docker create failed", failed.FailureMessage)
	}

	failedJob := getAttemptTestJob(t, repository, job.ID)
	if failedJob.State != StateRunning || failedJob.Progress != (Progress{Total: 1, Pending: 1}) {
		t.Errorf("job after attempt failure = state %q progress %+v, want running with pending retry", failedJob.State, failedJob.Progress)
	}
	if _, err := repository.MarkAttemptRunning(context.Background(), failed.ID, "container-001"); !errors.Is(err, ErrInvalidAttemptTransition) {
		t.Fatalf("failed-to-running error = %v, want %v", err, ErrInvalidAttemptTransition)
	}
}

func TestAttemptClaimsRespectJobParallelism(t *testing.T) {
	repository, _ := createAttemptTestJob(t, "integration:attempt-parallelism", 5, 2)

	first, err := repository.ClaimNextAttempt(context.Background(), "docker")
	if err != nil {
		t.Fatalf("claim first attempt: %v", err)
	}
	second, err := repository.ClaimNextAttempt(context.Background(), "docker")
	if err != nil {
		t.Fatalf("claim second attempt: %v", err)
	}
	if first.ShardIndex != 0 || second.ShardIndex != 1 {
		t.Errorf("claimed shard indexes = %d, %d, want 0, 1", first.ShardIndex, second.ShardIndex)
	}
	if _, err := repository.ClaimNextAttempt(context.Background(), "docker"); !errors.Is(err, ErrNoTaskAvailable) {
		t.Fatalf("claim beyond parallelism error = %v, want %v", err, ErrNoTaskAvailable)
	}

	if _, err := repository.MarkAttemptRunning(context.Background(), first.Attempt.ID, "container-001"); err != nil {
		t.Fatalf("mark first attempt running: %v", err)
	}
	if _, err := repository.CompleteAttempt(context.Background(), first.Attempt.ID); err != nil {
		t.Fatalf("complete first attempt: %v", err)
	}
	third, err := repository.ClaimNextAttempt(context.Background(), "docker")
	if err != nil {
		t.Fatalf("claim after capacity is released: %v", err)
	}
	if third.ShardIndex != 2 {
		t.Errorf("third shard index = %d, want 2", third.ShardIndex)
	}
}

func TestConcurrentAttemptCompletionFinalizesJob(t *testing.T) {
	repository, job := createAttemptTestJob(t, "integration:attempt-concurrent-completion", 2, 2)
	first, err := repository.ClaimNextAttempt(context.Background(), "docker")
	if err != nil {
		t.Fatalf("claim first attempt: %v", err)
	}
	second, err := repository.ClaimNextAttempt(context.Background(), "docker")
	if err != nil {
		t.Fatalf("claim second attempt: %v", err)
	}
	if _, err := repository.MarkAttemptRunning(context.Background(), first.Attempt.ID, "container-001"); err != nil {
		t.Fatalf("mark first attempt running: %v", err)
	}
	if _, err := repository.MarkAttemptRunning(context.Background(), second.Attempt.ID, "container-002"); err != nil {
		t.Fatalf("mark second attempt running: %v", err)
	}

	start := make(chan struct{})
	errorsByAttempt := make(chan error, 2)
	var completed sync.WaitGroup
	for _, attemptID := range []string{first.Attempt.ID, second.Attempt.ID} {
		completed.Add(1)
		go func() {
			defer completed.Done()
			<-start
			_, err := repository.CompleteAttempt(context.Background(), attemptID)
			errorsByAttempt <- err
		}()
	}
	close(start)
	completed.Wait()
	close(errorsByAttempt)
	for err := range errorsByAttempt {
		if err != nil {
			t.Fatalf("complete attempt concurrently: %v", err)
		}
	}

	finishedJob := getAttemptTestJob(t, repository, job.ID)
	if finishedJob.State != StateCompleted || finishedJob.Progress != (Progress{Total: 2, Completed: 2}) {
		t.Errorf("finished job = state %q progress %+v, want two completed tasks", finishedJob.State, finishedJob.Progress)
	}
}

func createAttemptTestJob(t *testing.T, key string, records, parallelism int) (*Repository, Job) {
	t.Helper()
	pool := openIntegrationDatabase(t, integrationDatabaseURL(t))
	t.Cleanup(pool.Close)
	deleteJobByKey(t, pool, key)
	t.Cleanup(func() { deleteJobByKey(t, pool, key) })

	inputFilename := filepath.Join(t.TempDir(), "records.jsonl")
	writeTestJSONL(t, inputFilename, records)
	repository, err := NewRepository(pool, "file:///tmp/mill-attempt-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	service, err := NewService(repository, JSONLPartitioner{}, parallelism)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	createdJob, created, err := service.Create(context.Background(), key, Submission{
		Executable: Executable{Image: "mill/jsonl-copy:dev"},
		Input:      InputSpec{URI: fileURI(inputFilename)},
	})
	if err != nil {
		t.Fatalf("create materialized job: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}
	return repository, createdJob
}

func getAttemptTestJob(t *testing.T, repository *Repository, jobID string) Job {
	t.Helper()
	job, err := repository.Get(context.Background(), jobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	return job
}
