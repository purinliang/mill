package job

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestServicePlansLogicalShardsAndReplaysWithoutInputFile(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	pool := openIntegrationDatabase(t, databaseURL)
	defer pool.Close()

	key := "integration:service-plan"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)

	inputFilename := filepath.Join(t.TempDir(), "records.jsonl")
	writeTestJSONL(t, inputFilename, 100)
	submission := Submission{
		Executable: Executable{Image: "mill/example:dev"},
		Input:      InputSpec{URI: fileURI(inputFilename)},
	}
	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	service, err := NewService(repository, JSONLPartitioner{}, 3)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	createdJob, created, err := service.Create(context.Background(), key, submission)
	if err != nil {
		t.Fatalf("create planned job: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}
	if createdJob.State != StateRunning || createdJob.Progress != (Progress{Total: 12, Pending: 12}) {
		t.Fatalf("created job = state %q progress %+v, want running with 12 pending", createdJob.State, createdJob.Progress)
	}
	if createdJob.Input.RecordCount != 100 || createdJob.Input.SHA256 == "" || createdJob.Parallelism != 3 {
		t.Errorf("input records = %d SHA = %q parallelism = %d, want 100, non-empty, 3", createdJob.Input.RecordCount, createdJob.Input.SHA256, createdJob.Parallelism)
	}

	if err := os.Remove(inputFilename); err != nil {
		t.Fatalf("remove test input: %v", err)
	}
	replayedJob, replayCreated, err := service.Create(context.Background(), key, submission)
	if err != nil {
		t.Fatalf("replay without input file: %v", err)
	}
	if replayCreated {
		t.Fatal("created = true on replay, want false")
	}
	if replayedJob.ID != createdJob.ID {
		t.Errorf("replayed job ID = %q, want %q", replayedJob.ID, createdJob.ID)
	}
}

func TestServiceConcurrentCreateMaterializesOneTaskSet(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	pool := openIntegrationDatabase(t, databaseURL)
	defer pool.Close()

	key := "integration:service-concurrent-plan"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)
	inputFilename := filepath.Join(t.TempDir(), "records.jsonl")
	writeTestJSONL(t, inputFilename, 100)
	submission := Submission{
		Executable: Executable{Image: "mill/example:dev"},
		Input:      InputSpec{URI: fileURI(inputFilename)},
	}
	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	service, err := NewService(repository, JSONLPartitioner{}, 3)
	if err != nil {
		t.Fatalf("create service: %v", err)
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
			job, created, err := service.Create(context.Background(), key, submission)
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
		if result.job.ID != jobID || result.job.Progress != (Progress{Total: 12, Pending: 12}) {
			t.Errorf("job = ID %q progress %+v, want shared ID %q and 12 pending", result.job.ID, result.job.Progress, jobID)
		}
	}
	if createdCount != 1 {
		t.Errorf("created count = %d, want 1", createdCount)
	}

	var taskCount int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM public.tasks WHERE job_id = $1::uuid
	`, jobID).Scan(&taskCount); err != nil {
		t.Fatalf("count materialized tasks: %v", err)
	}
	if taskCount != 12 {
		t.Errorf("task count = %d, want 12", taskCount)
	}
}

func TestServiceRejectsInvalidInputBeforeCreatingJob(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	pool := openIntegrationDatabase(t, databaseURL)
	defer pool.Close()

	key := "integration:service-invalid-input"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)
	inputFilename := filepath.Join(t.TempDir(), "invalid.jsonl")
	if err := os.WriteFile(inputFilename, []byte("{\n"), 0o600); err != nil {
		t.Fatalf("write invalid input: %v", err)
	}
	submission := Submission{
		Executable: Executable{Image: "mill/example:dev"},
		Input:      InputSpec{URI: fileURI(inputFilename)},
	}
	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	service, err := NewService(repository, JSONLPartitioner{}, 3)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	_, _, err = service.Create(context.Background(), key, submission)
	var validationError *ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("invalid input error = %v, want ValidationError", err)
	}

	var jobCount int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM public.jobs WHERE idempotency_key = $1
	`, key).Scan(&jobCount); err != nil {
		t.Fatalf("count jobs after invalid input: %v", err)
	}
	if jobCount != 0 {
		t.Fatalf("job count = %d after invalid input, want 0", jobCount)
	}
}

func TestServiceResumesPreparingJobWithStoredParallelism(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	pool := openIntegrationDatabase(t, databaseURL)
	defer pool.Close()

	key := "integration:service-resume-preparing"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)
	inputFilename := filepath.Join(t.TempDir(), "records.jsonl")
	writeTestJSONL(t, inputFilename, 100)
	submission := Submission{
		Executable: Executable{Image: "mill/example:dev"},
		Input:      InputSpec{URI: fileURI(inputFilename)},
	}
	plan, err := (JSONLPartitioner{}).Plan(context.Background(), submission.Input.URI, 3)
	if err != nil {
		t.Fatalf("plan input: %v", err)
	}
	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	preparingJob, created, err := repository.Create(
		context.Background(), key, submission, plan.InputSHA256, plan.RecordCount, 3,
	)
	if err != nil {
		t.Fatalf("create preparing job: %v", err)
	}
	if !created || preparingJob.State != StatePreparing {
		t.Fatalf("created = %t state = %q, want created preparing job", created, preparingJob.State)
	}

	restartedRepository, err := NewRepository(pool, "file:///tmp/changed-output-root")
	if err != nil {
		t.Fatalf("create restarted repository: %v", err)
	}
	restartedService, err := NewService(restartedRepository, JSONLPartitioner{}, 9)
	if err != nil {
		t.Fatalf("create restarted service: %v", err)
	}
	recoveredJob, retryCreated, err := restartedService.Create(context.Background(), key, submission)
	if err != nil {
		t.Fatalf("resume preparing job: %v", err)
	}
	if retryCreated {
		t.Fatal("created = true on recovery, want false")
	}
	if recoveredJob.ID != preparingJob.ID || recoveredJob.State != StateRunning {
		t.Fatalf("recovered job = ID %q state %q, want ID %q state running", recoveredJob.ID, recoveredJob.State, preparingJob.ID)
	}
	if recoveredJob.Parallelism != 3 || recoveredJob.Progress.Total != 12 {
		t.Errorf("recovered parallelism = %d tasks = %d, want stored parallelism 3 and 12 tasks", recoveredJob.Parallelism, recoveredJob.Progress.Total)
	}
	wantOutputURI := "file:///tmp/mill-output/jobs/" + preparingJob.ID + "/"
	if recoveredJob.Output.URI != wantOutputURI {
		t.Errorf("recovered output URI = %q, want original %q", recoveredJob.Output.URI, wantOutputURI)
	}
}

func writeTestJSONL(t *testing.T, filename string, records int) {
	t.Helper()
	var contents strings.Builder
	for index := range records {
		fmt.Fprintf(&contents, `{"record":%d}`+"\n", index)
	}
	if err := os.WriteFile(filename, []byte(contents.String()), 0o600); err != nil {
		t.Fatalf("write test JSONL: %v", err)
	}
}
