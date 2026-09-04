package job

import (
	"context"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestServiceMaterializesAndReplaysWithoutManifestFile(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	pool := openIntegrationDatabase(t, databaseURL)
	defer pool.Close()

	key := "integration:service-materialize"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)

	manifestFilename := filepath.Join(t.TempDir(), "manifest.json")
	writeTestManifest(t, manifestFilename)
	submission := Submission{
		Workload: Workload{Image: "mill/example:dev"},
		Dataset:  Dataset{ManifestURI: fileURI(manifestFilename)},
	}

	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	service, err := NewService(repository, FileManifestLoader{})
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	createdJob, created, err := service.Create(context.Background(), key, submission)
	if err != nil {
		t.Fatalf("create materialized job: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}
	if createdJob.State != StateRunning || createdJob.Progress != (Progress{Total: 2, Pending: 2}) {
		t.Fatalf("created job = state %q progress %+v, want running with 2 pending", createdJob.State, createdJob.Progress)
	}

	if err := os.Remove(manifestFilename); err != nil {
		t.Fatalf("remove test manifest: %v", err)
	}
	replayedJob, replayCreated, err := service.Create(context.Background(), key, submission)
	if err != nil {
		t.Fatalf("replay without manifest file: %v", err)
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

	key := "integration:service-concurrent-materialize"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)

	manifestFilename := filepath.Join(t.TempDir(), "manifest.json")
	writeTestManifest(t, manifestFilename)
	submission := Submission{
		Workload: Workload{Image: "mill/example:dev"},
		Dataset:  Dataset{ManifestURI: fileURI(manifestFilename)},
	}
	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	service, err := NewService(repository, FileManifestLoader{})
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
		if result.job.ID != jobID || result.job.Progress != (Progress{Total: 2, Pending: 2}) {
			t.Errorf("job = ID %q progress %+v, want shared ID %q and 2 pending", result.job.ID, result.job.Progress, jobID)
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
	if taskCount != 2 {
		t.Errorf("task count = %d, want 2", taskCount)
	}
}

func TestServiceRejectsInvalidManifestBeforeCreatingJob(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	pool := openIntegrationDatabase(t, databaseURL)
	defer pool.Close()

	key := "integration:service-invalid-manifest"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)

	manifestFilename := filepath.Join(t.TempDir(), "manifest.json")
	if err := os.WriteFile(manifestFilename, []byte(`{"version":1,"shards":[]}`), 0o600); err != nil {
		t.Fatalf("write invalid manifest: %v", err)
	}
	submission := Submission{
		Workload: Workload{Image: "mill/example:dev"},
		Dataset:  Dataset{ManifestURI: fileURI(manifestFilename)},
	}

	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	service, err := NewService(repository, FileManifestLoader{})
	if err != nil {
		t.Fatalf("create service: %v", err)
	}
	_, _, err = service.Create(context.Background(), key, submission)
	var validationError *ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("invalid manifest error = %v, want ValidationError", err)
	}

	var jobCount int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM public.jobs WHERE idempotency_key = $1
	`, key).Scan(&jobCount); err != nil {
		t.Fatalf("count jobs after invalid manifest: %v", err)
	}
	if jobCount != 0 {
		t.Fatalf("job count = %d after invalid manifest, want 0", jobCount)
	}
}

func TestServiceResumesPreparingJobAfterRestart(t *testing.T) {
	databaseURL := integrationDatabaseURL(t)
	pool := openIntegrationDatabase(t, databaseURL)
	defer pool.Close()

	key := "integration:service-resume-preparing"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)

	manifestFilename := filepath.Join(t.TempDir(), "manifest.json")
	writeTestManifest(t, manifestFilename)
	submission := Submission{
		Workload: Workload{Image: "mill/example:dev"},
		Dataset:  Dataset{ManifestURI: fileURI(manifestFilename)},
	}

	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	preparingJob, created, err := repository.Create(context.Background(), key, submission)
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
	restartedService, err := NewService(restartedRepository, FileManifestLoader{})
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
	wantOutputURI := "file:///tmp/mill-output/jobs/" + preparingJob.ID + "/"
	if recoveredJob.Output.URI != wantOutputURI {
		t.Errorf("recovered output URI = %q, want original %q", recoveredJob.Output.URI, wantOutputURI)
	}
}

func writeTestManifest(t *testing.T, filename string) {
	t.Helper()
	contents := []byte(`{
		"version": 1,
		"shards": [
			{"uri": "file:///data/shard-000.json"},
			{"uri": "file:///data/shard-001.json"}
		]
	}`)
	if err := os.WriteFile(filename, contents, 0o600); err != nil {
		t.Fatalf("write test manifest: %v", err)
	}
}

func fileURI(filename string) string {
	return (&url.URL{Scheme: "file", Path: filename}).String()
}
