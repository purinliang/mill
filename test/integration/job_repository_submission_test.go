// This file tests durable and idempotent submissions against PostgreSQL.
package integration_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	. "github.com/purinliang/mill/internal/job"
	. "github.com/purinliang/mill/internal/job/postgres"
)

func TestRepositoryCreateReplayGetAndPersist(t *testing.T) {
	pool := openIntegrationDatabase(t)
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

	createdJob, created, err := repository.Create(
		context.Background(),
		key,
		submission,
		integrationInputSHA256,
		100,
		3,
		integrationResources,
	)
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
	if createdJob.Input.SHA256 != integrationInputSHA256 ||
		createdJob.Input.RecordCount != 100 {
		t.Errorf(
			"input identity = SHA %q records %d; want SHA %q records 100",
			createdJob.Input.SHA256,
			createdJob.Input.RecordCount,
			integrationInputSHA256,
		)
	}
	if createdJob.Parallelism != 3 {
		t.Errorf("parallelism = %d, want 3", createdJob.Parallelism)
	}
	if createdJob.ResourceClass != ResourceClassSmall ||
		createdJob.Resources.MemoryRequestBytes != 128<<20 ||
		createdJob.Resources.MemoryLimitBytes != 128<<20 {
		t.Errorf(
			"created resources = class %q %+v",
			createdJob.ResourceClass,
			createdJob.Resources,
		)
	}
	if createdJob.Executable.Args == nil {
		t.Error("executable args are nil, want an empty array")
	}

	pool.Close()
	pool = openIntegrationDatabase(t)
	defer deleteJobByKey(t, pool, key)

	restartedRepository, err := NewRepository(pool, "file:///tmp/mill-output-b")
	if err != nil {
		t.Fatalf("create restarted repository: %v", err)
	}
	persistedJob, err := restartedRepository.Get(
		context.Background(),
		createdJob.ID,
	)
	if err != nil {
		t.Fatalf("get persisted job: %v", err)
	}
	if persistedJob.ID != createdJob.ID {
		t.Errorf("persisted ID = %q, want %q", persistedJob.ID, createdJob.ID)
	}

	replayedJob, replayCreated, err := restartedRepository.Create(
		context.Background(), key, submission,
		strings.Repeat("b", 64), 200, 9, integrationResources,
	)
	if err != nil {
		t.Fatalf("replay job: %v", err)
	}
	if replayCreated {
		t.Fatal("created = true on replay, want false")
	}
	if replayedJob.ID != createdJob.ID ||
		replayedJob.Output.URI != wantOutputURI ||
		replayedJob.Parallelism != 3 {
		t.Errorf(
			"replayed job = ID %q output %q parallelism %d",
			replayedJob.ID,
			replayedJob.Output.URI,
			replayedJob.Parallelism,
		)
	}

	conflictingSubmissions := map[string]Submission{
		"image": {
			Executable: Executable{Image: "mill/other:dev"},
			Input:      submission.Input,
		},
		"arguments": {
			Executable: Executable{
				Image: submission.Executable.Image,
				Args:  []string{"--changed"},
			},
			Input: submission.Input,
		},
		"input": {
			Executable: submission.Executable,
			Input:      InputSpec{URI: "file:///data/other.jsonl"},
		},
		"resource class": {
			Executable:    submission.Executable,
			Input:         submission.Input,
			ResourceClass: ResourceClassLarge,
		},
	}
	for name, conflictingSubmission := range conflictingSubmissions {
		t.Run("conflicting "+name, func(t *testing.T) {
			if _, _, err := restartedRepository.Create(
				context.Background(),
				key,
				conflictingSubmission,
				integrationInputSHA256,
				100,
				3,
				integrationResources,
			); !errors.Is(err, ErrIdempotencyConflict) {
				t.Fatalf(
					"conflicting create error = %v, want %v",
					err,
					ErrIdempotencyConflict,
				)
			}
		})
	}

	if _, err := pool.Exec(
		context.Background(),
		"UPDATE public.jobs SET state = 'invalid' WHERE id = $1::uuid",
		createdJob.ID,
	); err == nil {
		t.Fatal(
			"invalid state update succeeded, want database constraint error",
		)
	}
}

func TestRepositoryFindSubmission(t *testing.T) {
	pool := openIntegrationDatabase(t)
	defer pool.Close()

	key := "integration:find-submission"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)
	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatal(err)
	}
	submission := Submission{
		Executable: Executable{Image: "mill/example:dev"},
		Input:      InputSpec{URI: "file:///data/records.jsonl"},
	}

	value, found, err := repository.FindSubmission(
		context.Background(), key, submission,
	)
	if err != nil || found || value.ID != "" {
		t.Fatalf("initial lookup = %+v, found %t, error %v", value, found, err)
	}
	created, _, err := repository.Create(
		context.Background(),
		key,
		submission,
		integrationInputSHA256,
		1,
		1,
		integrationResources,
	)
	if err != nil {
		t.Fatal(err)
	}
	value, found, err = repository.FindSubmission(
		context.Background(), key, submission,
	)
	if err != nil || !found || value.ID != created.ID {
		t.Fatalf("stored lookup = %+v, found %t, error %v", value, found, err)
	}

	conflict := submission
	conflict.Executable.Image = "mill/other:dev"
	if _, _, err := repository.FindSubmission(
		context.Background(), key, conflict,
	); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting lookup error = %v", err)
	}
}

func TestRepositoryConcurrentIdempotentCreate(t *testing.T) {
	pool := openIntegrationDatabase(t)
	defer pool.Close()

	key := "integration:concurrent-create"
	deleteJobByKey(t, pool, key)
	defer deleteJobByKey(t, pool, key)
	repository, err := NewRepository(pool, "file:///tmp/mill-output")
	if err != nil {
		t.Fatalf("create repository: %v", err)
	}
	submission := Submission{
		Executable: Executable{
			Image: "mill/example:dev",
			Args:  []string{"--mode", "fast"},
		},
		Input: InputSpec{URI: "file:///data/records.jsonl"},
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
			job, created, err := repository.Create(
				context.Background(),
				key,
				submission,
				integrationInputSHA256,
				100,
				3,
				integrationResources,
			)
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
			t.Errorf(
				"job ID = %q, want shared ID %q",
				result.job.ID,
				jobID,
			)
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want 1", createdCount)
	}
}

type createResult struct {
	job     Job
	created bool
	err     error
}
