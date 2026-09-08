package job_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/purinliang/mill/internal/job"
)

const (
	publicTestSHA256 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	missingUUID      = "00000000-0000-7000-8000-000000000001"
)

func TestPublicRepositoryValidationContracts(t *testing.T) {
	repository, pool, _ := newPublicRepository(t)
	validSubmission := publicSubmission()
	validPlan := publicPlan()

	tests := map[string]func() error{
		"find missing idempotency key": func() error {
			_, _, err := repository.FindSubmission(context.Background(), "", validSubmission)
			return err
		},
		"find invalid submission": func() error {
			_, _, err := repository.FindSubmission(context.Background(), "public-validation", job.Submission{})
			return err
		},
		"find input with surrounding whitespace": func() error {
			submission := validSubmission
			submission.Input.URI = " file:///tmp/records.jsonl"
			_, _, err := repository.FindSubmission(context.Background(), "public-validation", submission)
			return err
		},
		"find input with credentials": func() error {
			submission := validSubmission
			submission.Input.URI = "s3://user@bucket/records.jsonl"
			_, _, err := repository.FindSubmission(context.Background(), "public-validation", submission)
			return err
		},
		"find input without S3 bucket": func() error {
			submission := validSubmission
			submission.Input.URI = "s3:///records.jsonl"
			_, _, err := repository.FindSubmission(context.Background(), "public-validation", submission)
			return err
		},
		"find input without S3 object": func() error {
			submission := validSubmission
			submission.Input.URI = "s3://bucket"
			_, _, err := repository.FindSubmission(context.Background(), "public-validation", submission)
			return err
		},
		"create missing idempotency key": func() error {
			_, _, err := repository.Create(context.Background(), "", validSubmission, publicTestSHA256, 1, 1)
			return err
		},
		"create invalid submission": func() error {
			_, _, err := repository.Create(context.Background(), "public-validation", job.Submission{}, publicTestSHA256, 1, 1)
			return err
		},
		"create invalid input digest": func() error {
			_, _, err := repository.Create(context.Background(), "public-validation", validSubmission, "not-a-digest", 1, 1)
			return err
		},
		"create empty input": func() error {
			_, _, err := repository.Create(context.Background(), "public-validation", validSubmission, publicTestSHA256, 0, 1)
			return err
		},
		"create invalid parallelism": func() error {
			_, _, err := repository.Create(context.Background(), "public-validation", validSubmission, publicTestSHA256, 1, 0)
			return err
		},
		"materialize invalid job ID": func() error {
			_, err := repository.Materialize(context.Background(), "not-a-uuid", validPlan)
			return err
		},
		"materialize invalid digest": func() error {
			plan := validPlan
			plan.InputSHA256 = "invalid"
			_, err := repository.Materialize(context.Background(), missingUUID, plan)
			return err
		},
		"materialize no shards": func() error {
			plan := validPlan
			plan.Shards = nil
			_, err := repository.Materialize(context.Background(), missingUUID, plan)
			return err
		},
		"materialize discontinuous shards": func() error {
			plan := validPlan
			plan.Shards = []job.LogicalShard{{StartByte: 1, EndByte: 2}}
			_, err := repository.Materialize(context.Background(), missingUUID, plan)
			return err
		},
		"get invalid job ID": func() error {
			_, err := repository.Get(context.Background(), "not-a-uuid")
			return err
		},
		"claim invalid executor": func() error {
			_, err := repository.ClaimNextAttempt(context.Background(), " ", "owner", 30*time.Second)
			return err
		},
		"claim invalid lease owner": func() error {
			_, err := repository.ClaimNextAttempt(context.Background(), "kubernetes", "", 30*time.Second)
			return err
		},
		"claim invalid lease duration": func() error {
			_, err := repository.ClaimNextAttempt(context.Background(), "kubernetes", "owner", 1500*time.Millisecond)
			return err
		},
		"get invalid attempt ID": func() error {
			_, err := repository.GetAttempt(context.Background(), "not-a-uuid")
			return err
		},
		"start invalid attempt ID": func() error {
			_, err := repository.MarkAttemptRunning(context.Background(), "not-a-uuid", missingUUID, "job-uid")
			return err
		},
		"start invalid lease token": func() error {
			_, err := repository.MarkAttemptRunning(context.Background(), missingUUID, "not-a-uuid", "job-uid")
			return err
		},
		"start blank external ID": func() error {
			_, err := repository.MarkAttemptRunning(context.Background(), missingUUID, missingUUID, " ")
			return err
		},
		"complete invalid attempt ID": func() error {
			_, err := repository.CompleteAttempt(context.Background(), "not-a-uuid", missingUUID)
			return err
		},
		"complete invalid lease token": func() error {
			_, err := repository.CompleteAttempt(context.Background(), missingUUID, "not-a-uuid")
			return err
		},
		"fail blank message": func() error {
			_, err := repository.FailAttempt(context.Background(), missingUUID, missingUUID, " ")
			return err
		},
		"fail oversized message": func() error {
			_, err := repository.FailAttempt(context.Background(), missingUUID, missingUUID, strings.Repeat("x", 4097))
			return err
		},
		"lease active invalid executor": func() error {
			_, err := repository.LeaseActiveAttempts(context.Background(), "", "owner", 30*time.Second)
			return err
		},
		"lease active invalid lease": func() error {
			_, err := repository.LeaseActiveAttempts(context.Background(), "kubernetes", "owner", 6*time.Minute)
			return err
		},
	}

	for name, operation := range tests {
		t.Run(name, func(t *testing.T) {
			requireInvalidArgument(t, operation())
		})
	}
	if _, err := job.NewRepository(pool, "s3://mill-output"); err != nil {
		t.Fatalf("NewRepository rejected an S3 bucket output root: %v", err)
	}
}

func TestPublicRepositoryNotFoundAndSubmissionLookup(t *testing.T) {
	repository, _, key := newPublicRepository(t)
	ctx := context.Background()
	submission := publicSubmission()

	if foundJob, found, err := repository.FindSubmission(ctx, key, submission); err != nil || found || foundJob.ID != "" {
		t.Fatalf("initial FindSubmission = job %+v found %t error %v", foundJob, found, err)
	}
	created, wasCreated, err := repository.Create(ctx, key, submission, publicTestSHA256, 1, 1)
	if err != nil || !wasCreated {
		t.Fatalf("Create = job %+v created %t error %v", created, wasCreated, err)
	}
	foundJob, found, err := repository.FindSubmission(ctx, key, submission)
	if err != nil || !found || foundJob.ID != created.ID {
		t.Fatalf("FindSubmission = job %+v found %t error %v", foundJob, found, err)
	}
	conflict := submission
	conflict.Executable.Image = "mill/other:dev"
	if _, _, err := repository.FindSubmission(ctx, key, conflict); !errors.Is(err, job.ErrIdempotencyConflict) {
		t.Fatalf("conflicting FindSubmission error = %v", err)
	}

	if _, err := repository.Get(ctx, missingUUID); !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Get missing error = %v", err)
	}
	if _, err := repository.Materialize(ctx, missingUUID, publicPlan()); !errors.Is(err, job.ErrNotFound) {
		t.Fatalf("Materialize missing error = %v", err)
	}
	if _, err := repository.GetAttempt(ctx, missingUUID); !errors.Is(err, job.ErrAttemptNotFound) {
		t.Fatalf("GetAttempt missing error = %v", err)
	}
	if _, err := repository.MarkAttemptRunning(ctx, missingUUID, missingUUID, "job-uid"); !errors.Is(err, job.ErrAttemptNotFound) {
		t.Fatalf("MarkAttemptRunning missing error = %v", err)
	}
	if _, err := repository.CompleteAttempt(ctx, missingUUID, missingUUID); !errors.Is(err, job.ErrAttemptNotFound) {
		t.Fatalf("CompleteAttempt missing error = %v", err)
	}
	if _, err := repository.FailAttempt(ctx, missingUUID, missingUUID, "failed"); !errors.Is(err, job.ErrAttemptNotFound) {
		t.Fatalf("FailAttempt missing error = %v", err)
	}
	results, err := repository.CompletedResults(ctx, missingUUID)
	if err != nil || len(results) != 0 {
		t.Fatalf("CompletedResults missing = %+v, %v", results, err)
	}
}

func TestPublicRepositorySurfacesDatabaseUnavailability(t *testing.T) {
	databaseURL := publicDatabaseURL(t)
	pool, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := job.NewRepository(pool, "file:///tmp/mill-public-output")
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	service, err := job.NewService(repository, job.JSONLPartitioner{}, 1)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()

	ctx := context.Background()
	validSubmission := publicSubmission()
	operations := map[string]func() error{
		"find submission": func() error {
			_, _, err := repository.FindSubmission(ctx, "closed-find", validSubmission)
			return err
		},
		"create": func() error {
			_, _, err := repository.Create(ctx, "closed-create", validSubmission, publicTestSHA256, 1, 1)
			return err
		},
		"materialize": func() error {
			_, err := repository.Materialize(ctx, missingUUID, publicPlan())
			return err
		},
		"get job": func() error {
			_, err := repository.Get(ctx, missingUUID)
			return err
		},
		"claim": func() error {
			_, err := repository.ClaimNextAttempt(ctx, "kubernetes", "owner", 30*time.Second)
			return err
		},
		"get attempt": func() error {
			_, err := repository.GetAttempt(ctx, missingUUID)
			return err
		},
		"start attempt": func() error {
			_, err := repository.MarkAttemptRunning(ctx, missingUUID, missingUUID, "job-uid")
			return err
		},
		"complete attempt": func() error {
			_, err := repository.CompleteAttempt(ctx, missingUUID, missingUUID)
			return err
		},
		"lease active attempts": func() error {
			_, err := repository.LeaseActiveAttempts(ctx, "kubernetes", "owner", 30*time.Second)
			return err
		},
		"list results": func() error {
			_, err := repository.CompletedResults(ctx, missingUUID)
			return err
		},
		"service create": func() error {
			_, _, err := service.Create(ctx, "closed-service-create", validSubmission)
			return err
		},
		"service get": func() error {
			_, err := service.Get(ctx, missingUUID)
			return err
		},
	}
	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if err := operation(); err == nil {
				t.Fatal("operation succeeded with a closed PostgreSQL pool")
			}
		})
	}
}

func TestPublicAttemptLeaseExpiryAndStartingTransition(t *testing.T) {
	repository, _, key := newPublicRepository(t)
	ctx := context.Background()
	created, _, err := repository.Create(ctx, key, publicSubmission(), publicTestSHA256, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Materialize(ctx, created.ID, publicPlan()); err != nil {
		t.Fatal(err)
	}
	claimed, err := repository.ClaimNextAttempt(ctx, "kubernetes", "initial-owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, err := repository.MarkAttemptRunning(ctx, claimed.Attempt.ID, claimed.Attempt.LeaseToken, "job-uid"); !errors.Is(err, job.ErrAttemptLeaseLost) {
		t.Fatalf("MarkAttemptRunning after expiry error = %v", err)
	}
	if _, err := repository.CompleteAttempt(ctx, claimed.Attempt.ID, claimed.Attempt.LeaseToken); !errors.Is(err, job.ErrAttemptLeaseLost) {
		t.Fatalf("CompleteAttempt after expiry error = %v", err)
	}

	renewed, err := repository.LeaseActiveAttempts(ctx, "kubernetes", "replacement-owner", 30*time.Second)
	if err != nil || len(renewed) != 1 {
		t.Fatalf("LeaseActiveAttempts = %+v, %v", renewed, err)
	}
	if _, err := repository.CompleteAttempt(ctx, renewed[0].Attempt.ID, renewed[0].Attempt.LeaseToken); !errors.Is(err, job.ErrInvalidAttemptTransition) {
		t.Fatalf("CompleteAttempt from starting error = %v", err)
	}
	if _, err := repository.FailAttempt(ctx, renewed[0].Attempt.ID, renewed[0].Attempt.LeaseToken, "creation failed"); err != nil {
		t.Fatalf("FailAttempt from starting: %v", err)
	}
}

func TestPublicMaterializationReplayRejectsChangedShardCount(t *testing.T) {
	repository, _, key := newPublicRepository(t)
	ctx := context.Background()
	created, _, err := repository.Create(ctx, key, publicSubmission(), publicTestSHA256, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Materialize(ctx, created.ID, publicPlan()); err != nil {
		t.Fatal(err)
	}
	changed := publicPlan()
	changed.Shards = []job.LogicalShard{{StartByte: 0, EndByte: 5}, {StartByte: 5, EndByte: 10}}
	if _, err := repository.Materialize(ctx, created.ID, changed); !errors.Is(err, job.ErrInputConflict) {
		t.Fatalf("Materialize changed shard count error = %v", err)
	}
}

func newPublicRepository(t *testing.T) (*job.Repository, *pgxpool.Pool, string) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), publicDatabaseURL(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	key := fmt.Sprintf("public:%s:%d", strings.ReplaceAll(t.Name(), "/", "-"), time.Now().UnixNano())
	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DELETE FROM public.jobs WHERE idempotency_key = $1", key); err != nil {
			t.Errorf("delete test job: %v", err)
		}
	})
	repository, err := job.NewRepository(pool, "file:///tmp/mill-public-output")
	if err != nil {
		t.Fatal(err)
	}
	return repository, pool, key
}

func publicDatabaseURL(t *testing.T) string {
	t.Helper()
	databaseURL := os.Getenv("MILL_TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("MILL_TEST_DATABASE_URL is not set")
	}
	return databaseURL
}

func publicSubmission() job.Submission {
	return job.Submission{
		Executable: job.Executable{Image: "mill/example:dev"},
		Input:      job.InputSpec{URI: "file:///tmp/records.jsonl"},
	}
}

func publicPlan() job.PartitionPlan {
	return job.PartitionPlan{
		InputSHA256: publicTestSHA256,
		RecordCount: 1,
		Shards:      []job.LogicalShard{{StartByte: 0, EndByte: 10}},
	}
}

func requireInvalidArgument(t *testing.T, err error) {
	t.Helper()
	var validationError *job.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("error = %v (%T), want *job.ValidationError", err, err)
	}
	if !validationError.InvalidArgument() {
		t.Fatal("ValidationError did not classify itself as an invalid argument")
	}
}
