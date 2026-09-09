// This file persists idempotent job submissions.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/purinliang/mill/internal/job"
)

func (r *Repository) FindSubmission(
	ctx context.Context,
	idempotencyKey string,
	submission job.Submission,
) (job.Job, bool, error) {
	if err := job.ValidateIdempotencyKey(idempotencyKey); err != nil {
		return job.Job{}, false, err
	}
	normalizedSubmission, err := job.NormalizeSubmission(submission)
	if err != nil {
		return job.Job{}, false, err
	}

	existingJob, err := queryJob(
		ctx,
		r.database,
		jobSelectByIdempotencyKey,
		idempotencyKey,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, false, nil
	}
	if err != nil {
		return job.Job{}, false, fmt.Errorf("find idempotent job: %w", err)
	}
	if !sameSubmission(existingJob, normalizedSubmission) {
		return job.Job{}, false, job.ErrIdempotencyConflict
	}
	return existingJob, true, nil
}

func (r *Repository) Create(
	ctx context.Context,
	idempotencyKey string,
	submission job.Submission,
	inputSHA256 string,
	inputRecordCount int64,
	parallelism int,
) (job.Job, bool, error) {
	if err := job.ValidateIdempotencyKey(idempotencyKey); err != nil {
		return job.Job{}, false, err
	}
	normalizedSubmission, err := job.NormalizeSubmission(submission)
	if err != nil {
		return job.Job{}, false, err
	}
	if err := job.ValidateInputIdentity(
		inputSHA256,
		inputRecordCount,
	); err != nil {
		return job.Job{}, false, err
	}
	if err := job.ValidateParallelism(parallelism); err != nil {
		return job.Job{}, false, err
	}
	resources, valid := job.ResolveResources(
		normalizedSubmission.ResourceClass,
	)
	if !valid {
		return job.Job{}, false, &job.ValidationError{
			Field:   "resource_class",
			Problem: "must be small, medium, or large",
		}
	}

	tx, err := r.database.Begin(ctx)
	if err != nil {
		return job.Job{}, false, fmt.Errorf(
			"begin job creation transaction: %w",
			err,
		)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var id string
	if err := tx.QueryRow(ctx, "SELECT uuidv7()::text").Scan(&id); err != nil {
		return job.Job{}, false, fmt.Errorf("generate job ID: %w", err)
	}

	outputRootURI, err := job.DeriveOutputRootURI(r.outputRootURI, id)
	if err != nil {
		return job.Job{}, false, err
	}

	var createdID string
	err = tx.QueryRow(ctx, `
		INSERT INTO public.jobs (
			id,
			idempotency_key,
			executable_image_ref,
			executable_args,
			input_uri,
			input_sha256,
			input_record_count,
			output_root_uri,
			parallelism,
			resource_class,
			workload_cpu_request_millis,
			workload_cpu_limit_millis,
			workload_memory_request_bytes,
			workload_memory_limit_bytes
		)
		VALUES (
			$1::uuid, $2, $3, $4, $5, $6, $7,
			$8, $9, $10, $11, $12, $13, $14
		)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id::text
	`,
		id,
		idempotencyKey,
		normalizedSubmission.Executable.Image,
		normalizedSubmission.Executable.Args,
		normalizedSubmission.Input.URI,
		inputSHA256,
		inputRecordCount,
		outputRootURI,
		parallelism,
		normalizedSubmission.ResourceClass,
		resources.CPURequestMillis,
		resources.CPULimitMillis,
		resources.MemoryRequestBytes,
		resources.MemoryLimitBytes,
	).Scan(&createdID)
	if err == nil {
		createdJob, err := queryJob(ctx, tx, jobSelectByID, createdID)
		if err != nil {
			return job.Job{}, false, fmt.Errorf(
				"read created job: %w",
				err,
			)
		}
		if err := tx.Commit(ctx); err != nil {
			return job.Job{}, false, fmt.Errorf(
				"commit job creation: %w",
				err,
			)
		}
		return createdJob, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	existingJob, err := queryJob(
		ctx,
		tx,
		jobSelectByIdempotencyKey,
		idempotencyKey,
	)
	if err != nil {
		return job.Job{}, false, fmt.Errorf(
			"read idempotent job: %w",
			err,
		)
	}
	if !sameSubmission(existingJob, normalizedSubmission) {
		return job.Job{}, false, job.ErrIdempotencyConflict
	}

	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, false, fmt.Errorf(
			"commit idempotent job lookup: %w",
			err,
		)
	}
	return existingJob, false, nil
}

func sameSubmission(value job.Job, submission job.Submission) bool {
	return value.Executable.Image == submission.Executable.Image &&
		slices.Equal(
			value.Executable.Args,
			submission.Executable.Args,
		) &&
		value.Input.URI == submission.Input.URI &&
		value.ResourceClass == submission.ResourceClass
}
