// This file persists job creation, task materialization, and status reads.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/purinliang/mill/internal/job"
)

const jobSelectColumns = `
	j.id::text,
	j.executable_image_ref,
	j.executable_args,
	j.input_uri,
	j.input_sha256,
	j.input_record_count,
	j.output_root_uri,
	j.parallelism,
	j.resource_class,
	j.workload_cpu_request_millis,
	j.workload_cpu_limit_millis,
	j.workload_memory_request_bytes,
	j.workload_memory_limit_bytes,
	j.state,
	COALESCE(j.task_count, 0),
	(SELECT count(*) FROM public.tasks AS t WHERE t.job_id = j.id AND t.state = 'pending'),
	(SELECT count(*) FROM public.tasks AS t WHERE t.job_id = j.id AND t.state = 'running'),
	(SELECT count(*) FROM public.tasks AS t WHERE t.job_id = j.id AND t.state = 'completed'),
	(SELECT count(*) FROM public.tasks AS t WHERE t.job_id = j.id AND t.state = 'failed'),
	j.created_at,
	j.updated_at`

const (
	jobSelectByID = `SELECT ` + jobSelectColumns + `
		FROM public.jobs AS j
		WHERE j.id = $1::uuid`
	jobSelectByIdempotencyKey = `SELECT ` + jobSelectColumns + `
		FROM public.jobs AS j
		WHERE j.idempotency_key = $1`
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

	existingJob, err := queryJob(ctx, r.database, jobSelectByIdempotencyKey, idempotencyKey)
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
	if err := job.ValidateInputIdentity(inputSHA256, inputRecordCount); err != nil {
		return job.Job{}, false, err
	}
	if err := job.ValidateParallelism(parallelism); err != nil {
		return job.Job{}, false, err
	}
	resources, valid := job.ResolveResources(normalizedSubmission.ResourceClass)
	if !valid {
		return job.Job{}, false, &job.ValidationError{
			Field:   "resource_class",
			Problem: "must be small, medium, or large",
		}
	}

	tx, err := r.database.Begin(ctx)
	if err != nil {
		return job.Job{}, false, fmt.Errorf("begin job creation transaction: %w", err)
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
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
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
			return job.Job{}, false, fmt.Errorf("read created job: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return job.Job{}, false, fmt.Errorf("commit job creation: %w", err)
		}
		return createdJob, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	existingJob, err := queryJob(ctx, tx, jobSelectByIdempotencyKey, idempotencyKey)
	if err != nil {
		return job.Job{}, false, fmt.Errorf("read idempotent job: %w", err)
	}
	if !sameSubmission(existingJob, normalizedSubmission) {
		return job.Job{}, false, job.ErrIdempotencyConflict
	}

	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, false, fmt.Errorf("commit idempotent job lookup: %w", err)
	}
	return existingJob, false, nil
}

func (r *Repository) Materialize(
	ctx context.Context,
	id string,
	plan job.PartitionPlan,
) (job.Job, error) {
	if !job.ValidID(id) {
		return job.Job{}, &job.ValidationError{Field: "job ID", Problem: "must be a UUID"}
	}
	if err := job.ValidatePartitionPlan(plan); err != nil {
		return job.Job{}, err
	}

	tx, err := r.database.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf("begin task materialization transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		state               job.State
		existingSHA256      *string
		existingRecordCount *int64
		existingTaskCount   *int
	)
	err = tx.QueryRow(ctx, `
		SELECT state, input_sha256, input_record_count, task_count
		FROM public.jobs
		WHERE id = $1::uuid
		FOR UPDATE
	`, id).Scan(&state, &existingSHA256, &existingRecordCount, &existingTaskCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, job.ErrNotFound
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("lock job for task materialization: %w", err)
	}
	if existingSHA256 == nil || existingRecordCount == nil ||
		*existingSHA256 != plan.InputSHA256 || *existingRecordCount != plan.RecordCount {
		return job.Job{}, job.ErrInputConflict
	}

	if state != job.StatePreparing {
		if existingTaskCount == nil || *existingTaskCount != len(plan.Shards) {
			return job.Job{}, job.ErrInputConflict
		}
		materializedJob, err := queryJob(ctx, tx, jobSelectByID, id)
		if err != nil {
			return job.Job{}, fmt.Errorf("read materialized job: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return job.Job{}, fmt.Errorf("commit materialized job lookup: %w", err)
		}
		return materializedJob, nil
	}

	rows := make([][]any, len(plan.Shards))
	for index, shard := range plan.Shards {
		rows[index] = []any{id, index, shard.StartByte, shard.EndByte}
	}
	inserted, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"public", "tasks"},
		[]string{"job_id", "shard_index", "input_start_byte", "input_end_byte"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return job.Job{}, fmt.Errorf("insert logical shard tasks: %w", err)
	}
	if inserted != int64(len(rows)) {
		return job.Job{}, fmt.Errorf("inserted %d logical shard tasks, want %d", inserted, len(rows))
	}

	command, err := tx.Exec(ctx, `
		UPDATE public.jobs
		SET task_count = $2,
			state = 'running',
			updated_at = now()
		WHERE id = $1::uuid AND state = 'preparing'
	`, id, len(plan.Shards))
	if err != nil {
		return job.Job{}, fmt.Errorf("finalize task materialization: %w", err)
	}
	if command.RowsAffected() != 1 {
		return job.Job{}, errors.New("finalize task materialization: preparing job was not updated")
	}

	materializedJob, err := queryJob(ctx, tx, jobSelectByID, id)
	if err != nil {
		return job.Job{}, fmt.Errorf("read materialized job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return job.Job{}, fmt.Errorf("commit task materialization: %w", err)
	}
	return materializedJob, nil
}

func (r *Repository) Get(ctx context.Context, id string) (job.Job, error) {
	if !job.ValidID(id) {
		return job.Job{}, &job.ValidationError{Field: "job ID", Problem: "must be a UUID"}
	}

	value, err := queryJob(ctx, r.database, jobSelectByID, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, job.ErrNotFound
	}
	if err != nil {
		return job.Job{}, fmt.Errorf("get job: %w", err)
	}
	return value, nil
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func queryJob(
	ctx context.Context,
	querier rowQuerier,
	query string,
	argument any,
) (job.Job, error) {
	return scanJob(querier.QueryRow(ctx, query, argument))
}

func scanJob(row pgx.Row) (job.Job, error) {
	var value job.Job
	var inputSHA256 *string
	var inputRecordCount *int64
	if err := row.Scan(
		&value.ID,
		&value.Executable.Image,
		&value.Executable.Args,
		&value.Input.URI,
		&inputSHA256,
		&inputRecordCount,
		&value.Output.URI,
		&value.Parallelism,
		&value.ResourceClass,
		&value.Resources.CPURequestMillis,
		&value.Resources.CPULimitMillis,
		&value.Resources.MemoryRequestBytes,
		&value.Resources.MemoryLimitBytes,
		&value.State,
		&value.Progress.Total,
		&value.Progress.Pending,
		&value.Progress.Running,
		&value.Progress.Completed,
		&value.Progress.Failed,
		&value.CreatedAt,
		&value.UpdatedAt,
	); err != nil {
		return job.Job{}, err
	}

	if value.Executable.Args == nil {
		value.Executable.Args = []string{}
	}
	if inputSHA256 != nil {
		value.Input.SHA256 = *inputSHA256
	}
	if inputRecordCount != nil {
		value.Input.RecordCount = *inputRecordCount
	}
	value.CreatedAt = value.CreatedAt.UTC()
	value.UpdatedAt = value.UpdatedAt.UTC()
	return value, nil
}

func sameSubmission(value job.Job, submission job.Submission) bool {
	return value.Executable.Image == submission.Executable.Image &&
		slices.Equal(value.Executable.Args, submission.Executable.Args) &&
		value.Input.URI == submission.Input.URI &&
		value.ResourceClass == submission.ResourceClass
}
