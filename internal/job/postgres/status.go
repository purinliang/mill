// This file reads persisted job status and progress.
package postgres

import (
	"context"
	"errors"
	"fmt"

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

func (r *Repository) Get(ctx context.Context, id string) (job.Job, error) {
	if !job.ValidID(id) {
		return job.Job{}, &job.ValidationError{
			Field:   "job ID",
			Problem: "must be a UUID",
		}
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
