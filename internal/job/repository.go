package job

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrIdempotencyConflict = errors.New("idempotency key is already associated with a different submission")
	ErrInputConflict       = errors.New("input differs from the logical shards already planned for the job")
	ErrNotFound            = errors.New("job not found")
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

type Repository struct {
	database      *pgxpool.Pool
	outputRootURI string
}

func NewRepository(database *pgxpool.Pool, outputRootURI string) (*Repository, error) {
	if database == nil {
		return nil, errors.New("PostgreSQL connection pool is required")
	}

	normalizedRoot, err := normalizeOutputRootURI(outputRootURI)
	if err != nil {
		return nil, fmt.Errorf("validate MILL_OUTPUT_ROOT_URI: %w", err)
	}

	return &Repository{database: database, outputRootURI: normalizedRoot}, nil
}

func (r *Repository) FindSubmission(ctx context.Context, idempotencyKey string, submission Submission) (Job, bool, error) {
	if err := validateIdempotencyKey(idempotencyKey); err != nil {
		return Job{}, false, err
	}
	normalizedSubmission, err := normalizeSubmission(submission)
	if err != nil {
		return Job{}, false, err
	}

	existingJob, err := queryJob(ctx, r.database, jobSelectByIdempotencyKey, idempotencyKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("find idempotent job: %w", err)
	}
	if !sameSubmission(existingJob, normalizedSubmission) {
		return Job{}, false, ErrIdempotencyConflict
	}
	return existingJob, true, nil
}

func (r *Repository) Create(
	ctx context.Context,
	idempotencyKey string,
	submission Submission,
	inputSHA256 string,
	inputRecordCount int64,
	parallelism int,
) (Job, bool, error) {
	if err := validateIdempotencyKey(idempotencyKey); err != nil {
		return Job{}, false, err
	}
	normalizedSubmission, err := normalizeSubmission(submission)
	if err != nil {
		return Job{}, false, err
	}
	if err := validateInputIdentity(inputSHA256, inputRecordCount); err != nil {
		return Job{}, false, err
	}
	if parallelism < 1 || parallelism > maxParallelism {
		return Job{}, false, &ValidationError{Field: "parallelism", Problem: "must be between 1 and 10000"}
	}

	tx, err := r.database.Begin(ctx)
	if err != nil {
		return Job{}, false, fmt.Errorf("begin job creation transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var id string
	if err := tx.QueryRow(ctx, "SELECT uuidv7()::text").Scan(&id); err != nil {
		return Job{}, false, fmt.Errorf("generate job ID: %w", err)
	}

	outputRootURI, err := deriveOutputRootURI(r.outputRootURI, id)
	if err != nil {
		return Job{}, false, err
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
			parallelism
		)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, $8, $9)
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
	).Scan(&createdID)
	if err == nil {
		createdJob, err := queryJob(ctx, tx, jobSelectByID, createdID)
		if err != nil {
			return Job{}, false, fmt.Errorf("read created job: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return Job{}, false, fmt.Errorf("commit job creation: %w", err)
		}
		return createdJob, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	existingJob, err := queryJob(ctx, tx, jobSelectByIdempotencyKey, idempotencyKey)
	if err != nil {
		return Job{}, false, fmt.Errorf("read idempotent job: %w", err)
	}
	if !sameSubmission(existingJob, normalizedSubmission) {
		return Job{}, false, ErrIdempotencyConflict
	}

	if err := tx.Commit(ctx); err != nil {
		return Job{}, false, fmt.Errorf("commit idempotent job lookup: %w", err)
	}
	return existingJob, false, nil
}

func (r *Repository) Materialize(ctx context.Context, id string, plan PartitionPlan) (Job, error) {
	if !validJobID(id) {
		return Job{}, &ValidationError{Field: "job ID", Problem: "must be a UUID"}
	}
	if err := validatePartitionPlan(plan); err != nil {
		return Job{}, err
	}

	tx, err := r.database.Begin(ctx)
	if err != nil {
		return Job{}, fmt.Errorf("begin task materialization transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		state               State
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
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("lock job for task materialization: %w", err)
	}
	if existingSHA256 == nil || existingRecordCount == nil ||
		*existingSHA256 != plan.InputSHA256 || *existingRecordCount != plan.RecordCount {
		return Job{}, ErrInputConflict
	}

	if state != StatePreparing {
		if existingTaskCount == nil || *existingTaskCount != len(plan.Shards) {
			return Job{}, ErrInputConflict
		}
		materializedJob, err := queryJob(ctx, tx, jobSelectByID, id)
		if err != nil {
			return Job{}, fmt.Errorf("read materialized job: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return Job{}, fmt.Errorf("commit materialized job lookup: %w", err)
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
		return Job{}, fmt.Errorf("insert logical shard tasks: %w", err)
	}
	if inserted != int64(len(rows)) {
		return Job{}, fmt.Errorf("inserted %d logical shard tasks, want %d", inserted, len(rows))
	}

	command, err := tx.Exec(ctx, `
		UPDATE public.jobs
		SET task_count = $2,
			state = 'running',
			updated_at = now()
		WHERE id = $1::uuid AND state = 'preparing'
	`, id, len(plan.Shards))
	if err != nil {
		return Job{}, fmt.Errorf("finalize task materialization: %w", err)
	}
	if command.RowsAffected() != 1 {
		return Job{}, errors.New("finalize task materialization: preparing job was not updated")
	}

	materializedJob, err := queryJob(ctx, tx, jobSelectByID, id)
	if err != nil {
		return Job{}, fmt.Errorf("read materialized job: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Job{}, fmt.Errorf("commit task materialization: %w", err)
	}
	return materializedJob, nil
}

func (r *Repository) Get(ctx context.Context, id string) (Job, error) {
	if !validJobID(id) {
		return Job{}, &ValidationError{Field: "job ID", Problem: "must be a UUID"}
	}

	job, err := queryJob(ctx, r.database, jobSelectByID, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func queryJob(ctx context.Context, querier rowQuerier, query string, argument any) (Job, error) {
	return scanJob(querier.QueryRow(ctx, query, argument))
}

func scanJob(row pgx.Row) (Job, error) {
	var job Job
	var inputSHA256 *string
	var inputRecordCount *int64
	if err := row.Scan(
		&job.ID,
		&job.Executable.Image,
		&job.Executable.Args,
		&job.Input.URI,
		&inputSHA256,
		&inputRecordCount,
		&job.Output.URI,
		&job.Parallelism,
		&job.State,
		&job.Progress.Total,
		&job.Progress.Pending,
		&job.Progress.Running,
		&job.Progress.Completed,
		&job.Progress.Failed,
		&job.CreatedAt,
		&job.UpdatedAt,
	); err != nil {
		return Job{}, err
	}

	if job.Executable.Args == nil {
		job.Executable.Args = []string{}
	}
	if inputSHA256 != nil {
		job.Input.SHA256 = *inputSHA256
	}
	if inputRecordCount != nil {
		job.Input.RecordCount = *inputRecordCount
	}
	job.CreatedAt = job.CreatedAt.UTC()
	job.UpdatedAt = job.UpdatedAt.UTC()
	return job, nil
}

func sameSubmission(job Job, submission Submission) bool {
	return job.Executable.Image == submission.Executable.Image &&
		slices.Equal(job.Executable.Args, submission.Executable.Args) &&
		job.Input.URI == submission.Input.URI
}
