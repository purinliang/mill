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
	ErrManifestConflict    = errors.New("dataset manifest differs from the materialized task set")
	ErrNotFound            = errors.New("job not found")
)

const jobSelectColumns = `
	j.id::text,
	j.workload_image_ref,
	j.workload_args,
	j.dataset_manifest_uri,
	j.dataset_manifest_sha256,
	j.output_uri,
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

func (r *Repository) Create(ctx context.Context, idempotencyKey string, submission Submission) (Job, bool, error) {
	if err := validateIdempotencyKey(idempotencyKey); err != nil {
		return Job{}, false, err
	}

	normalizedSubmission, err := normalizeSubmission(submission)
	if err != nil {
		return Job{}, false, err
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

	outputURI, err := deriveOutputURI(r.outputRootURI, id)
	if err != nil {
		return Job{}, false, err
	}

	var createdID string
	err = tx.QueryRow(ctx, `
		INSERT INTO public.jobs (
			id,
			idempotency_key,
			workload_image_ref,
			workload_args,
			dataset_manifest_uri,
			output_uri
		)
		VALUES ($1::uuid, $2, $3, $4, $5, $6)
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING id::text
	`,
		id,
		idempotencyKey,
		normalizedSubmission.Workload.Image,
		normalizedSubmission.Workload.Args,
		normalizedSubmission.Dataset.ManifestURI,
		outputURI,
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

func (r *Repository) Materialize(ctx context.Context, id string, manifest Manifest) (Job, error) {
	if !validJobID(id) {
		return Job{}, &ValidationError{Field: "job ID", Problem: "must be a UUID"}
	}
	if err := validateMaterializedManifest(manifest); err != nil {
		return Job{}, err
	}

	tx, err := r.database.Begin(ctx)
	if err != nil {
		return Job{}, fmt.Errorf("begin task materialization transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var (
		state          State
		existingSHA256 *string
		existingCount  *int
		jobOutputURI   string
	)
	err = tx.QueryRow(ctx, `
		SELECT state, dataset_manifest_sha256, task_count, output_uri
		FROM public.jobs
		WHERE id = $1::uuid
		FOR UPDATE
	`, id).Scan(&state, &existingSHA256, &existingCount, &jobOutputURI)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("lock job for task materialization: %w", err)
	}

	if state != StatePreparing {
		if existingSHA256 == nil || existingCount == nil ||
			*existingSHA256 != manifest.SHA256 || *existingCount != len(manifest.Shards) {
			return Job{}, ErrManifestConflict
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

	rows := make([][]any, len(manifest.Shards))
	for index, shard := range manifest.Shards {
		outputURI, err := deriveTaskOutputURI(jobOutputURI, index)
		if err != nil {
			return Job{}, err
		}
		rows[index] = []any{id, index, shard.URI, outputURI}
	}
	inserted, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"public", "tasks"},
		[]string{"job_id", "shard_index", "input_uri", "output_uri"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return Job{}, fmt.Errorf("insert materialized tasks: %w", err)
	}
	if inserted != int64(len(rows)) {
		return Job{}, fmt.Errorf("inserted %d materialized tasks, want %d", inserted, len(rows))
	}

	command, err := tx.Exec(ctx, `
		UPDATE public.jobs
		SET dataset_manifest_sha256 = $2,
			task_count = $3,
			state = 'running',
			updated_at = now()
		WHERE id = $1::uuid AND state = 'preparing'
	`, id, manifest.SHA256, len(manifest.Shards))
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
	var manifestSHA256 *string
	if err := row.Scan(
		&job.ID,
		&job.Workload.Image,
		&job.Workload.Args,
		&job.Dataset.ManifestURI,
		&manifestSHA256,
		&job.Output.URI,
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

	if job.Workload.Args == nil {
		job.Workload.Args = []string{}
	}
	if manifestSHA256 != nil {
		job.Dataset.ManifestSHA256 = *manifestSHA256
	}
	job.CreatedAt = job.CreatedAt.UTC()
	job.UpdatedAt = job.UpdatedAt.UTC()
	return job, nil
}

func sameSubmission(job Job, submission Submission) bool {
	return job.Workload.Image == submission.Workload.Image &&
		slices.Equal(job.Workload.Args, submission.Workload.Args) &&
		job.Dataset.ManifestURI == submission.Dataset.ManifestURI
}
