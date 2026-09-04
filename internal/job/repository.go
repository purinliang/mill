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
	ErrNotFound            = errors.New("job not found")
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

	createdJob, err := scanJob(tx.QueryRow(ctx, `
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
		RETURNING
			id::text,
			workload_image_ref,
			workload_args,
			dataset_manifest_uri,
			output_uri,
			state,
			created_at,
			updated_at
	`,
		id,
		idempotencyKey,
		normalizedSubmission.Workload.Image,
		normalizedSubmission.Workload.Args,
		normalizedSubmission.Dataset.ManifestURI,
		outputURI,
	))
	if err == nil {
		if err := tx.Commit(ctx); err != nil {
			return Job{}, false, fmt.Errorf("commit job creation: %w", err)
		}
		return createdJob, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, fmt.Errorf("insert job: %w", err)
	}

	existingJob, err := scanJob(tx.QueryRow(ctx, `
		SELECT
			id::text,
			workload_image_ref,
			workload_args,
			dataset_manifest_uri,
			output_uri,
			state,
			created_at,
			updated_at
		FROM public.jobs
		WHERE idempotency_key = $1
	`, idempotencyKey))
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

func (r *Repository) Get(ctx context.Context, id string) (Job, error) {
	if !validJobID(id) {
		return Job{}, &ValidationError{Field: "job ID", Problem: "must be a UUID"}
	}

	job, err := scanJob(r.database.QueryRow(ctx, `
		SELECT
			id::text,
			workload_image_ref,
			workload_args,
			dataset_manifest_uri,
			output_uri,
			state,
			created_at,
			updated_at
		FROM public.jobs
		WHERE id = $1::uuid
	`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

func scanJob(row pgx.Row) (Job, error) {
	var job Job
	if err := row.Scan(
		&job.ID,
		&job.Workload.Image,
		&job.Workload.Args,
		&job.Dataset.ManifestURI,
		&job.Output.URI,
		&job.State,
		&job.CreatedAt,
		&job.UpdatedAt,
	); err != nil {
		return Job{}, err
	}

	if job.Workload.Args == nil {
		job.Workload.Args = []string{}
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
