// This file atomically materializes logical shards as durable tasks.
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/purinliang/mill/internal/job"
)

func (r *Repository) Materialize(
	ctx context.Context,
	id string,
	shards job.ShardSet,
) (job.Job, error) {
	if !job.ValidID(id) {
		return job.Job{}, &job.ValidationError{
			Field:   "job ID",
			Problem: "must be a UUID",
		}
	}
	if err := job.ValidateShardSet(shards); err != nil {
		return job.Job{}, err
	}

	tx, err := r.database.Begin(ctx)
	if err != nil {
		return job.Job{}, fmt.Errorf(
			"begin task materialization transaction: %w",
			err,
		)
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
	`, id).Scan(
		&state,
		&existingSHA256,
		&existingRecordCount,
		&existingTaskCount,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return job.Job{}, job.ErrNotFound
	}
	if err != nil {
		return job.Job{}, fmt.Errorf(
			"lock job for task materialization: %w",
			err,
		)
	}
	if existingSHA256 == nil || existingRecordCount == nil ||
		*existingSHA256 != shards.InputSHA256 ||
		*existingRecordCount != shards.RecordCount {
		return job.Job{}, job.ErrInputConflict
	}

	if state != job.StatePreparing {
		if existingTaskCount == nil ||
			*existingTaskCount != len(shards.Shards) {
			return job.Job{}, job.ErrInputConflict
		}
		materializedJob, err := queryJob(ctx, tx, jobSelectByID, id)
		if err != nil {
			return job.Job{}, fmt.Errorf(
				"read materialized job: %w",
				err,
			)
		}
		if err := tx.Commit(ctx); err != nil {
			return job.Job{}, fmt.Errorf(
				"commit materialized job lookup: %w",
				err,
			)
		}
		return materializedJob, nil
	}

	rows := make([][]any, len(shards.Shards))
	for index, shard := range shards.Shards {
		rows[index] = []any{
			id,
			index,
			shard.StartByte,
			shard.EndByte,
		}
	}
	inserted, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"public", "tasks"},
		[]string{
			"job_id",
			"shard_index",
			"input_start_byte",
			"input_end_byte",
		},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return job.Job{}, fmt.Errorf(
			"insert logical shard tasks: %w",
			err,
		)
	}
	if inserted != int64(len(rows)) {
		return job.Job{}, fmt.Errorf(
			"inserted %d logical shard tasks, want %d",
			inserted,
			len(rows),
		)
	}

	command, err := tx.Exec(ctx, `
		UPDATE public.jobs
		SET task_count = $2,
			state = 'running',
			updated_at = now()
		WHERE id = $1::uuid AND state = 'preparing'
	`, id, len(shards.Shards))
	if err != nil {
		return job.Job{}, fmt.Errorf(
			"finalize task materialization: %w",
			err,
		)
	}
	if command.RowsAffected() != 1 {
		return job.Job{}, errors.New(
			"finalize task materialization: preparing job was not updated",
		)
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
