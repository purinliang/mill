package job

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/purinliang/mill/internal/execution"
)

var (
	ErrNoTaskAvailable          = execution.ErrNoTaskAvailable
	ErrAttemptNotFound          = execution.ErrAttemptNotFound
	ErrAttemptLeaseLost         = execution.ErrAttemptLeaseLost
	ErrInvalidAttemptTransition = execution.ErrInvalidAttemptTransition
)

// Fixed prototype policy: three total attempts, not three extra retries.
const MaxTaskAttempts = 3
const TaskRetryDelay = 5 * time.Second

const attemptSelectByID = `
	SELECT
		a.id::text,
		t.job_id::text,
		a.task_id::text,
		a.attempt_number,
		a.executor,
		a.state,
		a.external_id,
		a.failure_message,
		a.created_at,
		a.started_at,
		a.finished_at,
		a.updated_at,
		a.lease_owner,
		a.lease_token::text,
		a.lease_expires_at
	FROM public.attempts AS a
	JOIN public.tasks AS t ON t.id = a.task_id
	WHERE a.id = $1::uuid`

func (r *Repository) ClaimNextAttempt(ctx context.Context, executor, leaseOwner string, leaseDuration time.Duration) (ClaimedAttempt, error) {
	if err := validateExecutor(executor); err != nil {
		return ClaimedAttempt{}, err
	}
	leaseSeconds, err := validateLease(leaseOwner, leaseDuration)
	if err != nil {
		return ClaimedAttempt{}, err
	}

	tx, err := r.database.Begin(ctx)
	if err != nil {
		return ClaimedAttempt{}, fmt.Errorf("begin task claim transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var jobID string
	err = tx.QueryRow(ctx, `
		SELECT j.id::text
		FROM public.jobs AS j
		WHERE j.state = 'running'
			AND EXISTS (
				SELECT 1
				FROM public.tasks AS pending
				WHERE pending.job_id = j.id
					AND pending.state = 'pending'
					AND pending.available_at <= now()
					AND pending.input_start_byte IS NOT NULL
					AND pending.input_end_byte IS NOT NULL
			)
			AND (
				SELECT count(*)
				FROM public.tasks AS active
				WHERE active.job_id = j.id
					AND active.state = 'running'
			) < j.parallelism
		ORDER BY j.created_at, j.id
		FOR UPDATE OF j SKIP LOCKED
		LIMIT 1
	`).Scan(&jobID)
	if errors.Is(err, pgx.ErrNoRows) {
		return ClaimedAttempt{}, ErrNoTaskAvailable
	}
	if err != nil {
		return ClaimedAttempt{}, fmt.Errorf("select job with execution capacity: %w", err)
	}

	var claimed ClaimedAttempt
	claimed.Attempt.JobID = jobID
	err = tx.QueryRow(ctx, `
		SELECT
			t.id::text,
			t.shard_index,
			t.input_start_byte,
			t.input_end_byte,
			j.executable_image_ref,
			j.executable_args,
			j.input_uri,
			j.output_root_uri
		FROM public.tasks AS t
		JOIN public.jobs AS j ON j.id = t.job_id
		WHERE t.job_id = $1::uuid
			AND t.state = 'pending'
			AND t.available_at <= now()
			AND t.input_start_byte IS NOT NULL
			AND t.input_end_byte IS NOT NULL
		ORDER BY t.shard_index
		FOR UPDATE OF t
		LIMIT 1
	`, jobID).Scan(
		&claimed.Attempt.TaskID,
		&claimed.ShardIndex,
		&claimed.InputStartByte,
		&claimed.InputEndByte,
		&claimed.Executable.Image,
		&claimed.Executable.Args,
		&claimed.InputURI,
		&claimed.OutputURI,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return ClaimedAttempt{}, ErrNoTaskAvailable
	}
	if err != nil {
		return ClaimedAttempt{}, fmt.Errorf("select pending task: %w", err)
	}
	if claimed.Executable.Args == nil {
		claimed.Executable.Args = []string{}
	}

	err = tx.QueryRow(ctx, `
		INSERT INTO public.attempts (
			task_id, attempt_number, executor,
			lease_owner, lease_token, lease_expires_at
		)
		SELECT $1::uuid, COALESCE(max(attempt_number), 0) + 1, $2,
			$3, uuidv7(), clock_timestamp() + $4 * interval '1 second'
		FROM public.attempts
		WHERE task_id = $1::uuid
		RETURNING id::text
	`, claimed.Attempt.TaskID, executor, leaseOwner, leaseSeconds).Scan(&claimed.Attempt.ID)
	if err != nil {
		return ClaimedAttempt{}, fmt.Errorf("create task attempt: %w", err)
	}

	command, err := tx.Exec(ctx, `
		UPDATE public.tasks
		SET state = 'running', updated_at = now()
		WHERE id = $1::uuid AND state = 'pending'
	`, claimed.Attempt.TaskID)
	if err != nil {
		return ClaimedAttempt{}, fmt.Errorf("mark claimed task running: %w", err)
	}
	if command.RowsAffected() != 1 {
		return ClaimedAttempt{}, errors.New("mark claimed task running: pending task was not updated")
	}

	claimed.Attempt, err = queryAttempt(ctx, tx, claimed.Attempt.ID)
	if err != nil {
		return ClaimedAttempt{}, fmt.Errorf("read claimed attempt: %w", err)
	}
	claimed.OutputURI, err = deriveAttemptOutputURI(claimed.OutputURI, claimed.ShardIndex, claimed.Attempt.ID)
	if err != nil {
		return ClaimedAttempt{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE public.jobs SET updated_at = now() WHERE id = $1::uuid
	`, jobID); err != nil {
		return ClaimedAttempt{}, fmt.Errorf("update claimed job timestamp: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return ClaimedAttempt{}, fmt.Errorf("commit task claim: %w", err)
	}
	return claimed, nil
}

func (r *Repository) GetAttempt(ctx context.Context, id string) (Attempt, error) {
	if !validJobID(id) {
		return Attempt{}, &ValidationError{Field: "attempt ID", Problem: "must be a UUID"}
	}
	attempt, err := queryAttempt(ctx, r.database, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return Attempt{}, ErrAttemptNotFound
	}
	if err != nil {
		return Attempt{}, fmt.Errorf("get attempt: %w", err)
	}
	return attempt, nil
}

func (r *Repository) MarkAttemptRunning(ctx context.Context, id, leaseToken, externalID string) (Attempt, error) {
	if !validJobID(id) {
		return Attempt{}, &ValidationError{Field: "attempt ID", Problem: "must be a UUID"}
	}
	if !validJobID(leaseToken) {
		return Attempt{}, &ValidationError{Field: "lease token", Problem: "must be a UUID"}
	}
	if externalID == "" || externalID != strings.TrimSpace(externalID) || len(externalID) > 255 {
		return Attempt{}, &ValidationError{Field: "external execution ID", Problem: "must be 1 to 255 bytes with no surrounding whitespace"}
	}

	tx, err := r.database.Begin(ctx)
	if err != nil {
		return Attempt{}, fmt.Errorf("begin attempt start transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	attempt, leaseActive, err := lockAttemptForLease(ctx, tx, id, leaseToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return Attempt{}, ErrAttemptNotFound
	}
	if err != nil {
		return Attempt{}, fmt.Errorf("lock starting attempt: %w", err)
	}
	if attempt.State == AttemptStateRunning && attempt.ExternalID == externalID {
		if err := tx.Commit(ctx); err != nil {
			return Attempt{}, fmt.Errorf("commit running attempt replay: %w", err)
		}
		return attempt, nil
	}
	if !leaseActive {
		return Attempt{}, ErrAttemptLeaseLost
	}
	if attempt.State != AttemptStateStarting {
		return Attempt{}, invalidAttemptTransition(attempt.State, AttemptStateRunning)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE public.attempts
		SET state = 'running', external_id = $2, started_at = now(), updated_at = now()
		WHERE id = $1::uuid
	`, id, externalID); err != nil {
		return Attempt{}, fmt.Errorf("mark attempt running: %w", err)
	}
	attempt, err = queryAttempt(ctx, tx, id)
	if err != nil {
		return Attempt{}, fmt.Errorf("read running attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Attempt{}, fmt.Errorf("commit running attempt: %w", err)
	}
	return attempt, nil
}

func (r *Repository) CompleteAttempt(ctx context.Context, id, leaseToken string) (Attempt, error) {
	return r.finishAttempt(ctx, id, leaseToken, AttemptStateCompleted, "")
}

func (r *Repository) FailAttempt(ctx context.Context, id, leaseToken, failureMessage string) (Attempt, error) {
	if failureMessage == "" || strings.TrimSpace(failureMessage) == "" || len(failureMessage) > 4096 {
		return Attempt{}, &ValidationError{Field: "failure message", Problem: "must be 1 to 4096 bytes and not blank"}
	}
	return r.finishAttempt(ctx, id, leaseToken, AttemptStateFailed, failureMessage)
}

func (r *Repository) finishAttempt(ctx context.Context, id, leaseToken string, target AttemptState, failureMessage string) (Attempt, error) {
	if !validJobID(id) {
		return Attempt{}, &ValidationError{Field: "attempt ID", Problem: "must be a UUID"}
	}
	if !validJobID(leaseToken) {
		return Attempt{}, &ValidationError{Field: "lease token", Problem: "must be a UUID"}
	}

	tx, err := r.database.Begin(ctx)
	if err != nil {
		return Attempt{}, fmt.Errorf("begin attempt completion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	attempt, leaseActive, err := lockAttemptForLease(ctx, tx, id, leaseToken)
	if errors.Is(err, pgx.ErrNoRows) {
		return Attempt{}, ErrAttemptNotFound
	}
	if err != nil {
		return Attempt{}, fmt.Errorf("lock finishing attempt: %w", err)
	}
	if attempt.State == target {
		if err := tx.Commit(ctx); err != nil {
			return Attempt{}, fmt.Errorf("commit finished attempt replay: %w", err)
		}
		return attempt, nil
	}
	if !leaseActive {
		return Attempt{}, ErrAttemptLeaseLost
	}
	if target == AttemptStateCompleted && attempt.State != AttemptStateRunning {
		return Attempt{}, invalidAttemptTransition(attempt.State, target)
	}
	if target == AttemptStateFailed && attempt.State != AttemptStateStarting && attempt.State != AttemptStateRunning {
		return Attempt{}, invalidAttemptTransition(attempt.State, target)
	}
	var jobState State
	if err := tx.QueryRow(ctx, `
		SELECT state FROM public.jobs WHERE id = $1::uuid FOR UPDATE
	`, attempt.JobID).Scan(&jobState); err != nil {
		return Attempt{}, fmt.Errorf("lock job while finishing attempt: %w", err)
	}

	if _, err := tx.Exec(ctx, `
		UPDATE public.attempts
		SET state = $2, failure_message = NULLIF($3, ''), finished_at = now(), updated_at = now()
		WHERE id = $1::uuid
	`, id, target, failureMessage); err != nil {
		return Attempt{}, fmt.Errorf("mark attempt %s: %w", target, err)
	}
	taskState := string(target)
	if target == AttemptStateFailed && jobState == StateRunning && attempt.Number < MaxTaskAttempts {
		taskState = "pending"
	}
	// The failed attempt stays terminal. Only its logical task returns to pending;
	// the timestamp and state commit together, so restart cannot lose the delay.
	command, err := tx.Exec(ctx, `
		UPDATE public.tasks
		SET state = $2, updated_at = now(),
			available_at = CASE WHEN $2 = 'pending'
				THEN now() + $3 * interval '1 second' ELSE available_at END
		WHERE id = $1::uuid AND state = 'running'
	`, attempt.TaskID, taskState, int64(TaskRetryDelay/time.Second))
	if err != nil {
		return Attempt{}, fmt.Errorf("mark task %s: %w", taskState, err)
	}
	if command.RowsAffected() != 1 {
		return Attempt{}, fmt.Errorf("mark task %s: running task was not updated", taskState)
	}

	if taskState == "failed" {
		if _, err := tx.Exec(ctx, `
			UPDATE public.jobs SET state = 'failed', updated_at = now() WHERE id = $1::uuid
		`, attempt.JobID); err != nil {
			return Attempt{}, fmt.Errorf("mark job failed: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE public.jobs
			SET state = CASE
					WHEN NOT EXISTS (
						SELECT 1 FROM public.tasks
						WHERE job_id = $1::uuid AND state <> 'completed'
					) THEN 'completed'
					ELSE state
				END,
				updated_at = now()
			WHERE id = $1::uuid
		`, attempt.JobID); err != nil {
			return Attempt{}, fmt.Errorf("update job after attempt completion: %w", err)
		}
	}

	attempt, err = queryAttempt(ctx, tx, id)
	if err != nil {
		return Attempt{}, fmt.Errorf("read finished attempt: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Attempt{}, fmt.Errorf("commit attempt completion: %w", err)
	}
	return attempt, nil
}

func queryAttempt(ctx context.Context, querier rowQuerier, id string) (Attempt, error) {
	return scanAttempt(querier.QueryRow(ctx, attemptSelectByID, id))
}

func lockAttempt(ctx context.Context, tx pgx.Tx, id string) (Attempt, error) {
	return scanAttempt(tx.QueryRow(ctx, attemptSelectByID+" FOR UPDATE OF a", id))
}

func lockAttemptForLease(ctx context.Context, tx pgx.Tx, id, leaseToken string) (Attempt, bool, error) {
	attempt, err := lockAttempt(ctx, tx, id)
	if err != nil {
		return Attempt{}, false, err
	}
	if attempt.LeaseToken != leaseToken {
		return Attempt{}, false, ErrAttemptLeaseLost
	}
	var active bool
	if err := tx.QueryRow(ctx, `
		SELECT lease_expires_at > clock_timestamp()
		FROM public.attempts
		WHERE id = $1::uuid
	`, id).Scan(&active); err != nil {
		return Attempt{}, false, err
	}
	return attempt, active, nil
}

func scanAttempt(row pgx.Row) (Attempt, error) {
	var attempt Attempt
	var externalID, failureMessage, leaseOwner, leaseToken *string
	if err := row.Scan(
		&attempt.ID,
		&attempt.JobID,
		&attempt.TaskID,
		&attempt.Number,
		&attempt.Executor,
		&attempt.State,
		&externalID,
		&failureMessage,
		&attempt.CreatedAt,
		&attempt.StartedAt,
		&attempt.FinishedAt,
		&attempt.UpdatedAt,
		&leaseOwner,
		&leaseToken,
		&attempt.LeaseExpiresAt,
	); err != nil {
		return Attempt{}, err
	}
	if externalID != nil {
		attempt.ExternalID = *externalID
	}
	if failureMessage != nil {
		attempt.FailureMessage = *failureMessage
	}
	if leaseOwner != nil {
		attempt.LeaseOwner = *leaseOwner
	}
	if leaseToken != nil {
		attempt.LeaseToken = *leaseToken
	}
	attempt.CreatedAt = attempt.CreatedAt.UTC()
	attempt.StartedAt = utcTime(attempt.StartedAt)
	attempt.FinishedAt = utcTime(attempt.FinishedAt)
	attempt.LeaseExpiresAt = utcTime(attempt.LeaseExpiresAt)
	attempt.UpdatedAt = attempt.UpdatedAt.UTC()
	return attempt, nil
}

func utcTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	normalized := value.UTC()
	return &normalized
}

func validateExecutor(executor string) error {
	if executor == "" || executor != strings.TrimSpace(executor) || len(executor) > 63 {
		return &ValidationError{Field: "executor", Problem: "must be 1 to 63 bytes with no surrounding whitespace"}
	}
	return nil
}

func validateLease(owner string, duration time.Duration) (int64, error) {
	if owner == "" || owner != strings.TrimSpace(owner) || len(owner) > 255 {
		return 0, &ValidationError{Field: "lease owner", Problem: "must be 1 to 255 bytes with no surrounding whitespace"}
	}
	if duration < time.Second || duration > 5*time.Minute || duration%time.Second != 0 {
		return 0, &ValidationError{Field: "lease duration", Problem: "must be a whole number of seconds between 1 second and 5 minutes"}
	}
	return int64(duration / time.Second), nil
}

func deriveAttemptOutputURI(outputRootURI string, shardIndex int, attemptID string) (string, error) {
	outputURI, err := url.JoinPath(
		outputRootURI,
		"tasks",
		strconv.Itoa(shardIndex),
		"attempts",
		attemptID,
		"result.jsonl",
	)
	if err != nil {
		return "", fmt.Errorf("derive attempt output URI: %w", err)
	}
	return outputURI, nil
}

func invalidAttemptTransition(from, to AttemptState) error {
	return fmt.Errorf("%w: %s to %s", ErrInvalidAttemptTransition, from, to)
}
