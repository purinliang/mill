package job

import (
	"context"
	"fmt"
	"time"
)

// LeaseActiveAttempts renews this owner's active attempts and atomically takes
// over unowned or expired attempts. A takeover keeps the attempt identity, so
// the execution service reconciles the same external Kubernetes Job.
func (r *Repository) LeaseActiveAttempts(
	ctx context.Context,
	executor, leaseOwner string,
	leaseDuration time.Duration,
) ([]ClaimedAttempt, error) {
	if err := validateExecutor(executor); err != nil {
		return nil, err
	}
	leaseSeconds, err := validateLease(leaseOwner, leaseDuration)
	if err != nil {
		return nil, err
	}
	rows, err := r.database.Query(ctx, `
		WITH candidates AS (
			SELECT a.id
			FROM public.attempts AS a
			WHERE a.executor = $1
				AND a.state IN ('starting', 'running')
				AND (
					a.lease_owner = $2
					OR a.lease_expires_at IS NULL
					OR a.lease_expires_at <= clock_timestamp()
				)
			ORDER BY a.created_at, a.id
			FOR UPDATE SKIP LOCKED
		), leased AS (
			UPDATE public.attempts AS a
			SET lease_owner = $2,
				lease_token = CASE
					WHEN a.lease_owner = $2 AND a.lease_token IS NOT NULL
						THEN a.lease_token
					ELSE uuidv7()
				END,
				lease_expires_at = clock_timestamp() + $3 * interval '1 second',
				updated_at = now()
			FROM candidates AS c
			WHERE a.id = c.id
			RETURNING a.*
		)
		SELECT l.id::text, t.job_id::text, l.task_id::text,
			l.attempt_number, l.state, COALESCE(l.external_id, ''),
			l.lease_owner, l.lease_token::text, l.lease_expires_at,
			t.shard_index, t.input_start_byte, t.input_end_byte,
			j.executable_image_ref, j.executable_args, j.input_uri,
			j.output_root_uri, j.workload_cpu_request_millis,
			j.workload_cpu_limit_millis, j.workload_memory_request_bytes,
			j.workload_memory_limit_bytes
		FROM leased AS l
		JOIN public.tasks AS t ON t.id = l.task_id
		JOIN public.jobs AS j ON j.id = t.job_id
		ORDER BY l.created_at, l.id
	`, executor, leaseOwner, leaseSeconds)
	if err != nil {
		return nil, fmt.Errorf("lease active attempts: %w", err)
	}
	defer rows.Close()
	return scanClaimedAttempts(rows, executor)
}

type claimedAttemptRows interface {
	Next() bool
	Scan(...any) error
	Err() error
}

func scanClaimedAttempts(rows claimedAttemptRows, executor string) ([]ClaimedAttempt, error) {
	var active []ClaimedAttempt
	for rows.Next() {
		var a ClaimedAttempt
		if err := rows.Scan(&a.Attempt.ID, &a.Attempt.JobID, &a.Attempt.TaskID,
			&a.Attempt.Number, &a.Attempt.State, &a.Attempt.ExternalID,
			&a.Attempt.LeaseOwner, &a.Attempt.LeaseToken, &a.Attempt.LeaseExpiresAt,
			&a.ShardIndex, &a.InputStartByte, &a.InputEndByte, &a.Executable.Image,
			&a.Executable.Args, &a.InputURI, &a.OutputURI,
			&a.Resources.CPURequestMillis, &a.Resources.CPULimitMillis,
			&a.Resources.MemoryRequestBytes, &a.Resources.MemoryLimitBytes); err != nil {
			return nil, fmt.Errorf("read active attempt: %w", err)
		}
		a.Attempt.Executor = executor
		a.Attempt.LeaseExpiresAt = utcTime(a.Attempt.LeaseExpiresAt)
		outputURI, err := deriveAttemptOutputURI(a.OutputURI, a.ShardIndex, a.Attempt.ID)
		if err != nil {
			return nil, err
		}
		a.OutputURI = outputURI
		active = append(active, a)
	}
	return active, rows.Err()
}

// CompletedResults returns one successful attempt output per logical task.
func (r *Repository) CompletedResults(ctx context.Context, jobID string) ([]Result, error) {
	rows, err := r.database.Query(ctx, `
		SELECT t.id::text, t.shard_index, a.id::text, j.output_root_uri
		FROM public.tasks t
		JOIN public.jobs j ON j.id = t.job_id
		JOIN public.attempts a ON a.task_id = t.id
		WHERE t.job_id = $1::uuid AND t.state = 'completed' AND a.state = 'completed'
		ORDER BY t.shard_index`, jobID)
	if err != nil {
		return nil, fmt.Errorf("list results: %w", err)
	}
	defer rows.Close()
	results := []Result{}
	for rows.Next() {
		var result Result
		if err := rows.Scan(&result.TaskID, &result.ShardIndex, &result.AttemptID, &result.URI); err != nil {
			return nil, fmt.Errorf("read result: %w", err)
		}
		result.URI, err = deriveAttemptOutputURI(result.URI, result.ShardIndex, result.AttemptID)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}
