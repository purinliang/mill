package job

import (
	"context"
	"fmt"
)

// ActiveAttempts includes attempts from failed jobs: already dispatched work
// still needs to be observed even when another task has failed the job.
func (r *Repository) ActiveAttempts(ctx context.Context, executor string) ([]ClaimedAttempt, error) {
	rows, err := r.database.Query(ctx, `
		SELECT a.id::text, t.job_id::text, a.task_id::text, a.state,
			COALESCE(a.external_id, ''), t.shard_index, t.input_start_byte,
			t.input_end_byte, j.executable_image_ref, j.executable_args,
			j.input_uri, j.output_root_uri
		FROM public.attempts a
		JOIN public.tasks t ON t.id = a.task_id
		JOIN public.jobs j ON j.id = t.job_id
		WHERE a.executor = $1 AND a.state IN ('starting', 'running')
		ORDER BY a.created_at, a.id`, executor)
	if err != nil {
		return nil, fmt.Errorf("list active attempts: %w", err)
	}
	defer rows.Close()
	var active []ClaimedAttempt
	for rows.Next() {
		var a ClaimedAttempt
		if err := rows.Scan(&a.Attempt.ID, &a.Attempt.JobID, &a.Attempt.TaskID,
			&a.Attempt.State, &a.Attempt.ExternalID, &a.ShardIndex,
			&a.InputStartByte, &a.InputEndByte, &a.Executable.Image,
			&a.Executable.Args, &a.InputURI, &a.OutputURI); err != nil {
			return nil, fmt.Errorf("read active attempt: %w", err)
		}
		a.Attempt.Executor = executor
		a.OutputURI, err = deriveAttemptOutputURI(a.OutputURI, a.ShardIndex, a.Attempt.ID)
		if err != nil {
			return nil, err
		}
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
