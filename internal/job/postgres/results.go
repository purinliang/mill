// This file queries successful task outputs for completed jobs.
package postgres

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/purinliang/mill/internal/job"
)

func (r *Repository) CompletedResults(
	ctx context.Context,
	jobID string,
) ([]job.Result, error) {
	rows, err := r.database.Query(ctx, `
		SELECT t.id::text, t.shard_index, a.id::text, j.output_root_uri
		FROM public.tasks t
		JOIN public.jobs j ON j.id = t.job_id
		JOIN public.attempts a ON a.task_id = t.id
		WHERE t.job_id = $1::uuid
			AND t.state = 'completed'
			AND a.state = 'completed'
		ORDER BY t.shard_index`, jobID)
	if err != nil {
		return nil, fmt.Errorf("list results: %w", err)
	}
	defer rows.Close()

	results := []job.Result{}
	for rows.Next() {
		var result job.Result
		if err := rows.Scan(
			&result.TaskID,
			&result.ShardIndex,
			&result.AttemptID,
			&result.URI,
		); err != nil {
			return nil, fmt.Errorf("read result: %w", err)
		}
		result.URI, err = deriveResultURI(
			result.URI,
			result.ShardIndex,
			result.AttemptID,
		)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
	}
	return results, rows.Err()
}

func deriveResultURI(root string, shardIndex int, attemptID string) (string, error) {
	resultURI, err := url.JoinPath(
		root,
		"tasks",
		strconv.Itoa(shardIndex),
		"attempts",
		attemptID,
		"result.jsonl",
	)
	if err != nil {
		return "", fmt.Errorf("derive result URI: %w", err)
	}
	return resultURI, nil
}
