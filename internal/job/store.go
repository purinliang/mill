// This file defines the durable metadata operations required by the Job service.
package job

import "context"

// Store persists jobs and their logical tasks, progress, and results.
type Store interface {
	// FindSubmission finds an idempotent submission without creating it.
	FindSubmission(
		ctx context.Context,
		idempotencyKey string,
		submission Submission,
	) (Job, bool, error)

	// Create persists a preparing job after its input identity is known.
	Create(
		ctx context.Context,
		idempotencyKey string,
		submission Submission,
		inputSHA256 string,
		inputRecordCount int64,
		parallelism int,
	) (Job, bool, error)

	// Materialize persists logical tasks and makes their job runnable.
	Materialize(
		ctx context.Context,
		jobID string,
		shards ShardSet,
	) (Job, error)

	// Get retrieves one job and its current progress.
	Get(ctx context.Context, jobID string) (Job, error)

	// CompletedResults retrieves successful outputs in shard order.
	CompletedResults(ctx context.Context, jobID string) ([]Result, error)
}
