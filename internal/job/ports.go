// This file defines the storage and dataset-planning ports used by jobs.
package job

import "context"

// MaxParallelism is the largest execution concurrency accepted for one job.
const MaxParallelism = 10_000

// MaxTasksPerJob limits logical shard materialization for each job.
const MaxTasksPerJob = 10_000

type Store interface {
	FindSubmission(context.Context, string, Submission) (Job, bool, error)
	Create(context.Context, string, Submission, string, int64, int) (Job, bool, error)
	Materialize(context.Context, string, PartitionPlan) (Job, error)
	Get(context.Context, string) (Job, error)
	CompletedResults(context.Context, string) ([]Result, error)
}

// Planner examines one dataset and returns record-aligned logical shards. It
// describes work without copying the input or executing tasks.
type Planner interface {
	Plan(context.Context, string, int) (PartitionPlan, error)
}

// PartitionPlan records the stable input identity and logical shard ranges
// chosen for one job.
type PartitionPlan struct {
	InputSHA256 string
	RecordCount int64
	Shards      []LogicalShard
}

// LogicalShard is one non-empty half-open byte range [StartByte, EndByte).
type LogicalShard struct {
	StartByte int64
	EndByte   int64
}
