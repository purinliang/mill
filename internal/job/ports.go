// This file defines the storage and dataset-partitioning ports used by jobs.
package job

import "context"

// MaxParallelism is the largest execution concurrency accepted for one job.
const MaxParallelism = 10_000

// MaxTasksPerJob limits logical shard materialization for each job.
const MaxTasksPerJob = 10_000

type Store interface {
	FindSubmission(context.Context, string, Submission) (Job, bool, error)
	Create(context.Context, string, Submission, string, int64, int) (Job, bool, error)
	Materialize(context.Context, string, ShardSet) (Job, error)
	Get(context.Context, string) (Job, error)
	CompletedResults(context.Context, string) ([]Result, error)
}

// DatasetPartitioner divides one dataset into record-aligned logical shards. It
// describes work without copying the input or executing tasks.
type DatasetPartitioner interface {
	Partition(context.Context, string, int) (ShardSet, error)
}

// ShardSet records the stable input identity and logical shard ranges chosen
// for one job.
type ShardSet struct {
	// InputSHA256 identifies the exact bytes that were partitioned.
	InputSHA256 string

	// RecordCount is the number of JSONL records found in the input.
	RecordCount int64

	// Shards contains contiguous byte ranges in ascending input order.
	Shards []LogicalShard
}

// LogicalShard is one non-empty half-open byte range [StartByte, EndByte).
type LogicalShard struct {
	// StartByte is the inclusive offset within the input object.
	StartByte int64

	// EndByte is the exclusive offset within the input object.
	EndByte int64
}
