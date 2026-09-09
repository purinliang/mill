// This file defines dataset partitioning and logical shard descriptions.
package job

import "context"

// MaxParallelism is the largest execution concurrency accepted for one job.
const MaxParallelism = 10_000

// MaxTasksPerJob limits logical shard materialization for each job.
const MaxTasksPerJob = 10_000

// DatasetPartitioner divides one dataset into record-aligned logical shards. It
// describes work without copying the input or executing tasks.
type DatasetPartitioner interface {
	Partition(
		ctx context.Context,
		inputURI string,
		parallelism int,
	) (ShardSet, error)
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
