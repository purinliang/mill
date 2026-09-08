package job

import "context"

const MaxParallelism = 10_000
const MaxTasksPerJob = 10_000

type Store interface {
	FindSubmission(context.Context, string, Submission) (Job, bool, error)
	Create(context.Context, string, Submission, string, int64, int) (Job, bool, error)
	Materialize(context.Context, string, PartitionPlan) (Job, error)
	Get(context.Context, string) (Job, error)
	CompletedResults(context.Context, string) ([]Result, error)
}

type Planner interface {
	Plan(context.Context, string, int) (PartitionPlan, error)
}

type PartitionPlan struct {
	InputSHA256 string
	RecordCount int64
	Shards      []LogicalShard
}

type LogicalShard struct {
	StartByte int64
	EndByte   int64
}
