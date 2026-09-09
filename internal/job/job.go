// Package job defines Mill's submission and durable Job-service domain.
package job

import (
	"time"

	"github.com/purinliang/mill/internal/execution"
)

// State is the durable lifecycle state of one job.
type State string

const (
	// StatePreparing means Mill is partitioning input and creating tasks.
	StatePreparing State = "preparing"

	// StateRunning means at least one task is available or active.
	StateRunning State = "running"

	// StateCompleted means every task completed successfully.
	StateCompleted State = "completed"

	// StateFailed means at least one task exhausted its retry budget.
	StateFailed State = "failed"
)

// Output identifies the object-storage root assigned to a job.
type Output struct {
	URI string `json:"uri"`
}

// Progress counts logical tasks by their current state.
type Progress struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

// Job is the durable status representation returned by the Job service.
type Job struct {
	ID            string              `json:"id"`
	State         State               `json:"state"`
	Executable    Executable          `json:"executable"`
	Input         Input               `json:"input"`
	Output        Output              `json:"output"`
	Parallelism   int                 `json:"parallelism"`
	ResourceClass ResourceClass       `json:"resource_class"`
	Resources     execution.Resources `json:"resources"`
	Progress      Progress            `json:"progress"`
	Results       []Result            `json:"results,omitempty"`
	CreatedAt     time.Time           `json:"created_at"`
	UpdatedAt     time.Time           `json:"updated_at"`
}

// Result identifies the successful output of one logical task.
type Result struct {
	TaskID     string `json:"task_id"`
	ShardIndex int    `json:"shard_index"`
	AttemptID  string `json:"attempt_id"`
	URI        string `json:"uri"`
}
