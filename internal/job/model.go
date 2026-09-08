// This file defines job submissions, status, progress, and result models.
package job

import (
	"time"

	"github.com/purinliang/mill/internal/execution"
)

type State string

const (
	StatePreparing State = "preparing"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
)

type Executable = execution.Executable

type InputSpec struct {
	URI string `json:"uri"`
}

type Input struct {
	URI         string `json:"uri"`
	SHA256      string `json:"sha256,omitempty"`
	RecordCount int64  `json:"record_count,omitempty"`
}

type Output struct {
	URI string `json:"uri"`
}

type Submission struct {
	Executable    Executable    `json:"executable"`
	Input         InputSpec     `json:"input"`
	ResourceClass ResourceClass `json:"resource_class,omitempty"`
}

type Progress struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

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

type Result struct {
	TaskID     string `json:"task_id"`
	ShardIndex int    `json:"shard_index"`
	AttemptID  string `json:"attempt_id"`
	URI        string `json:"uri"`
}
