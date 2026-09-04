package job

import "time"

type State string

const (
	StatePreparing State = "preparing"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
)

type Executable struct {
	Image string   `json:"image"`
	Args  []string `json:"args"`
}

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
	Executable Executable `json:"executable"`
	Input      InputSpec  `json:"input"`
}

type Progress struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

type Job struct {
	ID          string     `json:"id"`
	State       State      `json:"state"`
	Executable  Executable `json:"executable"`
	Input       Input      `json:"input"`
	Output      Output     `json:"output"`
	Parallelism int        `json:"parallelism"`
	Progress    Progress   `json:"progress"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}
