package job

import "time"

type State string

type AttemptState string

const (
	StatePreparing State = "preparing"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
)

const (
	AttemptStateStarting  AttemptState = "starting"
	AttemptStateRunning   AttemptState = "running"
	AttemptStateCompleted AttemptState = "completed"
	AttemptStateFailed    AttemptState = "failed"
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

type Attempt struct {
	ID             string
	JobID          string
	TaskID         string
	Number         int
	Executor       string
	State          AttemptState
	ExternalID     string
	FailureMessage string
	CreatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
	UpdatedAt      time.Time
}

type ClaimedAttempt struct {
	Attempt        Attempt
	Executable     Executable
	ShardIndex     int
	InputURI       string
	InputStartByte int64
	InputEndByte   int64
	OutputURI      string
}
