package job

import "time"

type State string

const (
	StatePreparing State = "preparing"
	StateRunning   State = "running"
	StateCompleted State = "completed"
	StateFailed    State = "failed"
)

type Workload struct {
	Image string   `json:"image"`
	Args  []string `json:"args"`
}

type Dataset struct {
	ManifestURI string `json:"manifest_uri"`
}

type Output struct {
	URI string `json:"uri"`
}

type Submission struct {
	Workload Workload `json:"workload"`
	Dataset  Dataset  `json:"dataset"`
}

type Job struct {
	ID        string    `json:"id"`
	State     State     `json:"state"`
	Workload  Workload  `json:"workload"`
	Dataset   Dataset   `json:"dataset"`
	Output    Output    `json:"output"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
