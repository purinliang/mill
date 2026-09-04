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
	ManifestURI    string `json:"manifest_uri"`
	ManifestSHA256 string `json:"manifest_sha256,omitempty"`
}

type Output struct {
	URI string `json:"uri"`
}

type Submission struct {
	Workload Workload `json:"workload"`
	Dataset  Dataset  `json:"dataset"`
}

type Progress struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Running   int `json:"running"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

type Job struct {
	ID        string    `json:"id"`
	State     State     `json:"state"`
	Workload  Workload  `json:"workload"`
	Dataset   Dataset   `json:"dataset"`
	Output    Output    `json:"output"`
	Progress  Progress  `json:"progress"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
