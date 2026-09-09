// This file describes user submission and persisted input data.
package job

import "github.com/purinliang/mill/internal/execution"

// Executable identifies the trusted OCI image and its user arguments.
type Executable = execution.Executable

// InputSpec is the input supplied when submitting a job.
type InputSpec struct {
	URI string `json:"uri"`
}

// Input records the submitted URI and its identity after partitioning.
type Input struct {
	URI         string `json:"uri"`
	SHA256      string `json:"sha256,omitempty"`
	RecordCount int64  `json:"record_count,omitempty"`
}

// Submission is the user-provided intent for one batch job.
type Submission struct {
	Executable    Executable    `json:"executable"`
	Input         InputSpec     `json:"input"`
	ResourceClass ResourceClass `json:"resource_class,omitempty"`
}
