// This file describes runtime-neutral workload configuration.
package execution

// Executable identifies the trusted OCI image and its user arguments.
type Executable struct {
	Image string   `json:"image"`
	Args  []string `json:"args"`
}

// Resources defines runtime-neutral CPU and memory requests and limits.
type Resources struct {
	CPURequestMillis   int64 `json:"cpu_request_millis"`
	CPULimitMillis     int64 `json:"cpu_limit_millis"`
	MemoryRequestBytes int64 `json:"memory_request_bytes"`
	MemoryLimitBytes   int64 `json:"memory_limit_bytes"`
}
