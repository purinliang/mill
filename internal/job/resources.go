// This file maps workload resource classes to concrete resource bounds.
package job

import "github.com/purinliang/mill/internal/execution"

// ResourceClass names one server-defined workload resource profile.
type ResourceClass string

const (
	// ResourceClassSmall is the default low-memory workload profile.
	ResourceClassSmall ResourceClass = "small"

	// ResourceClassMedium is the intermediate workload profile.
	ResourceClassMedium ResourceClass = "medium"

	// ResourceClassLarge permits the largest V1 workload memory allocation.
	ResourceClassLarge ResourceClass = "large"
)

const mebibyte int64 = 1024 * 1024

// ResolveResources returns the runtime-neutral resources for class.
func ResolveResources(class ResourceClass) (execution.Resources, bool) {
	memory := int64(0)
	switch class {
	case ResourceClassSmall:
		memory = 128 * mebibyte
	case ResourceClassMedium:
		memory = 512 * mebibyte
	case ResourceClassLarge:
		memory = 2 * 1024 * mebibyte
	default:
		return execution.Resources{}, false
	}
	return execution.Resources{
		CPURequestMillis:   100,
		CPULimitMillis:     1000,
		MemoryRequestBytes: memory,
		MemoryLimitBytes:   memory,
	}, true
}
