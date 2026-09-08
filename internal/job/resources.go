package job

import "github.com/purinliang/mill/internal/execution"

type ResourceClass string

const (
	ResourceClassSmall  ResourceClass = "small"
	ResourceClassMedium ResourceClass = "medium"
	ResourceClassLarge  ResourceClass = "large"
)

const mebibyte int64 = 1024 * 1024

func resolveResources(class ResourceClass) (execution.Resources, bool) {
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
