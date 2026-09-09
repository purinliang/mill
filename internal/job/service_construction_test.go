// This file tests construction of the public Job service.
package job_test

import (
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/job"
)

func TestNewServiceValidatesDependenciesAndParallelism(t *testing.T) {
	store := &resourceCaptureStore{}
	partitioner := resourceTestPartitioner{}

	if _, err := job.NewService(nil, partitioner, 3); err == nil ||
		!strings.Contains(err.Error(), "store") {
		t.Fatalf("nil store error = %v", err)
	}
	if _, err := job.NewService(store, nil, 3); err == nil ||
		!strings.Contains(err.Error(), "partitioner") {
		t.Fatalf("nil partitioner error = %v", err)
	}
	for _, parallelism := range []int{0, job.MaxParallelism + 1} {
		_, err := job.NewService(store, partitioner, parallelism)
		if err == nil || !strings.Contains(err.Error(), "MILL_PARALLELISM") {
			t.Fatalf("parallelism %d error = %v", parallelism, err)
		}
	}
	if _, err := job.NewService(store, partitioner, 3); err != nil {
		t.Fatalf("valid configuration: %v", err)
	}
}
