// This file tests the public Job service and its workflow policy.
package job_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/execution"
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

func TestServiceResolvesWorkloadResourceClass(t *testing.T) {
	tests := []struct {
		name      string
		class     job.ResourceClass
		memoryMiB int64
	}{
		{name: "default", memoryMiB: 128},
		{name: "small", class: job.ResourceClassSmall, memoryMiB: 128},
		{name: "medium", class: job.ResourceClassMedium, memoryMiB: 512},
		{name: "large", class: job.ResourceClassLarge, memoryMiB: 2048},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &resourceCaptureStore{}
			service, err := job.NewService(
				store,
				resourceTestPartitioner{},
				3,
			)
			if err != nil {
				t.Fatal(err)
			}

			_, _, err = service.Create(
				context.Background(),
				"resource-policy-"+test.name,
				job.Submission{
					Executable: job.Executable{Image: "mill/example:dev"},
					Input: job.InputSpec{
						URI: "file:///tmp/records.jsonl",
					},
					ResourceClass: test.class,
				},
			)
			if err != nil {
				t.Fatal(err)
			}

			wantMemory := test.memoryMiB << 20
			want := execution.Resources{
				CPURequestMillis:   100,
				CPULimitMillis:     1000,
				MemoryRequestBytes: wantMemory,
				MemoryLimitBytes:   wantMemory,
			}
			if store.resources != want {
				t.Fatalf("resources = %+v, want %+v", store.resources, want)
			}
		})
	}
}

type resourceCaptureStore struct {
	resources execution.Resources
}

func (s *resourceCaptureStore) FindSubmission(
	context.Context,
	string,
	job.Submission,
) (job.Job, bool, error) {
	return job.Job{}, false, nil
}

func (s *resourceCaptureStore) Create(
	_ context.Context,
	_ string,
	_ job.Submission,
	_ string,
	_ int64,
	_ int,
	resources execution.Resources,
) (job.Job, bool, error) {
	s.resources = resources
	return job.Job{ID: "test-job", State: job.StatePreparing}, true, nil
}

func (s *resourceCaptureStore) Materialize(
	context.Context,
	string,
	job.ShardSet,
) (job.Job, error) {
	return job.Job{ID: "test-job", State: job.StateRunning}, nil
}

func (s *resourceCaptureStore) Get(
	context.Context,
	string,
) (job.Job, error) {
	return job.Job{}, nil
}

func (s *resourceCaptureStore) CompletedResults(
	context.Context,
	string,
) ([]job.Result, error) {
	return nil, nil
}

type resourceTestPartitioner struct{}

func (resourceTestPartitioner) Partition(
	context.Context,
	string,
	int,
) (job.ShardSet, error) {
	input := []byte("{\"text\":\"test record\"}\n")
	digest := sha256.Sum256(input)

	return job.ShardSet{
		InputSHA256: fmt.Sprintf("%x", digest),
		RecordCount: 1,
		Shards: []job.LogicalShard{
			{StartByte: 0, EndByte: int64(len(input))},
		},
	}, nil
}
