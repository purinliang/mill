package job_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/job"
)

type changingInput struct {
	contents []string
	opens    int
}

func (i *changingInput) Open(context.Context, string) (io.ReadCloser, error) {
	if i.opens >= len(i.contents) {
		return nil, errors.New("test input opened too many times")
	}
	contents := i.contents[i.opens]
	i.opens++
	return io.NopCloser(strings.NewReader(contents)), nil
}

func TestPartitionPlanRejectsInputChangedBetweenScans(t *testing.T) {
	input := &changingInput{contents: []string{
		"{\"record\":1}\n{\"record\":2}\n",
		"{\"record\":1}\n{\"record\":3}\n",
	}}
	partitioner := job.NewJSONLPartitioner(input)

	_, err := partitioner.Plan(context.Background(), "s3://mill-input/records.jsonl", 2)
	if err == nil {
		t.Fatal("Plan succeeded after the input changed between scans")
	}
	var validationError *job.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("Plan error type = %T, want *job.ValidationError", err)
	}
	if validationError.Field != "input.uri" || !strings.Contains(validationError.Problem, "changed") {
		t.Fatalf("Plan error = %+v", validationError)
	}
	if input.opens != 2 {
		t.Fatalf("input opened %d times, want 2", input.opens)
	}
}
