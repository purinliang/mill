package jsonl_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/job"
	"github.com/purinliang/mill/internal/job/jsonl"
)

type changingInput struct {
	contents []string
	opens    int
}

type failingInput struct {
	err error
}

func (i failingInput) Open(context.Context, string) (io.ReadCloser, error) {
	return nil, i.err
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
	partitioner := jsonl.NewPlanner(input)

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

func TestPartitionPlanReportsSecondScanAndCancellationFailures(t *testing.T) {
	t.Run("second open fails", func(t *testing.T) {
		input := &changingThenFailingInput{contents: "{\"record\":1}\n"}
		partitioner := jsonl.NewPlanner(input)

		_, err := partitioner.Plan(context.Background(), "s3://mill-input/records.jsonl", 1)
		if err == nil || !strings.Contains(err.Error(), "second scan failed") {
			t.Fatalf("Plan error = %v", err)
		}
		if input.opens != 2 {
			t.Fatalf("input opened %d times, want 2", input.opens)
		}
	})

	t.Run("context already cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		partitioner := jsonl.NewPlanner(&changingInput{contents: []string{"{\"record\":1}\n"}})

		_, err := partitioner.Plan(ctx, "s3://mill-input/records.jsonl", 1)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Plan error = %v, want context cancellation", err)
		}
	})

	t.Run("input cannot be opened", func(t *testing.T) {
		partitioner := jsonl.NewPlanner(failingInput{err: errors.New("storage unavailable")})

		_, err := partitioner.Plan(context.Background(), "s3://mill-input/records.jsonl", 1)
		if err == nil || !strings.Contains(err.Error(), "storage unavailable") {
			t.Fatalf("Plan error = %v", err)
		}
	})
}

type changingThenFailingInput struct {
	contents string
	opens    int
}

func (i *changingThenFailingInput) Open(context.Context, string) (io.ReadCloser, error) {
	i.opens++
	if i.opens == 1 {
		return io.NopCloser(strings.NewReader(i.contents)), nil
	}
	return nil, errors.New("second scan failed")
}
