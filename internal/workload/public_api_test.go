// This file tests the public workload contract and validation behavior.
package workload_test

import (
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/workload"
)

func TestPublicContractRejectsEveryInvalidInvocationField(t *testing.T) {
	tests := map[string]func(*workload.Invocation){
		"missing job ID": func(invocation *workload.Invocation) {
			invocation.JobID = "  "
		},
		"missing task ID": func(invocation *workload.Invocation) {
			invocation.TaskID = "  "
		},
		"negative shard index": func(invocation *workload.Invocation) {
			invocation.ShardIndex = -1
		},
		"negative input start": func(invocation *workload.Invocation) {
			invocation.InputStartByte = -1
		},
		"empty input range": func(invocation *workload.Invocation) {
			invocation.InputEndByte = invocation.InputStartByte
		},
		"relative input URI": func(invocation *workload.Invocation) {
			invocation.InputURI = "input.jsonl"
		},
		"relative output URI": func(invocation *workload.Invocation) {
			invocation.OutputURI = "output.jsonl"
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			invocation := validPublicInvocation()
			mutate(&invocation)

			if _, err := invocation.CommandArgs(); err == nil {
				t.Fatal("CommandArgs succeeded, want validation error")
			}
		})
	}
}

func TestParseArgsRejectsPositionalMillArguments(t *testing.T) {
	arguments, err := validPublicInvocation().CommandArgs()
	if err != nil {
		t.Fatal(err)
	}
	separator := -1
	for index, argument := range arguments {
		if argument == "--" {
			separator = index
			break
		}
	}
	if separator < 0 {
		t.Fatal("serialized arguments have no separator")
	}
	arguments = append(arguments[:separator], append([]string{"unexpected"}, arguments[separator:]...)...)

	_, err = workload.ParseArgs(arguments)
	if err == nil || !strings.Contains(err.Error(), "named flags") {
		t.Fatalf("ParseArgs error = %v, want named-flags validation error", err)
	}
}

func validPublicInvocation() workload.Invocation {
	return workload.Invocation{
		JobID:          "job-001",
		TaskID:         "task-001",
		ShardIndex:     0,
		InputURI:       "s3://mill-input/records.jsonl",
		InputStartByte: 0,
		InputEndByte:   100,
		OutputURI:      "s3://mill-output/jobs/job-001/tasks/0/result.jsonl",
		ExecutableArgs: []string{},
	}
}
