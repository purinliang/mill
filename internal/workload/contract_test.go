// This file tests workload argument serialization and parsing details.

package workload_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/workload"
)

func TestInvocationCommandArgsRoundTrip(t *testing.T) {
	want := workload.Invocation{
		JobID:          "job-001",
		TaskID:         "task-007",
		ShardIndex:     7,
		InputURI:       "file:///tmp/input.jsonl",
		InputStartByte: 120,
		InputEndByte:   240,
		OutputURI:      "file:///tmp/output.jsonl",
		ExecutableArgs: []string{"--mode", "fast", "positional value"},
	}

	arguments, err := want.CommandArgs()
	if err != nil {
		t.Fatalf("build command arguments: %v", err)
	}
	got, err := workload.ParseArgs(arguments)
	if err != nil {
		t.Fatalf("parse command arguments: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parsed invocation = %+v, want %+v", got, want)
	}
}

func TestInvocationCommandArgsUsesSeparatorForExecutableArguments(t *testing.T) {
	invocation := validInvocation()
	invocation.ExecutableArgs = []string{"--job-id", "this-belongs-to-the-user"}

	arguments, err := invocation.CommandArgs()
	if err != nil {
		t.Fatalf("build command arguments: %v", err)
	}
	parsed, err := workload.ParseArgs(arguments)
	if err != nil {
		t.Fatalf("parse command arguments: %v", err)
	}
	if !reflect.DeepEqual(parsed.ExecutableArgs, invocation.ExecutableArgs) {
		t.Fatalf("executable arguments = %v, want %v", parsed.ExecutableArgs, invocation.ExecutableArgs)
	}
}

func TestParseArgsRejectsInvalidContract(t *testing.T) {
	validArguments, err := validInvocation().CommandArgs()
	if err != nil {
		t.Fatalf("build valid command arguments: %v", err)
	}

	tests := map[string][]string{
		"missing separator": validArguments[:len(validArguments)-1],
		"unknown flag":      append([]string{"--unknown", "value"}, validArguments...),
		"missing job ID":    replaceArgument(validArguments, "--job-id", ""),
		"negative shard":    replaceArgument(validArguments, "--shard-index", "-1"),
		"empty range":       replaceArgument(validArguments, "--input-end-byte", "0"),
		"relative input":    replaceArgument(validArguments, "--input-uri", "input.jsonl"),
	}

	for name, arguments := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := workload.ParseArgs(arguments); err == nil {
				t.Fatal("ParseArgs succeeded, want an error")
			}
		})
	}
}

func TestParseArgsProducesNonNilEmptyExecutableArgs(t *testing.T) {
	arguments, err := validInvocation().CommandArgs()
	if err != nil {
		t.Fatalf("build command arguments: %v", err)
	}
	parsed, err := workload.ParseArgs(arguments)
	if err != nil {
		t.Fatalf("parse command arguments: %v", err)
	}
	if parsed.ExecutableArgs == nil || len(parsed.ExecutableArgs) != 0 {
		t.Fatalf("executable arguments = %v, want non-nil empty slice", parsed.ExecutableArgs)
	}
}

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
			invocation := validInvocation()
			mutate(&invocation)

			if _, err := invocation.CommandArgs(); err == nil {
				t.Fatal("CommandArgs succeeded, want validation error")
			}
		})
	}
}

func TestParseArgsRejectsPositionalMillArguments(t *testing.T) {
	arguments, err := validInvocation().CommandArgs()
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
	arguments = append(
		arguments[:separator],
		append([]string{"unexpected"}, arguments[separator:]...)...,
	)

	_, err = workload.ParseArgs(arguments)
	if err == nil || !strings.Contains(err.Error(), "named flags") {
		t.Fatalf(
			"ParseArgs error = %v, want named-flags validation error",
			err,
		)
	}
}

func validInvocation() workload.Invocation {
	return workload.Invocation{
		JobID:          "job-001",
		TaskID:         "task-001",
		ShardIndex:     0,
		InputURI:       "s3://mill-input/records.jsonl?versionId=one",
		InputStartByte: 0,
		InputEndByte:   100,
		OutputURI:      "s3://mill-output/jobs/job-001/tasks/0/result.jsonl",
		ExecutableArgs: []string{},
	}
}

func replaceArgument(arguments []string, flagName, value string) []string {
	replaced := append([]string(nil), arguments...)
	for index, argument := range replaced {
		if argument == flagName {
			replaced[index+1] = value
			return replaced
		}
	}
	panic("flag not found: " + strings.TrimPrefix(flagName, "--"))
}
