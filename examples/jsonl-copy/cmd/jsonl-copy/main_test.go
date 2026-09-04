package main

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/purinliang/mill/internal/workload"
)

func TestRunCopiesOnlyAssignedLogicalShard(t *testing.T) {
	directory := t.TempDir()
	inputFilename := filepath.Join(directory, "input.jsonl")
	outputFilename := filepath.Join(directory, "results", "task-1.jsonl")
	records := []byte("{\"record\":0}\n{\"record\":1}\n{\"record\":2}\n{\"record\":3}\n")
	if err := os.WriteFile(inputFilename, records, 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	start := int64(len("{\"record\":0}\n"))
	end := start + int64(len("{\"record\":1}\n{\"record\":2}\n"))
	arguments := commandArguments(t, workload.Invocation{
		JobID:          "job-001",
		TaskID:         "task-001",
		ShardIndex:     1,
		InputURI:       fileURI(inputFilename),
		InputStartByte: start,
		InputEndByte:   end,
		OutputURI:      fileURI(outputFilename),
		ExecutableArgs: []string{},
	})
	if err := run(arguments); err != nil {
		t.Fatalf("run workload: %v", err)
	}

	output, err := os.ReadFile(outputFilename)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	want := []byte("{\"record\":1}\n{\"record\":2}\n")
	if string(output) != string(want) {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestRunRejectsRangeBeyondInput(t *testing.T) {
	inputFilename := filepath.Join(t.TempDir(), "input.jsonl")
	if err := os.WriteFile(inputFilename, []byte("1\n"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	arguments := commandArguments(t, workload.Invocation{
		JobID:          "job-001",
		TaskID:         "task-001",
		ShardIndex:     0,
		InputURI:       fileURI(inputFilename),
		InputStartByte: 0,
		InputEndByte:   3,
		OutputURI:      fileURI(filepath.Join(t.TempDir(), "output.jsonl")),
		ExecutableArgs: []string{},
	})
	if err := run(arguments); err == nil {
		t.Fatal("run succeeded, want an error")
	}
}

func TestRunRejectsUnsupportedURIAndExecutableArguments(t *testing.T) {
	invocation := workload.Invocation{
		JobID:          "job-001",
		TaskID:         "task-001",
		ShardIndex:     0,
		InputURI:       "s3://bucket/input.jsonl",
		InputStartByte: 0,
		InputEndByte:   10,
		OutputURI:      "file:///tmp/output.jsonl",
		ExecutableArgs: []string{},
	}
	if err := run(commandArguments(t, invocation)); err == nil {
		t.Fatal("run with S3 input succeeded, want an error")
	}

	inputFilename := filepath.Join(t.TempDir(), "input.jsonl")
	if err := os.WriteFile(inputFilename, []byte("1\n"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}
	invocation.InputURI = fileURI(inputFilename)
	invocation.InputEndByte = 2
	invocation.ExecutableArgs = []string{"--unexpected"}
	if err := run(commandArguments(t, invocation)); err == nil {
		t.Fatal("run with executable arguments succeeded, want an error")
	}
}

func commandArguments(t *testing.T, invocation workload.Invocation) []string {
	t.Helper()
	arguments, err := invocation.CommandArgs()
	if err != nil {
		t.Fatalf("build command arguments: %v", err)
	}
	return arguments
}

func fileURI(filename string) string {
	return (&url.URL{Scheme: "file", Path: filename}).String()
}
