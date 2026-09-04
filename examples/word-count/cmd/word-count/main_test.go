package main

import (
	"net/url"
	"os"
	"path/filepath"
	"testing"

	"github.com/purinliang/mill/internal/workload"
)

func TestRunCountsOnlyAssignedLogicalShard(t *testing.T) {
	directory := t.TempDir()
	inputFilename := filepath.Join(directory, "input.jsonl")
	outputFilename := filepath.Join(directory, "results", "task-1.jsonl")
	first := []byte("{\"text\":\"ignored\"}\n")
	assigned := []byte("{\"text\":\"Hello, well-known world!\"}\n{\"text\":\"WORLD; end-to-end.\"}\n")
	last := []byte("{\"text\":\"also ignored\"}\n")
	contents := append(append(append([]byte{}, first...), assigned...), last...)
	if err := os.WriteFile(inputFilename, contents, 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	arguments := commandArguments(t, workload.Invocation{
		JobID:          "job-001",
		TaskID:         "task-001",
		ShardIndex:     1,
		InputURI:       fileURI(inputFilename),
		InputStartByte: int64(len(first)),
		InputEndByte:   int64(len(first) + len(assigned)),
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
	want := "" +
		"{\"word\":\"end-to-end\",\"count\":1}\n" +
		"{\"word\":\"hello\",\"count\":1}\n" +
		"{\"word\":\"well-known\",\"count\":1}\n" +
		"{\"word\":\"world\",\"count\":2}\n"
	if string(output) != want {
		t.Fatalf("output = %q, want %q", output, want)
	}
}

func TestRunRejectsInvalidInputAndExecutableArguments(t *testing.T) {
	directory := t.TempDir()
	inputFilename := filepath.Join(directory, "input.jsonl")
	if err := os.WriteFile(inputFilename, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write input: %v", err)
	}

	invocation := workload.Invocation{
		JobID:          "job-001",
		TaskID:         "task-001",
		ShardIndex:     0,
		InputURI:       fileURI(inputFilename),
		InputStartByte: 0,
		InputEndByte:   3,
		OutputURI:      fileURI(filepath.Join(directory, "output.jsonl")),
		ExecutableArgs: []string{},
	}
	if err := run(commandArguments(t, invocation)); err == nil {
		t.Fatal("run without text succeeded, want an error")
	}

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
