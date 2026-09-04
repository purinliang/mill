package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunMergesPartialCountFiles(t *testing.T) {
	directory := t.TempDir()
	first := filepath.Join(directory, "first.jsonl")
	second := filepath.Join(directory, "second.jsonl")
	if err := os.WriteFile(first, []byte("{\"word\":\"mill\",\"count\":2}\n{\"word\":\"well-known\",\"count\":1}\n"), 0o600); err != nil {
		t.Fatalf("write first partial counts: %v", err)
	}
	if err := os.WriteFile(second, []byte("{\"word\":\"batch\",\"count\":3}\n{\"word\":\"mill\",\"count\":4}\n"), 0o600); err != nil {
		t.Fatalf("write second partial counts: %v", err)
	}

	var output strings.Builder
	if err := run([]string{first, second}, &output); err != nil {
		t.Fatalf("merge partial counts: %v", err)
	}
	want := "" +
		"{\"word\":\"batch\",\"count\":3}\n" +
		"{\"word\":\"mill\",\"count\":6}\n" +
		"{\"word\":\"well-known\",\"count\":1}\n"
	if output.String() != want {
		t.Fatalf("output = %q, want %q", output.String(), want)
	}
}

func TestRunRequiresValidPartialCountFiles(t *testing.T) {
	if err := run(nil, &strings.Builder{}); err == nil {
		t.Fatal("run without partial counts succeeded, want an error")
	}
	if err := run([]string{filepath.Join(t.TempDir(), "missing.jsonl")}, &strings.Builder{}); err == nil {
		t.Fatal("run with missing partial counts succeeded, want an error")
	}
}
