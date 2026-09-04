package job

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateExecutor(t *testing.T) {
	for _, executor := range []string{"", " docker", "docker ", strings.Repeat("x", 64)} {
		if err := validateExecutor(executor); err == nil {
			t.Errorf("validateExecutor(%q) succeeded, want an error", executor)
		}
	}
	if err := validateExecutor("docker"); err != nil {
		t.Fatalf("validate Docker executor: %v", err)
	}
}

func TestDeriveAttemptOutputURI(t *testing.T) {
	got, err := deriveAttemptOutputURI(
		"file:///tmp/mill-output/jobs/job-1/",
		7,
		"attempt-2",
	)
	if err != nil {
		t.Fatalf("derive attempt output URI: %v", err)
	}
	want := "file:///tmp/mill-output/jobs/job-1/tasks/7/attempts/attempt-2/result.jsonl"
	if got != want {
		t.Fatalf("output URI = %q, want %q", got, want)
	}
}

func TestInvalidAttemptTransitionWrapsSentinel(t *testing.T) {
	err := invalidAttemptTransition(AttemptStateCompleted, AttemptStateRunning)
	if !errors.Is(err, ErrInvalidAttemptTransition) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidAttemptTransition)
	}
}
