package job

import (
	"errors"
	"strings"
	"testing"
	"time"
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

func TestValidateLease(t *testing.T) {
	for _, test := range []struct {
		owner    string
		duration time.Duration
	}{
		{"", 15 * time.Second},
		{" executor", 15 * time.Second},
		{strings.Repeat("x", 256), 15 * time.Second},
		{"executor", 0},
		{"executor", 1500 * time.Millisecond},
		{"executor", 6 * time.Minute},
	} {
		if _, err := validateLease(test.owner, test.duration); err == nil {
			t.Errorf("validateLease(%q, %s) succeeded", test.owner, test.duration)
		}
	}
	if seconds, err := validateLease("executor", 15*time.Second); err != nil || seconds != 15 {
		t.Fatalf("valid lease = %d, %v", seconds, err)
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

func TestDeriveS3AttemptOutputURI(t *testing.T) {
	got, err := deriveAttemptOutputURI("s3://mill-output/jobs/job-1/", 7, "attempt-2")
	if err != nil {
		t.Fatal(err)
	}
	if got != "s3://mill-output/jobs/job-1/tasks/7/attempts/attempt-2/result.jsonl" {
		t.Fatalf("output URI = %q", got)
	}
}

func TestInvalidAttemptTransitionWrapsSentinel(t *testing.T) {
	err := invalidAttemptTransition(AttemptStateCompleted, AttemptStateRunning)
	if !errors.Is(err, ErrInvalidAttemptTransition) {
		t.Fatalf("error = %v, want %v", err, ErrInvalidAttemptTransition)
	}
}
