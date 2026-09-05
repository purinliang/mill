package main

import (
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/purinliang/mill/internal/workload"
)

func TestFailOnceThenDelegateWithoutChangingInvocation(t *testing.T) {
	root := t.TempDir()
	inv := testInvocation(t, root, "once")
	calls := 0
	execute := func(args []string) error {
		calls++
		got, err := workload.ParseArgs(args)
		if err != nil || len(got.ExecutableArgs) != 0 || got.TaskID != inv.TaskID || got.InputEndByte != inv.InputEndByte || got.OutputURI != inv.OutputURI {
			t.Fatalf("delegated invocation=%+v err=%v", got, err)
		}
		return nil
	}
	invoke := func() error {
		args, err := inv.CommandArgs()
		if err != nil {
			t.Fatal(err)
		}
		return run(args, filepath.Join(root, "markers"), func(time.Duration) {}, execute)
	}
	if err := invoke(); err == nil || !strings.Contains(err.Error(), "injected failure") || calls != 0 {
		t.Fatalf("first attempt err=%v calls=%d", err, calls)
	}
	uri, _ := url.Parse(inv.OutputURI)
	if data, err := os.ReadFile(uri.Path); err != nil || !strings.Contains(string(data), "injected-invalid-output") {
		t.Fatalf("missing poisoned partial output: %s %v", data, err)
	}
	// A fresh invocation models a replacement Pod with a new attempt output path.
	inv.OutputURI = (&url.URL{Scheme: "file", Path: filepath.Join(root, "attempt-2", "result.jsonl")}).String()
	if err := invoke(); err != nil || calls != 1 {
		t.Fatalf("second attempt err=%v calls=%d", err, calls)
	}
	inv.JobID = "another-job"
	if err := invoke(); err == nil || calls != 1 {
		t.Fatal("another job inherited the first job's marker")
	}
}

func TestPermanentFailureAndOtherShards(t *testing.T) {
	root := t.TempDir()
	inv := testInvocation(t, root, "always")
	want := errors.New("mapper exit")
	calls := 0
	execute := func([]string) error { calls++; return want }
	for range 4 {
		args, _ := inv.CommandArgs()
		if err := run(args, root, func(time.Duration) {}, execute); err == nil || !strings.Contains(err.Error(), "injected failure") || calls != 0 {
			t.Fatalf("permanent injection: %v calls=%d", err, calls)
		}
	}
	inv.ShardIndex = 1
	args, _ := inv.CommandArgs()
	if err := run(args, root, func(time.Duration) {}, execute); !errors.Is(err, want) || calls != 1 {
		t.Fatalf("other shard should delegate and preserve error: %v calls=%d", err, calls)
	}
}

func TestRejectBadModeAndUnsafeMarkerIdentity(t *testing.T) {
	for _, mode := range []string{"", "random", "once"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			inv := testInvocation(t, root, mode)
			if mode == "once" {
				inv.TaskID = "../escape"
			}
			args, _ := inv.CommandArgs()
			if err := run(args, root, func(time.Duration) {}, func([]string) error { t.Fatal("unexpected execution"); return nil }); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestDelayHoldsOnlyTheInitialWaveThenDelegates(t *testing.T) {
	root := t.TempDir()
	var pauses []time.Duration
	executions := 0
	for shard := 0; shard < 4; shard++ {
		inv := testInvocation(t, root, "delay")
		inv.ShardIndex = shard
		args, err := inv.CommandArgs()
		if err != nil {
			t.Fatal(err)
		}
		if err := run(args, root, func(delay time.Duration) { pauses = append(pauses, delay) }, func([]string) error {
			executions++
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if executions != 4 || len(pauses) != 3 {
		t.Fatalf("executions=%d pauses=%v", executions, pauses)
	}
	for _, delay := range pauses {
		if delay != 15*time.Second {
			t.Fatalf("delay=%s, want 15s", delay)
		}
	}
}

func testInvocation(t *testing.T, root, mode string) workload.Invocation {
	t.Helper()
	return workload.Invocation{JobID: "job", TaskID: "task", ShardIndex: 0,
		InputURI: "file:///data/records.jsonl", InputStartByte: 0, InputEndByte: 10,
		OutputURI:      (&url.URL{Scheme: "file", Path: filepath.Join(root, "attempt-1", "result.jsonl")}).String(),
		ExecutableArgs: []string{mode}}
}
