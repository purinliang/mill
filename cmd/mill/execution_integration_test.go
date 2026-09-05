package main

import (
	"strings"
	"testing"
)

func TestExecutorInstanceIDsAreUnique(t *testing.T) {
	first, err := newExecutorInstanceID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := newExecutorInstanceID()
	if err != nil {
		t.Fatal(err)
	}
	if first == second || !strings.HasPrefix(first, "executor-") || len(first) != len("executor-")+32 {
		t.Fatalf("instance IDs = %q and %q", first, second)
	}
}
