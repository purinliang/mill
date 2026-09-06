package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/purinliang/mill/internal/coordinator"
	"github.com/purinliang/mill/internal/execution"
)

type unavailableStore struct {
	cancel context.CancelFunc
	calls  int
}

func (s *unavailableStore) LeaseActiveAttempts(context.Context, string, string) ([]execution.ClaimedAttempt, error) {
	s.calls++
	if s.calls == 2 {
		s.cancel()
	}
	return nil, errors.New("Job service unavailable")
}

func (s *unavailableStore) ClaimNextAttempt(context.Context, string, string) (execution.ClaimedAttempt, error) {
	return execution.ClaimedAttempt{}, execution.ErrNoTaskAvailable
}

func (s *unavailableStore) MarkAttemptRunning(context.Context, string, string, string) (execution.Attempt, error) {
	return execution.Attempt{}, nil
}

func (s *unavailableStore) CompleteAttempt(context.Context, string, string) (execution.Attempt, error) {
	return execution.Attempt{}, nil
}

func (s *unavailableStore) FailAttempt(context.Context, string, string, string) (execution.Attempt, error) {
	return execution.Attempt{}, nil
}

func TestConfigFromEnvironment(t *testing.T) {
	values := map[string]string{
		"MILL_JOB_GRPC_TARGET":                "127.0.0.1:9090",
		"MILL_EXECUTION_RPC_TIMEOUT":          "2s",
		"MILL_KUBE_CONTEXT":                   "kind-mill",
		"MILL_KUBE_NAMESPACE":                 "default",
		"MILL_KUBE_NODE":                      "mill-control-plane",
		"MILL_LOCAL_ROOT":                     "/tmp/mill",
		"MILL_NODE_ROOT":                      "/var/local/mill",
		"MILL_WORKLOAD_S3_REGION":             "us-east-1",
		"MILL_WORKLOAD_S3_ENDPOINT":           "http://storage:9000",
		"MILL_WORKLOAD_S3_CREDENTIALS_SECRET": "mill-storage",
	}
	configuration, err := configFromEnvironment(func(key string) string { return values[key] })
	if err != nil {
		t.Fatal(err)
	}
	if configuration.jobGRPCTarget != "127.0.0.1:9090" || configuration.rpcTimeout != 2*time.Second {
		t.Fatalf("RPC configuration = %+v", configuration)
	}
	if configuration.kubernetes.Context != "kind-mill" || configuration.kubernetes.Namespace != "default" ||
		configuration.kubernetes.S3CredentialsSecret != "mill-storage" {
		t.Fatalf("Kubernetes configuration = %+v", configuration.kubernetes)
	}
}

func TestConfigRequiresJobServiceAndBoundsTimeout(t *testing.T) {
	for _, target := range []string{"", " job:9090", strings.Repeat("x", 2049)} {
		values := map[string]string{"MILL_JOB_GRPC_TARGET": target}
		if _, err := configFromEnvironment(func(key string) string { return values[key] }); err == nil {
			t.Errorf("accepted Job service target %q", target)
		}
	}
	for _, timeout := range []string{"bad", "99ms", "31s"} {
		values := map[string]string{"MILL_JOB_GRPC_TARGET": "job:9090", "MILL_EXECUTION_RPC_TIMEOUT": timeout}
		if _, err := configFromEnvironment(func(key string) string { return values[key] }); err == nil {
			t.Errorf("accepted timeout %q", timeout)
		}
	}
}

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

func TestCoordinatorRetriesAfterJobServiceError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &unavailableStore{cancel: cancel}
	worker := &coordinator.Coordinator{Store: store, LeaseOwner: "executor-a"}
	err := runCoordinator(ctx, worker, time.Millisecond)
	if !errors.Is(err, context.Canceled) || store.calls != 2 {
		t.Fatalf("error = %v, lease calls = %d", err, store.calls)
	}
}
