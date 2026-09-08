// This file tests the execution process through its public service boundary.
package main_test

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/execution/rpc"
	executionv1 "github.com/purinliang/mill/internal/execution/rpc/v1"
	"github.com/purinliang/mill/internal/workload"
)

type runningReport struct {
	attemptID string
	token     string
	external  string
}

type processBackend struct {
	mu            sync.Mutex
	claimed       bool
	claimExecutor string
	claimOwner    string
	reports       chan runningReport
}

func (b *processBackend) LeaseActiveAttempts(context.Context, string, string, time.Duration) ([]execution.ClaimedAttempt, error) {
	return nil, nil
}

func (b *processBackend) ClaimNextAttempt(_ context.Context, executor, owner string, leaseDuration time.Duration) (execution.ClaimedAttempt, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.claimed {
		return execution.ClaimedAttempt{}, execution.ErrNoTaskAvailable
	}
	b.claimed = true
	b.claimExecutor = executor
	b.claimOwner = owner
	now := time.Now().UTC()
	expires := now.Add(leaseDuration)
	return execution.ClaimedAttempt{
		Attempt: execution.Attempt{
			ID: "attempt-1", JobID: "job-1", TaskID: "task-1", Number: 1,
			Executor: executor, State: execution.AttemptStateStarting,
			CreatedAt: now, UpdatedAt: now, LeaseOwner: owner,
			LeaseToken: "token-1", LeaseExpiresAt: &expires,
		},
		Executable:     execution.Executable{Image: "mill/word-count:dev", Args: []string{"--demo"}},
		ShardIndex:     4,
		InputURI:       "s3://mill-input/records.jsonl",
		InputStartByte: 400,
		InputEndByte:   500,
		OutputURI:      "s3://mill-output/jobs/job-1/tasks/4/result.jsonl",
		Resources: execution.Resources{
			CPURequestMillis: 100, CPULimitMillis: 500,
			MemoryRequestBytes: 64 << 20, MemoryLimitBytes: 128 << 20,
		},
	}, nil
}

func (b *processBackend) MarkAttemptRunning(_ context.Context, id, token, externalID string) (execution.Attempt, error) {
	b.reports <- runningReport{attemptID: id, token: token, external: externalID}
	now := time.Now().UTC()
	return execution.Attempt{
		ID: id, JobID: "job-1", TaskID: "task-1", Number: 1,
		Executor: "kubernetes", State: execution.AttemptStateRunning,
		ExternalID: externalID, CreatedAt: now, StartedAt: &now, UpdatedAt: now,
		LeaseToken: token,
	}, nil
}

func (b *processBackend) CompleteAttempt(context.Context, string, string) (execution.Attempt, error) {
	return execution.Attempt{}, execution.ErrInvalidAttemptTransition
}

func (b *processBackend) FailAttempt(context.Context, string, string, string) (execution.Attempt, error) {
	return execution.Attempt{}, execution.ErrInvalidAttemptTransition
}

func TestExecutionProcessClaimsAndDispatchesThroughPublicServices(t *testing.T) {
	backend := &processBackend{reports: make(chan runningReport, 1)}
	grpcAddress := startPublicExecutionService(t, backend)
	kubernetesServer, createdJobs := startPublicKubernetesAPI(t)
	kubeContext := writeProcessKubeconfig(t, kubernetesServer.URL)

	binary := filepath.Join(t.TempDir(), "mill-execution")
	build := exec.Command("go", "build", "-o", binary, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build execution service: %v\n%s", err, output)
	}

	logFilename := filepath.Join(t.TempDir(), "execution.log")
	logFile, err := os.Create(logFilename)
	if err != nil {
		t.Fatal(err)
	}
	defer logFile.Close()
	process := exec.Command(binary)
	process.Env = executionEnvironment(map[string]string{
		"MILL_JOB_GRPC_TARGET":                grpcAddress,
		"MILL_EXECUTION_RPC_TIMEOUT":          "1s",
		"MILL_KUBE_CONTEXT":                   kubeContext,
		"MILL_KUBE_IN_CLUSTER":                "false",
		"MILL_KUBE_NAMESPACE":                 "mill-workloads",
		"MILL_KUBE_NODE":                      "",
		"MILL_LOCAL_ROOT":                     "",
		"MILL_NODE_ROOT":                      "",
		"MILL_WORKLOAD_S3_REGION":             "us-east-1",
		"MILL_WORKLOAD_S3_ENDPOINT":           "",
		"MILL_WORKLOAD_S3_CREDENTIALS_SECRET": "",
	})
	process.Stdout = logFile
	process.Stderr = logFile
	if err := process.Start(); err != nil {
		t.Fatalf("start execution service: %v", err)
	}
	waitResult := make(chan error, 1)
	go func() { waitResult <- process.Wait() }()
	exited := false
	t.Cleanup(func() {
		if !exited {
			_ = process.Process.Kill()
			<-waitResult
		}
	})

	var created *batchv1.Job
	select {
	case created = <-createdJobs:
	case err := <-waitResult:
		exited = true
		t.Fatalf("execution service exited before creating a Job: %v; logs:\n%s", err, readExecutionLog(logFilename))
	case <-time.After(10 * time.Second):
		t.Fatalf("execution service did not create a Job; logs:\n%s", readExecutionLog(logFilename))
	}
	var report runningReport
	select {
	case report = <-backend.reports:
	case err := <-waitResult:
		exited = true
		t.Fatalf("execution service exited before reporting dispatch: %v; logs:\n%s", err, readExecutionLog(logFilename))
	case <-time.After(10 * time.Second):
		t.Fatalf("execution service did not report dispatch; logs:\n%s", readExecutionLog(logFilename))
	}

	if report != (runningReport{attemptID: "attempt-1", token: "token-1", external: "kubernetes-job-uid-1"}) {
		t.Fatalf("running report = %+v", report)
	}
	backend.mu.Lock()
	claimExecutor, claimOwner := backend.claimExecutor, backend.claimOwner
	backend.mu.Unlock()
	if claimExecutor != "kubernetes" || !strings.HasPrefix(claimOwner, "execution-") {
		t.Fatalf("claim identity = executor %q owner %q", claimExecutor, claimOwner)
	}
	if created.Name != "mill-attempt-1" || created.Namespace != "mill-workloads" {
		t.Fatalf("created Job = %s/%s", created.Namespace, created.Name)
	}
	invocation, err := workload.ParseArgs(created.Spec.Template.Spec.Containers[0].Args)
	if err != nil {
		t.Fatalf("parse workload invocation: %v", err)
	}
	if invocation.JobID != "job-1" || invocation.TaskID != "task-1" || invocation.ShardIndex != 4 ||
		invocation.InputStartByte != 400 || invocation.InputEndByte != 500 ||
		invocation.InputURI != "s3://mill-input/records.jsonl" ||
		invocation.OutputURI != "s3://mill-output/jobs/job-1/tasks/4/result.jsonl" ||
		len(invocation.ExecutableArgs) != 1 || invocation.ExecutableArgs[0] != "--demo" {
		t.Fatalf("workload invocation = %+v", invocation)
	}

	if err := process.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal execution service: %v", err)
	}
	select {
	case err := <-waitResult:
		exited = true
		if err != nil {
			t.Fatalf("execution service shutdown: %v; logs:\n%s", err, readExecutionLog(logFilename))
		}
	case <-time.After(10 * time.Second):
		t.Fatal("execution service did not stop after SIGTERM")
	}
}

func startPublicExecutionService(t *testing.T, backend executionrpc.Backend) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	service, err := executionrpc.NewServer(backend, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	executionv1.RegisterExecutionServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func startPublicKubernetesAPI(t *testing.T) (*httptest.Server, <-chan *batchv1.Job) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	decoder := serializer.NewCodecFactory(scheme).UniversalDeserializer()
	createdJobs := make(chan *batchv1.Job, 1)
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch request.Method {
		case http.MethodGet:
			if request.URL.Path != "/apis/batch/v1/namespaces/mill-workloads/jobs/mill-attempt-1" {
				t.Errorf("unexpected Kubernetes GET path %q", request.URL.Path)
				response.WriteHeader(http.StatusNotFound)
				return
			}
			response.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(response).Encode(&metav1.Status{
				Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound,
				Code: http.StatusNotFound,
			})
		case http.MethodPost:
			if request.URL.Path != "/apis/batch/v1/namespaces/mill-workloads/jobs" {
				t.Errorf("unexpected Kubernetes POST path %q", request.URL.Path)
				response.WriteHeader(http.StatusNotFound)
				return
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read Job: %v", err)
				return
			}
			object, _, err := decoder.Decode(body, nil, nil)
			if err != nil {
				t.Errorf("decode Job: %v", err)
				return
			}
			job, ok := object.(*batchv1.Job)
			if !ok {
				t.Errorf("created object type = %T", object)
				return
			}
			job.UID = types.UID("kubernetes-job-uid-1")
			createdJobs <- job.DeepCopy()
			response.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(response).Encode(job)
		default:
			http.Error(response, "unexpected request", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(server.Close)
	return server, createdJobs
}

func writeProcessKubeconfig(t *testing.T, server string) string {
	t.Helper()
	const contextName = "execution-process-test"
	filename := filepath.Join(t.TempDir(), "kubeconfig")
	config := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"test": {Server: server, InsecureSkipTLSVerify: true},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{"test": {}},
		Contexts: map[string]*clientcmdapi.Context{
			contextName: {Cluster: "test", AuthInfo: "test"},
		},
		CurrentContext: contextName,
	}
	if err := clientcmd.WriteToFile(config, filename); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", filename)
	return contextName
}

func executionEnvironment(overrides map[string]string) []string {
	environment := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if _, replaced := overrides[name]; !replaced {
			environment = append(environment, entry)
		}
	}
	for name, value := range overrides {
		environment = append(environment, name+"="+value)
	}
	return environment
}

func readExecutionLog(filename string) string {
	contents, err := os.ReadFile(filename)
	if err != nil {
		return "<cannot read log: " + err.Error() + ">"
	}
	return string(contents)
}
