// This file tests Kubernetes runtime behavior through its public API.
package kubernetes_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/execution/kubernetes"
	"github.com/purinliang/mill/internal/workload"
)

func TestRuntimeCreatesAJobThroughTheKubernetesAPI(t *testing.T) {
	var created *batchv1.Job
	scheme := runtime.NewScheme()
	if err := batchv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	decoder := serializer.NewCodecFactory(scheme).UniversalDeserializer()
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch request.Method {
		case http.MethodGet:
			if request.URL.Path != "/apis/batch/v1/namespaces/mill-workloads/jobs/mill-attempt-1" {
				http.Error(response, "unexpected path", http.StatusNotFound)
				return
			}
			response.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(response).Encode(&metav1.Status{
				Status: metav1.StatusFailure,
				Reason: metav1.StatusReasonNotFound,
				Code:   http.StatusNotFound,
				Details: &metav1.StatusDetails{
					Group: "batch", Kind: "jobs", Name: "mill-attempt-1",
				},
			})
		case http.MethodPost:
			if request.URL.Path != "/apis/batch/v1/namespaces/mill-workloads/jobs" {
				http.Error(response, "unexpected path", http.StatusNotFound)
				return
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				t.Errorf("read created Job: %v", err)
				return
			}
			object, _, err := decoder.Decode(body, nil, nil)
			if err != nil {
				t.Errorf("decode created Job: %v", err)
				return
			}
			var ok bool
			created, ok = object.(*batchv1.Job)
			if !ok {
				t.Errorf("created object type = %T, want *batchv1.Job", object)
				return
			}
			created.UID = types.UID("job-uid-1")
			response.WriteHeader(http.StatusCreated)
			_ = json.NewEncoder(response).Encode(created)
		default:
			http.Error(response, "unexpected request", http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	contextName := writeKubeconfig(t, server.URL)
	runtime, err := kubernetes.New(kubernetes.Config{
		Context:   contextName,
		Namespace: "mill-workloads",
		S3Region:  "us-east-1",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	claim := execution.ClaimedAttempt{
		Attempt: execution.Attempt{
			ID: "attempt-1", JobID: "job-1", TaskID: "task-1",
			State: execution.AttemptStateStarting,
		},
		Executable:     execution.Executable{Image: "mill/word-count:dev", Args: []string{"--demo"}},
		ShardIndex:     2,
		InputURI:       "s3://mill-input/records.jsonl",
		InputStartByte: 100,
		InputEndByte:   200,
		OutputURI:      "s3://mill-output/jobs/job-1/tasks/2/result.jsonl",
		Resources: execution.Resources{
			CPURequestMillis: 100, CPULimitMillis: 500,
			MemoryRequestBytes: 64 << 20, MemoryLimitBytes: 128 << 20,
		},
	}

	observation, err := runtime.Reconcile(context.Background(), claim)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if observation.ExternalID != "job-uid-1" || observation.Completed || observation.Failure != "" {
		t.Fatalf("observation = %+v", observation)
	}
	if created == nil {
		t.Fatal("Kubernetes API received no Job creation request")
	}
	if created.Name != "mill-attempt-1" || created.Namespace != "mill-workloads" {
		t.Fatalf("created Job identity = %s/%s", created.Namespace, created.Name)
	}
	if created.Labels["mill.dev/job-id"] != "job-1" || created.Labels["mill.dev/task-id"] != "task-1" || created.Labels["mill.dev/attempt-id"] != "attempt-1" {
		t.Fatalf("created Job labels = %v", created.Labels)
	}
	if created.Spec.BackoffLimit == nil || *created.Spec.BackoffLimit != 0 {
		t.Fatalf("created Job backoff limit = %v", created.Spec.BackoffLimit)
	}
	container := created.Spec.Template.Spec.Containers[0]
	invocation, err := workload.ParseArgs(container.Args)
	if err != nil {
		t.Fatalf("parse workload arguments: %v", err)
	}
	if invocation.JobID != "job-1" || invocation.TaskID != "task-1" || invocation.ShardIndex != 2 ||
		invocation.InputStartByte != 100 || invocation.InputEndByte != 200 ||
		invocation.InputURI != claim.InputURI || invocation.OutputURI != claim.OutputURI {
		t.Fatalf("workload invocation = %+v", invocation)
	}
	if container.Resources.Requests.Cpu().MilliValue() != 100 || container.Resources.Limits.Cpu().MilliValue() != 500 ||
		container.Resources.Requests.Memory().Value() != 64<<20 || container.Resources.Limits.Memory().Value() != 128<<20 {
		t.Fatalf("workload resources = %+v", container.Resources)
	}
}

func TestNewRejectsInvalidConfigurationBeforeContactingKubernetes(t *testing.T) {
	tests := []struct {
		name   string
		config kubernetes.Config
	}{
		{name: "missing namespace", config: kubernetes.Config{Context: "unused"}},
		{name: "missing client mode", config: kubernetes.Config{Namespace: "default"}},
		{name: "two client modes", config: kubernetes.Config{Namespace: "default", Context: "unused", InCluster: true}},
		{name: "partial local storage", config: kubernetes.Config{Namespace: "default", Context: "unused", Node: "node-1"}},
		{name: "unsafe local root", config: kubernetes.Config{Namespace: "default", Context: "unused", Node: "node-1", LocalRoot: "/", NodeRoot: "/data"}},
		{name: "endpoint without region", config: kubernetes.Config{Namespace: "default", Context: "unused", S3Endpoint: "http://s3.example"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := kubernetes.New(test.config); err == nil {
				t.Fatal("New accepted invalid configuration")
			}
		})
	}
}

func TestNewReportsKubernetesCredentialLoadingFailures(t *testing.T) {
	t.Run("missing kubeconfig context", func(t *testing.T) {
		t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "missing-kubeconfig"))
		if _, err := kubernetes.New(kubernetes.Config{Context: "missing", Namespace: "default"}); err == nil || !strings.Contains(err.Error(), "load kubeconfig") {
			t.Fatalf("New error = %v", err)
		}
	})

	t.Run("outside a Kubernetes Pod", func(t *testing.T) {
		t.Setenv("KUBERNETES_SERVICE_HOST", "")
		t.Setenv("KUBERNETES_SERVICE_PORT", "")
		if _, err := kubernetes.New(kubernetes.Config{InCluster: true, Namespace: "default"}); err == nil || !strings.Contains(err.Error(), "in-cluster") {
			t.Fatalf("New error = %v", err)
		}
	})
}

func TestReconcileReportsInvalidClaimsWithoutCreatingJobs(t *testing.T) {
	created := 0
	runtime := newPublicRuntime(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		if request.Method == http.MethodGet {
			writeJobStatus(response, http.StatusNotFound, metav1.StatusReasonNotFound, "missing")
			return
		}
		created++
		http.Error(response, "unexpected creation", http.StatusInternalServerError)
	}), kubernetes.Config{S3Region: "us-east-1"})

	tests := map[string]func(*execution.ClaimedAttempt){
		"invalid resources": func(claim *execution.ClaimedAttempt) {
			claim.Resources.CPURequestMillis = 0
		},
		"malformed input URI": func(claim *execution.ClaimedAttempt) {
			claim.InputURI = "://bad"
		},
		"invalid S3 input URI": func(claim *execution.ClaimedAttempt) {
			claim.InputURI = "s3://bucket/"
		},
		"invalid output URI": func(claim *execution.ClaimedAttempt) {
			claim.OutputURI = "relative-output.jsonl"
		},
		"invalid workload invocation": func(claim *execution.ClaimedAttempt) {
			claim.Attempt.JobID = ""
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			claim := validPublicClaim()
			mutate(&claim)
			observation, err := runtime.Reconcile(context.Background(), claim)
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if observation.Failure == "" {
				t.Fatalf("observation = %+v, want claim failure", observation)
			}
		})
	}

	withoutS3 := newPublicRuntime(t, http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		writeJobStatus(response, http.StatusNotFound, metav1.StatusReasonNotFound, "missing")
	}), kubernetes.Config{})
	observation, err := withoutS3.Reconcile(context.Background(), validPublicClaim())
	if err != nil {
		t.Fatalf("Reconcile without S3 configuration: %v", err)
	}
	if !strings.Contains(observation.Failure, "S3 region") {
		t.Fatalf("observation = %+v, want missing S3 configuration", observation)
	}

	localClaim := validPublicClaim()
	localClaim.InputURI = "file:///tmp/input/records.jsonl"
	observation, err = runtime.Reconcile(context.Background(), localClaim)
	if err != nil {
		t.Fatalf("Reconcile local claim: %v", err)
	}
	if !strings.Contains(observation.Failure, "local storage") {
		t.Fatalf("observation = %+v, want missing local-storage configuration", observation)
	}

	if created != 0 {
		t.Fatalf("Kubernetes API received %d creation requests for invalid claims", created)
	}
}

func TestReconcileRecoversAnAlreadyCreatedJob(t *testing.T) {
	gets := 0
	runtime := newPublicRuntime(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		switch request.Method {
		case http.MethodGet:
			gets++
			if gets == 1 {
				writeJobStatus(response, http.StatusNotFound, metav1.StatusReasonNotFound, "missing")
				return
			}
			_ = json.NewEncoder(response).Encode(publicJob(validPublicClaim(), "existing-uid"))
		case http.MethodPost:
			writeJobStatus(response, http.StatusConflict, metav1.StatusReasonAlreadyExists, "already exists")
		default:
			http.Error(response, "unexpected request", http.StatusMethodNotAllowed)
		}
	}), kubernetes.Config{S3Region: "us-east-1"})

	observation, err := runtime.Reconcile(context.Background(), validPublicClaim())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if observation.ExternalID != "existing-uid" || gets != 2 {
		t.Fatalf("observation = %+v, GET count = %d", observation, gets)
	}
}

func TestReconcileTurnsInvalidKubernetesJobsIntoBoundedFailures(t *testing.T) {
	message := strings.Repeat("invalid manifest; ", 400)
	runtime := newPublicRuntime(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		if request.Method == http.MethodGet {
			writeJobStatus(response, http.StatusNotFound, metav1.StatusReasonNotFound, "missing")
			return
		}
		writeJobStatus(response, http.StatusUnprocessableEntity, metav1.StatusReasonInvalid, message)
	}), kubernetes.Config{S3Region: "us-east-1"})

	observation, err := runtime.Reconcile(context.Background(), validPublicClaim())
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(observation.Failure) != 4096 {
		t.Fatalf("failure length = %d, want 4096", len(observation.Failure))
	}
}

func TestReconcileRejectsIdentityMismatchAndIgnoresFalseConditions(t *testing.T) {
	t.Run("identity mismatch", func(t *testing.T) {
		claim := validPublicClaim()
		job := publicJob(claim, "job-uid")
		job.Labels["mill.dev/task-id"] = "another-task"
		runtime := runtimeReturningJob(t, job)

		if _, err := runtime.Reconcile(context.Background(), claim); err == nil || !strings.Contains(err.Error(), "identity") {
			t.Fatalf("Reconcile error = %v", err)
		}
	})

	t.Run("false terminal condition", func(t *testing.T) {
		claim := validPublicClaim()
		job := publicJob(claim, "job-uid")
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionFalse}}
		runtime := runtimeReturningJob(t, job)

		observation, err := runtime.Reconcile(context.Background(), claim)
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if observation.Completed || observation.Failure != "" {
			t.Fatalf("observation = %+v, want non-terminal", observation)
		}
	})
}

func newPublicRuntime(t *testing.T, handler http.Handler, config kubernetes.Config) *kubernetes.Runtime {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	config.Context = writeKubeconfig(t, server.URL)
	config.Namespace = "mill-workloads"
	runtime, err := kubernetes.New(config)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return runtime
}

func runtimeReturningJob(t *testing.T, job *batchv1.Job) *kubernetes.Runtime {
	t.Helper()
	return newPublicRuntime(t, http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		response.Header().Set("Content-Type", "application/json")
		if request.Method != http.MethodGet {
			http.Error(response, "unexpected request", http.StatusMethodNotAllowed)
			return
		}
		_ = json.NewEncoder(response).Encode(job)
	}), kubernetes.Config{S3Region: "us-east-1"})
}

func validPublicClaim() execution.ClaimedAttempt {
	return execution.ClaimedAttempt{
		Attempt: execution.Attempt{
			ID: "attempt-1", JobID: "job-1", TaskID: "task-1",
			State: execution.AttemptStateStarting,
		},
		Executable:     execution.Executable{Image: "mill/word-count:dev", Args: []string{"--demo"}},
		ShardIndex:     2,
		InputURI:       "s3://mill-input/records.jsonl",
		InputStartByte: 100,
		InputEndByte:   200,
		OutputURI:      "s3://mill-output/jobs/job-1/tasks/2/result.jsonl",
		Resources: execution.Resources{
			CPURequestMillis: 100, CPULimitMillis: 500,
			MemoryRequestBytes: 64 << 20, MemoryLimitBytes: 128 << 20,
		},
	}
}

func publicJob(claim execution.ClaimedAttempt, uid string) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{
		Name: "mill-" + claim.Attempt.ID,
		UID:  types.UID(uid),
		Labels: map[string]string{
			"mill.dev/attempt-id": claim.Attempt.ID,
			"mill.dev/job-id":     claim.Attempt.JobID,
			"mill.dev/task-id":    claim.Attempt.TaskID,
		},
	}}
}

func writeJobStatus(response http.ResponseWriter, code int, reason metav1.StatusReason, message string) {
	response.WriteHeader(code)
	_ = json.NewEncoder(response).Encode(&metav1.Status{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
		Status:   metav1.StatusFailure,
		Reason:   reason,
		Message:  message,
		Code:     int32(code),
		Details:  &metav1.StatusDetails{Group: "batch", Kind: "jobs", Name: "mill-attempt-1"},
	})
}

func writeKubeconfig(t *testing.T, server string) string {
	t.Helper()
	const contextName = "public-api-test"
	filename := t.TempDir() + "/kubeconfig"
	config := clientcmdapi.Config{
		Clusters: map[string]*clientcmdapi.Cluster{
			"test": {Server: server, InsecureSkipTLSVerify: true},
		},
		AuthInfos: map[string]*clientcmdapi.AuthInfo{
			"test": {},
		},
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
