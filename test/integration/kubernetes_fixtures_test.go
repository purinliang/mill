// This file provides fixtures for Kubernetes adapter integration tests.
package integration_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/execution/kubernetes"
)

func newKubernetesRuntime(
	t *testing.T,
	handler http.Handler,
	config kubernetes.Config,
) *kubernetes.Runtime {
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

func runtimeReturningKubernetesJob(
	t *testing.T,
	job *batchv1.Job,
) *kubernetes.Runtime {
	t.Helper()
	return newKubernetesRuntime(
		t,
		http.HandlerFunc(func(
			response http.ResponseWriter,
			request *http.Request,
		) {
			response.Header().Set("Content-Type", "application/json")
			if request.Method != http.MethodGet {
				http.Error(
					response,
					"unexpected request",
					http.StatusMethodNotAllowed,
				)
				return
			}
			_ = json.NewEncoder(response).Encode(job)
		}),
		kubernetes.Config{S3Region: "us-east-1"},
	)
}

func validKubernetesClaim() execution.ClaimedAttempt {
	return execution.ClaimedAttempt{
		Attempt: execution.Attempt{
			ID:     "attempt-1",
			JobID:  "job-1",
			TaskID: "task-1",
			State:  execution.AttemptStateStarting,
		},
		Executable: execution.Executable{
			Image: "mill/word-count:dev", Args: []string{"--demo"},
		},
		ShardIndex:     2,
		InputURI:       "s3://mill-input/records.jsonl",
		InputStartByte: 100,
		InputEndByte:   200,
		OutputURI:      "s3://mill-output/jobs/job-1/tasks/2/result.jsonl",
		Resources: execution.Resources{
			CPURequestMillis:   100,
			CPULimitMillis:     500,
			MemoryRequestBytes: 64 << 20,
			MemoryLimitBytes:   128 << 20,
		},
	}
}

func kubernetesJob(
	claim execution.ClaimedAttempt,
	uid string,
) *batchv1.Job {
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

func writeKubernetesStatus(
	response http.ResponseWriter,
	code int,
	reason metav1.StatusReason,
	message string,
) {
	response.WriteHeader(code)
	_ = json.NewEncoder(response).Encode(&metav1.Status{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
		Status:   metav1.StatusFailure,
		Reason:   reason,
		Message:  message,
		Code:     int32(code),
		Details: &metav1.StatusDetails{
			Group: "batch", Kind: "jobs", Name: "mill-attempt-1",
		},
	})
}

func writeKubeconfig(t *testing.T, server string) string {
	t.Helper()
	const contextName = "kubernetes-adapter-test"
	filename := t.TempDir() + "/kubeconfig"
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
