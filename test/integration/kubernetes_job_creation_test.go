// This file tests Kubernetes Job creation through the adapter's public API.
package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"

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
	server := httptest.NewServer(http.HandlerFunc(func(
		response http.ResponseWriter,
		request *http.Request,
	) {
		response.Header().Set("Content-Type", "application/json")
		switch request.Method {
		case http.MethodGet:
			if request.URL.Path !=
				"/apis/batch/v1/namespaces/mill-workloads/jobs/mill-attempt-1" {
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
			if request.URL.Path !=
				"/apis/batch/v1/namespaces/mill-workloads/jobs" {
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
		Context: contextName, Namespace: "mill-workloads", S3Region: "us-east-1",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	claim := validKubernetesClaim()
	observation, err := runtime.Reconcile(context.Background(), claim)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if observation.ExternalID != "job-uid-1" || observation.Completed ||
		observation.Failure != "" {
		t.Fatalf("observation = %+v", observation)
	}
	if created == nil {
		t.Fatal("Kubernetes API received no Job creation request")
	}
	if created.Name != "mill-attempt-1" ||
		created.Namespace != "mill-workloads" {
		t.Fatalf("created Job identity = %s/%s", created.Namespace, created.Name)
	}
	if created.Labels["mill.dev/job-id"] != "job-1" ||
		created.Labels["mill.dev/task-id"] != "task-1" ||
		created.Labels["mill.dev/attempt-id"] != "attempt-1" {
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
	if invocation.JobID != "job-1" || invocation.TaskID != "task-1" ||
		invocation.ShardIndex != 2 || invocation.InputStartByte != 100 ||
		invocation.InputEndByte != 200 || invocation.InputURI != claim.InputURI ||
		invocation.OutputURI != claim.OutputURI {
		t.Fatalf("workload invocation = %+v", invocation)
	}
	if container.Resources.Requests.Cpu().MilliValue() != 100 ||
		container.Resources.Limits.Cpu().MilliValue() != 500 ||
		container.Resources.Requests.Memory().Value() != 64<<20 ||
		container.Resources.Limits.Memory().Value() != 128<<20 {
		t.Fatalf("workload resources = %+v", container.Resources)
	}
}
