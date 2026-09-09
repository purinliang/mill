// This file tests Kubernetes observation and create-response recovery.
package integration_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/purinliang/mill/internal/execution/kubernetes"
)

func TestReconcileRecoversAnAlreadyCreatedJob(t *testing.T) {
	gets := 0
	runtime := newKubernetesRuntime(
		t,
		http.HandlerFunc(func(
			response http.ResponseWriter,
			request *http.Request,
		) {
			response.Header().Set("Content-Type", "application/json")
			switch request.Method {
			case http.MethodGet:
				gets++
				if gets == 1 {
					writeKubernetesStatus(
						response,
						http.StatusNotFound,
						metav1.StatusReasonNotFound,
						"missing",
					)
					return
				}
				_ = json.NewEncoder(response).Encode(
					kubernetesJob(validKubernetesClaim(), "existing-uid"),
				)
			case http.MethodPost:
				writeKubernetesStatus(
					response,
					http.StatusConflict,
					metav1.StatusReasonAlreadyExists,
					"already exists",
				)
			default:
				http.Error(
					response,
					"unexpected request",
					http.StatusMethodNotAllowed,
				)
			}
		}),
		kubernetes.Config{S3Region: "us-east-1"},
	)

	observation, err := runtime.Reconcile(
		context.Background(),
		validKubernetesClaim(),
	)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if observation.ExternalID != "existing-uid" || gets != 2 {
		t.Fatalf("observation = %+v, GET count = %d", observation, gets)
	}
}

func TestReconcileTurnsInvalidJobsIntoBoundedFailures(t *testing.T) {
	message := strings.Repeat("invalid manifest; ", 400)
	runtime := newKubernetesRuntime(
		t,
		http.HandlerFunc(func(
			response http.ResponseWriter,
			request *http.Request,
		) {
			response.Header().Set("Content-Type", "application/json")
			if request.Method == http.MethodGet {
				writeKubernetesStatus(
					response,
					http.StatusNotFound,
					metav1.StatusReasonNotFound,
					"missing",
				)
				return
			}
			writeKubernetesStatus(
				response,
				http.StatusUnprocessableEntity,
				metav1.StatusReasonInvalid,
				message,
			)
		}),
		kubernetes.Config{S3Region: "us-east-1"},
	)

	observation, err := runtime.Reconcile(
		context.Background(),
		validKubernetesClaim(),
	)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(observation.Failure) != 4096 {
		t.Fatalf("failure length = %d, want 4096", len(observation.Failure))
	}
}

func TestReconcileRejectsIdentityMismatchAndFalseConditions(t *testing.T) {
	t.Run("identity mismatch", func(t *testing.T) {
		claim := validKubernetesClaim()
		job := kubernetesJob(claim, "job-uid")
		job.Labels["mill.dev/task-id"] = "another-task"
		runtime := runtimeReturningKubernetesJob(t, job)

		_, err := runtime.Reconcile(context.Background(), claim)
		if err == nil || !strings.Contains(err.Error(), "identity") {
			t.Fatalf("Reconcile error = %v", err)
		}
	})

	t.Run("false terminal condition", func(t *testing.T) {
		claim := validKubernetesClaim()
		job := kubernetesJob(claim, "job-uid")
		job.Status.Conditions = []batchv1.JobCondition{{
			Type: batchv1.JobComplete, Status: corev1.ConditionFalse,
		}}
		runtime := runtimeReturningKubernetesJob(t, job)

		observation, err := runtime.Reconcile(context.Background(), claim)
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if observation.Completed || observation.Failure != "" {
			t.Fatalf("observation = %+v, want non-terminal", observation)
		}
	})
}
