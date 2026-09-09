// This file tests invalid claims at the Kubernetes adapter boundary.
package integration_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/execution/kubernetes"
)

func TestReconcileReportsInvalidClaimsWithoutCreatingJobs(t *testing.T) {
	created := 0
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
			created++
			http.Error(
				response,
				"unexpected creation",
				http.StatusInternalServerError,
			)
		}),
		kubernetes.Config{S3Region: "us-east-1"},
	)

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
			claim := validKubernetesClaim()
			mutate(&claim)
			observation, err := runtime.Reconcile(
				context.Background(), claim,
			)
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if observation.Failure == "" {
				t.Fatalf("observation = %+v, want claim failure", observation)
			}
		})
	}

	withoutS3 := newKubernetesRuntime(
		t,
		http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
			response.Header().Set("Content-Type", "application/json")
			writeKubernetesStatus(
				response,
				http.StatusNotFound,
				metav1.StatusReasonNotFound,
				"missing",
			)
		}),
		kubernetes.Config{},
	)
	observation, err := withoutS3.Reconcile(
		context.Background(),
		validKubernetesClaim(),
	)
	if err != nil {
		t.Fatalf("Reconcile without S3 configuration: %v", err)
	}
	if !strings.Contains(observation.Failure, "S3 region") {
		t.Fatalf("observation = %+v, want missing S3 configuration", observation)
	}

	localClaim := validKubernetesClaim()
	localClaim.InputURI = "file:///tmp/input/records.jsonl"
	observation, err = runtime.Reconcile(context.Background(), localClaim)
	if err != nil {
		t.Fatalf("Reconcile local claim: %v", err)
	}
	if !strings.Contains(observation.Failure, "local storage") {
		t.Fatalf("observation = %+v, want missing local storage", observation)
	}
	if created != 0 {
		t.Fatalf(
			"Kubernetes API received %d creations for invalid claims",
			created,
		)
	}
}
