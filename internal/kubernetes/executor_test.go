package kubernetes

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/job"
	"github.com/purinliang/mill/internal/workload"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	batchclient "k8s.io/client-go/kubernetes/typed/batch/v1"
	"k8s.io/client-go/rest"
)

func testClaim() job.ClaimedAttempt {
	return job.ClaimedAttempt{Attempt: job.Attempt{ID: "attempt-1", JobID: "job-1", TaskID: "task-1", State: job.AttemptStateStarting},
		Executable: job.Executable{Image: "mill/word-count:dev", Args: []string{"--user-arg"}}, ShardIndex: 2,
		InputURI: "file:///local/input/records.jsonl", InputStartByte: 100, InputEndByte: 200,
		OutputURI: "file:///local/output/job-1/tasks/2/attempts/attempt-1/result.jsonl"}
}

func testConfig() Config {
	return Config{Context: "kind-mill", Namespace: "default", Node: "mill-control-plane", LocalRoot: "/local", NodeRoot: "/var/local/mill"}
}

func TestManifestPreservesRangeAndSeparatesMounts(t *testing.T) {
	e := Executor{config: testConfig()}
	m, err := e.manifest(testClaim())
	if err != nil {
		t.Fatal(err)
	}
	spec := m.Spec.Template.Spec
	invocation, err := workload.ParseArgs(spec.Containers[0].Args)
	if err != nil {
		t.Fatal(err)
	}
	if invocation.InputURI != "file:///data/records.jsonl" || invocation.InputStartByte != 100 || invocation.InputEndByte != 200 ||
		invocation.OutputURI != "file:///output/job-1/tasks/2/attempts/attempt-1/result.jsonl" || invocation.ExecutableArgs[0] != "--user-arg" {
		t.Fatalf("invocation=%+v", invocation)
	}
	if !spec.Containers[0].VolumeMounts[0].ReadOnly || spec.Containers[0].VolumeMounts[1].ReadOnly {
		t.Fatal("wrong mount permissions")
	}
	if spec.Volumes[0].HostPath.Path != "/var/local/mill/input" || spec.Volumes[1].HostPath.Path != "/var/local/mill/output" {
		t.Fatal("wrong node paths")
	}
	if *m.Spec.BackoffLimit != 0 || *m.Spec.Parallelism != 1 || spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Fatal("unexpected native retries/parallelism")
	}
}

func TestRejectPathsOutsideConfiguredRoots(t *testing.T) {
	e := Executor{config: testConfig()}
	for _, uri := range []string{"s3://bucket/file", "file:///local/input/../../etc/passwd", "file:///local/inputs/file", "file:///local/input"} {
		if _, err := e.relativeURI(uri, "input"); err == nil {
			t.Errorf("accepted %s", uri)
		}
	}
	c := testConfig()
	c.NodeRoot = "/"
	if c.validate() == nil {
		t.Fatal("accepted root filesystem mount")
	}
}

func TestRecoverLostCreateResponseAndObserveTerminalCondition(t *testing.T) {
	var stored *batchv1.Job
	creates := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			creates++
			stored = &batchv1.Job{}
			if err := json.NewDecoder(r.Body).Decode(stored); err != nil {
				t.Error(err)
			}
			stored.UID = types.UID("uid-1")
			w.WriteHeader(500) // The server stored the Job but the client did not get its response.
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"InternalError","code":500}`))
			return
		}
		if stored == nil {
			w.WriteHeader(404)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`))
			return
		}
		_ = json.NewEncoder(w).Encode(stored)
	}))
	defer server.Close()
	client, err := batchclient.NewForConfig(&rest.Config{Host: server.URL, ContentConfig: rest.ContentConfig{ContentType: "application/json"}})
	if err != nil {
		t.Fatal(err)
	}
	e := Executor{jobs: client.Jobs("default"), config: testConfig()}
	claim := testClaim()
	if _, err := e.Reconcile(context.Background(), claim); err == nil {
		t.Fatal("expected lost-response error")
	}
	observed, err := e.Reconcile(context.Background(), claim)
	if err != nil || observed.ExternalID != "uid-1" || creates != 1 {
		t.Fatalf("recovery=%+v err=%v creates=%d", observed, err, creates)
	}
	claim.Attempt.State = job.AttemptStateRunning
	claim.Attempt.ExternalID = "uid-1"
	// An early success signal must not free the slot while Pods terminate.
	stored.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue}}
	observed, err = e.Reconcile(context.Background(), claim)
	if err != nil || observed.Completed {
		t.Fatalf("premature completion: %+v %v", observed, err)
	}
	stored.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	observed, err = e.Reconcile(context.Background(), claim)
	if err != nil || !observed.Completed {
		t.Fatalf("completion=%+v %v", observed, err)
	}
	stored.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "DeadlineExceeded"}}
	observed, err = e.Reconcile(context.Background(), claim)
	if err != nil || !strings.Contains(observed.Failure, "DeadlineExceeded") {
		t.Fatalf("failure=%+v %v", observed, err)
	}
	stored.UID = "replacement"
	if _, err := e.Reconcile(context.Background(), claim); err == nil {
		t.Fatal("accepted changed UID")
	}
	stored = nil
	if _, err := e.Reconcile(context.Background(), claim); err == nil {
		t.Fatal("expected missing running Job error")
	}
	if creates != 1 {
		t.Fatal("recreated missing running job")
	}
}
