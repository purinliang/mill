package job

import (
	"strings"
	"testing"
)

func TestNormalizeSubmission(t *testing.T) {
	submission, err := normalizeSubmission(Submission{
		Workload: Workload{Image: "mill/example:dev"},
		Dataset:  Dataset{ManifestURI: "file:///data/example/../manifest.json"},
	})
	if err != nil {
		t.Fatalf("normalize submission: %v", err)
	}

	if submission.Dataset.ManifestURI != "file:///data/manifest.json" {
		t.Errorf("manifest URI = %q, want %q", submission.Dataset.ManifestURI, "file:///data/manifest.json")
	}
	if submission.Workload.Args == nil {
		t.Fatal("workload args are nil, want an empty array")
	}
	if len(submission.Workload.Args) != 0 {
		t.Fatalf("workload args = %v, want empty", submission.Workload.Args)
	}
}

func TestNormalizeSubmissionRejectsInvalidFields(t *testing.T) {
	tests := []struct {
		name       string
		submission Submission
	}{
		{
			name: "empty image",
			submission: Submission{
				Dataset: Dataset{ManifestURI: "file:///data/manifest.json"},
			},
		},
		{
			name: "image whitespace",
			submission: Submission{
				Workload: Workload{Image: " mill/example:dev"},
				Dataset:  Dataset{ManifestURI: "file:///data/manifest.json"},
			},
		},
		{
			name: "s3 manifest",
			submission: Submission{
				Workload: Workload{Image: "mill/example:dev"},
				Dataset:  Dataset{ManifestURI: "s3://bucket/manifest.json"},
			},
		},
		{
			name: "manifest directory",
			submission: Submission{
				Workload: Workload{Image: "mill/example:dev"},
				Dataset:  Dataset{ManifestURI: "file:///data/"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := normalizeSubmission(test.submission); err == nil {
				t.Fatal("normalize submission succeeded, want an error")
			}
		})
	}
}

func TestValidateIdempotencyKey(t *testing.T) {
	for _, key := range []string{"", " key", "key ", strings.Repeat("x", maxIdempotencyKeyBytes+1)} {
		if err := validateIdempotencyKey(key); err == nil {
			t.Errorf("validateIdempotencyKey(%q) succeeded, want an error", key)
		}
	}

	if err := validateIdempotencyKey("client-request:001"); err != nil {
		t.Fatalf("validate valid key: %v", err)
	}
}

func TestNormalizeOutputRootAndDeriveOutputURI(t *testing.T) {
	root, err := normalizeOutputRootURI("file:///var/lib/mill/output/")
	if err != nil {
		t.Fatalf("normalize output root: %v", err)
	}
	if root != "file:///var/lib/mill/output" {
		t.Fatalf("root = %q, want %q", root, "file:///var/lib/mill/output")
	}

	outputURI, err := deriveOutputURI(root, "0198b7c9-1d24-7000-8000-000000000001")
	if err != nil {
		t.Fatalf("derive output URI: %v", err)
	}
	want := "file:///var/lib/mill/output/jobs/0198b7c9-1d24-7000-8000-000000000001/"
	if outputURI != want {
		t.Fatalf("output URI = %q, want %q", outputURI, want)
	}

	taskOutputURI, err := deriveTaskOutputURI(outputURI, 7)
	if err != nil {
		t.Fatalf("derive task output URI: %v", err)
	}
	taskWant := want + "tasks/7/"
	if taskOutputURI != taskWant {
		t.Fatalf("task output URI = %q, want %q", taskOutputURI, taskWant)
	}
}

func TestNormalizeOutputRootRejectsUnsafeLocations(t *testing.T) {
	for _, uri := range []string{
		"file:///",
		"file://server/output",
		"relative/output",
		"s3://bucket/output",
		"file:///output?mode=test",
	} {
		t.Run(uri, func(t *testing.T) {
			if _, err := normalizeOutputRootURI(uri); err == nil {
				t.Fatal("normalize output root succeeded, want an error")
			}
		})
	}
}

func TestValidJobID(t *testing.T) {
	if !validJobID("0198b7c9-1d24-7000-8000-000000000001") {
		t.Fatal("valid UUID was rejected")
	}
	for _, id := range []string{"", "not-a-uuid", "0198b7c91d2470008000000000000001"} {
		if validJobID(id) {
			t.Errorf("invalid UUID %q was accepted", id)
		}
	}
}
