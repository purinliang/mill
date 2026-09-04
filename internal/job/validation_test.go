package job

import (
	"strings"
	"testing"
)

func TestNormalizeSubmission(t *testing.T) {
	submission, err := normalizeSubmission(Submission{
		Executable: Executable{Image: "mill/example:dev"},
		Input:      InputSpec{URI: "file:///data/example/../records.jsonl"},
	})
	if err != nil {
		t.Fatalf("normalize submission: %v", err)
	}

	if submission.Input.URI != "file:///data/records.jsonl" {
		t.Errorf("input URI = %q, want %q", submission.Input.URI, "file:///data/records.jsonl")
	}
	if submission.Executable.Args == nil {
		t.Fatal("executable args are nil, want an empty array")
	}
	if len(submission.Executable.Args) != 0 {
		t.Fatalf("executable args = %v, want empty", submission.Executable.Args)
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
				Input: InputSpec{URI: "file:///data/records.jsonl"},
			},
		},
		{
			name: "image whitespace",
			submission: Submission{
				Executable: Executable{Image: " mill/example:dev"},
				Input:      InputSpec{URI: "file:///data/records.jsonl"},
			},
		},
		{
			name: "s3 input",
			submission: Submission{
				Executable: Executable{Image: "mill/example:dev"},
				Input:      InputSpec{URI: "s3://bucket/records.jsonl"},
			},
		},
		{
			name: "input directory",
			submission: Submission{
				Executable: Executable{Image: "mill/example:dev"},
				Input:      InputSpec{URI: "file:///data/"},
			},
		},
		{
			name: "reserved argument",
			submission: Submission{
				Executable: Executable{Image: "mill/example:dev", Args: []string{"--mill-task-id=override"}},
				Input:      InputSpec{URI: "file:///data/records.jsonl"},
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

	outputURI, err := deriveOutputRootURI(root, "0198b7c9-1d24-7000-8000-000000000001")
	if err != nil {
		t.Fatalf("derive output URI: %v", err)
	}
	want := "file:///var/lib/mill/output/jobs/0198b7c9-1d24-7000-8000-000000000001/"
	if outputURI != want {
		t.Fatalf("output URI = %q, want %q", outputURI, want)
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
