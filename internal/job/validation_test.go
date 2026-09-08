package job

import (
	"strings"
	"testing"
)

func TestNormalizeSubmission(t *testing.T) {
	submission, err := NormalizeSubmission(Submission{
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
	if submission.ResourceClass != ResourceClassSmall {
		t.Fatalf("resource class = %q, want %q", submission.ResourceClass, ResourceClassSmall)
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
			name: "unsupported input scheme",
			submission: Submission{
				Executable: Executable{Image: "mill/example:dev"},
				Input:      InputSpec{URI: "https://example.com/records.jsonl"},
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
			name: "unknown resource class",
			submission: Submission{
				Executable:    Executable{Image: "mill/example:dev"},
				Input:         InputSpec{URI: "file:///data/records.jsonl"},
				ResourceClass: "huge",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NormalizeSubmission(test.submission); err == nil {
				t.Fatal("normalize submission succeeded, want an error")
			}
		})
	}
}

func TestValidateIdempotencyKey(t *testing.T) {
	for _, key := range []string{"", " key", "key ", strings.Repeat("x", maxIdempotencyKeyBytes+1)} {
		if err := ValidateIdempotencyKey(key); err == nil {
			t.Errorf("ValidateIdempotencyKey(%q) succeeded, want an error", key)
		}
	}

	if err := ValidateIdempotencyKey("client-request:001"); err != nil {
		t.Fatalf("validate valid key: %v", err)
	}
}

func TestNormalizeOutputRootAndDeriveOutputURI(t *testing.T) {
	root, err := NormalizeOutputRootURI("file:///var/lib/mill/output/")
	if err != nil {
		t.Fatalf("normalize output root: %v", err)
	}
	if root != "file:///var/lib/mill/output" {
		t.Fatalf("root = %q, want %q", root, "file:///var/lib/mill/output")
	}

	outputURI, err := DeriveOutputRootURI(root, "0198b7c9-1d24-7000-8000-000000000001")
	if err != nil {
		t.Fatalf("derive output URI: %v", err)
	}
	want := "file:///var/lib/mill/output/jobs/0198b7c9-1d24-7000-8000-000000000001/"
	if outputURI != want {
		t.Fatalf("output URI = %q, want %q", outputURI, want)
	}

	s3Root, err := NormalizeOutputRootURI("s3://mill-results/prefix/")
	if err != nil {
		t.Fatalf("normalize S3 output root: %v", err)
	}
	if s3Root != "s3://mill-results/prefix" {
		t.Fatalf("S3 root = %q", s3Root)
	}
	s3Output, err := DeriveOutputRootURI(s3Root, "0198b7c9-1d24-7000-8000-000000000001")
	if err != nil {
		t.Fatal(err)
	}
	if s3Output != "s3://mill-results/prefix/jobs/0198b7c9-1d24-7000-8000-000000000001/" {
		t.Fatalf("S3 output = %q", s3Output)
	}
}

func TestNormalizeOutputRootRejectsUnsafeLocations(t *testing.T) {
	for _, uri := range []string{
		"file:///",
		"file://server/output",
		"relative/output",
		"https://example.com/output",
		"file:///output?mode=test",
	} {
		t.Run(uri, func(t *testing.T) {
			if _, err := NormalizeOutputRootURI(uri); err == nil {
				t.Fatal("normalize output root succeeded, want an error")
			}
		})
	}
}

func TestValidJobID(t *testing.T) {
	if !ValidID("0198b7c9-1d24-7000-8000-000000000001") {
		t.Fatal("valid UUID was rejected")
	}
	for _, id := range []string{"", "not-a-uuid", "0198b7c91d2470008000000000000001"} {
		if ValidID(id) {
			t.Errorf("invalid UUID %q was accepted", id)
		}
	}
}
