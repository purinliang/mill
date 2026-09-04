package job

import (
	"encoding/hex"
	"fmt"
	"net/url"
	"path"
	"strings"
)

const maxIdempotencyKeyBytes = 255

type ValidationError struct {
	Field   string
	Problem string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s %s", e.Field, e.Problem)
}

func normalizeSubmission(submission Submission) (Submission, error) {
	if submission.Workload.Image == "" || submission.Workload.Image != strings.TrimSpace(submission.Workload.Image) {
		return Submission{}, &ValidationError{Field: "workload.image", Problem: "must be non-empty and have no surrounding whitespace"}
	}

	manifestURI, err := normalizeLocalFileURI(submission.Dataset.ManifestURI, false)
	if err != nil {
		return Submission{}, &ValidationError{Field: "dataset.manifest_uri", Problem: err.Error()}
	}

	args := append([]string(nil), submission.Workload.Args...)
	if args == nil {
		args = []string{}
	}

	return Submission{
		Workload: Workload{
			Image: submission.Workload.Image,
			Args:  args,
		},
		Dataset: Dataset{ManifestURI: manifestURI},
	}, nil
}

func validateIdempotencyKey(key string) error {
	if key == "" {
		return &ValidationError{Field: "Idempotency-Key", Problem: "is required"}
	}
	if key != strings.TrimSpace(key) {
		return &ValidationError{Field: "Idempotency-Key", Problem: "must not have surrounding whitespace"}
	}
	if len(key) > maxIdempotencyKeyBytes {
		return &ValidationError{Field: "Idempotency-Key", Problem: "must be at most 255 bytes"}
	}
	return nil
}

func normalizeOutputRootURI(raw string) (string, error) {
	return normalizeLocalFileURI(raw, true)
}

func normalizeLocalFileURI(raw string, directory bool) (string, error) {
	if raw == "" || raw != strings.TrimSpace(raw) {
		return "", fmt.Errorf("must be non-empty and have no surrounding whitespace")
	}
	if !strings.HasPrefix(raw, "file://") {
		return "", fmt.Errorf("must be an absolute file:// URI")
	}

	parsed, err := url.ParseRequestURI(raw)
	if err != nil {
		return "", fmt.Errorf("must be a valid URI")
	}
	if parsed.Scheme != "file" || parsed.Host != "" || parsed.User != nil || parsed.Opaque != "" || !path.IsAbs(parsed.Path) {
		return "", fmt.Errorf("must be an absolute local file:// URI")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("must not contain a query or fragment")
	}

	cleanPath := path.Clean(parsed.Path)
	if cleanPath == "/" {
		return "", fmt.Errorf("must not refer to the filesystem root")
	}
	if !directory && strings.HasSuffix(parsed.Path, "/") {
		return "", fmt.Errorf("must refer to a manifest file")
	}

	parsed.Path = cleanPath
	parsed.RawPath = ""
	return parsed.String(), nil
}

func deriveOutputURI(outputRootURI, id string) (string, error) {
	outputURI, err := url.JoinPath(outputRootURI, "jobs", id)
	if err != nil {
		return "", fmt.Errorf("derive output URI: %w", err)
	}
	return outputURI + "/", nil
}

func validJobID(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return false
	}
	compact := id[:8] + id[9:13] + id[14:18] + id[19:23] + id[24:]
	_, err := hex.DecodeString(compact)
	return err == nil
}
