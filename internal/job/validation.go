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
	if submission.Executable.Image == "" || submission.Executable.Image != strings.TrimSpace(submission.Executable.Image) {
		return Submission{}, &ValidationError{Field: "executable.image", Problem: "must be non-empty and have no surrounding whitespace"}
	}

	inputURI, err := normalizeInputURI(submission.Input.URI)
	if err != nil {
		return Submission{}, &ValidationError{Field: "input.uri", Problem: err.Error()}
	}

	args := append([]string(nil), submission.Executable.Args...)
	if args == nil {
		args = []string{}
	}

	return Submission{
		Executable: Executable{
			Image: submission.Executable.Image,
			Args:  args,
		},
		Input: InputSpec{URI: inputURI},
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
	return normalizeObjectURI(raw, false)
}

func normalizeInputURI(raw string) (string, error) {
	normalized, err := normalizeObjectURI(raw, true)
	if err != nil {
		return "", err
	}
	if strings.HasSuffix(raw, "/") {
		return "", fmt.Errorf("must refer to a JSONL file")
	}
	return normalized, nil
}

func normalizeObjectURI(raw string, requireObject bool) (string, error) {
	if raw == "" || raw != strings.TrimSpace(raw) {
		return "", fmt.Errorf("must be non-empty and have no surrounding whitespace")
	}
	parsed, err := url.ParseRequestURI(raw)
	if err != nil {
		return "", fmt.Errorf("must be a valid URI")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("must not contain a query or fragment")
	}
	if parsed.User != nil || parsed.Opaque != "" {
		return "", fmt.Errorf("must be an absolute file:// or s3:// URI")
	}
	switch parsed.Scheme {
	case "file":
		if parsed.Host != "" || !path.IsAbs(parsed.Path) {
			return "", fmt.Errorf("must be an absolute local file:// URI")
		}
		cleanPath := path.Clean(parsed.Path)
		if cleanPath == "/" {
			return "", fmt.Errorf("must not refer to the filesystem root")
		}
		parsed.Path = cleanPath
		parsed.RawPath = ""
		return parsed.String(), nil
	case "s3":
		if parsed.Host == "" || parsed.Port() != "" || strings.Contains(parsed.Host, ":") {
			return "", fmt.Errorf("must contain one S3 bucket name")
		}
		key := strings.TrimPrefix(parsed.Path, "/")
		if requireObject && key == "" {
			return "", fmt.Errorf("must contain an S3 object key")
		}
		if key != "" {
			parsed.Path = "/" + path.Clean(key)
		} else {
			parsed.Path = ""
		}
		parsed.RawPath = ""
		return strings.TrimSuffix(parsed.String(), "/"), nil
	default:
		return "", fmt.Errorf("scheme must be file or s3")
	}
}

func deriveOutputRootURI(outputRootURI, id string) (string, error) {
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
