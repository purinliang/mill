// This file owns job submission, URI, identity, and shard-set rules.
package job

import (
	"crypto/sha256"
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

// InvalidArgument lets transport adapters classify validation failures without
// depending on the job package's concrete error type.
func (e *ValidationError) InvalidArgument() bool {
	return true
}

func NormalizeSubmission(submission Submission) (Submission, error) {
	if submission.Executable.Image == "" ||
		submission.Executable.Image != strings.TrimSpace(
			submission.Executable.Image,
		) {
		return Submission{}, &ValidationError{
			Field:   "executable.image",
			Problem: "must be non-empty and have no surrounding whitespace",
		}
	}

	inputURI, err := NormalizeInputURI(submission.Input.URI)
	if err != nil {
		return Submission{}, &ValidationError{Field: "input.uri", Problem: err.Error()}
	}

	args := append([]string(nil), submission.Executable.Args...)
	if args == nil {
		args = []string{}
	}

	resourceClass := submission.ResourceClass
	if resourceClass == "" {
		resourceClass = ResourceClassSmall
	}
	if _, valid := ResolveResources(resourceClass); !valid {
		return Submission{}, &ValidationError{
			Field:   "resource_class",
			Problem: "must be small, medium, or large",
		}
	}

	return Submission{
		Executable: Executable{
			Image: submission.Executable.Image,
			Args:  args,
		},
		Input:         InputSpec{URI: inputURI},
		ResourceClass: resourceClass,
	}, nil
}

func ValidateIdempotencyKey(key string) error {
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

func NormalizeOutputRootURI(raw string) (string, error) {
	return normalizeObjectURI(raw, false)
}

func NormalizeInputURI(raw string) (string, error) {
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

func DeriveOutputRootURI(outputRootURI, id string) (string, error) {
	outputURI, err := url.JoinPath(outputRootURI, "jobs", id)
	if err != nil {
		return "", fmt.Errorf("derive output URI: %w", err)
	}
	return outputURI + "/", nil
}

func ValidID(id string) bool {
	if len(id) != 36 || id[8] != '-' || id[13] != '-' || id[18] != '-' || id[23] != '-' {
		return false
	}
	compact := id[:8] + id[9:13] + id[14:18] + id[19:23] + id[24:]
	_, err := hex.DecodeString(compact)
	return err == nil
}

func ValidateInputIdentity(inputSHA256 string, recordCount int64) error {
	decodedSHA256, err := hex.DecodeString(inputSHA256)
	if err != nil || len(decodedSHA256) != sha256.Size ||
		inputSHA256 != strings.ToLower(inputSHA256) {
		return &ValidationError{
			Field:   "input SHA-256",
			Problem: "must be 64 lowercase hexadecimal characters",
		}
	}
	if recordCount < 1 {
		return &ValidationError{
			Field:   "input record count",
			Problem: "must be positive",
		}
	}
	return nil
}

// ValidateParallelism reports whether parallelism is within Mill's supported
// per-job execution range.
func ValidateParallelism(parallelism int) error {
	if parallelism < 1 || parallelism > MaxParallelism {
		return &ValidationError{
			Field:   "parallelism",
			Problem: fmt.Sprintf("must be between 1 and %d", MaxParallelism),
		}
	}
	return nil
}

// ValidateShardSet reports whether shards completely and contiguously cover a
// non-empty input object.
func ValidateShardSet(shards ShardSet) error {
	if err := ValidateInputIdentity(
		shards.InputSHA256,
		shards.RecordCount,
	); err != nil {
		return err
	}
	if len(shards.Shards) < 1 || len(shards.Shards) > MaxTasksPerJob {
		return &ValidationError{
			Field: "logical shards",
			Problem: fmt.Sprintf(
				"must contain between 1 and %d ranges", MaxTasksPerJob,
			),
		}
	}
	var previousEnd int64
	for index, shard := range shards.Shards {
		if shard.StartByte != previousEnd || shard.EndByte <= shard.StartByte {
			return &ValidationError{
				Field:   fmt.Sprintf("logical shard %d", index),
				Problem: "must be a contiguous non-empty byte range",
			}
		}
		previousEnd = shard.EndByte
	}
	return nil
}
