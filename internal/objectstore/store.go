// Package objectstore opens Mill inputs and publishes workload outputs through
// absolute file:// and s3:// URIs.
package objectstore

import (
	"context"
	"errors"
	"io"
)

// Config configures the optional S3 backend. Its zero value enables only local
// file access.
type Config struct {
	// Region enables S3 access using the default AWS configuration chain.
	Region string

	// Endpoint selects an S3-compatible service instead of the AWS endpoint.
	// It requires Region to be set.
	Endpoint string
}

// Store reads inputs and publishes outputs through supported object URIs. A
// Store is safe for concurrent use, but it does not coordinate writes to the
// same URI.
type Store struct {
	s3 *s3Backend
}

type sectionReadCloser struct {
	io.Reader
	closer io.Closer
}

func (r *sectionReadCloser) Close() error {
	return r.closer.Close()
}

// New constructs a Store from config. An empty Region creates a file-only
// store; a non-empty Region loads the default AWS configuration for S3.
func New(ctx context.Context, config Config) (*Store, error) {
	if config.Endpoint != "" && config.Region == "" {
		return nil, errors.New(
			"S3 region is required when a custom endpoint is configured",
		)
	}
	if config.Region == "" {
		return &Store{}, nil
	}

	s3, err := newS3Backend(ctx, config.Region, config.Endpoint)
	if err != nil {
		return nil, err
	}
	return &Store{s3: s3}, nil
}

// Open opens the complete object at rawURI. The caller must close the returned
// reader.
func (s *Store) Open(
	ctx context.Context,
	rawURI string,
) (io.ReadCloser, error) {
	location, err := parseURI(rawURI)
	if err != nil {
		return nil, err
	}
	switch location.scheme {
	case "file":
		return openFile(location.filename)
	case "s3":
		if s.s3 == nil {
			return nil, errors.New("S3 is not configured")
		}
		return s.s3.open(ctx, location.bucket, location.key)
	default:
		panic("validated URI has unknown scheme")
	}
}

// OpenRange opens the byte range [start, end) from the object at rawURI. The
// caller must close the returned reader. The range must be non-negative and
// non-empty.
func (s *Store) OpenRange(
	ctx context.Context,
	rawURI string,
	start, end int64,
) (io.ReadCloser, error) {
	if start < 0 || end <= start {
		return nil, errors.New("byte range must be non-negative and non-empty")
	}
	location, err := parseURI(rawURI)
	if err != nil {
		return nil, err
	}
	switch location.scheme {
	case "file":
		return openFileRange(location.filename, start, end)
	case "s3":
		if s.s3 == nil {
			return nil, errors.New("S3 is not configured")
		}
		return s.s3.openRange(
			ctx,
			location.bucket,
			location.key,
			start,
			end,
		)
	default:
		panic("validated URI has unknown scheme")
	}
}

// Put atomically publishes one complete object at rawURI from body's current
// position. It does not coordinate concurrent writers: callers must assign a
// unique URI to each logical write. If writers target the same URI, a complete
// later write may replace an earlier one.
func (s *Store) Put(
	ctx context.Context,
	rawURI string,
	body io.ReadSeeker,
) error {
	location, err := parseURI(rawURI)
	if err != nil {
		return err
	}
	switch location.scheme {
	case "file":
		return putFile(location.filename, body)
	case "s3":
		if s.s3 == nil {
			return errors.New("S3 is not configured")
		}
		return s.s3.put(ctx, location.bucket, location.key, body)
	default:
		panic("validated URI has unknown scheme")
	}
}
