// Package objectstore opens Mill inputs and publishes workload outputs by URI.
// This file defines the public Store API and delegates by URI scheme.
package objectstore

import (
	"context"
	"errors"
	"io"
)

type Config struct {
	Region   string
	Endpoint string
}

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

// New creates a file-only store when Region and Endpoint are empty. Setting a
// region enables S3; Endpoint is only needed by S3-compatible local services.
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

// Put publishes one complete object. File writes use rename for local
// atomicity; S3 exposes the replacement only after PutObject succeeds.
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
