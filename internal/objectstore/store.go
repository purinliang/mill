// Package objectstore opens Mill inputs and publishes workload outputs by URI.
package objectstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type Config struct {
	Region   string
	Endpoint string
}

type Store struct {
	s3 *s3.Client
}

// New creates a file-only store when Region and Endpoint are empty. Setting a
// region enables S3; Endpoint is only needed by S3-compatible local services.
func New(ctx context.Context, config Config) (*Store, error) {
	if config.Endpoint != "" && config.Region == "" {
		return nil, errors.New("S3 region is required when a custom endpoint is configured")
	}
	if config.Region == "" {
		return &Store{}, nil
	}

	awsConfig, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(config.Region))
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	client := s3.NewFromConfig(awsConfig, func(options *s3.Options) {
		if config.Endpoint != "" {
			options.BaseEndpoint = aws.String(config.Endpoint)
			options.UsePathStyle = true
		}
	})
	return &Store{s3: client}, nil
}

func (s *Store) Open(ctx context.Context, rawURI string) (io.ReadCloser, error) {
	location, err := parseURI(rawURI)
	if err != nil {
		return nil, err
	}
	switch location.scheme {
	case "file":
		file, err := os.Open(location.filename)
		if err != nil {
			return nil, err
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if !info.Mode().IsRegular() {
			_ = file.Close()
			return nil, errors.New("file URI must refer to a regular file")
		}
		return file, nil
	case "s3":
		if s.s3 == nil {
			return nil, errors.New("S3 is not configured")
		}
		output, err := s.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: &location.bucket, Key: &location.key})
		if err != nil {
			return nil, err
		}
		return output.Body, nil
	default:
		panic("validated URI has unknown scheme")
	}
}

func (s *Store) OpenRange(ctx context.Context, rawURI string, start, end int64) (io.ReadCloser, error) {
	if start < 0 || end <= start {
		return nil, errors.New("byte range must be non-negative and non-empty")
	}
	location, err := parseURI(rawURI)
	if err != nil {
		return nil, err
	}
	switch location.scheme {
	case "file":
		file, err := os.Open(location.filename)
		if err != nil {
			return nil, err
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		if !info.Mode().IsRegular() {
			_ = file.Close()
			return nil, errors.New("file URI must refer to a regular file")
		}
		if end > info.Size() {
			_ = file.Close()
			return nil, fmt.Errorf("byte range ends at %d beyond object size %d", end, info.Size())
		}
		return &sectionReadCloser{Reader: io.NewSectionReader(file, start, end-start), closer: file}, nil
	case "s3":
		if s.s3 == nil {
			return nil, errors.New("S3 is not configured")
		}
		byteRange := fmt.Sprintf("bytes=%d-%d", start, end-1)
		output, err := s.s3.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &location.bucket,
			Key:    &location.key,
			Range:  &byteRange,
		})
		if err != nil {
			return nil, err
		}
		return output.Body, nil
	default:
		panic("validated URI has unknown scheme")
	}
}

// Put publishes one complete object. File writes use rename for local atomicity;
// S3 PutObject exposes the replacement only after the request succeeds.
func (s *Store) Put(ctx context.Context, rawURI string, body io.ReadSeeker) error {
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
		_, err := s.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: &location.bucket, Key: &location.key, Body: body})
		return err
	default:
		panic("validated URI has unknown scheme")
	}
}

type location struct {
	scheme   string
	filename string
	bucket   string
	key      string
}

func parseURI(raw string) (location, error) {
	parsed, err := url.ParseRequestURI(raw)
	if err != nil || parsed.User != nil || parsed.Opaque != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return location{}, errors.New("object URI must be an absolute file:// or s3:// URI without a query or fragment")
	}
	switch parsed.Scheme {
	case "file":
		if parsed.Host != "" {
			return location{}, errors.New("file URI must not contain a host")
		}
		filename, err := url.PathUnescape(parsed.EscapedPath())
		if err != nil || !filepath.IsAbs(filename) || filepath.Clean(filename) == "/" {
			return location{}, errors.New("file URI must contain an absolute path other than /")
		}
		return location{scheme: "file", filename: filepath.Clean(filename)}, nil
	case "s3":
		if parsed.Host == "" || parsed.Port() != "" || strings.Contains(parsed.Host, ":") {
			return location{}, errors.New("S3 URI must contain one bucket name")
		}
		key, err := url.PathUnescape(strings.TrimPrefix(parsed.EscapedPath(), "/"))
		if err != nil || key == "" || strings.HasSuffix(parsed.Path, "/") {
			return location{}, errors.New("S3 URI must contain an object key")
		}
		return location{scheme: "s3", bucket: parsed.Host, key: key}, nil
	default:
		return location{}, errors.New("object URI scheme must be file or s3")
	}
}

type sectionReadCloser struct {
	io.Reader
	closer io.Closer
}

func (r *sectionReadCloser) Close() error { return r.closer.Close() }

func putFile(filename string, body io.Reader) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0o755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(filename), ".mill-output-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary output: %w", err)
	}
	temporaryFilename := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryFilename)
		}
	}()
	if _, err := io.Copy(temporary, body); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close output: %w", err)
	}
	if err := os.Rename(temporaryFilename, filename); err != nil {
		return fmt.Errorf("publish output: %w", err)
	}
	committed = true
	return nil
}
