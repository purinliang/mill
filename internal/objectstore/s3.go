// This file implements S3 client construction, reads, ranges, and writes.

package objectstore

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type s3Backend struct {
	client *s3.Client
}

func newS3Backend(
	ctx context.Context,
	region, endpoint string,
) (*s3Backend, error) {
	config, err := awsconfig.LoadDefaultConfig(
		ctx,
		awsconfig.WithRegion(region),
	)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	client := s3.NewFromConfig(config, func(options *s3.Options) {
		if endpoint != "" {
			options.BaseEndpoint = aws.String(endpoint)
			options.UsePathStyle = true
		}
	})
	return &s3Backend{client: client}, nil
}

func (b *s3Backend) open(
	ctx context.Context,
	bucket, key string,
) (io.ReadCloser, error) {
	output, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func (b *s3Backend) openRange(
	ctx context.Context,
	bucket, key string,
	start, end int64,
) (io.ReadCloser, error) {
	byteRange := fmt.Sprintf("bytes=%d-%d", start, end-1)
	output, err := b.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Range:  &byteRange,
	})
	if err != nil {
		return nil, err
	}
	return output.Body, nil
}

func (b *s3Backend) put(
	ctx context.Context,
	bucket, key string,
	body io.ReadSeeker,
) error {
	_, err := b.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   body,
	})
	return err
}
