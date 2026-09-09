// This file tests construction of the PostgreSQL Job repository.
package postgres_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/purinliang/mill/internal/execution"
	"github.com/purinliang/mill/internal/job"
	jobpostgres "github.com/purinliang/mill/internal/job/postgres"
)

func TestNewRepositoryValidatesConfiguration(t *testing.T) {
	if _, err := jobpostgres.NewRepository(
		nil,
		"file:///tmp/mill-output",
	); err == nil || !strings.Contains(err.Error(), "PostgreSQL") {
		t.Fatalf("nil database error = %v", err)
	}

	pool := &pgxpool.Pool{}
	if _, err := jobpostgres.NewRepository(
		pool,
		"relative/output",
	); err == nil || !strings.Contains(err.Error(), "MILL_OUTPUT_ROOT_URI") {
		t.Fatalf("invalid output root error = %v", err)
	}
	if _, err := jobpostgres.NewRepository(
		pool,
		"file:///tmp/mill-output",
	); err != nil {
		t.Fatalf("valid configuration: %v", err)
	}
	if _, err := jobpostgres.NewRepository(
		pool,
		"s3://mill-output",
	); err != nil {
		t.Fatalf("valid S3 output root: %v", err)
	}
}

func TestRepositoryReportsClosedDatabase(t *testing.T) {
	pool, err := pgxpool.New(
		context.Background(),
		"postgresql://mill:mill@127.0.0.1:1/mill",
	)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := jobpostgres.NewRepository(
		pool,
		"file:///tmp/mill-output",
	)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()

	input := []byte("{\"text\":\"test record\"}\n")
	digest := sha256.Sum256(input)
	inputSHA256 := fmt.Sprintf("%x", digest)
	submission := job.Submission{
		Executable: job.Executable{Image: "mill/example:dev"},
		Input:      job.InputSpec{URI: "file:///tmp/records.jsonl"},
	}
	jobID := "00000000-0000-7000-8000-000000000001"
	operations := map[string]func() error{
		"find submission": func() error {
			_, _, err := repository.FindSubmission(
				context.Background(), "request-1", submission,
			)
			return err
		},
		"create": func() error {
			_, _, err := repository.Create(
				context.Background(),
				"request-1",
				submission,
				inputSHA256,
				1,
				1,
				execution.Resources{
					CPURequestMillis:   100,
					CPULimitMillis:     1000,
					MemoryRequestBytes: 128 << 20,
					MemoryLimitBytes:   128 << 20,
				},
			)
			return err
		},
		"materialize": func() error {
			_, err := repository.Materialize(
				context.Background(),
				jobID,
				job.ShardSet{
					InputSHA256: inputSHA256,
					RecordCount: 1,
					Shards: []job.LogicalShard{
						{StartByte: 0, EndByte: int64(len(input))},
					},
				},
			)
			return err
		},
		"get status": func() error {
			_, err := repository.Get(context.Background(), jobID)
			return err
		},
		"list results": func() error {
			_, err := repository.CompletedResults(
				context.Background(), jobID,
			)
			return err
		},
	}

	for name, operation := range operations {
		t.Run(name, func(t *testing.T) {
			if err := operation(); err == nil {
				t.Fatal("operation succeeded with a closed database")
			}
		})
	}
}
