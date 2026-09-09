// This file tests task materialization validation at the PostgreSQL boundary.
package postgres_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/purinliang/mill/internal/job"
	jobpostgres "github.com/purinliang/mill/internal/job/postgres"
)

func TestTaskMethodsRejectInvalidArguments(t *testing.T) {
	repository, err := jobpostgres.NewRepository(
		&pgxpool.Pool{},
		"file:///tmp/mill-output",
	)
	if err != nil {
		t.Fatal(err)
	}
	input := []byte("{\"text\":\"test record\"}\n")
	digest := sha256.Sum256(input)
	valid := job.ShardSet{
		InputSHA256: fmt.Sprintf("%x", digest),
		RecordCount: 1,
		Shards: []job.LogicalShard{
			{StartByte: 0, EndByte: int64(len(input))},
		},
	}
	missingID := "00000000-0000-7000-8000-000000000001"

	tests := map[string]func() error{
		"invalid job ID": func() error {
			_, err := repository.Materialize(
				context.Background(), "not-a-uuid", valid,
			)
			return err
		},
		"invalid digest": func() error {
			shards := valid
			shards.InputSHA256 = "invalid"
			_, err := repository.Materialize(
				context.Background(), missingID, shards,
			)
			return err
		},
		"no shards": func() error {
			shards := valid
			shards.Shards = nil
			_, err := repository.Materialize(
				context.Background(), missingID, shards,
			)
			return err
		},
		"discontinuous shards": func() error {
			shards := valid
			shards.Shards[0].StartByte = 1
			_, err := repository.Materialize(
				context.Background(), missingID, shards,
			)
			return err
		},
		"get invalid job ID": func() error {
			_, err := repository.Get(context.Background(), "not-a-uuid")
			return err
		},
	}

	for name, operation := range tests {
		t.Run(name, func(t *testing.T) {
			requireInvalidArgument(t, operation())
		})
	}
}
