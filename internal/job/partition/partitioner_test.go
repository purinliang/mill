// This file tests dataset partitioning through its public API.

package partition_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/purinliang/mill/internal/job"
	"github.com/purinliang/mill/internal/job/partition"
	"github.com/purinliang/mill/internal/objectstore"
)

type memoryInput struct {
	contents []byte
}

func (m memoryInput) Open(context.Context, string) (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(m.contents)), nil
}

type changingInput struct {
	contents []string
	opens    int
}

func (i *changingInput) Open(
	context.Context,
	string,
) (io.ReadCloser, error) {
	if i.opens >= len(i.contents) {
		return nil, errors.New("test input opened too many times")
	}
	contents := i.contents[i.opens]
	i.opens++
	return io.NopCloser(strings.NewReader(contents)), nil
}

type failingInput struct {
	err error
}

func (i failingInput) Open(context.Context, string) (io.ReadCloser, error) {
	return nil, i.err
}

type changingThenFailingInput struct {
	contents string
	opens    int
}

func (i *changingThenFailingInput) Open(
	context.Context,
	string,
) (io.ReadCloser, error) {
	i.opens++
	if i.opens == 1 {
		return io.NopCloser(strings.NewReader(i.contents)), nil
	}
	return nil, errors.New("second scan failed")
}

func TestPartitionCreatesContiguousLogicalShards(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "records with spaces.jsonl")
	var input strings.Builder
	for index := range 100 {
		fmt.Fprintf(&input, `{"record":%d}`+"\n", index)
	}
	contents := []byte(input.String())
	if err := os.WriteFile(filename, contents, 0o600); err != nil {
		t.Fatalf("write JSONL input: %v", err)
	}

	shards, err := localPartitioner(t).Partition(
		context.Background(),
		fileURI(filename),
		3,
	)
	if err != nil {
		t.Fatalf("partition dataset: %v", err)
	}
	if shards.RecordCount != 100 {
		t.Errorf("record count = %d, want 100", shards.RecordCount)
	}
	if len(shards.Shards) != 12 {
		t.Fatalf("shard count = %d, want 12", len(shards.Shards))
	}
	digest := sha256.Sum256(contents)
	if want := hex.EncodeToString(digest[:]); shards.InputSHA256 != want {
		t.Errorf("input SHA-256 = %q, want %q", shards.InputSHA256, want)
	}

	previousEnd := int64(0)
	recordsSeen := 0
	for index, shard := range shards.Shards {
		if shard.StartByte != previousEnd {
			t.Errorf(
				"shard %d starts at %d, want previous end %d",
				index,
				shard.StartByte,
				previousEnd,
			)
		}
		if shard.EndByte <= shard.StartByte {
			t.Errorf("shard %d has invalid range %+v", index, shard)
		}
		part := contents[shard.StartByte:shard.EndByte]
		if part[len(part)-1] != '\n' {
			t.Errorf("shard %d does not end at a record boundary", index)
		}
		recordsSeen += len(strings.Split(
			strings.TrimSuffix(string(part), "\n"),
			"\n",
		))
		previousEnd = shard.EndByte
	}
	if previousEnd != int64(len(contents)) {
		t.Errorf(
			"last shard ends at %d, want input length %d",
			previousEnd,
			len(contents),
		)
	}
	if recordsSeen != 100 {
		t.Errorf("records across shards = %d, want 100", recordsSeen)
	}
}

func TestPartitionUsesAtMostOneShardPerRecord(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "small.jsonl")
	if err := os.WriteFile(filename, []byte("1\n2\n"), 0o600); err != nil {
		t.Fatalf("write JSONL input: %v", err)
	}
	shards, err := localPartitioner(t).Partition(
		context.Background(),
		fileURI(filename),
		3,
	)
	if err != nil {
		t.Fatalf("partition dataset: %v", err)
	}
	if len(shards.Shards) != 2 {
		t.Errorf("shard count = %d, want 2", len(shards.Shards))
	}
}

func TestPartitionReadsInputThroughConfiguredObjectReader(t *testing.T) {
	contents := []byte("{\"record\":1}\n{\"record\":2}\n")
	partitioner := partition.New(memoryInput{contents: contents})
	shards, err := partitioner.Partition(
		context.Background(),
		"s3://mill-input/records.jsonl",
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if shards.RecordCount != 2 || len(shards.Shards) != 2 ||
		shards.Shards[1].EndByte != int64(len(contents)) {
		t.Fatalf("shards = %+v", shards)
	}
}

func TestPartitionRejectsInvalidJSONL(t *testing.T) {
	tests := map[string]string{
		"empty file":   "",
		"invalid JSON": "{\n",
		"blank record": "{\"record\":1}\n\n",
		"two per line": "1 2\n",
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "invalid.jsonl")
			if err := os.WriteFile(
				filename,
				[]byte(contents),
				0o600,
			); err != nil {
				t.Fatalf("write JSONL input: %v", err)
			}
			if _, err := localPartitioner(t).Partition(
				context.Background(),
				fileURI(filename),
				3,
			); err == nil {
				t.Fatal("Partition succeeded, want an error")
			}
		})
	}
}

func TestPartitionRejectsInvalidLocationAndParallelism(t *testing.T) {
	partitioner := localPartitioner(t)
	for _, inputURI := range []string{
		"file:///definitely/not/present/input.jsonl",
		"s3://bucket/input.jsonl",
		fileURI(t.TempDir()) + "/",
	} {
		if _, err := partitioner.Partition(
			context.Background(),
			inputURI,
			3,
		); err == nil {
			t.Errorf("Partition(%q) succeeded, want an error", inputURI)
		}
	}
	if _, err := partitioner.Partition(
		context.Background(),
		"file:///data/input.jsonl",
		0,
	); err == nil {
		t.Fatal("Partition with zero parallelism succeeded")
	}
}

func TestPartitionRejectsInputChangedBetweenScans(t *testing.T) {
	input := &changingInput{contents: []string{
		"{\"record\":1}\n{\"record\":2}\n",
		"{\"record\":1}\n{\"record\":3}\n",
	}}
	partitioner := partition.New(input)

	_, err := partitioner.Partition(
		context.Background(),
		"s3://mill-input/records.jsonl",
		2,
	)
	if err == nil {
		t.Fatal("Partition succeeded after the input changed")
	}
	var validationError *job.ValidationError
	if !errors.As(err, &validationError) {
		t.Fatalf("Partition error type = %T, want ValidationError", err)
	}
	if validationError.Field != "input.uri" ||
		!strings.Contains(validationError.Problem, "changed") {
		t.Fatalf("Partition error = %+v", validationError)
	}
	if input.opens != 2 {
		t.Fatalf("input opened %d times, want 2", input.opens)
	}
}

func TestPartitionReportsReaderAndCancellationFailures(t *testing.T) {
	t.Run("second open fails", func(t *testing.T) {
		input := &changingThenFailingInput{
			contents: "{\"record\":1}\n",
		}
		partitioner := partition.New(input)

		_, err := partitioner.Partition(
			context.Background(),
			"s3://mill-input/records.jsonl",
			1,
		)
		if err == nil || !strings.Contains(err.Error(), "second scan failed") {
			t.Fatalf("Partition error = %v", err)
		}
		if input.opens != 2 {
			t.Fatalf("input opened %d times, want 2", input.opens)
		}
	})

	t.Run("context already cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		partitioner := partition.New(&changingInput{
			contents: []string{"{\"record\":1}\n"},
		})

		_, err := partitioner.Partition(
			ctx,
			"s3://mill-input/records.jsonl",
			1,
		)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Partition error = %v, want cancellation", err)
		}
	})

	t.Run("input cannot be opened", func(t *testing.T) {
		partitioner := partition.New(failingInput{
			err: errors.New("storage unavailable"),
		})

		_, err := partitioner.Partition(
			context.Background(),
			"s3://mill-input/records.jsonl",
			1,
		)
		if err == nil || !strings.Contains(err.Error(), "storage unavailable") {
			t.Fatalf("Partition error = %v", err)
		}
	})
}

func TestPartitionRequiresObjectReader(t *testing.T) {
	_, err := (partition.Partitioner{}).Partition(
		context.Background(),
		"file:///data/input.jsonl",
		1,
	)
	if err == nil || !strings.Contains(err.Error(), "object reader") {
		t.Fatalf("Partition error = %v", err)
	}
}

func localPartitioner(t *testing.T) partition.Partitioner {
	t.Helper()
	objects, err := objectstore.New(
		context.Background(),
		objectstore.Config{},
	)
	if err != nil {
		t.Fatal(err)
	}
	return partition.New(objects)
}

func fileURI(filename string) string {
	return (&url.URL{Scheme: "file", Path: filename}).String()
}
