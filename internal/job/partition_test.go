package job

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestJSONLPartitionerCreatesContiguousLogicalShards(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "records with spaces.jsonl")
	var input strings.Builder
	for index := range 100 {
		fmt.Fprintf(&input, `{"record":%d}`+"\n", index)
	}
	contents := []byte(input.String())
	if err := os.WriteFile(filename, contents, 0o600); err != nil {
		t.Fatalf("write JSONL input: %v", err)
	}

	plan, err := (JSONLPartitioner{}).Plan(context.Background(), fileURI(filename), 3)
	if err != nil {
		t.Fatalf("plan logical shards: %v", err)
	}
	if plan.RecordCount != 100 {
		t.Errorf("record count = %d, want 100", plan.RecordCount)
	}
	if len(plan.Shards) != 12 {
		t.Fatalf("shard count = %d, want 12", len(plan.Shards))
	}
	digest := sha256.Sum256(contents)
	if want := hex.EncodeToString(digest[:]); plan.InputSHA256 != want {
		t.Errorf("input SHA-256 = %q, want %q", plan.InputSHA256, want)
	}

	previousEnd := int64(0)
	recordsSeen := 0
	for index, shard := range plan.Shards {
		if shard.StartByte != previousEnd {
			t.Errorf("shard %d starts at %d, want previous end %d", index, shard.StartByte, previousEnd)
		}
		if shard.EndByte <= shard.StartByte {
			t.Errorf("shard %d has empty or negative range %+v", index, shard)
		}
		part := contents[shard.StartByte:shard.EndByte]
		if part[len(part)-1] != '\n' {
			t.Errorf("shard %d does not end at a JSONL record boundary", index)
		}
		recordsSeen += len(strings.Split(strings.TrimSuffix(string(part), "\n"), "\n"))
		previousEnd = shard.EndByte
	}
	if previousEnd != int64(len(contents)) {
		t.Errorf("last shard ends at %d, want input length %d", previousEnd, len(contents))
	}
	if recordsSeen != 100 {
		t.Errorf("records across shards = %d, want 100", recordsSeen)
	}
}

func TestJSONLPartitionerUsesAtMostOneShardPerRecord(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "small.jsonl")
	if err := os.WriteFile(filename, []byte("1\n2\n"), 0o600); err != nil {
		t.Fatalf("write JSONL input: %v", err)
	}
	plan, err := (JSONLPartitioner{}).Plan(context.Background(), fileURI(filename), 3)
	if err != nil {
		t.Fatalf("plan logical shards: %v", err)
	}
	if len(plan.Shards) != 2 {
		t.Errorf("shard count = %d, want 2", len(plan.Shards))
	}
}

func TestJSONLPartitionerRejectsInvalidInput(t *testing.T) {
	tests := map[string]string{
		"empty file":   "",
		"invalid JSON": "{\n",
		"blank record": "{\"record\":1}\n\n",
		"two per line": "1 2\n",
	}
	for name, contents := range tests {
		t.Run(name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "invalid.jsonl")
			if err := os.WriteFile(filename, []byte(contents), 0o600); err != nil {
				t.Fatalf("write JSONL input: %v", err)
			}
			if _, err := (JSONLPartitioner{}).Plan(context.Background(), fileURI(filename), 3); err == nil {
				t.Fatal("plan logical shards succeeded, want an error")
			}
		})
	}
}

func TestJSONLPartitionerRejectsInvalidLocationAndParallelism(t *testing.T) {
	partitioner := JSONLPartitioner{}
	for _, inputURI := range []string{
		"file:///definitely/not/present/input.jsonl",
		"s3://bucket/input.jsonl",
		fileURI(t.TempDir()) + "/",
	} {
		if _, err := partitioner.Plan(context.Background(), inputURI, 3); err == nil {
			t.Errorf("Plan(%q) succeeded, want an error", inputURI)
		}
	}
	if _, err := partitioner.Plan(context.Background(), "file:///data/input.jsonl", 0); err == nil {
		t.Fatal("Plan with zero parallelism succeeded, want an error")
	}
}

func fileURI(filename string) string {
	return (&url.URL{Scheme: "file", Path: filename}).String()
}
