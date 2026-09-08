package jsonl

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/purinliang/mill/internal/job"
	"github.com/purinliang/mill/internal/objectstore"
)

const (
	shardsPerWorker     = 4
	maxJSONLRecordBytes = 16 << 20
)

type inputOpener interface {
	Open(context.Context, string) (io.ReadCloser, error)
}

type Planner struct {
	objects inputOpener
}

func NewPlanner(objects inputOpener) Planner {
	return Planner{objects: objects}
}

func (p Planner) Plan(
	ctx context.Context,
	inputURI string,
	parallelism int,
) (job.PartitionPlan, error) {
	if parallelism < 1 || parallelism > job.MaxParallelism {
		return job.PartitionPlan{}, &job.ValidationError{
			Field:   "parallelism",
			Problem: "must be between 1 and 10000",
		}
	}
	normalizedURI, err := job.NormalizeInputURI(inputURI)
	if err != nil {
		return job.PartitionPlan{}, &job.ValidationError{Field: "input.uri", Problem: err.Error()}
	}
	objects := p.objects
	if objects == nil {
		// Preserve the useful zero value for file-based unit tests and local use.
		objects = &objectstore.Store{}
	}

	firstScan, err := scanJSONL(ctx, objects, normalizedURI, nil)
	if err != nil {
		return job.PartitionPlan{}, err
	}
	targetShards := parallelism * shardsPerWorker
	if targetShards > job.MaxTasksPerJob {
		targetShards = job.MaxTasksPerJob
	}
	if int64(targetShards) > firstScan.recordCount {
		targetShards = int(firstScan.recordCount)
	}
	recordsPerShard := (firstScan.recordCount + int64(targetShards) - 1) / int64(targetShards)

	shards := make([]job.LogicalShard, 0, targetShards)
	var shardStart, lastRecordEnd int64
	var recordsInShard int64
	secondScan, err := scanJSONL(ctx, objects, normalizedURI, func(_, _, recordEnd int64) {
		recordsInShard++
		lastRecordEnd = recordEnd
		if recordsInShard == recordsPerShard {
			shards = append(shards, job.LogicalShard{StartByte: shardStart, EndByte: recordEnd})
			shardStart = recordEnd
			recordsInShard = 0
		}
	})
	if err != nil {
		return job.PartitionPlan{}, err
	}
	if recordsInShard > 0 {
		shards = append(shards, job.LogicalShard{StartByte: shardStart, EndByte: lastRecordEnd})
	}
	if secondScan.sha256 != firstScan.sha256 || secondScan.recordCount != firstScan.recordCount {
		return job.PartitionPlan{}, &job.ValidationError{
			Field:   "input.uri",
			Problem: "changed while Mill was planning logical shards",
		}
	}

	return job.PartitionPlan{
		InputSHA256: firstScan.sha256,
		RecordCount: firstScan.recordCount,
		Shards:      shards,
	}, nil
}

type jsonlScan struct {
	sha256      string
	recordCount int64
}

func scanJSONL(
	ctx context.Context,
	objects inputOpener,
	inputURI string,
	visit func(int64, int64, int64),
) (jsonlScan, error) {
	input, err := objects.Open(ctx, inputURI)
	if err != nil {
		return jsonlScan{}, &job.ValidationError{
			Field:   "input.uri",
			Problem: "cannot be opened: " + err.Error(),
		}
	}
	defer input.Close()

	hash := sha256.New()
	reader := bufio.NewReader(input)
	var offset, recordCount int64
	for {
		if err := ctx.Err(); err != nil {
			return jsonlScan{}, err
		}
		start := offset
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			if len(line) > maxJSONLRecordBytes {
				return jsonlScan{}, &job.ValidationError{
					Field:   fmt.Sprintf("input record %d", recordCount),
					Problem: "must be at most 16 MiB",
				}
			}
			if _, err := hash.Write(line); err != nil {
				return jsonlScan{}, fmt.Errorf("hash JSONL input: %w", err)
			}
			offset += int64(len(line))
			record := bytes.TrimSpace(bytes.TrimSuffix(line, []byte{'\n'}))
			if len(record) == 0 || !json.Valid(record) {
				return jsonlScan{}, &job.ValidationError{
					Field:   fmt.Sprintf("input record %d", recordCount),
					Problem: "must be one valid JSON value on one line",
				}
			}
			if visit != nil {
				visit(recordCount, start, offset)
			}
			recordCount++
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return jsonlScan{}, fmt.Errorf("read JSONL input: %w", readErr)
		}
	}
	if recordCount == 0 {
		return jsonlScan{}, &job.ValidationError{
			Field:   "input.uri",
			Problem: "must contain at least one JSONL record",
		}
	}
	return jsonlScan{sha256: hex.EncodeToString(hash.Sum(nil)), recordCount: recordCount}, nil
}
