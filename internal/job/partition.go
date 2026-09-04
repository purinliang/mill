package job

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
	"net/url"
	"os"
	"strings"
)

const (
	shardsPerWorker     = 4
	maxTasksPerJob      = 10_000
	maxParallelism      = 10_000
	maxJSONLRecordBytes = 16 << 20
)

type PartitionPlan struct {
	InputSHA256 string
	RecordCount int64
	Shards      []LogicalShard
}

type LogicalShard struct {
	StartByte int64
	EndByte   int64
}

type JSONLPartitioner struct{}

func (JSONLPartitioner) Plan(ctx context.Context, inputURI string, parallelism int) (PartitionPlan, error) {
	if parallelism < 1 || parallelism > maxParallelism {
		return PartitionPlan{}, &ValidationError{Field: "parallelism", Problem: "must be between 1 and 10000"}
	}
	normalizedURI, err := normalizeInputURI(inputURI)
	if err != nil {
		return PartitionPlan{}, &ValidationError{Field: "input.uri", Problem: err.Error()}
	}
	filename, err := localFilePath(normalizedURI)
	if err != nil {
		return PartitionPlan{}, err
	}

	firstScan, err := scanJSONL(ctx, filename, nil)
	if err != nil {
		return PartitionPlan{}, err
	}
	targetShards := parallelism * shardsPerWorker
	if targetShards > maxTasksPerJob {
		targetShards = maxTasksPerJob
	}
	if int64(targetShards) > firstScan.recordCount {
		targetShards = int(firstScan.recordCount)
	}
	recordsPerShard := (firstScan.recordCount + int64(targetShards) - 1) / int64(targetShards)

	shards := make([]LogicalShard, 0, targetShards)
	var shardStart, lastRecordEnd int64
	var recordsInShard int64
	secondScan, err := scanJSONL(ctx, filename, func(_, _, recordEnd int64) {
		recordsInShard++
		lastRecordEnd = recordEnd
		if recordsInShard == recordsPerShard {
			shards = append(shards, LogicalShard{StartByte: shardStart, EndByte: recordEnd})
			shardStart = recordEnd
			recordsInShard = 0
		}
	})
	if err != nil {
		return PartitionPlan{}, err
	}
	if recordsInShard > 0 {
		shards = append(shards, LogicalShard{StartByte: shardStart, EndByte: lastRecordEnd})
	}
	if secondScan.sha256 != firstScan.sha256 || secondScan.recordCount != firstScan.recordCount {
		return PartitionPlan{}, &ValidationError{Field: "input.uri", Problem: "changed while Mill was planning logical shards"}
	}

	return PartitionPlan{
		InputSHA256: firstScan.sha256,
		RecordCount: firstScan.recordCount,
		Shards:      shards,
	}, nil
}

type jsonlScan struct {
	sha256      string
	recordCount int64
}

func scanJSONL(ctx context.Context, filename string, visit func(int64, int64, int64)) (jsonlScan, error) {
	file, err := os.Open(filename)
	if err != nil {
		return jsonlScan{}, &ValidationError{Field: "input.uri", Problem: "cannot be opened"}
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return jsonlScan{}, fmt.Errorf("inspect JSONL input: %w", err)
	}
	if !info.Mode().IsRegular() {
		return jsonlScan{}, &ValidationError{Field: "input.uri", Problem: "must refer to a regular file"}
	}

	hash := sha256.New()
	reader := bufio.NewReader(file)
	var offset, recordCount int64
	for {
		if err := ctx.Err(); err != nil {
			return jsonlScan{}, err
		}
		start := offset
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			if len(line) > maxJSONLRecordBytes {
				return jsonlScan{}, &ValidationError{
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
				return jsonlScan{}, &ValidationError{
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
		return jsonlScan{}, &ValidationError{Field: "input.uri", Problem: "must contain at least one JSONL record"}
	}
	return jsonlScan{sha256: hex.EncodeToString(hash.Sum(nil)), recordCount: recordCount}, nil
}

func localFilePath(inputURI string) (string, error) {
	parsed, err := url.Parse(inputURI)
	if err != nil {
		return "", fmt.Errorf("parse normalized input URI: %w", err)
	}
	filename, err := url.PathUnescape(parsed.EscapedPath())
	if err != nil {
		return "", &ValidationError{Field: "input.uri", Problem: "contains an invalid escaped path"}
	}
	return filename, nil
}

func validatePartitionPlan(plan PartitionPlan) error {
	if err := validateInputIdentity(plan.InputSHA256, plan.RecordCount); err != nil {
		return err
	}
	if len(plan.Shards) < 1 || len(plan.Shards) > maxTasksPerJob {
		return &ValidationError{Field: "logical shards", Problem: "must contain between 1 and 10000 ranges"}
	}
	var previousEnd int64
	for index, shard := range plan.Shards {
		if shard.StartByte != previousEnd || shard.EndByte <= shard.StartByte {
			return &ValidationError{Field: fmt.Sprintf("logical shard %d", index), Problem: "must be a contiguous non-empty byte range"}
		}
		previousEnd = shard.EndByte
	}
	return nil
}

func validateInputIdentity(inputSHA256 string, recordCount int64) error {
	decodedSHA256, err := hex.DecodeString(inputSHA256)
	if err != nil || len(decodedSHA256) != sha256.Size || inputSHA256 != strings.ToLower(inputSHA256) {
		return &ValidationError{Field: "input SHA-256", Problem: "must be 64 lowercase hexadecimal characters"}
	}
	if recordCount < 1 {
		return &ValidationError{Field: "input record count", Problem: "must be positive"}
	}
	return nil
}
