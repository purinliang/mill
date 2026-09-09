// Package partition divides job input datasets into logical byte-range shards.
// The current implementation recognizes JSON Lines record boundaries.
package partition

import (
	"context"
	"errors"
	"io"

	"github.com/purinliang/mill/internal/job"
)

const shardsPerWorker = 4

// ObjectReader opens complete dataset objects for partitioning.
type ObjectReader interface {
	Open(context.Context, string) (io.ReadCloser, error)
}

// Partitioner divides JSONL inputs without retaining every record boundary in
// memory. It reads each input twice to keep its memory use bounded.
type Partitioner struct {
	objects ObjectReader
}

// New constructs a Partitioner that reads inputs through objects.
func New(objects ObjectReader) Partitioner {
	return Partitioner{objects: objects}
}

// Partition validates inputURI and returns its identity and logical shards.
func (p Partitioner) Partition(
	ctx context.Context,
	inputURI string,
	parallelism int,
) (job.ShardSet, error) {
	if p.objects == nil {
		return job.ShardSet{}, errors.New("partition object reader is required")
	}
	if err := job.ValidateParallelism(parallelism); err != nil {
		return job.ShardSet{}, err
	}
	normalizedURI, err := job.NormalizeInputURI(inputURI)
	if err != nil {
		return job.ShardSet{}, &job.ValidationError{
			Field:   "input.uri",
			Problem: err.Error(),
		}
	}
	firstScan, err := scanJSONL(ctx, p.objects, normalizedURI, nil)
	if err != nil {
		return job.ShardSet{}, err
	}
	targetShards := parallelism * shardsPerWorker
	if targetShards > job.MaxTasksPerJob {
		targetShards = job.MaxTasksPerJob
	}
	if int64(targetShards) > firstScan.recordCount {
		targetShards = int(firstScan.recordCount)
	}
	recordsPerShard := (firstScan.recordCount + int64(targetShards) - 1) /
		int64(targetShards)

	shards := make([]job.LogicalShard, 0, targetShards)
	var shardStart, lastRecordEnd int64
	var recordsInShard int64
	secondScan, err := scanJSONL(
		ctx,
		p.objects,
		normalizedURI,
		func(_, _, recordEnd int64) {
			recordsInShard++
			lastRecordEnd = recordEnd
			if recordsInShard == recordsPerShard {
				shards = append(shards, job.LogicalShard{
					StartByte: shardStart,
					EndByte:   recordEnd,
				})
				shardStart = recordEnd
				recordsInShard = 0
			}
		},
	)
	if err != nil {
		return job.ShardSet{}, err
	}
	if recordsInShard > 0 {
		shards = append(shards, job.LogicalShard{
			StartByte: shardStart,
			EndByte:   lastRecordEnd,
		})
	}
	if secondScan.sha256 != firstScan.sha256 ||
		secondScan.recordCount != firstScan.recordCount {
		return job.ShardSet{}, &job.ValidationError{
			Field:   "input.uri",
			Problem: "changed while Mill was partitioning logical shards",
		}
	}

	return job.ShardSet{
		InputSHA256: firstScan.sha256,
		RecordCount: firstScan.recordCount,
		Shards:      shards,
	}, nil
}
