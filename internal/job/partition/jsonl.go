// This file implements JSON Lines scanning and record-boundary detection.

package partition

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
)

const maxJSONLRecordBytes = 16 << 20

type jsonlScan struct {
	sha256      string
	recordCount int64
}

func scanJSONL(
	ctx context.Context,
	objects ObjectReader,
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
			record := bytes.TrimSpace(
				bytes.TrimSuffix(line, []byte{'\n'}),
			)
			if len(record) == 0 || !json.Valid(record) {
				return jsonlScan{}, &job.ValidationError{
					Field: fmt.Sprintf(
						"input record %d",
						recordCount,
					),
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
			return jsonlScan{}, fmt.Errorf(
				"read JSONL input: %w",
				readErr,
			)
		}
	}
	if recordCount == 0 {
		return jsonlScan{}, &job.ValidationError{
			Field:   "input.uri",
			Problem: "must contain at least one JSONL record",
		}
	}
	return jsonlScan{
		sha256:      hex.EncodeToString(hash.Sum(nil)),
		recordCount: recordCount,
	}, nil
}
