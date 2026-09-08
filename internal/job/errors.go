package job

import "errors"

var (
	ErrIdempotencyConflict = errors.New(
		"idempotency key is already associated with a different submission",
	)
	ErrInputConflict = errors.New(
		"input differs from the logical shards already planned for the job",
	)
	ErrNotFound = errors.New("job not found")
)
