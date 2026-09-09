// This file coordinates submission, partitioning, persistence, and status.
package job

import (
	"context"
	"errors"
	"fmt"
)

type Service struct {
	store       Store
	partitioner DatasetPartitioner
	parallelism int
}

func NewService(
	store Store,
	partitioner DatasetPartitioner,
	parallelism int,
) (*Service, error) {
	if store == nil {
		return nil, errors.New("job store is required")
	}
	if partitioner == nil {
		return nil, errors.New("dataset partitioner is required")
	}
	if err := ValidateParallelism(parallelism); err != nil {
		return nil, fmt.Errorf("MILL_PARALLELISM: %w", err)
	}
	return &Service{
		store:       store,
		partitioner: partitioner,
		parallelism: parallelism,
	}, nil
}

func (s *Service) Create(
	ctx context.Context,
	idempotencyKey string,
	submission Submission,
) (Job, bool, error) {
	normalizedSubmission, err := NormalizeSubmission(submission)
	if err != nil {
		return Job{}, false, err
	}
	existingJob, found, err := s.store.FindSubmission(ctx, idempotencyKey, normalizedSubmission)
	if err != nil {
		return Job{}, false, err
	}
	if found && existingJob.State != StatePreparing {
		return existingJob, false, nil
	}

	parallelism := s.parallelism
	if found {
		parallelism = existingJob.Parallelism
	}
	shards, err := s.partitioner.Partition(
		ctx,
		normalizedSubmission.Input.URI,
		parallelism,
	)
	if err != nil {
		return Job{}, false, err
	}
	if found {
		materializedJob, err := s.store.Materialize(
			ctx,
			existingJob.ID,
			shards,
		)
		return materializedJob, false, err
	}

	createdJob, created, err := s.store.Create(
		ctx,
		idempotencyKey,
		normalizedSubmission,
		shards.InputSHA256,
		shards.RecordCount,
		parallelism,
	)
	if err != nil {
		return Job{}, false, err
	}
	if createdJob.State != StatePreparing {
		return createdJob, created, nil
	}
	if createdJob.Parallelism != parallelism {
		shards, err = s.partitioner.Partition(
			ctx,
			normalizedSubmission.Input.URI,
			createdJob.Parallelism,
		)
		if err != nil {
			return Job{}, false, err
		}
	}

	materializedJob, err := s.store.Materialize(
		ctx,
		createdJob.ID,
		shards,
	)
	if err != nil {
		return Job{}, false, err
	}
	return materializedJob, created, nil
}

func (s *Service) Get(ctx context.Context, id string) (Job, error) {
	value, err := s.store.Get(ctx, id)
	if err != nil {
		return Job{}, err
	}
	if value.State == StateCompleted {
		value.Results, err = s.store.CompletedResults(ctx, id)
	}
	return value, err
}
