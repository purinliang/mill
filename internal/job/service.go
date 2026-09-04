package job

import (
	"context"
	"errors"
)

type Service struct {
	repository  *Repository
	partitioner JSONLPartitioner
	parallelism int
}

func NewService(repository *Repository, partitioner JSONLPartitioner, parallelism int) (*Service, error) {
	if repository == nil {
		return nil, errors.New("job repository is required")
	}
	if parallelism < 1 || parallelism > maxParallelism {
		return nil, errors.New("MILL_PARALLELISM must be between 1 and 10000")
	}
	return &Service{repository: repository, partitioner: partitioner, parallelism: parallelism}, nil
}

func (s *Service) Create(ctx context.Context, idempotencyKey string, submission Submission) (Job, bool, error) {
	normalizedSubmission, err := normalizeSubmission(submission)
	if err != nil {
		return Job{}, false, err
	}
	existingJob, found, err := s.repository.FindSubmission(ctx, idempotencyKey, normalizedSubmission)
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
	plan, err := s.partitioner.Plan(ctx, normalizedSubmission.Input.URI, parallelism)
	if err != nil {
		return Job{}, false, err
	}
	if found {
		materializedJob, err := s.repository.Materialize(ctx, existingJob.ID, plan)
		return materializedJob, false, err
	}

	createdJob, created, err := s.repository.Create(
		ctx,
		idempotencyKey,
		normalizedSubmission,
		plan.InputSHA256,
		plan.RecordCount,
		parallelism,
	)
	if err != nil {
		return Job{}, false, err
	}
	if createdJob.State != StatePreparing {
		return createdJob, created, nil
	}
	if createdJob.Parallelism != parallelism {
		plan, err = s.partitioner.Plan(ctx, normalizedSubmission.Input.URI, createdJob.Parallelism)
		if err != nil {
			return Job{}, false, err
		}
	}

	materializedJob, err := s.repository.Materialize(ctx, createdJob.ID, plan)
	if err != nil {
		return Job{}, false, err
	}
	return materializedJob, created, nil
}

func (s *Service) Get(ctx context.Context, id string) (Job, error) {
	value, err := s.repository.Get(ctx, id)
	if err != nil {
		return Job{}, err
	}
	if value.State == StateCompleted {
		value.Results, err = s.repository.CompletedResults(ctx, id)
	}
	return value, err
}
