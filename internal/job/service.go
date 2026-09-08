// This file coordinates submission, planning, persistence, and status reads.
package job

import (
	"context"
	"errors"
)

type Service struct {
	store       Store
	planner     Planner
	parallelism int
}

func NewService(store Store, planner Planner, parallelism int) (*Service, error) {
	if store == nil {
		return nil, errors.New("job store is required")
	}
	if planner == nil {
		return nil, errors.New("dataset planner is required")
	}
	if parallelism < 1 || parallelism > MaxParallelism {
		return nil, errors.New("MILL_PARALLELISM must be between 1 and 10000")
	}
	return &Service{store: store, planner: planner, parallelism: parallelism}, nil
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
	plan, err := s.planner.Plan(ctx, normalizedSubmission.Input.URI, parallelism)
	if err != nil {
		return Job{}, false, err
	}
	if found {
		materializedJob, err := s.store.Materialize(ctx, existingJob.ID, plan)
		return materializedJob, false, err
	}

	createdJob, created, err := s.store.Create(
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
		plan, err = s.planner.Plan(ctx, normalizedSubmission.Input.URI, createdJob.Parallelism)
		if err != nil {
			return Job{}, false, err
		}
	}

	materializedJob, err := s.store.Materialize(ctx, createdJob.ID, plan)
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
