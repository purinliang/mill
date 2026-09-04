package job

import (
	"context"
	"errors"
)

type Service struct {
	repository *Repository
	manifests  ManifestLoader
}

func NewService(repository *Repository, manifests ManifestLoader) (*Service, error) {
	if repository == nil {
		return nil, errors.New("job repository is required")
	}
	if manifests == nil {
		return nil, errors.New("manifest loader is required")
	}
	return &Service{repository: repository, manifests: manifests}, nil
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

	manifest, err := s.manifests.Load(ctx, normalizedSubmission.Dataset.ManifestURI)
	if err != nil {
		return Job{}, false, err
	}
	if found {
		materializedJob, err := s.repository.Materialize(ctx, existingJob.ID, manifest)
		return materializedJob, false, err
	}

	createdJob, created, err := s.repository.Create(ctx, idempotencyKey, normalizedSubmission)
	if err != nil {
		return Job{}, false, err
	}
	if createdJob.State != StatePreparing {
		return createdJob, created, nil
	}

	materializedJob, err := s.repository.Materialize(ctx, createdJob.ID, manifest)
	if err != nil {
		return Job{}, false, err
	}
	return materializedJob, created, nil
}

func (s *Service) Get(ctx context.Context, id string) (Job, error) {
	return s.repository.Get(ctx, id)
}
