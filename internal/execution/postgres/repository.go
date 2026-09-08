package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/purinliang/mill/internal/execution"
)

type Repository struct {
	database *pgxpool.Pool
}

func NewRepository(database *pgxpool.Pool) (*Repository, error) {
	if database == nil {
		return nil, errors.New("PostgreSQL connection pool is required")
	}
	return &Repository{database: database}, nil
}

type ValidationError struct {
	Field   string
	Problem string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s %s", e.Field, e.Problem)
}

func (e *ValidationError) InvalidArgument() bool {
	return true
}

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

type Attempt = execution.Attempt
type AttemptState = execution.AttemptState
type ClaimedAttempt = execution.ClaimedAttempt

const (
	AttemptStateStarting  = execution.AttemptStateStarting
	AttemptStateRunning   = execution.AttemptStateRunning
	AttemptStateCompleted = execution.AttemptStateCompleted
	AttemptStateFailed    = execution.AttemptStateFailed
)

var (
	ErrNoTaskAvailable          = execution.ErrNoTaskAvailable
	ErrAttemptNotFound          = execution.ErrAttemptNotFound
	ErrAttemptLeaseLost         = execution.ErrAttemptLeaseLost
	ErrInvalidAttemptTransition = execution.ErrInvalidAttemptTransition
)
