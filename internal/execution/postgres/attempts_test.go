// This file tests public validation at the execution repository boundary.
package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	executionpostgres "github.com/purinliang/mill/internal/execution/postgres"
)

func TestRepositoryRejectsInvalidAttemptRequestsBeforeDatabaseAccess(
	t *testing.T,
) {
	repository, err := executionpostgres.NewRepository(&pgxpool.Pool{})
	if err != nil {
		t.Fatal(err)
	}

	tests := map[string]func() error{
		"blank executor": func() error {
			_, err := repository.ClaimNextAttempt(
				context.Background(),
				" ",
				"owner",
				15*time.Second,
			)
			return err
		},
		"fractional lease": func() error {
			_, err := repository.ClaimNextAttempt(
				context.Background(),
				"kubernetes",
				"owner",
				1500*time.Millisecond,
			)
			return err
		},
		"blank lease owner": func() error {
			_, err := repository.ClaimNextAttempt(
				context.Background(),
				"kubernetes",
				"",
				15*time.Second,
			)
			return err
		},
		"invalid attempt ID": func() error {
			_, err := repository.GetAttempt(context.Background(), "not-a-uuid")
			return err
		},
		"blank external ID": func() error {
			_, err := repository.MarkAttemptRunning(
				context.Background(),
				"00000000-0000-7000-8000-000000000001",
				"00000000-0000-7000-8000-000000000002",
				" ",
			)
			return err
		},
		"invalid lease token": func() error {
			_, err := repository.CompleteAttempt(
				context.Background(),
				"00000000-0000-7000-8000-000000000001",
				"not-a-uuid",
			)
			return err
		},
		"blank failure message": func() error {
			_, err := repository.FailAttempt(
				context.Background(),
				"00000000-0000-7000-8000-000000000001",
				"00000000-0000-7000-8000-000000000002",
				" ",
			)
			return err
		},
		"oversized failure message": func() error {
			_, err := repository.FailAttempt(
				context.Background(),
				"00000000-0000-7000-8000-000000000001",
				"00000000-0000-7000-8000-000000000002",
				strings.Repeat("x", 4097),
			)
			return err
		},
		"blank active-attempt executor": func() error {
			_, err := repository.LeaseActiveAttempts(
				context.Background(),
				"",
				"owner",
				15*time.Second,
			)
			return err
		},
		"oversized lease owner": func() error {
			_, err := repository.LeaseActiveAttempts(
				context.Background(),
				"kubernetes",
				strings.Repeat("x", 256),
				15*time.Second,
			)
			return err
		},
	}

	for name, operation := range tests {
		t.Run(name, func(t *testing.T) {
			var invalidArgument interface {
				InvalidArgument() bool
			}
			err := operation()
			if !errors.As(err, &invalidArgument) ||
				!invalidArgument.InvalidArgument() {
				t.Fatalf("error = %v, want invalid argument", err)
			}
		})
	}
}
