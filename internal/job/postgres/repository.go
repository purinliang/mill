package postgres

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/purinliang/mill/internal/job"
)

type Repository struct {
	database      *pgxpool.Pool
	outputRootURI string
}

func NewRepository(
	database *pgxpool.Pool,
	outputRootURI string,
) (*Repository, error) {
	if database == nil {
		return nil, errors.New("PostgreSQL connection pool is required")
	}

	normalizedRoot, err := job.NormalizeOutputRootURI(outputRootURI)
	if err != nil {
		return nil, fmt.Errorf("validate MILL_OUTPUT_ROOT_URI: %w", err)
	}

	return &Repository{
		database:      database,
		outputRootURI: normalizedRoot,
	}, nil
}
