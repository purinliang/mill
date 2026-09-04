package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/purinliang/mill/internal/coordinator"
	"github.com/purinliang/mill/internal/job"
	"github.com/purinliang/mill/internal/kubernetes"
)

type executionLoop struct {
	coordinator *coordinator.Coordinator
	lock        *pgx.Conn
}

func configureExecution(ctx context.Context, databaseURL string, repository *job.Repository) (*executionLoop, error) {
	mode := os.Getenv("MILL_EXECUTOR")
	if mode == "" {
		return nil, nil
	}
	if mode != "kubernetes" {
		return nil, errors.New("MILL_EXECUTOR must be empty or kubernetes")
	}
	executor, err := kubernetes.New(kubernetes.Config{
		Context: os.Getenv("MILL_KUBE_CONTEXT"), Namespace: os.Getenv("MILL_KUBE_NAMESPACE"),
		Node: os.Getenv("MILL_KUBE_NODE"), LocalRoot: os.Getenv("MILL_LOCAL_ROOT"), NodeRoot: os.Getenv("MILL_NODE_ROOT"),
	})
	if err != nil {
		return nil, err
	}
	connection, err := acquireCoordinatorLock(ctx, databaseURL)
	if err != nil {
		return nil, err
	}
	return &executionLoop{lock: connection, coordinator: &coordinator.Coordinator{
		Store: repository, Executor: executor, Logger: log.Default(),
	}}, nil
}

func acquireCoordinatorLock(ctx context.Context, databaseURL string) (*pgx.Conn, error) {
	// One coordinator per database for this prototype. A dedicated connection
	// holds the session lock; closing it releases ownership, including on crash.
	lockCtx, cancel := context.WithTimeout(ctx, databaseStartupTimeout)
	defer cancel()
	connection, err := pgx.Connect(lockCtx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect coordinator lock: %w", err)
	}
	var acquired bool
	err = connection.QueryRow(lockCtx, "SELECT pg_try_advisory_lock(1835625580, 1)").Scan(&acquired)
	if err != nil || !acquired {
		_ = connection.Close(context.Background())
		if err != nil {
			return nil, fmt.Errorf("acquire coordinator lock: %w", err)
		}
		return nil, errors.New("another Mill coordinator owns this database")
	}
	return connection, nil
}

func (e *executionLoop) close() {
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	_ = e.lock.Close(ctx)
}

func (e *executionLoop) run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		// If the lock connection is lost, stop dispatching instead of continuing
		// as an owner whose session lock may have been released by PostgreSQL.
		checkCtx, cancel := context.WithTimeout(ctx, databaseStartupTimeout)
		err := e.lock.Ping(checkCtx)
		cancel()
		if err != nil {
			return fmt.Errorf("coordinator ownership connection: %w", err)
		}
		if err := e.coordinator.Tick(ctx); err != nil && ctx.Err() == nil {
			log.Printf("coordinator tick: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
