package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/purinliang/mill/internal/coordinator"
	"github.com/purinliang/mill/internal/job"
	"github.com/purinliang/mill/internal/kubernetes"
)

type executionLoop struct {
	coordinator *coordinator.Coordinator
}

const attemptLeaseDuration = 15 * time.Second

func configureExecution(repository *job.Repository) (*executionLoop, error) {
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
		S3Region: os.Getenv("MILL_WORKLOAD_S3_REGION"), S3Endpoint: os.Getenv("MILL_WORKLOAD_S3_ENDPOINT"),
		S3CredentialsSecret: os.Getenv("MILL_WORKLOAD_S3_CREDENTIALS_SECRET"),
	})
	if err != nil {
		return nil, err
	}
	leaseOwner, err := newExecutorInstanceID()
	if err != nil {
		return nil, err
	}
	return &executionLoop{coordinator: &coordinator.Coordinator{
		Store: repository, Executor: executor, Logger: log.Default(),
		LeaseOwner: leaseOwner, LeaseDuration: attemptLeaseDuration,
	}}, nil
}

func newExecutorInstanceID() (string, error) {
	identifier := make([]byte, 16)
	if _, err := rand.Read(identifier); err != nil {
		return "", fmt.Errorf("generate executor instance ID: %w", err)
	}
	return "executor-" + hex.EncodeToString(identifier), nil
}

func (e *executionLoop) run(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
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
