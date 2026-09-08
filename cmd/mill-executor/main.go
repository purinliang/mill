package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/purinliang/mill/internal/coordinator"
	"github.com/purinliang/mill/internal/executionrpc"
	executionv1 "github.com/purinliang/mill/internal/executionrpc/v1"
	"github.com/purinliang/mill/internal/kubernetes"
)

const (
	defaultRPCTimeout   = 3 * time.Second
	coordinatorInterval = time.Second
)

type config struct {
	jobGRPCTarget string
	rpcTimeout    time.Duration
	kubernetes    kubernetes.Config
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	configuration, err := configFromEnvironment(os.Getenv)
	if err != nil {
		log.Fatalf("configure Mill executor: %v", err)
	}
	if err := run(ctx, configuration); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("run Mill executor: %v", err)
	}
}

func configFromEnvironment(getenv func(string) string) (config, error) {
	target := getenv("MILL_JOB_GRPC_TARGET")
	if target == "" || target != strings.TrimSpace(target) || len(target) > 2048 {
		return config{}, errors.New("MILL_JOB_GRPC_TARGET must be 1 to 2048 bytes with no surrounding whitespace")
	}
	timeout := defaultRPCTimeout
	if value := getenv("MILL_EXECUTION_RPC_TIMEOUT"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil || parsed < 100*time.Millisecond || parsed > 30*time.Second {
			return config{}, errors.New("MILL_EXECUTION_RPC_TIMEOUT must be a duration between 100ms and 30s")
		}
		timeout = parsed
	}
	inCluster, err := parseInCluster(getenv("MILL_KUBE_IN_CLUSTER"))
	if err != nil {
		return config{}, err
	}
	return config{
		jobGRPCTarget: target,
		rpcTimeout:    timeout,
		kubernetes: kubernetes.Config{
			Context:             getenv("MILL_KUBE_CONTEXT"),
			InCluster:           inCluster,
			Namespace:           getenv("MILL_KUBE_NAMESPACE"),
			Node:                getenv("MILL_KUBE_NODE"),
			LocalRoot:           getenv("MILL_LOCAL_ROOT"),
			NodeRoot:            getenv("MILL_NODE_ROOT"),
			S3Region:            getenv("MILL_WORKLOAD_S3_REGION"),
			S3Endpoint:          getenv("MILL_WORKLOAD_S3_ENDPOINT"),
			S3CredentialsSecret: getenv("MILL_WORKLOAD_S3_CREDENTIALS_SECRET"),
		},
	}, nil
}

func parseInCluster(value string) (bool, error) {
	switch value {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, errors.New("MILL_KUBE_IN_CLUSTER must be true or false")
	}
}

func run(ctx context.Context, configuration config) error {
	connection, err := grpc.NewClient(
		configuration.jobGRPCTarget,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(executionrpc.MaxMessageBytes),
			grpc.MaxCallSendMsgSize(executionrpc.MaxMessageBytes),
		),
	)
	if err != nil {
		return fmt.Errorf("create Job service gRPC client: %w", err)
	}
	defer connection.Close()
	store, err := executionrpc.NewClient(executionv1.NewExecutionServiceClient(connection), configuration.rpcTimeout)
	if err != nil {
		return err
	}
	executor, err := kubernetes.New(configuration.kubernetes)
	if err != nil {
		return err
	}
	leaseOwner, err := newExecutorInstanceID()
	if err != nil {
		return err
	}
	worker := &coordinator.Coordinator{
		Store: store, Executor: executor, Logger: log.Default(), LeaseOwner: leaseOwner,
	}
	log.Printf("Mill executor instance=%s job_service=%s", leaseOwner, configuration.jobGRPCTarget)
	return runCoordinator(ctx, worker, coordinatorInterval)
}

func runCoordinator(ctx context.Context, worker *coordinator.Coordinator, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if err := worker.Tick(ctx); err != nil && ctx.Err() == nil {
			log.Printf("coordinator tick: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func newExecutorInstanceID() (string, error) {
	identifier := make([]byte, 16)
	if _, err := rand.Read(identifier); err != nil {
		return "", fmt.Errorf("generate executor instance ID: %w", err)
	}
	return "executor-" + hex.EncodeToString(identifier), nil
}
