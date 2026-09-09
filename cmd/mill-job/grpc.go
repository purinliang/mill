// This file owns the Job process's execution gRPC listener lifecycle.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"

	"github.com/purinliang/mill/internal/execution/rpc"
	executionv1 "github.com/purinliang/mill/internal/execution/rpc/v1"
)

const attemptLeaseDuration = 15 * time.Second

type executionRPCService struct {
	server *grpc.Server
	errors <-chan error
}

func startExecutionRPC(address string, backend executionrpc.Backend) (*executionRPCService, error) {
	if address == "" {
		return nil, nil
	}
	server, err := newExecutionRPCServer(backend)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen execution gRPC: %w", err)
	}
	errors := make(chan error, 1)
	go func() {
		errors <- server.Serve(listener)
	}()
	log.Printf("Mill execution gRPC server listening on %s", listener.Addr())
	return &executionRPCService{server: server, errors: errors}, nil
}

func newExecutionRPCServer(backend executionrpc.Backend) (*grpc.Server, error) {
	executionServer, err := executionrpc.NewServer(backend, attemptLeaseDuration)
	if err != nil {
		return nil, err
	}
	server := grpc.NewServer(
		grpc.MaxRecvMsgSize(executionrpc.MaxMessageBytes),
		grpc.MaxSendMsgSize(executionrpc.MaxMessageBytes),
	)
	executionv1.RegisterExecutionServiceServer(server, executionServer)
	return server, nil
}

func (s *executionRPCService) shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.server.Stop()
		<-done
		return ctx.Err()
	}
}

func (s *executionRPCService) stop() {
	s.server.Stop()
}

func executionRPCServeError(err error) error {
	if err == nil || errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return fmt.Errorf("serve execution gRPC: %w", err)
}
