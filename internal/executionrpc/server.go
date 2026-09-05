package executionrpc

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/purinliang/mill/internal/execution"
	executionv1 "github.com/purinliang/mill/internal/executionrpc/v1"
)

// Backend is implemented by the Job service's durable metadata repository.
// The lease duration is supplied by the server so executor replicas cannot
// choose their own ownership policy.
type Backend interface {
	LeaseActiveAttempts(context.Context, string, string, time.Duration) ([]execution.ClaimedAttempt, error)
	ClaimNextAttempt(context.Context, string, string, time.Duration) (execution.ClaimedAttempt, error)
	MarkAttemptRunning(context.Context, string, string, string) (execution.Attempt, error)
	CompleteAttempt(context.Context, string, string) (execution.Attempt, error)
	FailAttempt(context.Context, string, string, string) (execution.Attempt, error)
}

type Server struct {
	executionv1.UnimplementedExecutionServiceServer
	backend       Backend
	leaseDuration time.Duration
}

func NewServer(backend Backend, leaseDuration time.Duration) (*Server, error) {
	if backend == nil {
		return nil, errors.New("execution RPC backend is required")
	}
	if leaseDuration < time.Second || leaseDuration > 5*time.Minute || leaseDuration%time.Second != 0 {
		return nil, errors.New("execution RPC lease duration must be a whole number of seconds between 1 second and 5 minutes")
	}
	return &Server{backend: backend, leaseDuration: leaseDuration}, nil
}

func (s *Server) LeaseActiveAttempts(ctx context.Context, request *executionv1.LeaseActiveAttemptsRequest) (*executionv1.LeaseActiveAttemptsResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	attempts, err := s.backend.LeaseActiveAttempts(ctx, request.Executor, request.LeaseOwner, s.leaseDuration)
	if err != nil {
		return nil, serverError(err)
	}
	response := &executionv1.LeaseActiveAttemptsResponse{Attempts: make([]*executionv1.ClaimedAttempt, 0, len(attempts))}
	for _, attempt := range attempts {
		response.Attempts = append(response.Attempts, claimedAttemptToProto(attempt))
	}
	return response, nil
}

func (s *Server) ClaimNextAttempt(ctx context.Context, request *executionv1.ClaimNextAttemptRequest) (*executionv1.ClaimNextAttemptResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	attempt, err := s.backend.ClaimNextAttempt(ctx, request.Executor, request.LeaseOwner, s.leaseDuration)
	if err != nil {
		return nil, serverError(err)
	}
	return &executionv1.ClaimNextAttemptResponse{Attempt: claimedAttemptToProto(attempt)}, nil
}

func (s *Server) MarkAttemptRunning(ctx context.Context, request *executionv1.MarkAttemptRunningRequest) (*executionv1.AttemptResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	attempt, err := s.backend.MarkAttemptRunning(ctx, request.AttemptId, request.LeaseToken, request.ExternalId)
	return attemptResponse(attempt, err)
}

func (s *Server) CompleteAttempt(ctx context.Context, request *executionv1.CompleteAttemptRequest) (*executionv1.AttemptResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	attempt, err := s.backend.CompleteAttempt(ctx, request.AttemptId, request.LeaseToken)
	return attemptResponse(attempt, err)
}

func (s *Server) FailAttempt(ctx context.Context, request *executionv1.FailAttemptRequest) (*executionv1.AttemptResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	attempt, err := s.backend.FailAttempt(ctx, request.AttemptId, request.LeaseToken, request.FailureMessage)
	return attemptResponse(attempt, err)
}

func attemptResponse(attempt execution.Attempt, err error) (*executionv1.AttemptResponse, error) {
	if err != nil {
		return nil, serverError(err)
	}
	return &executionv1.AttemptResponse{Attempt: attemptToProto(attempt)}, nil
}

func serverError(err error) error {
	var validation interface {
		InvalidArgument() bool
	}
	switch {
	case errors.As(err, &validation) && validation.InvalidArgument():
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, execution.ErrNoTaskAvailable), errors.Is(err, execution.ErrAttemptNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, execution.ErrAttemptLeaseLost):
		return status.Error(codes.Aborted, err.Error())
	case errors.Is(err, execution.ErrInvalidAttemptTransition):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, "execution state operation failed")
	}
}
