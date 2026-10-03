package grpcapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log"
	"strings"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/agent"
	"dtm/internal/agentexecution"
	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/runtime"
	storageport "dtm/internal/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidStepHandler         = errors.New("invalid step handler")
	ErrInvalidExecutionRepository = errors.New("invalid agent execution repository")
)

type StepHandler interface {
	Execute(context.Context, mapper.MappedStep) (execution.StepResult, error)
}

type AgentExecutionServer struct {
	dtmv1.UnimplementedAgentExecutionServiceServer
	dtmv1.UnimplementedResourceInvocationServiceServer

	handler           StepHandler
	store             *stepExecutionStore
	logger            *log.Logger
	invocationAdapter ResourceInvocationAdapter
	fenceValidator    ExecutionFenceValidator
}

func WithAgentExecutionLogger(logger *log.Logger) AgentExecutionServerOption {
	return func(server *AgentExecutionServer) error {
		if logger != nil {
			server.logger = logger
		}
		return nil
	}
}

type AgentExecutionServerOption func(*AgentExecutionServer) error

func WithExecutionRepository(
	repository agentexecution.Repository,
) AgentExecutionServerOption {
	return func(server *AgentExecutionServer) error {
		if isNilDependency(repository) {
			return ErrInvalidExecutionRepository
		}
		server.store = newStepExecutionStore(repository)
		return nil
	}
}

func NewAgentExecutionServer(
	handler StepHandler,
	options ...AgentExecutionServerOption,
) (*AgentExecutionServer, error) {
	if isNilDependency(handler) {
		return nil, ErrInvalidStepHandler
	}
	server := &AgentExecutionServer{
		handler:        handler,
		store:          newStepExecutionStore(agentexecution.NoopRepository{}),
		logger:         log.Default(),
		fenceValidator: noopExecutionFenceValidator{},
	}
	for _, option := range options {
		if option == nil {
			return nil, ErrInvalidExecutionRepository
		}
		if err := option(server); err != nil {
			return nil, err
		}
	}
	return server, nil
}

func (server *AgentExecutionServer) ExecuteStep(
	ctx context.Context,
	request *dtmv1.ExecuteStepRequest,
) (*dtmv1.ExecuteStepResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "execute request is required")
	}
	if strings.TrimSpace(request.GetTaskId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "task ID is required")
	}
	step, err := mappedStepFromProto(request.GetStep())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid mapped step: %v", err)
	}
	taskID := model.TaskID(strings.TrimSpace(request.GetTaskId()))
	idempotencyKey := strings.TrimSpace(request.GetIdempotencyKey())
	if idempotencyKey == "" {
		idempotencyKey = runtime.IdempotencyKey(taskID, step.ID)
	}

	result, executeErr, replayed := server.executeMappedStep(ctx, idempotencyKey, agentexecution.NewFingerprint(taskID, step), step)
	if contextErr := ctx.Err(); contextErr != nil {
		switch {
		case errors.Is(contextErr, context.Canceled):
			return nil, status.Error(codes.Canceled, contextErr.Error())
		case errors.Is(contextErr, context.DeadlineExceeded):
			return nil, status.Error(codes.DeadlineExceeded, contextErr.Error())
		}
	}
	if executeErr != nil {
		if errors.Is(executeErr, storageport.ErrStorageIntegrity) {
			server.logger.Printf("operation=execute_step record_type=agent_execution error_class=storage_integrity key_hash_prefix=%s", agentExecutionKeyHashPrefix(idempotencyKey))
		}
		switch {
		case errors.Is(executeErr, context.Canceled):
			return nil, status.Error(codes.Canceled, executeErr.Error())
		case errors.Is(executeErr, context.DeadlineExceeded):
			return nil, status.Error(codes.DeadlineExceeded, executeErr.Error())
		case errors.Is(executeErr, storageport.ErrStorageUnavailable):
			return nil, status.Error(codes.Unavailable, "storage is temporarily unavailable")
		case errors.Is(executeErr, storageport.ErrStorageReadOnly),
			errors.Is(executeErr, storageport.ErrStorageFull),
			errors.Is(executeErr, storageport.ErrStorageIO),
			errors.Is(executeErr, storageport.ErrStorageCorrupt),
			errors.Is(executeErr, storageport.ErrStorageIntegrity),
			errors.Is(executeErr, storageport.ErrStorageClosed):
			return nil, status.Error(codes.Internal, "persistent storage operation failed")
		case errors.Is(executeErr, agent.ErrUnsupportedCapability):
			return nil, status.Errorf(codes.FailedPrecondition, "execute step: %v", executeErr)
		case errors.Is(executeErr, ErrIdempotencyConflict):
			return nil, status.Error(codes.FailedPrecondition, executeErr.Error())
		case result.Status != execution.StatusFailed:
			return nil, status.Errorf(codes.Internal, "execute step returned no failed result: %v", executeErr)
		}
	}

	validated, err := validateHandlerResult(step, result)
	if err != nil {
		return nil, status.Error(codes.Internal, "step handler returned an invalid result")
	}

	converted, err := stepResultToProto(validated)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode step result: %v", err)
	}
	return &dtmv1.ExecuteStepResponse{Result: converted, Replayed: replayed}, nil
}

func agentExecutionKeyHashPrefix(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:12]
}

func validateHandlerResult(
	step mapper.MappedStep,
	result execution.StepResult,
) (execution.StepResult, error) {
	validated, err := execution.NewStepResult(
		result.StepID,
		result.NodeID,
		result.Status,
		result.Output,
		result.Error,
	)
	if err != nil {
		return execution.StepResult{}, err
	}
	if validated.StepID != step.ID || validated.NodeID != step.NodeID {
		return execution.StepResult{}, errors.New("step handler returned a result for another step")
	}
	return validated, nil
}

var _ StepHandler = (*agent.Router)(nil)
