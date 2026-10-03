package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"strings"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/runtime"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidResolver = errors.New("invalid endpoint resolver")
	ErrInvalidDialer   = errors.New("invalid grpc dialer")
	ErrRemoteExecution = errors.New("remote execution failed")
)

type Resolver interface {
	Resolve(model.NodeID) (string, error)
}

type Dialer func(context.Context, string) (*grpc.ClientConn, error)

type RemoteExecutor struct {
	resolver Resolver
	dialer   Dialer
}

func NewRemoteExecutor(resolver Resolver, dialer Dialer) (*RemoteExecutor, error) {
	if isNilDependency(resolver) {
		return nil, ErrInvalidResolver
	}
	if dialer == nil {
		return nil, ErrInvalidDialer
	}
	return &RemoteExecutor{resolver: resolver, dialer: dialer}, nil
}

func (executor *RemoteExecutor) Execute(
	ctx context.Context,
	taskID model.TaskID,
	step mapper.MappedStep,
) (execution.StepResult, error) {
	return executor.ExecuteAttempt(ctx, taskID, step, runtime.StepAttempt{
		Number:         1,
		IdempotencyKey: runtime.IdempotencyKey(taskID, step.ID),
	})
}

func (executor *RemoteExecutor) ExecuteAttempt(
	ctx context.Context,
	taskID model.TaskID,
	step mapper.MappedStep,
	attempt runtime.StepAttempt,
) (execution.StepResult, error) {
	if attempt.Number == 0 || strings.TrimSpace(attempt.IdempotencyKey) == "" {
		return execution.StepResult{}, fmt.Errorf("%w: invalid execution attempt", ErrRemoteExecution)
	}
	address, err := executor.resolver.Resolve(step.NodeID)
	if err != nil {
		return execution.StepResult{}, fmt.Errorf(
			"%w: %w: resolve node %q: %w",
			runtime.ErrRetryableExecution,
			ErrRemoteExecution,
			step.NodeID,
			err,
		)
	}

	connection, err := executor.dialer(ctx, address)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return execution.StepResult{}, fmt.Errorf(
			"%w: %w: dial node %q: %w",
			runtime.ErrRetryableExecution,
			ErrRemoteExecution,
			step.NodeID,
			err,
		)
	}
	if connection == nil {
		return execution.StepResult{}, fmt.Errorf("%w: dial node %q returned a nil connection", ErrRemoteExecution, step.NodeID)
	}
	defer connection.Close()

	response, err := dtmv1.NewAgentExecutionServiceClient(connection).ExecuteStep(ctx, &dtmv1.ExecuteStepRequest{
		TaskId:         string(taskID),
		Step:           mappedStepToProto(step),
		IdempotencyKey: attempt.IdempotencyKey,
		Attempt:        attempt.Number,
	})
	if err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return execution.StepResult{}, fmt.Errorf("%w: execute step remotely: %v", contextErr, err)
		}
		if isRetryableRPCError(err) {
			return execution.StepResult{}, fmt.Errorf(
				"%w: %w: execute step on node %q: %w",
				runtime.ErrRetryableExecution,
				ErrRemoteExecution,
				step.NodeID,
				err,
			)
		}
		return execution.StepResult{}, fmt.Errorf("%w: execute step on node %q: %w", ErrRemoteExecution, step.NodeID, err)
	}
	if response == nil || response.GetResult() == nil {
		return execution.StepResult{}, fmt.Errorf("%w: agent returned an empty response", ErrRemoteExecution)
	}

	result, err := stepResultFromProto(response.GetResult())
	if err != nil {
		return execution.StepResult{}, fmt.Errorf("%w: decode agent response: %v", ErrRemoteExecution, err)
	}
	if result.StepID != step.ID || result.NodeID != step.NodeID {
		return execution.StepResult{}, fmt.Errorf(
			"%w: agent returned result for step %q on node %q",
			ErrRemoteExecution,
			result.StepID,
			result.NodeID,
		)
	}
	return result, nil
}

func isRetryableRPCError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted:
		return true
	default:
		return false
	}
}

var _ runtime.Executor = (*RemoteExecutor)(nil)
var _ runtime.AttemptExecutor = (*RemoteExecutor)(nil)
