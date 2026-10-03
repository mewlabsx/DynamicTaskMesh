package invocation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
	meshruntime "dtm/internal/runtime"
)

var ErrInvalidInvocationExecutor = errors.New("invalid invocation executor")

type InvocationExecutor struct {
	service *InvocationService
	adapter *LegacyCapabilityAdapter
	timeout time.Duration
}

type InvocationExecutorOption func(*InvocationExecutor) error

func WithInvocationTimeout(timeout time.Duration) InvocationExecutorOption {
	return func(executor *InvocationExecutor) error {
		if timeout < 0 || timeout > MaxInvocationTimeout {
			return ErrInvalidInvocationExecutor
		}
		executor.timeout = timeout
		return nil
	}
}

func NewInvocationExecutor(service *InvocationService, adapter *LegacyCapabilityAdapter, options ...InvocationExecutorOption) (*InvocationExecutor, error) {
	if service == nil || adapter == nil {
		return nil, ErrInvalidInvocationExecutor
	}
	executor := &InvocationExecutor{service: service, adapter: adapter}
	for _, option := range options {
		if option == nil {
			return nil, ErrInvalidInvocationExecutor
		}
		if err := option(executor); err != nil {
			return nil, err
		}
	}
	return executor, nil
}

func (executor *InvocationExecutor) Execute(
	ctx context.Context,
	taskID model.TaskID,
	step mapper.MappedStep,
) (execution.StepResult, error) {
	return executor.ExecuteAttempt(ctx, taskID, step, meshruntime.StepAttempt{
		Number:         1,
		IdempotencyKey: meshruntime.IdempotencyKey(taskID, step.ID),
	})
}

func (executor *InvocationExecutor) ExecuteAttempt(
	ctx context.Context,
	taskID model.TaskID,
	step mapper.MappedStep,
	attempt meshruntime.StepAttempt,
) (execution.StepResult, error) {
	if executor == nil || executor.service == nil || executor.adapter == nil {
		return execution.StepResult{}, ErrInvalidInvocationExecutor
	}
	if strings.TrimSpace(string(taskID)) == "" || strings.TrimSpace(string(step.ID)) == "" || strings.TrimSpace(string(step.NodeID)) == "" {
		return execution.StepResult{}, fmt.Errorf("%w: task, step and node are required", ErrInvalidInvocationExecutor)
	}
	if attempt.Number == 0 || strings.TrimSpace(attempt.IdempotencyKey) == "" {
		return execution.StepResult{}, fmt.Errorf("%w: invalid execution attempt", ErrInvalidInvocationExecutor)
	}
	if err := step.ResourceRef.Validate(); err != nil {
		return execution.StepResult{}, &InvocationError{Code: ErrorCodeInvalidRequest, Message: "mapped step has no valid ResourceRef", Cause: err, Source: "invocation-executor", Classification: "request_validation"}
	}
	operation, err := executor.adapter.OperationForCapability(step.Capability)
	if err != nil {
		return execution.StepResult{}, &InvocationError{Code: ErrorCodeUnsupportedOperation, Message: err.Error(), Cause: err, Source: "resource-capability-adapter", Classification: "operation_mapping"}
	}
	payload, err := executor.adapter.EncodeRequest(step, operation)
	if err != nil {
		return execution.StepResult{}, &InvocationError{Code: ErrorCodeInvalidRequest, Message: err.Error(), Cause: err, Source: "resource-capability-adapter", Classification: "payload_encode"}
	}
	invocationID, err := NewInvocationID()
	if err != nil {
		return execution.StepResult{}, &InvocationError{Code: ErrorCodeProtocolError, Message: err.Error(), Cause: err, Source: "invocation-executor", Classification: "id_generation"}
	}
	request := InvocationRequest{
		InvocationID: invocationID,
		Target:       step.ResourceRef,
		Operation:    operation,
		Payload:      payload,
		Metadata: InvocationMetadata{
			TaskID:         taskID,
			StepID:         step.ID,
			Attempt:        attempt.Number,
			IdempotencyKey: attempt.IdempotencyKey,
			Timeout:        executor.timeout,
		},
	}
	result, err := executor.service.Invoke(ctx, request)
	if err != nil {
		return execution.StepResult{}, mapInvocationError(err)
	}
	output, err := executor.adapter.DecodeOutput(result.Payload)
	if err != nil {
		invocationErr := &InvocationError{Code: ErrorCodeProtocolError, Message: err.Error(), Cause: err, Source: "resource-capability-adapter", Classification: "payload_decode"}
		return execution.StepResult{}, mapInvocationError(invocationErr)
	}
	stepResult, err := execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, output, "")
	if err != nil {
		invocationErr := &InvocationError{Code: ErrorCodeProtocolError, Message: "construct StepResult", Cause: err, Source: "invocation-executor", Classification: "result_conversion"}
		return execution.StepResult{}, mapInvocationError(invocationErr)
	}
	return stepResult, nil
}

func mapInvocationError(err error) error {
	if err == nil {
		return nil
	}
	var invocationErr *InvocationError
	if !errors.As(err, &invocationErr) {
		return err
	}
	if invocationErr.Code == ErrorCodeTimeout {
		return errors.Join(context.DeadlineExceeded, invocationErr)
	}
	if invocationErr.Code == ErrorCodeCanceled {
		return errors.Join(context.Canceled, invocationErr)
	}
	if invocationErr.Retryable {
		return fmt.Errorf("%w: %w", meshruntime.ErrRetryableExecution, invocationErr)
	}
	return invocationErr
}

func (executor *InvocationExecutor) Service() *InvocationService {
	if executor == nil {
		return nil
	}
	return executor.service
}

var _ meshruntime.Executor = (*InvocationExecutor)(nil)
var _ meshruntime.AttemptExecutor = (*InvocationExecutor)(nil)
