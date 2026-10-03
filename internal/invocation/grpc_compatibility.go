package invocation

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/runtime"
	"dtm/internal/transport/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidGRPCCompatibilityTransport = errors.New("invalid grpc compatibility transport")
)

// GrpcCompatibilityTransport is intentionally a thin M1 adapter. It uses the
// already-resolved endpoint and delegates all protobuf conversion, ExecuteStep
// handling, idempotency and dial/close lifecycle to the existing RemoteExecutor.
type GrpcCompatibilityTransport struct {
	dialer  grpcapi.Dialer
	adapter *LegacyCapabilityAdapter
}

func NewGrpcCompatibilityTransport(dialer grpcapi.Dialer, adapter *LegacyCapabilityAdapter) (*GrpcCompatibilityTransport, error) {
	if dialer == nil || adapter == nil {
		return nil, ErrInvalidGRPCCompatibilityTransport
	}
	return &GrpcCompatibilityTransport{dialer: dialer, adapter: adapter}, nil
}

func (transport *GrpcCompatibilityTransport) Invoke(
	ctx context.Context,
	target ResolvedInvocationTarget,
	request InvocationRequest,
) (InvocationResult, error) {
	if transport == nil || transport.dialer == nil || transport.adapter == nil {
		return InvocationResult{}, &InvocationError{Code: ErrorCodeInvalidRequest, Message: ErrInvalidGRPCCompatibilityTransport.Error(), DispatchState: DispatchStateNotDispatched, Source: "legacy-grpc", Classification: "invalid_dependency"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := request.Validate(); err != nil {
		return InvocationResult{}, &InvocationError{Code: ErrorCodeInvalidRequest, Message: err.Error(), Cause: err, DispatchState: DispatchStateNotDispatched, Source: "legacy-grpc", Classification: "request_validation"}
	}
	if target.ResourceRef != request.Target || target.Operation != request.Operation {
		return InvocationResult{}, &InvocationError{Code: ErrorCodeProtocolError, Message: "transport target does not match invocation request", DispatchState: DispatchStateNotDispatched, Source: "legacy-grpc", Classification: "target_correlation"}
	}
	if err := target.Validate(); err != nil {
		return InvocationResult{}, &InvocationError{Code: ErrorCodeProtocolError, Message: "invalid resolved transport target", Cause: err, DispatchState: DispatchStateNotDispatched, Source: "legacy-grpc", Classification: "target_validation"}
	}
	if target.Endpoint.TransportID != TransportGRPC {
		return InvocationResult{}, &InvocationError{Code: ErrorCodeTransportFailure, Message: fmt.Sprintf("unsupported compatibility transport %q", target.Endpoint.TransportID), DispatchState: DispatchStateNotDispatched, Source: "legacy-grpc", Classification: "transport_selection"}
	}

	capability, inputs, err := transport.adapter.DecodeRequest(request.Operation, request.Payload)
	if err != nil {
		return InvocationResult{}, &InvocationError{Code: ErrorCodeProtocolError, Message: err.Error(), Cause: err, DispatchState: DispatchStateNotDispatched, Source: "resource-capability-adapter", Classification: "payload_decode"}
	}
	idempotencyMode, err := transport.adapter.IdempotencyModeForCapability(capability)
	if err != nil {
		return InvocationResult{}, &InvocationError{Code: ErrorCodeProtocolError, Message: err.Error(), Cause: err, DispatchState: DispatchStateNotDispatched, Source: "resource-capability-adapter", Classification: "operation_mapping"}
	}
	step := mapper.MappedStep{
		ID:              request.Metadata.StepID,
		Capability:      capability,
		IdempotencyMode: idempotencyMode,
		NodeID:          target.ResourceRef.OwnerNodeID,
		ResourceRef:     target.ResourceRef,
		Inputs:          inputs,
	}

	// RemoteExecutor still accepts a node resolver. Supplying a per-invocation
	// immutable resolver prevents it from selecting a different endpoint while
	// preserving its existing ExecuteStep/protobuf implementation.
	tracker := &legacyDispatchTracker{}
	legacyExecutor, err := grpcapi.NewRemoteExecutor(staticEndpointResolver{address: target.Endpoint.Address}, tracker.wrap(transport.dialer))
	if err != nil {
		return InvocationResult{}, &InvocationError{Code: ErrorCodeTransportFailure, Message: err.Error(), Cause: err, DispatchState: DispatchStateNotDispatched, Source: "legacy-grpc", Classification: "compatibility/conservative"}
	}
	stepResult, err := legacyExecutor.ExecuteAttempt(ctx, request.Metadata.TaskID, step, runtime.StepAttempt{
		Number:         request.Metadata.Attempt,
		IdempotencyKey: request.Metadata.IdempotencyKey,
	})
	if err != nil {
		invocationErr := classifyLegacyGRPCError(ctx, err)
		if tracker.connectionReady {
			invocationErr.DispatchState = DispatchStateMayHaveDispatched
		} else {
			invocationErr.DispatchState = DispatchStateNotDispatched
			invocationErr.OutcomeUnknown = false
		}
		return InvocationResult{}, invocationErr
	}
	if stepResult.StepID != request.Metadata.StepID || stepResult.NodeID != target.ResourceRef.OwnerNodeID {
		return InvocationResult{}, &InvocationError{Code: ErrorCodeProtocolError, Message: "legacy response identity does not match invocation", DispatchState: DispatchStateRemoteOutcomeReceived, Source: "legacy-grpc", Classification: "response_correlation"}
	}
	if stepResult.Status != execution.StatusSucceeded {
		message := stepResult.Error
		if message == "" {
			message = fmt.Sprintf("legacy execution returned status %q", stepResult.Status)
		}
		return InvocationResult{}, &InvocationError{Code: ErrorCodeExecutionFailure, Message: message, DispatchState: DispatchStateRemoteOutcomeReceived, Source: "legacy-grpc", Classification: "handler_result"}
	}
	payload, err := transport.adapter.EncodeOutput(stepResult.Output)
	if err != nil {
		return InvocationResult{}, &InvocationError{Code: ErrorCodeProtocolError, Message: err.Error(), Cause: err, DispatchState: DispatchStateRemoteOutcomeReceived, Source: "resource-capability-adapter", Classification: "payload_encode"}
	}

	return InvocationResult{
		InvocationID: request.InvocationID,
		Payload:      payload,
		Metadata: ExecutionMetadata{
			Transport:                  TransportGRPC,
			TransportProfile:           TransportProfileLegacy,
			Endpoint:                   target.Endpoint.Address,
			CallerFenceValidation:      FenceValidationPassed,
			ExecutionFenceValidation:   FenceValidationUnconfirmed,
			ExecutionFenceMode:         ExecutionFenceModeNone,
			AcceptedTargetConfirmation: FenceValidationUnconfirmed,
			Status:                     "succeeded",
			RequestBytes:               len(request.Payload),
			ResponseBytes:              len(payload),
		},
	}, nil
}

func (transport *GrpcCompatibilityTransport) Close() error { return nil }

type staticEndpointResolver struct{ address string }

type legacyDispatchTracker struct {
	connectionReady bool
}

func (tracker *legacyDispatchTracker) wrap(dialer grpcapi.Dialer) grpcapi.Dialer {
	return func(ctx context.Context, address string) (*grpc.ClientConn, error) {
		connection, err := dialer(ctx, address)
		if connection != nil {
			tracker.connectionReady = true
		}
		return connection, err
	}
}

func (resolver staticEndpointResolver) Resolve(_ model.NodeID) (string, error) {
	if strings.TrimSpace(resolver.address) == "" {
		return "", errors.New("resolved endpoint is empty")
	}
	return resolver.address, nil
}

func classifyLegacyGRPCError(ctx context.Context, err error) *InvocationError {
	if errors.Is(err, context.DeadlineExceeded) || (ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded)) || status.Code(err) == codes.DeadlineExceeded {
		return &InvocationError{Code: ErrorCodeTimeout, Message: err.Error(), Cause: err, Source: "legacy-grpc", Classification: "context"}
	}
	if errors.Is(err, context.Canceled) || (ctx != nil && errors.Is(ctx.Err(), context.Canceled)) || status.Code(err) == codes.Canceled {
		return &InvocationError{Code: ErrorCodeCanceled, Message: err.Error(), Cause: err, Source: "legacy-grpc", Classification: "context"}
	}
	if errors.Is(err, runtime.ErrRetryableExecution) {
		return &InvocationError{
			Code: ErrorCodeTransportFailure, Message: err.Error(), Cause: err,
			Retryable: true, OutcomeUnknown: true, Source: "legacy-grpc", Classification: "compatibility/conservative",
		}
	}
	// Existing ExecuteStep exposes no typed distinction for failed-precondition,
	// idempotency or fencing. Preserve a conservative transport classification
	// rather than guessing a stronger M2 error code.
	return &InvocationError{
		Code: ErrorCodeTransportFailure, Message: err.Error(), Cause: err,
		OutcomeUnknown: true, Source: "legacy-grpc", Classification: "compatibility/conservative",
	}
}

var _ Transport = (*GrpcCompatibilityTransport)(nil)
