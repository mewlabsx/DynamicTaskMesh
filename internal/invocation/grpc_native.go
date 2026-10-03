package invocation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/execution"
	"dtm/internal/transport/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrInvalidNativeGRPCTransport = errors.New("invalid native grpc invocation transport")

// NativeGrpcInvocationTransport is the first-class Resource Invocation gRPC
// transport. It reuses a ClientConn per resolved endpoint and owns those
// connections until Close; it does not perform retry, remap, or endpoint
// selection.
type NativeGrpcInvocationTransport struct {
	dialer      grpcapi.Dialer
	mu          sync.Mutex
	connections map[string]*grpc.ClientConn
	closed      bool
}

func NewNativeGrpcInvocationTransport(dialer grpcapi.Dialer) (*NativeGrpcInvocationTransport, error) {
	if dialer == nil {
		return nil, ErrInvalidNativeGRPCTransport
	}
	return &NativeGrpcInvocationTransport{
		dialer:      dialer,
		connections: make(map[string]*grpc.ClientConn),
	}, nil
}

func (transport *NativeGrpcInvocationTransport) TransportProfile() string {
	return TransportProfileNative
}

func (transport *NativeGrpcInvocationTransport) Invoke(
	ctx context.Context,
	target ResolvedInvocationTarget,
	request InvocationRequest,
) (InvocationResult, error) {
	if transport == nil || transport.dialer == nil {
		return InvocationResult{}, &InvocationError{
			Code: ErrorCodeInvalidRequest, Message: ErrInvalidNativeGRPCTransport.Error(),
			DispatchState: DispatchStateNotDispatched, Source: "native-grpc", Classification: "invalid_dependency",
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := request.Validate(); err != nil {
		return InvocationResult{}, &InvocationError{
			Code: ErrorCodeInvalidRequest, Message: err.Error(), Cause: err,
			DispatchState: DispatchStateNotDispatched, Source: "native-grpc", Classification: "request_validation",
		}
	}
	if target.ResourceRef != request.Target || target.Operation != request.Operation {
		return InvocationResult{}, &InvocationError{
			Code: ErrorCodeProtocolError, Message: "native transport target does not match invocation request",
			DispatchState: DispatchStateNotDispatched, Source: "native-grpc", Classification: "target_correlation",
		}
	}
	if err := target.Validate(); err != nil {
		return InvocationResult{}, &InvocationError{
			Code: ErrorCodeProtocolError, Message: "invalid resolved native transport target", Cause: err,
			DispatchState: DispatchStateNotDispatched, Source: "native-grpc", Classification: "target_validation",
		}
	}
	if target.Endpoint.TransportID != TransportGRPC {
		return InvocationResult{}, &InvocationError{
			Code: ErrorCodeTransportFailure, Message: fmt.Sprintf("unsupported native transport %q", target.Endpoint.TransportID),
			DispatchState: DispatchStateNotDispatched, Source: "native-grpc", Classification: "transport_selection",
		}
	}
	connection, err := transport.connection(ctx, target.Endpoint.Address)
	if err != nil {
		return InvocationResult{}, classifyNativeGRPCError(ctx, err)
	}
	response, err := dtmv1.NewResourceInvocationServiceClient(connection).InvokeResource(ctx, nativeRequest(request))
	if err != nil {
		return InvocationResult{}, classifyNativeGRPCError(ctx, err)
	}
	if response == nil {
		return InvocationResult{}, &InvocationError{
			Code: ErrorCodeProtocolError, Message: "native invocation returned an empty response",
			DispatchState: DispatchStateRemoteOutcomeReceived, Source: "native-grpc", Classification: "empty_response",
		}
	}
	if nativeError := response.GetError(); nativeError != nil {
		if !bytesEqual(nativeError.GetInvocationId(), request.InvocationID.Bytes()) {
			return InvocationResult{}, &InvocationError{
				Code: ErrorCodeProtocolError, Message: "native invocation error identity does not match request",
				DispatchState: DispatchStateRemoteOutcomeReceived, Source: "native-grpc", Classification: "error_correlation",
			}
		}
		return InvocationResult{}, invocationErrorFromProto(nativeError)
	}
	result := response.GetResult()
	if result == nil {
		return InvocationResult{}, &InvocationError{
			Code: ErrorCodeProtocolError, Message: "native invocation response has no result or error",
			DispatchState: DispatchStateRemoteOutcomeReceived, Source: "native-grpc", Classification: "response_shape",
		}
	}
	if !bytesEqual(result.GetInvocationId(), request.InvocationID.Bytes()) {
		return InvocationResult{}, &InvocationError{
			Code: ErrorCodeProtocolError, Message: "native invocation result identity does not match request",
			DispatchState: DispatchStateRemoteOutcomeReceived, Source: "native-grpc", Classification: "response_correlation",
		}
	}
	payload := append([]byte(nil), result.GetPayload()...)
	return InvocationResult{
		InvocationID: request.InvocationID,
		Payload:      payload,
		Metadata: ExecutionMetadata{
			Transport:                  TransportGRPC,
			TransportProfile:           TransportProfileNative,
			Endpoint:                   target.Endpoint.Address,
			CallerFenceValidation:      FenceValidationPassed,
			ExecutionFenceValidation:   FenceValidationUnconfirmed,
			ExecutionFenceMode:         execution.ExecutionFenceModeNone,
			AcceptedTargetConfirmation: FenceValidationUnconfirmed,
			Status:                     InvocationStatusSuccess,
			RequestBytes:               len(request.Payload),
			ResponseBytes:              len(payload),
		},
	}, nil
}

func (transport *NativeGrpcInvocationTransport) connection(ctx context.Context, address string) (*grpc.ClientConn, error) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.closed {
		return nil, &InvocationError{
			Code: ErrorCodeTransportFailure, Message: "native grpc transport is closed",
			DispatchState: DispatchStateNotDispatched, Source: "native-grpc", Classification: "lifecycle",
		}
	}
	if connection := transport.connections[address]; connection != nil {
		return connection, nil
	}
	connection, err := transport.dialer(ctx, address)
	if err != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return nil, fmt.Errorf("dial native endpoint %q: %w", address, err)
	}
	if connection == nil {
		return nil, fmt.Errorf("dial native endpoint %q returned a nil connection", address)
	}
	transport.connections[address] = connection
	return connection, nil
}

func (transport *NativeGrpcInvocationTransport) Close() error {
	if transport == nil {
		return nil
	}
	transport.mu.Lock()
	if transport.closed {
		transport.mu.Unlock()
		return nil
	}
	transport.closed = true
	connections := make([]*grpc.ClientConn, 0, len(transport.connections))
	for address, connection := range transport.connections {
		connections = append(connections, connection)
		delete(transport.connections, address)
	}
	transport.mu.Unlock()
	var closeErr error
	for _, connection := range connections {
		closeErr = errors.Join(closeErr, connection.Close())
	}
	return closeErr
}

func nativeRequest(request InvocationRequest) *dtmv1.InvocationRequest {
	return &dtmv1.InvocationRequest{
		InvocationId: request.InvocationID.Bytes(),
		ResourceRef: &dtmv1.ResourceRef{
			ResourceId:          string(request.Target.ResourceID),
			ResourceGeneration:  uint64(request.Target.ResourceGeneration),
			OwnerNodeId:         string(request.Target.OwnerNodeID),
			OwnerNodeGeneration: request.Target.OwnerNodeGeneration,
			RegistrationId:      request.Target.RegistrationID,
		},
		OperationId: string(request.Operation),
		Payload:     append([]byte(nil), request.Payload...),
		Metadata: &dtmv1.InvocationMetadata{
			TaskId:         string(request.Metadata.TaskID),
			StepId:         string(request.Metadata.StepID),
			Attempt:        request.Metadata.Attempt,
			IdempotencyKey: request.Metadata.IdempotencyKey,
			TimeoutMillis:  request.Metadata.Timeout.Milliseconds(),
		},
	}
}

func invocationErrorFromProto(input *dtmv1.InvocationError) *InvocationError {
	code := nativeErrorCode(input.GetCode())
	source := strings.TrimSpace(input.GetSource())
	if source == "" {
		source = "native-grpc"
	}
	executionFenceMode := execution.ExecutionFenceModeNone
	if source == "execution-fence" {
		executionFenceMode = execution.ExecutionFenceModeLocalValidator
	}
	dispatchState := DispatchState(input.GetDispatchState())
	if dispatchState.Validate() != nil {
		dispatchState = DispatchStateRemoteOutcomeReceived
	}
	return &InvocationError{
		Code:               code,
		Message:            input.GetMessage(),
		Retryable:          input.GetRetryable(),
		OutcomeUnknown:     input.GetOutcomeUnknown(),
		DispatchState:      dispatchState,
		Source:             source,
		Classification:     input.GetClassification(),
		ExecutionFenceMode: executionFenceMode,
	}
}

func nativeErrorCode(value string) ErrorCode {
	code := ErrorCode(strings.TrimSpace(value))
	if code.Validate() == nil {
		return code
	}
	return ErrorCodeTransportFailure
}

func classifyNativeGRPCError(ctx context.Context, err error) *InvocationError {
	if err == nil {
		return nil
	}
	var invocationErr *InvocationError
	if errors.As(err, &invocationErr) {
		clone := *invocationErr
		if clone.Source == "" {
			clone.Source = "native-grpc"
		}
		return &clone
	}
	if ctx != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return &InvocationError{Code: ErrorCodeTimeout, Message: err.Error(), Cause: err, OutcomeUnknown: true, Retryable: true, DispatchState: DispatchStateMayHaveDispatched, Source: "native-grpc", Classification: "context"}
	}
	if ctx != nil && errors.Is(ctx.Err(), context.Canceled) || status.Code(err) == codes.Canceled {
		return &InvocationError{Code: ErrorCodeCanceled, Message: err.Error(), Cause: err, OutcomeUnknown: true, DispatchState: DispatchStateMayHaveDispatched, Source: "native-grpc", Classification: "context"}
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return &InvocationError{Code: ErrorCodeInvalidRequest, Message: err.Error(), Cause: err, DispatchState: DispatchStateNotDispatched, Source: "native-grpc", Classification: "request_validation"}
	case codes.NotFound:
		return &InvocationError{Code: ErrorCodeResourceNotFound, Message: err.Error(), Cause: err, DispatchState: DispatchStateNotDispatched, Source: "native-grpc", Classification: "remote_lookup"}
	case codes.Unavailable, codes.ResourceExhausted, codes.Aborted:
		return &InvocationError{Code: ErrorCodeTransportFailure, Message: err.Error(), Cause: err, Retryable: true, OutcomeUnknown: true, DispatchState: DispatchStateMayHaveDispatched, Source: "native-grpc", Classification: "rpc_status"}
	default:
		return &InvocationError{Code: ErrorCodeTransportFailure, Message: err.Error(), Cause: err, OutcomeUnknown: true, DispatchState: DispatchStateMayHaveDispatched, Source: "native-grpc", Classification: "rpc_status"}
	}
}

func bytesEqual(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

var _ Transport = (*NativeGrpcInvocationTransport)(nil)
