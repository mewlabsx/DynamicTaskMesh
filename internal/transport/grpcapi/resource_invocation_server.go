package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/agent"
	"dtm/internal/agentexecution"
	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	resourceErrorNotFound       = "RESOURCE_NOT_FOUND"
	resourceErrorStale          = "RESOURCE_STALE"
	resourceErrorFenceRejected  = "FENCE_REJECTED"
	resourceErrorUnsupported    = "UNSUPPORTED_OPERATION"
	resourceErrorInvalidRequest = "INVALID_REQUEST"
	resourceErrorTransport      = "TRANSPORT_FAILURE"
	resourceErrorTimeout        = "TIMEOUT"
	resourceErrorCanceled       = "CANCELED"
	resourceErrorExecution      = "EXECUTION_FAILURE"
	resourceErrorProtocol       = "PROTOCOL_ERROR"
	resourceErrorIdempotency    = "IDEMPOTENCY_CONFLICT"

	dispatchNotDispatched         = "not_dispatched"
	dispatchRemoteOutcomeReceived = "remote_outcome_received"
)

// ResourceInvocationAdapter is the small capability boundary shared by the
// legacy ExecuteStep adapter and the native Invocation RPC. It keeps payload
// decoding and encoding outside the gRPC server's protocol code.
type ResourceInvocationAdapter interface {
	DecodeRequest(model.OperationID, []byte) (model.Capability, map[string]string, error)
	IdempotencyModeForCapability(model.Capability) (model.IdempotencyMode, error)
	EncodeOutput(map[string]any) ([]byte, error)
}

// ExecutionFenceValidator is an M2 preparation seam. A validator may reject
// a stale or wrong-owner ResourceRef before the handler is entered. The
// default is deliberately a no-op: M2 does not claim native execution-side
// fencing until Authority readback is available.
type ExecutionFenceValidator interface {
	Validate(context.Context, model.ResourceRef, model.OperationID) error
}

// ExecutionFenceModeProvider is an optional companion to
// ExecutionFenceValidator. Existing validators that do not expose a mode are
// conservatively treated as no-op validators. A provider may report local
// validation, but M2 rejects authority-confirmed mode because no Authority
// receipt is available at this boundary.
type ExecutionFenceModeProvider interface {
	ExecutionFenceMode() execution.ExecutionFenceMode
}

// ExecutionFenceMode is re-exported at the server boundary so callers and
// tests do not need to depend on the lower-level execution package for the
// validator seam.
type ExecutionFenceMode = execution.ExecutionFenceMode

const (
	ExecutionFenceModeNone               = execution.ExecutionFenceModeNone
	ExecutionFenceModeLocalValidator     = execution.ExecutionFenceModeLocalValidator
	ExecutionFenceModeAuthorityConfirmed = execution.ExecutionFenceModeAuthorityConfirmed
)

var ErrAuthorityBackedExecutionFenceUnavailable = errors.New("authority-backed execution fence is unavailable in v0.6.0 M2")

// ExecutionFenceModeOf returns the mode declared by a validator. Validators
// without the optional provider retain the source-compatible no-op behavior.
func ExecutionFenceModeOf(validator ExecutionFenceValidator) execution.ExecutionFenceMode {
	if isNilDependency(validator) {
		return execution.ExecutionFenceModeNone
	}
	provider, ok := validator.(ExecutionFenceModeProvider)
	if !ok {
		return execution.ExecutionFenceModeNone
	}
	mode := provider.ExecutionFenceMode()
	if mode == "" {
		return execution.ExecutionFenceModeNone
	}
	return mode
}

type noopExecutionFenceValidator struct{}

func (noopExecutionFenceValidator) Validate(context.Context, model.ResourceRef, model.OperationID) error {
	return nil
}

func (noopExecutionFenceValidator) ExecutionFenceMode() execution.ExecutionFenceMode {
	return execution.ExecutionFenceModeNone
}

// ResourceFenceError carries a typed, pre-handler fence result without making
// the transport package depend on Invocation Core's domain package.
type ResourceFenceError struct {
	Code           string
	Message        string
	DispatchState  string
	Classification string
}

func (err *ResourceFenceError) Error() string {
	if err == nil {
		return "<nil>"
	}
	return err.Message
}

// StaticExecutionFenceValidator is useful for a local authority snapshot and
// deterministic tests. It compares the complete ResourceRef, not only the
// ResourceID or owner node.
type StaticExecutionFenceValidator struct {
	snapshots map[model.ResourceID]model.ResourceRef
}

func (validator *StaticExecutionFenceValidator) ExecutionFenceMode() execution.ExecutionFenceMode {
	return execution.ExecutionFenceModeLocalValidator
}

func NewStaticExecutionFenceValidator(refs ...model.ResourceRef) (*StaticExecutionFenceValidator, error) {
	snapshots := make(map[model.ResourceID]model.ResourceRef, len(refs))
	for _, ref := range refs {
		if err := ref.Validate(); err != nil {
			return nil, fmt.Errorf("invalid execution fence snapshot: %w", err)
		}
		if _, exists := snapshots[ref.ResourceID]; exists {
			return nil, fmt.Errorf("duplicate execution fence snapshot for resource %q", ref.ResourceID)
		}
		snapshots[ref.ResourceID] = ref
	}
	return &StaticExecutionFenceValidator{snapshots: snapshots}, nil
}

func validateExecutionFenceMode(mode execution.ExecutionFenceMode) error {
	if err := mode.Validate(); err != nil {
		return err
	}
	if mode == execution.ExecutionFenceModeAuthorityConfirmed {
		return ErrAuthorityBackedExecutionFenceUnavailable
	}
	return nil
}

func (validator *StaticExecutionFenceValidator) Validate(_ context.Context, ref model.ResourceRef, _ model.OperationID) error {
	if validator == nil {
		return nil
	}
	current, exists := validator.snapshots[ref.ResourceID]
	if !exists {
		return &ResourceFenceError{
			Code: resourceErrorNotFound, Message: "resource is not present in the local execution snapshot",
			DispatchState: dispatchNotDispatched, Classification: "execution_snapshot_missing",
		}
	}
	if current.OwnerNodeID != ref.OwnerNodeID {
		return &ResourceFenceError{
			Code: resourceErrorFenceRejected, Message: "resource owner does not match the local execution snapshot",
			DispatchState: dispatchNotDispatched, Classification: "execution_owner_mismatch",
		}
	}
	if current != ref {
		return &ResourceFenceError{
			Code: resourceErrorStale, Message: "resource ref does not match the local execution snapshot",
			DispatchState: dispatchNotDispatched, Classification: "execution_snapshot_stale",
		}
	}
	return nil
}

func WithInvocationAdapter(adapter ResourceInvocationAdapter) AgentExecutionServerOption {
	return func(server *AgentExecutionServer) error {
		if isNilDependency(adapter) {
			return ErrInvalidStepHandler
		}
		server.invocationAdapter = adapter
		return nil
	}
}

func WithExecutionFenceValidator(validator ExecutionFenceValidator) AgentExecutionServerOption {
	return func(server *AgentExecutionServer) error {
		if isNilDependency(validator) {
			return ErrInvalidStepHandler
		}
		if err := validateExecutionFenceMode(ExecutionFenceModeOf(validator)); err != nil {
			return fmt.Errorf("execution fence validator: %w", err)
		}
		server.fenceValidator = validator
		return nil
	}
}

// ExecutionFenceMode reports the local execution-boundary mode configured on
// the server. It cannot report Authority confirmation in M2.
func (server *AgentExecutionServer) ExecutionFenceMode() execution.ExecutionFenceMode {
	if server == nil {
		return execution.ExecutionFenceModeNone
	}
	mode := ExecutionFenceModeOf(server.fenceValidator)
	if validateExecutionFenceMode(mode) != nil {
		return execution.ExecutionFenceModeNone
	}
	return mode
}

func (server *AgentExecutionServer) executeMappedStep(
	ctx context.Context,
	idempotencyKey string,
	fingerprint agentexecution.Fingerprint,
	step mapper.MappedStep,
) (execution.StepResult, error, bool) {
	return server.store.execute(ctx, idempotencyKey, fingerprint, func() (execution.StepResult, error) {
		result, executeErr := server.handler.Execute(ctx, step)
		if executeErr != nil && result.Status == "" {
			return result, executeErr
		}
		validated, validationErr := validateHandlerResult(step, result)
		if validationErr != nil {
			return execution.StepResult{}, validationErr
		}
		return validated, executeErr
	})
}

// InvokeResource is the native Resource Invocation server boundary. It does
// not discover, select, retry, or remap resources; those remain caller-side
// responsibilities owned by InvocationService and Runtime.
func (server *AgentExecutionServer) InvokeResource(
	ctx context.Context,
	request *dtmv1.InvocationRequest,
) (*dtmv1.InvocationResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "invocation request is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ref, operation, metadata, err := nativeInvocationInputs(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid invocation request: %v", err)
	}
	if server.invocationAdapter == nil {
		return nativeInvocationErrorResponse(request.GetInvocationId(), resourceErrorProtocol, "native invocation adapter is not configured", false, false, "agent", "adapter_unavailable", dispatchNotDispatched), nil
	}
	invokeContext := ctx
	if metadata.Timeout > 0 {
		var cancel context.CancelFunc
		invokeContext, cancel = context.WithTimeout(ctx, metadata.Timeout)
		defer cancel()
	}
	if err := server.fenceValidator.Validate(invokeContext, ref, operation); err != nil {
		return nativeInvocationErrorFromGoError(request.GetInvocationId(), err), nil
	}

	capability, inputs, err := server.invocationAdapter.DecodeRequest(operation, request.GetPayload())
	if err != nil {
		return nativeInvocationErrorResponse(request.GetInvocationId(), resourceErrorInvalidRequest, err.Error(), false, false, "resource-capability-adapter", "payload_decode", dispatchNotDispatched), nil
	}
	idempotencyMode, err := server.invocationAdapter.IdempotencyModeForCapability(capability)
	if err != nil {
		return nativeInvocationErrorResponse(request.GetInvocationId(), resourceErrorUnsupported, err.Error(), false, false, "resource-capability-adapter", "operation_mapping", dispatchNotDispatched), nil
	}
	step := mapper.MappedStep{
		ID:              metadata.StepID,
		Capability:      capability,
		IdempotencyMode: idempotencyMode,
		NodeID:          ref.OwnerNodeID,
		ResourceRef:     ref,
		Inputs:          inputs,
	}
	fingerprint := agentexecution.NewInvocationFingerprint(metadata.TaskID, step, operation, request.GetPayload())
	result, executeErr, _ := server.executeMappedStep(invokeContext, metadata.IdempotencyKey, fingerprint, step)
	if contextErr := invokeContext.Err(); contextErr != nil {
		if errors.Is(contextErr, context.Canceled) {
			return nil, status.Error(codes.Canceled, contextErr.Error())
		}
		if errors.Is(contextErr, context.DeadlineExceeded) {
			return nil, status.Error(codes.DeadlineExceeded, contextErr.Error())
		}
	}
	if executeErr != nil {
		if errors.Is(executeErr, agent.ErrUnsupportedCapability) {
			return nativeInvocationErrorResponse(request.GetInvocationId(), resourceErrorUnsupported, executeErr.Error(), false, false, "agent", "capability", dispatchRemoteOutcomeReceived), nil
		}
		if errors.Is(executeErr, ErrIdempotencyConflict) {
			return nativeInvocationErrorResponse(request.GetInvocationId(), resourceErrorIdempotency, executeErr.Error(), false, false, "agent", "idempotency_conflict", dispatchRemoteOutcomeReceived), nil
		}
		if result.Status == execution.StatusFailed {
			message := result.Error
			if message == "" {
				message = executeErr.Error()
			}
			return nativeInvocationErrorResponse(request.GetInvocationId(), resourceErrorExecution, message, false, false, "agent", "handler_result", dispatchRemoteOutcomeReceived), nil
		}
		return nil, status.Error(codes.Internal, "native resource execution failed")
	}
	if result.Status != execution.StatusSucceeded {
		return nativeInvocationErrorResponse(request.GetInvocationId(), resourceErrorProtocol, "native handler returned an invalid result", false, false, "agent", "result_validation", dispatchRemoteOutcomeReceived), nil
	}
	payload, err := server.invocationAdapter.EncodeOutput(result.Output)
	if err != nil {
		return nativeInvocationErrorResponse(request.GetInvocationId(), resourceErrorProtocol, err.Error(), false, false, "resource-capability-adapter", "payload_encode", dispatchRemoteOutcomeReceived), nil
	}
	return &dtmv1.InvocationResponse{Outcome: &dtmv1.InvocationResponse_Result{Result: &dtmv1.InvocationResult{
		InvocationId: append([]byte(nil), request.GetInvocationId()...),
		Payload:      payload,
	}}}, nil
}

type nativeInvocationMetadata struct {
	TaskID         model.TaskID
	StepID         model.StepID
	Attempt        uint32
	IdempotencyKey string
	Timeout        time.Duration
}

func nativeInvocationInputs(request *dtmv1.InvocationRequest) (model.ResourceRef, model.OperationID, nativeInvocationMetadata, error) {
	if len(request.GetInvocationId()) != 16 || isZeroBytes(request.GetInvocationId()) {
		return model.ResourceRef{}, "", nativeInvocationMetadata{}, errors.New("invocation ID must be 16 non-zero bytes")
	}
	inputRef := request.GetResourceRef()
	if inputRef == nil {
		return model.ResourceRef{}, "", nativeInvocationMetadata{}, errors.New("resource ref is required")
	}
	ref, err := model.NewResourceRef(
		model.ResourceID(strings.TrimSpace(inputRef.GetResourceId())),
		model.ResourceGeneration(inputRef.GetResourceGeneration()),
		model.NodeID(strings.TrimSpace(inputRef.GetOwnerNodeId())),
		inputRef.GetOwnerNodeGeneration(),
		inputRef.GetRegistrationId(),
	)
	if err != nil {
		return model.ResourceRef{}, "", nativeInvocationMetadata{}, err
	}
	operation := model.OperationID(strings.TrimSpace(request.GetOperationId()))
	if err := operation.Validate(); err != nil {
		return model.ResourceRef{}, "", nativeInvocationMetadata{}, fmt.Errorf("operation: %w", err)
	}
	inputMetadata := request.GetMetadata()
	if inputMetadata == nil {
		return model.ResourceRef{}, "", nativeInvocationMetadata{}, errors.New("metadata is required")
	}
	metadata := nativeInvocationMetadata{
		TaskID:         model.TaskID(strings.TrimSpace(inputMetadata.GetTaskId())),
		StepID:         model.StepID(strings.TrimSpace(inputMetadata.GetStepId())),
		Attempt:        inputMetadata.GetAttempt(),
		IdempotencyKey: strings.TrimSpace(inputMetadata.GetIdempotencyKey()),
		Timeout:        time.Duration(inputMetadata.GetTimeoutMillis()) * time.Millisecond,
	}
	if metadata.TaskID == "" || metadata.StepID == "" || metadata.Attempt == 0 || metadata.IdempotencyKey == "" {
		return model.ResourceRef{}, "", nativeInvocationMetadata{}, errors.New("task ID, step ID, positive attempt, and idempotency key are required")
	}
	if inputMetadata.GetTimeoutMillis() < 0 || inputMetadata.GetTimeoutMillis() > int64((5*time.Minute)/time.Millisecond) {
		return model.ResourceRef{}, "", nativeInvocationMetadata{}, errors.New("timeout_millis is outside the invocation timeout bound")
	}
	return ref, operation, metadata, nil
}

func nativeInvocationErrorFromGoError(invocationID []byte, err error) *dtmv1.InvocationResponse {
	var fenceErr *ResourceFenceError
	if errors.As(err, &fenceErr) {
		return nativeInvocationErrorResponse(invocationID, fenceErr.Code, fenceErr.Message, false, false, "execution-fence", fenceErr.Classification, fenceErr.DispatchState)
	}
	return nativeInvocationErrorResponse(invocationID, resourceErrorProtocol, err.Error(), false, false, "execution-fence", "validation", dispatchNotDispatched)
}

func nativeInvocationErrorResponse(invocationID []byte, code, message string, retryable, outcomeUnknown bool, source, classification, dispatchState string) *dtmv1.InvocationResponse {
	return &dtmv1.InvocationResponse{Outcome: &dtmv1.InvocationResponse_Error{Error: &dtmv1.InvocationError{
		InvocationId: append([]byte(nil), invocationID...), Code: code, Message: message, Retryable: retryable, OutcomeUnknown: outcomeUnknown,
		Source: source, Classification: classification, DispatchState: dispatchState,
	}}}
}

func isZeroBytes(value []byte) bool {
	for _, item := range value {
		if item != 0 {
			return false
		}
	}
	return true
}

var _ dtmv1.ResourceInvocationServiceServer = (*AgentExecutionServer)(nil)
