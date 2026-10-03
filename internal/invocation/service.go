package invocation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"dtm/internal/execution"
	"dtm/internal/model"
)

var (
	ErrInvalidInvocationService = errors.New("invalid invocation service")
	ErrNoCompatibleTransport    = errors.New("no compatible invocation transport")
)

type TargetResolver interface {
	Resolve(context.Context, model.ResourceRef, model.OperationID) ([]ResolvedInvocationTarget, error)
}

type InvocationObservation struct {
	Request InvocationRequest
	Target  *ResolvedInvocationTarget
	Result  *InvocationResult
	Error   *InvocationError
	// Metadata is the InvocationService's terminal observation. It is present
	// for both success and failure so observers never have to infer lifecycle
	// or fence state from the presence of Result/Error.
	Metadata ExecutionMetadata
}

type InvocationObserver func(context.Context, InvocationObservation) error

type InvocationService struct {
	resolver  TargetResolver
	transport Transport
	observer  InvocationObserver
	now       func() time.Time
}

type InvocationServiceOption func(*InvocationService) error

func WithInvocationObserver(observer InvocationObserver) InvocationServiceOption {
	return func(service *InvocationService) error {
		if observer == nil {
			return ErrInvalidInvocationService
		}
		service.observer = observer
		return nil
	}
}

// WithObserver is intentionally short for package-local composition and tests.
func WithObserver(observer InvocationObserver) InvocationServiceOption {
	return WithInvocationObserver(observer)
}

func NewInvocationService(resolver TargetResolver, transport Transport, options ...InvocationServiceOption) (*InvocationService, error) {
	if isNilServiceDependency(resolver) || isNilServiceDependency(transport) {
		return nil, ErrInvalidInvocationService
	}
	service := &InvocationService{resolver: resolver, transport: transport, now: time.Now}
	for _, option := range options {
		if option == nil {
			return nil, ErrInvalidInvocationService
		}
		if err := option(service); err != nil {
			return nil, err
		}
	}
	return service, nil
}

func NewService(resolver TargetResolver, transport Transport, options ...InvocationServiceOption) (*InvocationService, error) {
	return NewInvocationService(resolver, transport, options...)
}

func (service *InvocationService) Invoke(ctx context.Context, request InvocationRequest) (InvocationResult, error) {
	if service == nil || isNilServiceDependency(service.resolver) || isNilServiceDependency(service.transport) {
		return InvocationResult{}, &InvocationError{Code: ErrorCodeInvalidRequest, Message: ErrInvalidInvocationService.Error(), Source: "service", Classification: "invalid_dependency"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	start := service.now().UTC()
	if err := request.Validate(); err != nil {
		invocationErr := &InvocationError{Code: ErrorCodeInvalidRequest, Message: err.Error(), Cause: err, Source: "invocation-service", Classification: "request_validation"}
		service.observeFailure(ctx, request, start, nil, invocationErr, FenceValidationNotRun, FenceValidationNotRun, FenceValidationNotRun)
		return InvocationResult{}, invocationErr
	}

	invokeContext := ctx
	var cancel context.CancelFunc
	if request.Metadata.Timeout > 0 {
		invokeContext, cancel = context.WithTimeout(ctx, request.Metadata.Timeout)
		defer cancel()
	}

	targets, err := service.resolver.Resolve(invokeContext, request.Target, request.Operation)
	if err != nil {
		invocationErr := normalizeInvocationError(err, ErrorCodeResourceNotFound, "invocation-service")
		service.observeFailure(ctx, request, start, nil, invocationErr, callerFenceForResolverFailure(invocationErr), FenceValidationNotRun, FenceValidationNotRun)
		return InvocationResult{}, invocationErr
	}
	target, err := chooseGRPCTarget(targets, request)
	if err != nil {
		invocationErr := normalizeInvocationError(err, ErrorCodeTransportFailure, "invocation-service")
		callerFence := FenceValidationNotRun
		if len(targets) > 0 {
			callerFence = FenceValidationPassed
		}
		service.observeFailure(ctx, request, start, nil, invocationErr, callerFence, FenceValidationNotRun, FenceValidationNotRun)
		return InvocationResult{}, invocationErr
	}

	result, err := service.transport.Invoke(invokeContext, target, request)
	if err != nil {
		invocationErr := normalizeInvocationError(err, ErrorCodeTransportFailure, service.transportSource())
		executionFence, acceptedTarget := failureExecutionFence(invocationErr)
		service.observeFailure(ctx, request, start, &target, invocationErr, FenceValidationPassed, executionFence, acceptedTarget)
		return InvocationResult{}, invocationErr
	}
	if result.InvocationID != request.InvocationID {
		invocationErr := &InvocationError{
			Code:    ErrorCodeProtocolError,
			Message: fmt.Sprintf("invocation result id %q does not match request %q", result.InvocationID, request.InvocationID),
			Source:  "invocation-service", Classification: "correlation_mismatch",
		}
		service.observeFailure(ctx, request, start, &target, invocationErr, FenceValidationPassed, FenceValidationUnconfirmed, FenceValidationUnconfirmed)
		return InvocationResult{}, invocationErr
	}
	if target.Endpoint.TransportID == TransportGRPC &&
		(result.Metadata.TransportProfile == "" || result.Metadata.TransportProfile == TransportProfileLegacy) {
		if result.Metadata.ExecutionFenceMode != "" && result.Metadata.ExecutionFenceMode != ExecutionFenceModeNone {
			invocationErr := &InvocationError{Code: ErrorCodeProtocolError, Message: "legacy transport claimed execution fence validation", Source: "invocation-service", Classification: "legacy_fence_mode"}
			service.observeFailure(ctx, request, start, &target, invocationErr, FenceValidationPassed, FenceValidationUnconfirmed, FenceValidationUnconfirmed)
			return InvocationResult{}, invocationErr
		}
		if result.Metadata.ExecutionFenceValidation != "" && result.Metadata.ExecutionFenceValidation != FenceValidationUnconfirmed {
			invocationErr := &InvocationError{Code: ErrorCodeProtocolError, Message: "legacy transport claimed a confirmed execution fence", Source: "invocation-service", Classification: "legacy_fence_claim"}
			service.observeFailure(ctx, request, start, &target, invocationErr, FenceValidationPassed, FenceValidationUnconfirmed, FenceValidationUnconfirmed)
			return InvocationResult{}, invocationErr
		}
		if result.Metadata.AcceptedTarget != nil || (result.Metadata.AcceptedTargetConfirmation != "" && result.Metadata.AcceptedTargetConfirmation != FenceValidationUnconfirmed) {
			invocationErr := &InvocationError{Code: ErrorCodeProtocolError, Message: "legacy transport returned an unconfirmed accepted target", Source: "invocation-service", Classification: "legacy_target_receipt"}
			service.observeFailure(ctx, request, start, &target, invocationErr, FenceValidationPassed, FenceValidationUnconfirmed, FenceValidationUnconfirmed)
			return InvocationResult{}, invocationErr
		}
	}
	if err := result.Validate(); err != nil {
		invocationErr := &InvocationError{Code: ErrorCodeProtocolError, Message: "invalid invocation result", Cause: err, Source: "invocation-service", Classification: "result_validation"}
		service.observeFailure(ctx, request, start, &target, invocationErr, FenceValidationPassed, FenceValidationUnconfirmed, FenceValidationUnconfirmed)
		return InvocationResult{}, invocationErr
	}

	result.Payload = append([]byte(nil), result.Payload...)
	result.Metadata = result.Metadata.Clone()
	// Invocation timing belongs to this service. A transport may return its
	// own completion timestamp, but it must never replace the invocation
	// lifecycle start or duration in the public observation.
	result.Metadata.Start = start
	result.Metadata.Duration = service.durationSince(start)
	result.Metadata.RequestBytes = len(request.Payload)
	result.Metadata.ResponseBytes = len(result.Payload)
	result.Metadata.Transport = target.Endpoint.TransportID
	if result.Metadata.TransportProfile == "" {
		result.Metadata.TransportProfile = service.transportProfile()
	}
	result.Metadata.Endpoint = target.Endpoint.Address
	result.Metadata.CallerFenceValidation = FenceValidationPassed
	if result.Metadata.ExecutionFenceMode == "" {
		result.Metadata.ExecutionFenceMode = execution.ExecutionFenceModeNone
	}
	if result.Metadata.ExecutionFenceValidation == "" {
		if result.Metadata.ExecutionFenceMode == execution.ExecutionFenceModeLocalValidator {
			result.Metadata.ExecutionFenceValidation = FenceValidationPassed
		} else {
			result.Metadata.ExecutionFenceValidation = FenceValidationUnconfirmed
		}
	}
	if result.Metadata.AcceptedTargetConfirmation == "" {
		result.Metadata.AcceptedTargetConfirmation = FenceValidationUnconfirmed
	}
	result.Metadata.Status = InvocationStatusSuccess
	service.observe(ctx, InvocationObservation{Request: cloneRequest(request), Target: targetPtr(target), Result: &result, Metadata: result.Metadata.Clone()})
	return result, nil
}

func (service *InvocationService) observeFailure(
	ctx context.Context,
	request InvocationRequest,
	start time.Time,
	target *ResolvedInvocationTarget,
	invocationErr *InvocationError,
	callerFence string,
	executionFence string,
	acceptedTarget string,
) {
	if invocationErr == nil {
		return
	}
	metadata := ExecutionMetadata{
		CallerFenceValidation:      callerFence,
		ExecutionFenceValidation:   executionFence,
		ExecutionFenceMode:         executionFenceModeForError(invocationErr),
		AcceptedTargetConfirmation: acceptedTarget,
		Start:                      start,
		Duration:                   service.durationSince(start),
		Status:                     InvocationStatusFailed,
		ErrorCode:                  invocationErr.Code,
		ErrorSource:                invocationErr.Source,
		ErrorClassification:        invocationErr.Classification,
		OutcomeUnknown:             invocationErr.OutcomeUnknown,
		RequestBytes:               len(request.Payload),
	}
	var observationTarget *ResolvedInvocationTarget
	if target != nil {
		clone := target.Clone()
		observationTarget = &clone
		metadata.Transport = target.Endpoint.TransportID
		metadata.TransportProfile = service.transportProfile()
		metadata.Endpoint = target.Endpoint.Address
	}
	service.observe(ctx, InvocationObservation{
		Request:  cloneRequest(request),
		Target:   observationTarget,
		Error:    invocationErr,
		Metadata: metadata,
	})
}

type transportProfileProvider interface {
	TransportProfile() string
}

func (service *InvocationService) transportProfile() string {
	if service != nil && service.transport != nil {
		if provider, ok := service.transport.(transportProfileProvider); ok {
			if profile := provider.TransportProfile(); profile != "" {
				return profile
			}
		}
	}
	return TransportProfileLegacy
}

func (service *InvocationService) transportSource() string {
	if service.transportProfile() == TransportProfileNative {
		return "native-grpc"
	}
	return "legacy-grpc"
}

func (service *InvocationService) durationSince(start time.Time) time.Duration {
	duration := service.now().UTC().Sub(start)
	if duration < 0 {
		return 0
	}
	return duration
}

func callerFenceForResolverFailure(invocationErr *InvocationError) string {
	if invocationErr == nil {
		return FenceValidationNotRun
	}
	switch invocationErr.Code {
	case ErrorCodeResourceStale, ErrorCodeFenceRejected, ErrorCodeUnsupportedOperation:
		return FenceValidationRejected
	case ErrorCodeResourceNotFound:
		if invocationErr.Source == "endpoint-directory" || invocationErr.Classification == "compatibility_endpoint_missing" {
			return FenceValidationRejected
		}
		return FenceValidationNotRun
	default:
		return FenceValidationNotRun
	}
}

func failureExecutionFence(invocationErr *InvocationError) (string, string) {
	if invocationErr == nil {
		return FenceValidationNotRun, FenceValidationNotRun
	}
	if invocationErr.Source == "execution-fence" || invocationErr.ExecutionFenceMode == execution.ExecutionFenceModeLocalValidator {
		return FenceValidationRejected, FenceValidationNotRun
	}
	if invocationErr.OutcomeUnknown {
		return FenceValidationUnconfirmed, FenceValidationUnconfirmed
	}
	if invocationErr.DispatchState == DispatchStateNotDispatched {
		return FenceValidationNotRun, FenceValidationNotRun
	}
	if invocationErr.DispatchState == DispatchStateMayHaveDispatched || invocationErr.DispatchState == DispatchStateRemoteOutcomeReceived ||
		invocationErr.Code == ErrorCodeExecutionFailure {
		return FenceValidationUnconfirmed, FenceValidationUnconfirmed
	}
	// A transport error without a certainty receipt is conservative: the
	// legacy request may have crossed the RPC boundary even when its taxonomy
	// does not say OutcomeUnknown explicitly.
	return FenceValidationUnconfirmed, FenceValidationUnconfirmed
}

func executionFenceModeForError(invocationErr *InvocationError) execution.ExecutionFenceMode {
	if invocationErr == nil {
		return execution.ExecutionFenceModeNone
	}
	if invocationErr.ExecutionFenceMode == execution.ExecutionFenceModeLocalValidator {
		return execution.ExecutionFenceModeLocalValidator
	}
	if invocationErr.Source == "execution-fence" {
		return execution.ExecutionFenceModeLocalValidator
	}
	return execution.ExecutionFenceModeNone
}

func chooseGRPCTarget(targets []ResolvedInvocationTarget, request InvocationRequest) (ResolvedInvocationTarget, error) {
	if len(targets) == 0 {
		return ResolvedInvocationTarget{}, &InvocationError{Code: ErrorCodeResourceNotFound, Message: "authoritative resolver returned no target", Source: "resolver", Classification: "empty_resolution"}
	}
	for _, target := range targets {
		if target.ResourceRef != request.Target || target.Operation != request.Operation {
			return ResolvedInvocationTarget{}, &InvocationError{Code: ErrorCodeProtocolError, Message: "resolver returned a target not bound to the exact request", Source: "invocation-service", Classification: "target_correlation"}
		}
		if err := target.Validate(); err != nil {
			return ResolvedInvocationTarget{}, &InvocationError{Code: ErrorCodeProtocolError, Message: "resolver returned invalid target", Cause: err, Source: "invocation-service", Classification: "target_validation"}
		}
		if target.Endpoint.TransportID == TransportGRPC {
			return target, nil
		}
	}
	return ResolvedInvocationTarget{}, &InvocationError{Code: ErrorCodeTransportFailure, Message: ErrNoCompatibleTransport.Error(), Source: "invocation-service", Classification: "transport_selection"}
}

func normalizeInvocationError(err error, fallback ErrorCode, source string) *InvocationError {
	if err == nil {
		return nil
	}
	var invocationErr *InvocationError
	if errors.As(err, &invocationErr) {
		clone := *invocationErr
		if clone.Source == "" {
			clone.Source = source
		}
		if clone.Classification == "" {
			clone.Classification = "compatibility/conservative"
		}
		return &clone
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &InvocationError{Code: ErrorCodeTimeout, Message: err.Error(), Cause: err, Source: source, Classification: "context"}
	}
	if errors.Is(err, context.Canceled) {
		return &InvocationError{Code: ErrorCodeCanceled, Message: err.Error(), Cause: err, Source: source, Classification: "context"}
	}
	return &InvocationError{Code: fallback, Message: err.Error(), Cause: err, Source: source, Classification: "compatibility/conservative"}
}

func (service *InvocationService) observe(ctx context.Context, observation InvocationObservation) {
	if service == nil || service.observer == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Evidence is observational. A sink failure must not change invocation
	// semantics or cause a second attempt in the Runtime.
	_ = service.observer(ctx, observation)
}

func cloneRequest(request InvocationRequest) InvocationRequest {
	request.Payload = append([]byte(nil), request.Payload...)
	return request
}

func targetPtr(target ResolvedInvocationTarget) *ResolvedInvocationTarget {
	clone := target.Clone()
	return &clone
}

func isNilServiceDependency(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var _ TargetResolver = (*AuthoritativeTargetResolver)(nil)
