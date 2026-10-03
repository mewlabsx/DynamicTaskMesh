package kernel

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"unicode/utf8"

	"dtm/internal/kernel/execution"
)

// The logical execution facade is deliberately an in-process composition
// boundary. It keeps the logical contract separate from the manager APIs and
// does not define a syscall, RPC, wire, persistence, or remote protocol.

type LogicalExecutionStatus string

const (
	LogicalExecutionRejected   LogicalExecutionStatus = "REJECTED"
	LogicalExecutionAccepted   LogicalExecutionStatus = "ACCEPTED"
	LogicalExecutionAllocated  LogicalExecutionStatus = "ALLOCATED"
	LogicalExecutionDispatched LogicalExecutionStatus = "DISPATCHED"
	LogicalExecutionRunning    LogicalExecutionStatus = "RUNNING"
	LogicalExecutionCompleted  LogicalExecutionStatus = "COMPLETED"
	LogicalExecutionFailed     LogicalExecutionStatus = "FAILED"
	LogicalExecutionUnknown    LogicalExecutionStatus = "UNKNOWN"
)

type ProviderCrossing string

const (
	ProviderCrossingNotCrossedDefinite ProviderCrossing = "NOT_CROSSED_DEFINITE"
	ProviderCrossingCrossed            ProviderCrossing = "CROSSED"
	ProviderCrossingUncertain          ProviderCrossing = "CROSSING_UNCERTAIN"
)

// Short aliases keep the values easy to use without adding another semantic
// vocabulary. CROSSING_UNCERTAIN is reserved and is never emitted by this
// bounded local implementation.
const (
	NotCrossedDefinite = ProviderCrossingNotCrossedDefinite
	Crossed            = ProviderCrossingCrossed
	CrossingUncertain  = ProviderCrossingUncertain
)

type RetrySafetyFact string

const (
	RetrySafetyNotDeclared     RetrySafetyFact = "NOT_DECLARED"
	RetrySafetyExplicitSafe    RetrySafetyFact = "EXPLICITLY_SAFE"
	RetrySafetyUnsafeOrUnknown RetrySafetyFact = "UNSAFE_OR_UNKNOWN"
)

const (
	NotDeclared     = RetrySafetyNotDeclared
	ExplicitlySafe  = RetrySafetyExplicitSafe
	UnsafeOrUnknown = RetrySafetyUnsafeOrUnknown
)

type LogicalErrorPhase string

const (
	LogicalPhaseRequestValidation   LogicalErrorPhase = "REQUEST_VALIDATION"
	LogicalPhaseAuthorityValidation LogicalErrorPhase = "AUTHORITY_VALIDATION"
	LogicalPhaseAllocation          LogicalErrorPhase = "ALLOCATION"
	LogicalPhasePreProvider         LogicalErrorPhase = "PRE_PROVIDER"
	LogicalPhaseProviderCrossing    LogicalErrorPhase = "PROVIDER_CROSSING"
	LogicalPhasePostProvider        LogicalErrorPhase = "POST_PROVIDER"
	LogicalPhaseObservation         LogicalErrorPhase = "OBSERVATION"
	LogicalPhaseResolution          LogicalErrorPhase = "RESOLUTION"
	LogicalPhaseRelease             LogicalErrorPhase = "RELEASE"
)

type LogicalErrorCategory string

const (
	LogicalErrorAuthorizationDenied       LogicalErrorCategory = "AUTHORIZATION_DENIED"
	LogicalErrorInvalidOrStaleAuthority   LogicalErrorCategory = "INVALID_OR_STALE_AUTHORITY"
	LogicalErrorCapacityUnavailable       LogicalErrorCategory = "CAPACITY_UNAVAILABLE"
	LogicalErrorInvalidRequest            LogicalErrorCategory = "INVALID_REQUEST"
	LogicalErrorInvocationConflict        LogicalErrorCategory = "INVOCATION_CONFLICT"
	LogicalErrorProviderNotAvailable      LogicalErrorCategory = "PROVIDER_NOT_AVAILABLE"
	LogicalErrorProviderCrossingUncertain LogicalErrorCategory = "PROVIDER_CROSSING_UNCERTAIN"
	LogicalErrorExecutionUnresolved       LogicalErrorCategory = "EXECUTION_UNRESOLVED"
	LogicalErrorResolutionNotAllowed      LogicalErrorCategory = "RESOLUTION_NOT_ALLOWED"
	LogicalErrorAlreadyReleased           LogicalErrorCategory = "ALREADY_RELEASED"
	LogicalErrorAlreadyResolved           LogicalErrorCategory = "ALREADY_RESOLVED"
	LogicalErrorInvariantViolation        LogicalErrorCategory = "INVARIANT_VIOLATION"
	LogicalErrorInternalFailure           LogicalErrorCategory = "INTERNAL_FAILURE"
)

// LogicalErrorInfo is the typed semantic error surface. Diagnostic is
// descriptive only; callers must use Category and the other typed dimensions
// rather than parsing Error().
type LogicalErrorInfo struct {
	Category LogicalErrorCategory
	Phase    LogicalErrorPhase
	// ProviderCrossingAvailable is false when the Kernel has no authoritative
	// crossing fact for this result. In that case ProviderCrossing is not
	// interpretable and does not mean NOT_CROSSED_DEFINITE, CROSSED, or
	// CROSSING_UNCERTAIN.
	ProviderCrossingAvailable bool
	ProviderCrossing          ProviderCrossing
	RetrySafetyFact           RetrySafetyFact
	InvocationID              InvocationID
	RequestID                 string
	ExecutionAllocationID     ExecutionAllocationID
	RelatedObject             ObjectReference
	ReconciliationRequired    bool
	Diagnostic                string

	cause error
}

// LogicalExecutionError is a descriptive alias for callers that prefer the
// operation-specific name.
type LogicalExecutionError = LogicalErrorInfo

func (info *LogicalErrorInfo) Error() string {
	if info == nil {
		return ""
	}
	if strings.TrimSpace(info.Diagnostic) == "" {
		return string(info.Category)
	}
	return fmt.Sprintf("%s: %s", info.Category, info.Diagnostic)
}

func (info *LogicalErrorInfo) Unwrap() error {
	if info == nil {
		return nil
	}
	return info.cause
}

func (info *LogicalErrorInfo) Clone() *LogicalErrorInfo {
	if info == nil {
		return nil
	}
	clone := *info
	return &clone
}

var (
	ErrInvalidLogicalExecutionFacade = errors.New("invalid logical execution facade")
	ErrLogicalInvocationConflict     = errors.New("logical invocation binding conflicts with existing invocation")
	ErrLogicalResolutionNotAllowed   = errors.New("logical occupancy resolution is not allowed")
	ErrTrustedResolutionUnavailable  = errors.New("trusted occupancy resolution source is unavailable")
	ErrTrustedResolutionInvalid      = errors.New("trusted occupancy resolution source returned invalid evidence")
)

// LogicalExecutionRequest is the in-process logical input. RequestID is
// optional request-message correlation and is intentionally excluded from the
// immutable InvocationBinding.
type LogicalExecutionRequest struct {
	InvocationID       InvocationID
	RequestID          string
	RootTaskRef        string
	ChildTaskRef       string
	ExecutionContextID ExecutionContextID
	CapabilityHandleID CapabilityHandleID
	CallerIdentity     string
	Scope              string
	Operation          string
	Payload            []byte
}

func (request LogicalExecutionRequest) Clone() LogicalExecutionRequest {
	request.Payload = append([]byte(nil), request.Payload...)
	return request
}

func (request LogicalExecutionRequest) Validate() error {
	if err := request.InvocationID.Validate(); err != nil {
		return fmt.Errorf("invocation identity: %w", err)
	}
	if strings.ContainsRune(request.RequestID, '\x00') || !utf8.ValidString(request.RequestID) {
		return errors.New("request identity is invalid")
	}
	return (execution.ExecutionRequest{
		InvocationID:       request.InvocationID,
		RootTaskRef:        request.RootTaskRef,
		ChildTaskRef:       request.ChildTaskRef,
		ExecutionContextID: request.ExecutionContextID,
		CapabilityHandleID: request.CapabilityHandleID,
		CallerIdentity:     request.CallerIdentity,
		Scope:              request.Scope,
		Operation:          request.Operation,
		Payload:            request.Payload,
	}).Validate()
}

func (request LogicalExecutionRequest) executionRequest() execution.ExecutionRequest {
	return execution.ExecutionRequest{
		InvocationID:       request.InvocationID,
		RootTaskRef:        request.RootTaskRef,
		ChildTaskRef:       request.ChildTaskRef,
		ExecutionContextID: request.ExecutionContextID,
		CapabilityHandleID: request.CapabilityHandleID,
		CallerIdentity:     request.CallerIdentity,
		Scope:              request.Scope,
		Operation:          request.Operation,
		Payload:            append([]byte(nil), request.Payload...),
	}
}

type invocationBinding struct {
	ExecutionContextID ExecutionContextID
	CapabilityHandleID CapabilityHandleID
	Operation          string
	Payload            []byte
	RootTaskRef        string
	ChildTaskRef       string
}

func newInvocationBinding(request LogicalExecutionRequest) invocationBinding {
	return invocationBinding{
		ExecutionContextID: request.ExecutionContextID,
		CapabilityHandleID: request.CapabilityHandleID,
		Operation:          request.Operation,
		Payload:            append([]byte(nil), request.Payload...),
		RootTaskRef:        request.RootTaskRef,
		ChildTaskRef:       request.ChildTaskRef,
	}
}

func (binding invocationBinding) equal(other invocationBinding) bool {
	return binding.ExecutionContextID == other.ExecutionContextID &&
		binding.CapabilityHandleID == other.CapabilityHandleID &&
		binding.Operation == other.Operation &&
		bytes.Equal(binding.Payload, other.Payload) &&
		binding.RootTaskRef == other.RootTaskRef &&
		binding.ChildTaskRef == other.ChildTaskRef
}

// LogicalExecutionResult is a stable value snapshot. It contains no manager,
// lock, provider, or registry pointer. Observation is historical evidence;
// CurrentOccupancy is the authoritative per-allocation conclusion.
type LogicalExecutionResult struct {
	InvocationID          InvocationID
	RequestID             string
	ExecutionAllocationID ExecutionAllocationID
	Status                LogicalExecutionStatus
	// ProviderCrossingAvailable is false when the Kernel has no authoritative
	// crossing fact for this result. In that case ProviderCrossing is not
	// interpretable and does not mean NOT_CROSSED_DEFINITE, CROSSED, or
	// CROSSING_UNCERTAIN.
	ProviderCrossingAvailable bool
	ProviderCrossing          ProviderCrossing
	Observation               ExecutionObservation
	ObservationAvailable      bool
	CurrentOccupancy          CurrentOccupancy
	Lifecycle                 AllocationLifecycle
	EventID                   EventID
	ReconciliationRequired    bool
	RetrySafetyFact           RetrySafetyFact
	ErrorInfo                 *LogicalErrorInfo
	Diagnostic                string
}

func (result LogicalExecutionResult) Clone() LogicalExecutionResult {
	result.Observation = result.Observation.Clone()
	result.ErrorInfo = result.ErrorInfo.Clone()
	return result
}

type InvocationObservationState string

const (
	InvocationObservationNotKnown             InvocationObservationState = "NOT_KNOWN"
	InvocationObservationAcceptedNotAllocated InvocationObservationState = "ACCEPTED_NOT_ALLOCATED"
	InvocationObservationAllocatedNotCrossed  InvocationObservationState = "ALLOCATED_NOT_CROSSED"
	InvocationObservationDefinitive           InvocationObservationState = "DEFINITIVE_OBSERVATION"
	InvocationObservationUnresolvedUnknown    InvocationObservationState = "UNRESOLVED_UNKNOWN"
	InvocationObservationEndedReleased        InvocationObservationState = "ENDED_RELEASED"
	InvocationObservationInvariantUnknown     InvocationObservationState = "INVARIANT_UNKNOWN"
)

// LogicalInvocationObservation embeds the stable result so its semantic
// fields are available on both RequestExecution and ObserveInvocation paths.
type LogicalInvocationObservation struct {
	LogicalExecutionResult
	State InvocationObservationState
}

type InvocationObservation = LogicalInvocationObservation

func (observation LogicalInvocationObservation) Clone() LogicalInvocationObservation {
	observation.LogicalExecutionResult = observation.LogicalExecutionResult.Clone()
	return observation
}

// TrustedResolutionLookup is the exact authoritative target presented to an
// independently admitted trusted source. It contains no user-supplied
// authority and cannot itself authorize a transition.
type TrustedResolutionLookup struct {
	InvocationID          InvocationID
	ExecutionAllocationID ExecutionAllocationID
	ResourceID            ResourceID
	OwnershipFence        OwnershipFence
	CurrentOccupancy      CurrentOccupancy
}

// TrustedResolutionSource is an explicit ingress for already admitted
// Resource-authoritative evidence. The facade never obtains authority from a
// ResourceID on behalf of a logical requester.
type TrustedResolutionSource interface {
	ResolveExecutionOccupancy(lookup TrustedResolutionLookup) (ExecutionOccupancyResolutionRequest, error)
}

// TrustedResolutionSourceFunc adapts a function to TrustedResolutionSource.
type TrustedResolutionSourceFunc func(TrustedResolutionLookup) (ExecutionOccupancyResolutionRequest, error)

func (source TrustedResolutionSourceFunc) ResolveExecutionOccupancy(lookup TrustedResolutionLookup) (ExecutionOccupancyResolutionRequest, error) {
	if source == nil {
		return ExecutionOccupancyResolutionRequest{}, ErrTrustedResolutionUnavailable
	}
	return source(lookup)
}

type LogicalOccupancyResolutionRequest struct {
	InvocationID InvocationID
	RequestID    string
	Diagnostic   string
}

func (request LogicalOccupancyResolutionRequest) Validate() error {
	if err := request.InvocationID.Validate(); err != nil {
		return fmt.Errorf("invocation identity: %w", err)
	}
	if strings.ContainsRune(request.RequestID, '\x00') || !utf8.ValidString(request.RequestID) {
		return errors.New("request identity is invalid")
	}
	if strings.ContainsRune(request.Diagnostic, '\x00') || !utf8.ValidString(request.Diagnostic) {
		return errors.New("resolution diagnostic is invalid")
	}
	return nil
}

type LogicalResolutionStatus = ResolutionStatus

const (
	LogicalResolutionResolved        = ResolutionStatusResolved
	LogicalResolutionAlreadyResolved = ResolutionStatusAlreadyResolved
	LogicalResolutionRejected        = ResolutionStatusRejected
)

type LogicalResolutionResult struct {
	InvocationID          InvocationID
	RequestID             string
	ExecutionAllocationID ExecutionAllocationID
	ResourceID            ResourceID
	Status                LogicalResolutionStatus
	// ProviderCrossingAvailable is false when no authoritative crossing fact
	// is available; the ProviderCrossing value is then not interpretable.
	ProviderCrossingAvailable bool
	ProviderCrossing          ProviderCrossing
	CurrentOccupancy          CurrentOccupancy
	Lifecycle                 AllocationLifecycle
	EventID                   EventID
	ReconciliationRequired    bool
	RetrySafetyFact           RetrySafetyFact
	ErrorInfo                 *LogicalErrorInfo
	Diagnostic                string
}

func (result LogicalResolutionResult) Clone() LogicalResolutionResult {
	result.ErrorInfo = result.ErrorInfo.Clone()
	return result
}

// LogicalExecutionFacade composes the existing Kernel ownership boundary.
// The registry is correlation/idempotency state only; authoritative Resource
// claims, allocation lifecycle, occupancy, observations, and EventRecords
// remain owned by the underlying Kernel managers.
type LogicalExecutionFacade struct {
	kernel                  *Kernel
	trustedResolutionSource TrustedResolutionSource

	mu          sync.Mutex
	invocations map[InvocationID]*logicalInvocationRecord
}

func NewLogicalExecutionFacade(kernel *Kernel, sources ...TrustedResolutionSource) (*LogicalExecutionFacade, error) {
	if kernel == nil || kernel.Resources == nil || kernel.Capabilities == nil || kernel.Executions == nil || kernel.Events == nil {
		return nil, ErrInvalidLogicalExecutionFacade
	}
	if len(sources) > 1 {
		return nil, fmt.Errorf("%w: only one trusted resolution source is allowed", ErrInvalidLogicalExecutionFacade)
	}
	var source TrustedResolutionSource
	if len(sources) == 1 {
		source = sources[0]
		if isNilTrustedResolutionSource(source) {
			source = nil
		}
	}
	return &LogicalExecutionFacade{
		kernel:                  kernel,
		trustedResolutionSource: source,
		invocations:             make(map[InvocationID]*logicalInvocationRecord),
	}, nil
}

// NewExecutionFacade is a concise compatibility constructor alias.
func NewExecutionFacade(kernel *Kernel, sources ...TrustedResolutionSource) (*LogicalExecutionFacade, error) {
	return NewLogicalExecutionFacade(kernel, sources...)
}

func isNilTrustedResolutionSource(source TrustedResolutionSource) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func (facade *LogicalExecutionFacade) RequestExecution(request LogicalExecutionRequest) (LogicalExecutionResult, error) {
	if facade == nil || facade.kernel == nil {
		return logicalExecutionFailure(request.InvocationID, request.RequestID, LogicalErrorInternalFailure, LogicalPhaseRequestValidation, "", RetrySafetyUnsafeOrUnknown, true, ErrInvalidLogicalExecutionFacade)
	}
	request = request.Clone()
	if err := request.Validate(); err != nil {
		return logicalExecutionFailure(request.InvocationID, request.RequestID, LogicalErrorInvalidRequest, LogicalPhaseRequestValidation, "", RetrySafetyNotDeclared, false, err)
	}

	binding := newInvocationBinding(request)
	accepted := LogicalExecutionResult{
		InvocationID:              request.InvocationID,
		RequestID:                 request.RequestID,
		Status:                    LogicalExecutionAccepted,
		ProviderCrossingAvailable: true,
		ProviderCrossing:          ProviderCrossingNotCrossedDefinite,
		RetrySafetyFact:           RetrySafetyNotDeclared,
	}
	facade.mu.Lock()
	if existing, exists := facade.invocations[request.InvocationID]; exists {
		facade.mu.Unlock()
		existingBinding, _, _, _, _, _ := existing.snapshot()
		if !binding.equal(existingBinding) {
			return logicalExecutionFailure(request.InvocationID, request.RequestID, LogicalErrorInvocationConflict, LogicalPhaseRequestValidation, "", RetrySafetyUnsafeOrUnknown, false, ErrLogicalInvocationConflict)
		}
		<-existing.done
		_, _, allocationID, _, _, stored := existing.snapshot()
		if allocationID == "" && stored.ErrorInfo != nil {
			result := existingResultForRequest(existing, request.RequestID)
			return result, logicalResultError(result)
		}
		observed, observeErr := facade.observeKnownInvocation(existing, allocationID, request.RequestID)
		return observed.LogicalExecutionResult, observeErr
	}
	record := &logicalInvocationRecord{
		binding:      binding,
		done:         make(chan struct{}),
		dispatchDone: make(chan struct{}),
		stage:        logicalStageAccepted,
		result:       accepted,
	}
	facade.invocations[request.InvocationID] = record
	facade.mu.Unlock()

	allocationResult, allocation, allocationErr := facade.requestExecutionAllocationStage(request, record)
	if allocationErr != nil || !allocationResult.Granted {
		if allocationErr == nil {
			allocationErr = errors.New("execution allocation was not granted")
		}
		result, structuredErr := logicalAllocationFailure(request, allocationResult, allocationErr)
		record.setFinished(result)
		facade.removeUnacceptedRecord(request.InvocationID, record)
		return result, structuredErr
	}

	observation, dispatchErr := facade.kernel.DispatchExecution(allocation.ID)
	providerCrossed := facade.kernel.executionProviderCrossed(allocation.ID)
	return facade.completeRequestExecution(request, record, allocation, observation, dispatchErr, providerCrossed)
}

func (facade *LogicalExecutionFacade) removeUnacceptedRecord(id InvocationID, record *logicalInvocationRecord) {
	facade.mu.Lock()
	if current, exists := facade.invocations[id]; exists && current == record {
		delete(facade.invocations, id)
	}
	facade.mu.Unlock()
}

func (facade *LogicalExecutionFacade) ObserveInvocation(invocationID InvocationID) (LogicalInvocationObservation, error) {
	if err := invocationID.Validate(); err != nil {
		result, structuredErr := logicalExecutionFailure(invocationID, "", LogicalErrorInvalidRequest, LogicalPhaseRequestValidation, "", RetrySafetyNotDeclared, false, err)
		return LogicalInvocationObservation{LogicalExecutionResult: result, State: InvocationObservationInvariantUnknown}, structuredErr
	}
	if facade == nil || facade.kernel == nil {
		result, structuredErr := logicalExecutionFailure(invocationID, "", LogicalErrorInternalFailure, LogicalPhaseObservation, "", RetrySafetyUnsafeOrUnknown, true, ErrInvalidLogicalExecutionFacade)
		return LogicalInvocationObservation{LogicalExecutionResult: result, State: InvocationObservationInvariantUnknown}, structuredErr
	}
	facade.mu.Lock()
	record, exists := facade.invocations[invocationID]
	facade.mu.Unlock()
	if !exists {
		return LogicalInvocationObservation{
			LogicalExecutionResult: LogicalExecutionResult{
				InvocationID:              invocationID,
				Status:                    LogicalExecutionUnknown,
				ProviderCrossingAvailable: false,
				ProviderCrossing:          "",
				RetrySafetyFact:           RetrySafetyUnsafeOrUnknown,
				ReconciliationRequired:    true,
				Diagnostic:                "no accepted in-process invocation record is known",
			},
			State: InvocationObservationNotKnown,
		}, nil
	}

	_, stage, allocationID, _, _, stored := record.snapshot()
	if stage == logicalStageDispatching {
		<-record.dispatchDone
	}
	return facade.observeKnownInvocation(record, allocationID, stored.RequestID)
}

func (facade *LogicalExecutionFacade) SubmitOccupancyResolution(request LogicalOccupancyResolutionRequest) (LogicalResolutionResult, error) {
	if err := request.Validate(); err != nil {
		return logicalResolutionFailure(request, LogicalErrorInvalidRequest, LogicalPhaseRequestValidation, "", RetrySafetyNotDeclared, false, err)
	}
	if facade == nil || facade.kernel == nil {
		return logicalResolutionFailure(request, LogicalErrorInternalFailure, LogicalPhaseResolution, "", RetrySafetyUnsafeOrUnknown, true, ErrInvalidLogicalExecutionFacade)
	}
	facade.mu.Lock()
	record, exists := facade.invocations[request.InvocationID]
	facade.mu.Unlock()
	if !exists {
		return logicalResolutionFailure(request, LogicalErrorResolutionNotAllowed, LogicalPhaseResolution, "", RetrySafetyUnsafeOrUnknown, true, ErrLogicalResolutionNotAllowed)
	}
	<-record.done

	record.resolutionMu.Lock()
	defer record.resolutionMu.Unlock()
	_, _, allocationID, _, _, stored := record.snapshot()
	crossing := recordCrossing(record, stored)
	if allocationID == "" {
		return logicalResolutionFailure(request, LogicalErrorResolutionNotAllowed, LogicalPhaseResolution, crossing, RetrySafetyNotDeclared, false, ErrLogicalResolutionNotAllowed)
	}
	allocation, err := facade.kernel.GetExecutionAllocation(allocationID)
	if err != nil {
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorInvariantViolation, err)
	}
	claimHeld, err := facade.kernel.Resources.ExecutionOccupancyForAllocation(allocation.Descriptor.ResourceID, allocation.ID)
	if err != nil {
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorInvariantViolation, err)
	}
	observation, observationErr := facade.kernel.GetExecutionObservation(allocation.ID)
	if observationErr != nil && !errors.Is(observationErr, ErrAllocationNotRunning) {
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorInvariantViolation, observationErr)
	}
	if allocation.Lifecycle == AllocationReleased && allocation.CurrentOccupancy == CurrentOccupancyEnded && !claimHeld {
		if observationErr == nil && observation.Occupancy == OccupancyConclusionUnknown {
			// The terminal state plus the immutable UNKNOWN observation is the
			// authoritative proof of a prior Phase 2 resolution. The registry
			// marker, when present, is correlation only and does not decide this
			// status; this also recognizes a resolution accepted outside facade.
			return facade.resolutionAlreadyResolved(record, request)
		}
		if record.resolutionEvent() != "" {
			return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorInvariantViolation, ErrExecutionOwnershipInvariant)
		}
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorAlreadyReleased, errors.New("execution allocation is already released"))
	}
	if record.resolutionEvent() != "" {
		// A local correlation marker cannot authorize or prove a transition.
		// If authoritative state is not terminal, the marker conflicts with
		// the state machine and must fail closed.
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorInvariantViolation, ErrExecutionOwnershipInvariant)
	}
	if allocation.Lifecycle != AllocationRunning || allocation.CurrentOccupancy != CurrentOccupancyUnknown || !claimHeld {
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorResolutionNotAllowed, ErrLogicalResolutionNotAllowed)
	}
	if facade.trustedResolutionSource == nil {
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorResolutionNotAllowed, ErrTrustedResolutionUnavailable)
	}

	lookup := TrustedResolutionLookup{
		InvocationID:          request.InvocationID,
		ExecutionAllocationID: allocation.ID,
		ResourceID:            allocation.Descriptor.ResourceID,
		OwnershipFence:        allocation.OwnershipFence,
		CurrentOccupancy:      allocation.CurrentOccupancy,
	}
	trustedRequest, sourceErr := facade.trustedResolutionSource.ResolveExecutionOccupancy(lookup)
	if sourceErr != nil {
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorResolutionNotAllowed, sourceErr)
	}
	if err := validateTrustedResolutionRequest(trustedRequest, lookup); err != nil {
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorResolutionNotAllowed, err)
	}

	resolution, resolveErr := facade.kernel.ResolveExecutionOccupancy(trustedRequest)
	if resolveErr != nil {
		category := LogicalErrorResolutionNotAllowed
		if errors.Is(resolveErr, ErrExecutionOwnershipInvariant) {
			category = LogicalErrorInvariantViolation
		} else if errors.Is(resolveErr, ErrResolutionAuthorityDenied) || errors.Is(resolveErr, ErrResolutionAuthorityMismatch) {
			category = LogicalErrorInvalidOrStaleAuthority
		}
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, category, resolveErr)
	}
	if resolution.Status == ResolutionStatusAlreadyResolved {
		// The underlying Phase 2 owner can win a race with another trusted
		// path. Re-read authoritative state rather than using a facade marker.
		return facade.resolutionAlreadyResolved(record, request)
	}

	result := LogicalResolutionResult{
		InvocationID:              request.InvocationID,
		RequestID:                 request.RequestID,
		ExecutionAllocationID:     allocation.ID,
		ResourceID:                allocation.Descriptor.ResourceID,
		Status:                    resolution.Status,
		ProviderCrossingAvailable: crossing != "",
		ProviderCrossing:          crossing,
		CurrentOccupancy:          CurrentOccupancyEnded,
		Lifecycle:                 AllocationReleased,
		EventID:                   resolution.EventID,
		ReconciliationRequired:    false,
		RetrySafetyFact:           RetrySafetyUnsafeOrUnknown,
		Diagnostic:                resolution.Reason,
	}
	if resolution.Status == ResolutionStatusResolved {
		record.setResolutionEvent(resolution.EventID)
	} else {
		result.ErrorInfo = newLogicalError(LogicalErrorResolutionNotAllowed, LogicalPhaseResolution, result.ProviderCrossing, result.RetrySafetyFact, LogicalExecutionResult{InvocationID: request.InvocationID, RequestID: request.RequestID, ExecutionAllocationID: allocation.ID, ProviderCrossingAvailable: result.ProviderCrossingAvailable}, false, errors.New(resolution.Reason))
		return result, result.ErrorInfo
	}

	updated := stored.Clone()
	updated.RequestID = request.RequestID
	updated.ExecutionAllocationID = allocation.ID
	updated.ProviderCrossingAvailable = result.ProviderCrossingAvailable
	updated.ProviderCrossing = crossing
	updated.CurrentOccupancy = CurrentOccupancyEnded
	updated.Lifecycle = AllocationReleased
	updated.Status = LogicalExecutionUnknown
	updated.ReconciliationRequired = true
	updated.RetrySafetyFact = RetrySafetyUnsafeOrUnknown
	updated.ErrorInfo = nil
	updated.Diagnostic = "occupancy resolved to ENDED; execution outcome remains historical UNKNOWN"
	record.setResult(updated)
	return result, nil
}

func validateTrustedResolutionRequest(request ExecutionOccupancyResolutionRequest, lookup TrustedResolutionLookup) error {
	if err := request.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrTrustedResolutionInvalid, err)
	}
	if request.AllocationID != lookup.ExecutionAllocationID || request.ResourceID != lookup.ResourceID {
		return fmt.Errorf("%w: exact allocation/resource binding does not match", ErrTrustedResolutionInvalid)
	}
	if request.Authority.Fence != lookup.OwnershipFence {
		return fmt.Errorf("%w: ownership fence does not match", ErrTrustedResolutionInvalid)
	}
	if request.Authority.ResourceID != lookup.ResourceID {
		return fmt.Errorf("%w: resolution authority ResourceID does not match", ErrTrustedResolutionInvalid)
	}
	return nil
}
