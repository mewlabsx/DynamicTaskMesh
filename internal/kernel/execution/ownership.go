package execution

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	legacyinvocation "dtm/internal/invocation"
	"dtm/internal/kernel/identity"
	"dtm/internal/kernel/resource"
)

type ResourceID = identity.ResourceID
type CapabilityDeclarationID = identity.CapabilityDeclarationID
type CapabilityInstanceID = identity.CapabilityInstanceID
type CapabilityHandleID = identity.CapabilityHandleID
type ExecutionAllocationID = identity.ExecutionAllocationID
type EventID = identity.EventID
type OwnershipFence = resource.OwnershipFence
type ExecutionResolutionAuthority = resource.ExecutionResolutionAuthority

// InvocationID is the existing invocation correlation identity. Gate B does
// not introduce a second ExecutionRequestID with equivalent semantics.
type InvocationID = legacyinvocation.InvocationID

func NewInvocationID() (InvocationID, error) {
	return legacyinvocation.NewInvocationID()
}

func NewExecutionAllocationID() (ExecutionAllocationID, error) {
	return identity.NewExecutionAllocationID()
}

var (
	ErrInvalidExecutionRequest      = errors.New("invalid execution request")
	ErrAuthorityDenied              = errors.New("execution authority denied")
	ErrResourceNotFound             = errors.New("execution resource not found")
	ErrResourceUnavailable          = errors.New("execution resource is unavailable")
	ErrResourceBusy                 = errors.New("execution resource is busy")
	ErrExecutionConstraintViolation = errors.New("execution constraint violation")
	ErrInvalidExecutionAllocation   = errors.New("invalid execution allocation")
	ErrAllocationNotFound           = errors.New("execution allocation not found")
	ErrAlreadyReleased              = errors.New("execution allocation is already released")
	ErrReleaseNotAllowed            = errors.New("execution allocation release is not allowed")
	ErrAllocationNotRunning         = errors.New("execution allocation is not running")
	ErrAlreadyDispatched            = errors.New("execution allocation has already been dispatched")
	ErrInvalidExecutionObservation  = errors.New("invalid execution observation")
	ErrProviderUnavailable          = errors.New("execution provider is unavailable")
	ErrBindingMismatch              = errors.New("execution allocation binding mismatch")
	ErrOccupancyMismatch            = errors.New("execution occupancy does not match allocation")
	ErrInvalidExecutionResolution   = errors.New("invalid execution occupancy resolution request")
	ErrResolutionNotAllowed         = errors.New("execution occupancy resolution is not allowed")
	ErrResolutionBindingMismatch    = errors.New("execution occupancy resolution binding mismatch")
	ErrExecutionOwnershipInvariant  = errors.New("execution ownership invariant violation")
)

// ExecutionRequest is one concrete Capability invocation request. Task
// lineage fields are opaque correlation values; this package does not create,
// inspect, schedule, or otherwise own RootTask or ChildTask objects.
type ExecutionRequest struct {
	InvocationID       InvocationID
	RootTaskRef        string
	ChildTaskRef       string
	ExecutionContextID ExecutionContextID
	CapabilityHandleID CapabilityHandleID
	CallerIdentity     string
	Scope              string
	Operation          string
	Payload            []byte
}

func (request ExecutionRequest) Validate() error {
	if err := request.ExecutionContextID.Validate(); err != nil {
		return fmt.Errorf("%w: execution context: %v", ErrInvalidExecutionRequest, err)
	}
	if err := request.CapabilityHandleID.Validate(); err != nil {
		return fmt.Errorf("%w: capability handle: %v", ErrInvalidExecutionRequest, err)
	}
	if err := validateOpaqueReference(request.RootTaskRef, "root task reference"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidExecutionRequest, err)
	}
	if err := validateOpaqueReference(request.ChildTaskRef, "child task reference"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidExecutionRequest, err)
	}
	if strings.ContainsRune(request.CallerIdentity, '\x00') || !utf8.ValidString(request.CallerIdentity) {
		return fmt.Errorf("%w: invalid caller identity", ErrInvalidExecutionRequest)
	}
	if request.Scope != "" && (strings.TrimSpace(request.Scope) == "" || strings.ContainsRune(request.Scope, '\x00') || !utf8.ValidString(request.Scope)) {
		return fmt.Errorf("%w: invalid scope", ErrInvalidExecutionRequest)
	}
	if err := validateOperation(request.Operation); err != nil {
		return fmt.Errorf("%w: operation: %v", ErrInvalidExecutionRequest, err)
	}
	if !request.InvocationID.IsZero() {
		if err := request.InvocationID.Validate(); err != nil {
			return fmt.Errorf("%w: invocation identity: %v", ErrInvalidExecutionRequest, err)
		}
	}
	return nil
}

func (request ExecutionRequest) Clone() ExecutionRequest {
	request.Payload = append([]byte(nil), request.Payload...)
	return request
}

// ExecutionDescriptor is the canonical, immutable-by-convention binding
// captured at allocation time. It is a value embedded in an allocation, not
// a separately addressable Kernel object or descriptor store.
type ExecutionDescriptor struct {
	InvocationID            InvocationID
	RootTaskRef             string
	ChildTaskRef            string
	ExecutionContextID      ExecutionContextID
	CapabilityHandleID      CapabilityHandleID
	CapabilityInstanceID    CapabilityInstanceID
	CapabilityDeclarationID CapabilityDeclarationID
	ResourceID              ResourceID
	CallerIdentity          string
	Scope                   string
	Operation               string
	Payload                 []byte
}

func (descriptor ExecutionDescriptor) Validate() error {
	if err := descriptor.InvocationID.Validate(); err != nil {
		return fmt.Errorf("%w: invocation identity: %v", ErrInvalidExecutionAllocation, err)
	}
	if err := descriptor.ExecutionContextID.Validate(); err != nil {
		return fmt.Errorf("%w: execution context: %v", ErrInvalidExecutionAllocation, err)
	}
	if err := descriptor.CapabilityHandleID.Validate(); err != nil {
		return fmt.Errorf("%w: capability handle: %v", ErrInvalidExecutionAllocation, err)
	}
	if err := descriptor.CapabilityInstanceID.Validate(); err != nil {
		return fmt.Errorf("%w: capability instance: %v", ErrInvalidExecutionAllocation, err)
	}
	if err := descriptor.CapabilityDeclarationID.Validate(); err != nil {
		return fmt.Errorf("%w: capability declaration: %v", ErrInvalidExecutionAllocation, err)
	}
	if err := descriptor.ResourceID.Validate(); err != nil {
		return fmt.Errorf("%w: resource: %v", ErrInvalidExecutionAllocation, err)
	}
	if err := validateOpaqueReference(descriptor.RootTaskRef, "root task reference"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidExecutionAllocation, err)
	}
	if err := validateOpaqueReference(descriptor.ChildTaskRef, "child task reference"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidExecutionAllocation, err)
	}
	if strings.TrimSpace(descriptor.CallerIdentity) == "" || strings.ContainsRune(descriptor.CallerIdentity, '\x00') || !utf8.ValidString(descriptor.CallerIdentity) {
		return fmt.Errorf("%w: caller identity is required", ErrInvalidExecutionAllocation)
	}
	if strings.TrimSpace(descriptor.Scope) == "" || strings.ContainsRune(descriptor.Scope, '\x00') || !utf8.ValidString(descriptor.Scope) {
		return fmt.Errorf("%w: scope is required", ErrInvalidExecutionAllocation)
	}
	if err := validateOperation(descriptor.Operation); err != nil {
		return fmt.Errorf("%w: operation: %v", ErrInvalidExecutionAllocation, err)
	}
	return nil
}

func (descriptor ExecutionDescriptor) Clone() ExecutionDescriptor {
	descriptor.Payload = append([]byte(nil), descriptor.Payload...)
	return descriptor
}

type AllocationLifecycle string

const (
	AllocationRunning  AllocationLifecycle = "RUNNING"
	AllocationReleased AllocationLifecycle = "RELEASED"

	ExecutionAllocationRunning  = AllocationRunning
	ExecutionAllocationReleased = AllocationReleased
)

type AllocationState = AllocationLifecycle

// CurrentOccupancy is Kernel's authoritative current occupancy state for one
// exact ExecutionAllocation. It is deliberately distinct from the
// historical ExecutionObservation.Occupancy field.
type CurrentOccupancy string

const (
	CurrentOccupancyNotEstablished CurrentOccupancy = "NOT_ESTABLISHED"
	CurrentOccupancyUnknown        CurrentOccupancy = "UNKNOWN"
	CurrentOccupancyEnded          CurrentOccupancy = "ENDED"

	OccupancyNotEstablished = CurrentOccupancyNotEstablished
	OccupancyUnknownCurrent = CurrentOccupancyUnknown
	OccupancyEndedCurrent   = CurrentOccupancyEnded
)

func (occupancy CurrentOccupancy) Validate() error {
	switch occupancy {
	case CurrentOccupancyNotEstablished, CurrentOccupancyUnknown, CurrentOccupancyEnded:
		return nil
	default:
		return fmt.Errorf("%w: unsupported current occupancy %q", ErrInvalidExecutionAllocation, occupancy)
	}
}

// ExecutionAllocation owns one Resource execution-capacity claim and its
// frozen binding. Lifecycle is intentionally only RUNNING or RELEASED;
// dispatch claim/completion is private coordination state, not a lifecycle.
type ExecutionAllocation struct {
	ID               ExecutionAllocationID
	Lifecycle        AllocationLifecycle
	CurrentOccupancy CurrentOccupancy
	OwnershipFence   OwnershipFence
	Descriptor       ExecutionDescriptor
}

func (allocation ExecutionAllocation) Validate() error {
	if err := allocation.ID.Validate(); err != nil {
		return fmt.Errorf("%w: identity: %v", ErrInvalidExecutionAllocation, err)
	}
	switch allocation.Lifecycle {
	case AllocationRunning, AllocationReleased:
	default:
		return fmt.Errorf("%w: unsupported lifecycle %q", ErrInvalidExecutionAllocation, allocation.Lifecycle)
	}
	if err := allocation.Descriptor.Validate(); err != nil {
		return err
	}
	if err := allocation.CurrentOccupancy.Validate(); err != nil {
		return err
	}
	if err := allocation.OwnershipFence.Validate(); err != nil {
		return fmt.Errorf("%w: ownership fence: %v", ErrInvalidExecutionAllocation, err)
	}
	switch allocation.CurrentOccupancy {
	case CurrentOccupancyNotEstablished, CurrentOccupancyUnknown:
		if allocation.Lifecycle != AllocationRunning {
			return fmt.Errorf("%w: %s occupancy requires RUNNING lifecycle", ErrInvalidExecutionAllocation, allocation.CurrentOccupancy)
		}
	case CurrentOccupancyEnded:
		if allocation.Lifecycle != AllocationReleased {
			return fmt.Errorf("%w: ENDED occupancy requires RELEASED lifecycle", ErrInvalidExecutionAllocation)
		}
	}
	return nil
}

func (allocation ExecutionAllocation) Clone() ExecutionAllocation {
	allocation.Descriptor = allocation.Descriptor.Clone()
	return allocation
}

type ExecutionOutcome string

const (
	ExecutionOutcomeSuccess ExecutionOutcome = "SUCCESS"
	ExecutionOutcomeFailed  ExecutionOutcome = "FAILED"
	ExecutionOutcomeUnknown ExecutionOutcome = "UNKNOWN"

	OutcomeSuccess = ExecutionOutcomeSuccess
	OutcomeFailed  = ExecutionOutcomeFailed
	OutcomeUnknown = ExecutionOutcomeUnknown
)

type OccupancyConclusion string

const (
	OccupancyConclusionEnded   OccupancyConclusion = "ENDED"
	OccupancyConclusionUnknown OccupancyConclusion = "UNKNOWN"

	OccupancyEnded   = OccupancyConclusionEnded
	OccupancyUnknown = OccupancyConclusionUnknown
)

// ExecutionObservation keeps outcome certainty separate from physical
// occupancy certainty. In particular, UNKNOWN is a first-class result and is
// never normalized to FAILED by this package.
type ExecutionObservation struct {
	AllocationID ExecutionAllocationID
	InvocationID InvocationID
	Outcome      ExecutionOutcome
	Occupancy    OccupancyConclusion
	Reason       string
}

func (observation ExecutionObservation) Validate() error {
	if err := observation.AllocationID.Validate(); err != nil {
		return fmt.Errorf("%w: allocation identity: %v", ErrInvalidExecutionObservation, err)
	}
	if err := observation.InvocationID.Validate(); err != nil {
		return fmt.Errorf("%w: invocation identity: %v", ErrInvalidExecutionObservation, err)
	}
	switch observation.Outcome {
	case ExecutionOutcomeSuccess, ExecutionOutcomeFailed, ExecutionOutcomeUnknown:
	default:
		return fmt.Errorf("%w: unsupported outcome %q", ErrInvalidExecutionObservation, observation.Outcome)
	}
	switch observation.Occupancy {
	case OccupancyConclusionEnded, OccupancyConclusionUnknown:
	default:
		return fmt.Errorf("%w: unsupported occupancy %q", ErrInvalidExecutionObservation, observation.Occupancy)
	}
	if strings.ContainsRune(observation.Reason, '\x00') || !utf8.ValidString(observation.Reason) {
		return fmt.Errorf("%w: invalid reason", ErrInvalidExecutionObservation)
	}
	return nil
}

func (observation ExecutionObservation) Clone() ExecutionObservation {
	return observation
}

// ResolutionEvidence is validated transition input. It has no Kernel Object
// identity, lifecycle, manager, store, or authority independent of the
// Resource-issued ExecutionResolutionAuthority carried by a request.
type ResolutionEvidence struct {
	Occupancy OccupancyConclusion
	Reason    string
}

func (evidence ResolutionEvidence) Validate() error {
	if evidence.Occupancy != OccupancyConclusionEnded {
		return fmt.Errorf("%w: authoritative occupancy conclusion must be ENDED", ErrInvalidExecutionResolution)
	}
	if strings.ContainsRune(evidence.Reason, '\x00') || !utf8.ValidString(evidence.Reason) {
		return fmt.Errorf("%w: invalid evidence reason", ErrInvalidExecutionResolution)
	}
	return nil
}

func (evidence ResolutionEvidence) Clone() ResolutionEvidence {
	return evidence
}

// ExecutionOccupancyResolutionRequest binds one authoritative ENDED
// conclusion to one exact allocation, Resource, manager-issued authority,
// and its same-fence evidence. It is a transition input rather than a Kernel
// Object.
type ExecutionOccupancyResolutionRequest struct {
	AllocationID ExecutionAllocationID
	ResourceID   ResourceID
	Authority    ExecutionResolutionAuthority
	Evidence     ResolutionEvidence
}

// ExecutionResolutionRequest is the concise compatibility name for the
// bounded occupancy-resolution transition input.
type ExecutionResolutionRequest = ExecutionOccupancyResolutionRequest

func (request ExecutionOccupancyResolutionRequest) Validate() error {
	if err := request.AllocationID.Validate(); err != nil {
		return fmt.Errorf("%w: allocation identity: %v", ErrInvalidExecutionResolution, err)
	}
	if err := request.ResourceID.Validate(); err != nil {
		return fmt.Errorf("%w: resource identity: %v", ErrInvalidExecutionResolution, err)
	}
	if err := request.Authority.Validate(); err != nil {
		return fmt.Errorf("%w: authority: %w", ErrInvalidExecutionResolution, err)
	}
	if request.Authority.ResourceID != request.ResourceID {
		return fmt.Errorf("%w: authority ResourceID does not match request", ErrInvalidExecutionResolution)
	}
	if err := request.Evidence.Validate(); err != nil {
		return err
	}
	return nil
}

func (request ExecutionOccupancyResolutionRequest) Clone() ExecutionOccupancyResolutionRequest {
	request.Authority = request.Authority.Clone()
	request.Evidence = request.Evidence.Clone()
	return request
}

type ResolutionStatus string

const (
	ResolutionStatusResolved           ResolutionStatus = "RESOLVED"
	ResolutionStatusAlreadyResolved    ResolutionStatus = "ALREADY_RESOLVED"
	ResolutionStatusRejected           ResolutionStatus = "REJECTED"
	ResolutionStatusAllocationNotFound ResolutionStatus = "ALLOCATION_NOT_FOUND"
	ResolutionStatusInvariantViolation ResolutionStatus = "INVARIANT_VIOLATION"

	ResolutionResolved        = ResolutionStatusResolved
	ResolutionAlreadyResolved = ResolutionStatusAlreadyResolved
	ResolutionRejected        = ResolutionStatusRejected
)

// ExecutionResolutionResult reports whether the exact bounded transition
// was accepted or was an idempotent repeat. Rejected results carry an error
// and never represent a successful state change.
type ExecutionResolutionResult struct {
	AllocationID ExecutionAllocationID
	Status       ResolutionStatus
	EventID      EventID
	Reason       string
}

type ExecutionOccupancyResolutionResult = ExecutionResolutionResult

func (result ExecutionResolutionResult) Clone() ExecutionResolutionResult {
	return result
}

type AllocationReason string

const (
	AllocationReasonInvalidRequest               AllocationReason = "INVALID_REQUEST"
	AllocationReasonAuthorityDenied              AllocationReason = "AUTHORITY_DENIED"
	AllocationReasonResourceNotFound             AllocationReason = "RESOURCE_NOT_FOUND"
	AllocationReasonResourceUnavailable          AllocationReason = "RESOURCE_UNAVAILABLE"
	AllocationReasonResourceBusy                 AllocationReason = "RESOURCE_BUSY"
	AllocationReasonExecutionConstraintViolation AllocationReason = "EXECUTION_CONSTRAINT_VIOLATION"

	TryAllocateInvalidRequest               = AllocationReasonInvalidRequest
	TryAllocateAuthorityDenied              = AllocationReasonAuthorityDenied
	TryAllocateResourceNotFound             = AllocationReasonResourceNotFound
	TryAllocateResourceUnavailable          = AllocationReasonResourceUnavailable
	TryAllocateResourceBusy                 = AllocationReasonResourceBusy
	TryAllocateExecutionConstraintViolation = AllocationReasonExecutionConstraintViolation
)

// TryAllocateResult makes expected non-grant outcomes explicit. The error
// return carries a typed diagnostic for callers that need errors.Is; a
// RESOURCE_BUSY result is still an allocation decision, never an execution
// failure or Provider result.
type TryAllocateResult struct {
	Granted    bool
	Allocation ExecutionAllocation
	Reason     AllocationReason
}

func (result TryAllocateResult) Clone() TryAllocateResult {
	result.Allocation = result.Allocation.Clone()
	return result
}

func (result TryAllocateResult) IsGranted() bool {
	return result.Granted && result.Allocation.Lifecycle == AllocationRunning
}

type NotGrantedError struct {
	Reason AllocationReason
	Cause  error
}

func (err *NotGrantedError) Error() string {
	if err == nil {
		return "execution allocation not granted"
	}
	if err.Cause == nil {
		return string(err.Reason)
	}
	return fmt.Sprintf("%s: %v", err.Reason, err.Cause)
}

func (err *NotGrantedError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

type ReleaseStatus string

const (
	ReleaseStatusReleased           ReleaseStatus = "RELEASED"
	ReleaseStatusAlreadyReleased    ReleaseStatus = "ALREADY_RELEASED"
	ReleaseStatusAllocationNotFound ReleaseStatus = "ALLOCATION_NOT_FOUND"
	ReleaseStatusNotAllowed         ReleaseStatus = "RELEASE_NOT_ALLOWED"

	ReleaseReleased           = ReleaseStatusReleased
	ReleaseAlreadyReleased    = ReleaseStatusAlreadyReleased
	ReleaseAllocationNotFound = ReleaseStatusAllocationNotFound
	ReleaseNotAllowed         = ReleaseStatusNotAllowed
)

type ReleaseResult struct {
	AllocationID ExecutionAllocationID
	Status       ReleaseStatus
	Reason       string
}

func (result ReleaseResult) Clone() ReleaseResult {
	return result
}

func validateOperation(operation string) error {
	if strings.TrimSpace(operation) == "" || strings.ContainsRune(operation, '\x00') || !utf8.ValidString(operation) {
		return errors.New("operation is required")
	}
	return nil
}

func validateOpaqueReference(value, name string) error {
	if value == "" {
		return nil
	}
	if strings.TrimSpace(value) == "" || strings.ContainsRune(value, '\x00') || !utf8.ValidString(value) {
		return fmt.Errorf("%s is invalid", name)
	}
	return nil
}
