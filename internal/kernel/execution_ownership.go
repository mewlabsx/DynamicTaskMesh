package kernel

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"dtm/internal/kernel/capability"
	"dtm/internal/kernel/event"
	"dtm/internal/kernel/execution"
	"dtm/internal/kernel/identity"
	"dtm/internal/kernel/resource"
)

// ExecutionOperationConstraintKey is the only currently interpreted generic
// execution constraint. Other declaration metadata remains outside this
// bounded Kernel primitive until a later milestone defines its semantics.
const ExecutionOperationConstraintKey = "dtm.kernel.execution.operation"

type InvocationID = execution.InvocationID
type ExecutionRequest = execution.ExecutionRequest
type ExecutionDescriptor = execution.ExecutionDescriptor
type ExecutionAllocation = execution.ExecutionAllocation
type ExecutionAllocationID = execution.ExecutionAllocationID
type ExecutionObservation = execution.ExecutionObservation
type ExecutionOutcome = execution.ExecutionOutcome
type OccupancyConclusion = execution.OccupancyConclusion
type AllocationLifecycle = execution.AllocationLifecycle
type AllocationState = execution.AllocationState
type AllocationReason = execution.AllocationReason
type TryAllocateResult = execution.TryAllocateResult
type ReleaseStatus = execution.ReleaseStatus
type ReleaseResult = execution.ReleaseResult

func NewInvocationID() (InvocationID, error) {
	return execution.NewInvocationID()
}

func NewExecutionAllocationID() (ExecutionAllocationID, error) {
	return execution.NewExecutionAllocationID()
}

const (
	AllocationRunning  = execution.AllocationRunning
	AllocationReleased = execution.AllocationReleased

	ExecutionAllocationRunning  = execution.AllocationRunning
	ExecutionAllocationReleased = execution.AllocationReleased

	ExecutionOutcomeSuccess = execution.ExecutionOutcomeSuccess
	ExecutionOutcomeFailed  = execution.ExecutionOutcomeFailed
	ExecutionOutcomeUnknown = execution.ExecutionOutcomeUnknown
	OutcomeSuccess          = execution.OutcomeSuccess
	OutcomeFailed           = execution.OutcomeFailed
	OutcomeUnknown          = execution.OutcomeUnknown

	OccupancyConclusionEnded       = execution.OccupancyConclusionEnded
	OccupancyConclusionUnknown     = execution.OccupancyConclusionUnknown
	OccupancyEnded                 = execution.OccupancyEnded
	OccupancyUnknown               = execution.OccupancyUnknown
	CurrentOccupancyNotEstablished = execution.CurrentOccupancyNotEstablished
	CurrentOccupancyUnknown        = execution.CurrentOccupancyUnknown
	CurrentOccupancyEnded          = execution.CurrentOccupancyEnded
	OccupancyNotEstablished        = execution.OccupancyNotEstablished
	OccupancyUnknownCurrent        = execution.OccupancyUnknownCurrent
	OccupancyEndedCurrent          = execution.OccupancyEndedCurrent

	AllocationReasonInvalidRequest               = execution.AllocationReasonInvalidRequest
	AllocationReasonAuthorityDenied              = execution.AllocationReasonAuthorityDenied
	AllocationReasonResourceNotFound             = execution.AllocationReasonResourceNotFound
	AllocationReasonResourceUnavailable          = execution.AllocationReasonResourceUnavailable
	AllocationReasonResourceBusy                 = execution.AllocationReasonResourceBusy
	AllocationReasonExecutionConstraintViolation = execution.AllocationReasonExecutionConstraintViolation

	TryAllocateInvalidRequest               = execution.TryAllocateInvalidRequest
	TryAllocateAuthorityDenied              = execution.TryAllocateAuthorityDenied
	TryAllocateResourceNotFound             = execution.TryAllocateResourceNotFound
	TryAllocateResourceUnavailable          = execution.TryAllocateResourceUnavailable
	TryAllocateResourceBusy                 = execution.TryAllocateResourceBusy
	TryAllocateExecutionConstraintViolation = execution.TryAllocateExecutionConstraintViolation

	ReleaseStatusReleased           = execution.ReleaseStatusReleased
	ReleaseStatusAlreadyReleased    = execution.ReleaseStatusAlreadyReleased
	ReleaseStatusAllocationNotFound = execution.ReleaseStatusAllocationNotFound
	ReleaseStatusNotAllowed         = execution.ReleaseStatusNotAllowed
	ReleaseReleased                 = execution.ReleaseReleased
	ReleaseAlreadyReleased          = execution.ReleaseAlreadyReleased
	ReleaseAllocationNotFound       = execution.ReleaseAllocationNotFound
	ReleaseNotAllowed               = execution.ReleaseNotAllowed

	ResolutionStatusResolved           = execution.ResolutionStatusResolved
	ResolutionStatusAlreadyResolved    = execution.ResolutionStatusAlreadyResolved
	ResolutionStatusRejected           = execution.ResolutionStatusRejected
	ResolutionStatusAllocationNotFound = execution.ResolutionStatusAllocationNotFound
	ResolutionStatusInvariantViolation = execution.ResolutionStatusInvariantViolation
	ResolutionResolved                 = execution.ResolutionResolved
	ResolutionAlreadyResolved          = execution.ResolutionAlreadyResolved
	ResolutionRejected                 = execution.ResolutionRejected
)

var (
	ErrInvalidExecutionRequest      = execution.ErrInvalidExecutionRequest
	ErrAuthorityDenied              = execution.ErrAuthorityDenied
	ErrResourceNotFound             = execution.ErrResourceNotFound
	ErrResourceUnavailable          = execution.ErrResourceUnavailable
	ErrResourceBusy                 = execution.ErrResourceBusy
	ErrExecutionResourceUnavailable = execution.ErrResourceUnavailable
	ErrExecutionResourceBusy        = execution.ErrResourceBusy
	ErrExecutionConstraintViolation = execution.ErrExecutionConstraintViolation
	ErrInvalidExecutionAllocation   = execution.ErrInvalidExecutionAllocation
	ErrAllocationNotFound           = execution.ErrAllocationNotFound
	ErrAlreadyReleased              = execution.ErrAlreadyReleased
	ErrReleaseNotAllowed            = execution.ErrReleaseNotAllowed
	ErrAllocationNotRunning         = execution.ErrAllocationNotRunning
	ErrAlreadyDispatched            = execution.ErrAlreadyDispatched
	ErrInvalidExecutionObservation  = execution.ErrInvalidExecutionObservation
	ErrProviderUnavailable          = execution.ErrProviderUnavailable
	ErrBindingMismatch              = execution.ErrBindingMismatch
	ErrOccupancyMismatch            = execution.ErrOccupancyMismatch
	ErrInvalidExecutionResolution   = execution.ErrInvalidExecutionResolution
	ErrResolutionNotAllowed         = execution.ErrResolutionNotAllowed
	ErrResolutionBindingMismatch    = execution.ErrResolutionBindingMismatch
	ErrExecutionOwnershipInvariant  = execution.ErrExecutionOwnershipInvariant
	ErrResolutionAuthorityDenied    = resource.ErrResolutionAuthorityDenied
	ErrResolutionAuthorityMismatch  = resource.ErrResolutionAuthorityMismatch
	ErrOwnershipFenceMismatch       = resource.ErrOwnershipFenceMismatch
)

type executionAllocationRecord struct {
	allocation        execution.ExecutionAllocation
	dispatchClaimed   bool
	dispatchCompleted bool
	providerCrossed   bool
	observation       execution.ExecutionObservation
}

// executionOwnershipManager is intentionally a small coordination owner,
// not a general queue/service/store. Its mutex linearizes allocation records
// and the dispatch claim; Resource.Manager's mutex atomically owns slot
// occupancy at the Resource boundary.
type executionOwnershipManager struct {
	mu           sync.Mutex
	resources    *resource.Manager
	capabilities *capability.Manager
	executions   *execution.Manager
	events       *event.Manager
	allocations  map[execution.ExecutionAllocationID]*executionAllocationRecord
}

func newExecutionOwnershipManager(
	resources *resource.Manager,
	capabilities *capability.Manager,
	executions *execution.Manager,
	events *event.Manager,
) *executionOwnershipManager {
	return &executionOwnershipManager{
		resources:    resources,
		capabilities: capabilities,
		executions:   executions,
		events:       events,
		allocations:  make(map[execution.ExecutionAllocationID]*executionAllocationRecord),
	}
}

// NewWithProvider is a convenience constructor for a Kernel with one local
// Provider seam. New() remains source-compatible and can be followed by
// SetProvider or used by the existing manager-only tests.
func NewWithProvider(provider capability.CapabilityProvider) (*Kernel, error) {
	if isNilCapabilityProvider(provider) {
		return nil, execution.ErrProviderUnavailable
	}
	kernel := New(provider)
	return kernel, nil
}

// SetProvider configures the existing internal CapabilityProvider seam for
// DispatchExecution. It does not discover, route, retry, or remotely manage
// providers.
func (kernel *Kernel) SetProvider(provider capability.CapabilityProvider) error {
	if kernel == nil || isNilCapabilityProvider(provider) {
		return execution.ErrProviderUnavailable
	}
	kernel.providerMu.Lock()
	kernel.provider = provider
	kernel.providerMu.Unlock()
	return nil
}

func (kernel *Kernel) providerSnapshot() capability.CapabilityProvider {
	if kernel == nil {
		return nil
	}
	kernel.providerMu.RLock()
	provider := kernel.provider
	kernel.providerMu.RUnlock()
	return provider
}

// TryAllocateExecution validates K2 authority and, only after that barrier,
// atomically consumes one Resource-owned execution slot and creates a
// RUNNING ExecutionAllocation. It never calls a Provider.
func (kernel *Kernel) TryAllocateExecution(request execution.ExecutionRequest) (execution.TryAllocateResult, error) {
	if kernel == nil || kernel.ownership == nil || kernel.Capabilities == nil || kernel.Resources == nil {
		return allocationNotGranted(execution.AllocationReasonInvalidRequest, execution.ErrInvalidExecutionRequest)
	}
	request = request.Clone()
	if err := request.Validate(); err != nil {
		return allocationNotGranted(execution.AllocationReasonInvalidRequest, err)
	}
	if request.InvocationID.IsZero() {
		invocationID, err := execution.NewInvocationID()
		if err != nil {
			return allocationNotGranted(execution.AllocationReasonInvalidRequest, err)
		}
		request.InvocationID = invocationID
	}

	validated, err := kernel.Capabilities.ValidateHandleRequest(capability.HandleValidationRequest{
		ExecutionContextID: request.ExecutionContextID,
		CapabilityHandleID: request.CapabilityHandleID,
		CallerIdentity:     request.CallerIdentity,
		Scope:              request.Scope,
	})
	if err != nil {
		return allocationNotGranted(allocationReasonForAuthorityError(err), err)
	}
	if err := validateSupportedExecutionConstraint(validated.CapabilityDeclaration, request.Operation); err != nil {
		return allocationNotGranted(execution.AllocationReasonExecutionConstraintViolation, err)
	}
	allocationID, err := execution.NewExecutionAllocationID()
	if err != nil {
		return allocationNotGranted(execution.AllocationReasonInvalidRequest, err)
	}

	owner := kernel.ownership
	owner.mu.Lock()
	defer owner.mu.Unlock()

	acquired, ownershipFence, err := owner.resources.TryAcquireExecutionWithFence(validated.Resource.ID, allocationID)
	if err != nil {
		return allocationNotGranted(allocationReasonForResourceError(err), err)
	}
	if !acquired {
		return allocationNotGranted(execution.AllocationReasonResourceBusy, resource.ErrResourceBusy)
	}

	descriptor := execution.ExecutionDescriptor{
		InvocationID:            request.InvocationID,
		RootTaskRef:             request.RootTaskRef,
		ChildTaskRef:            request.ChildTaskRef,
		ExecutionContextID:      validated.ExecutionContext.ID,
		CapabilityHandleID:      validated.CapabilityHandle.ID,
		CapabilityInstanceID:    validated.CapabilityInstance.ID,
		CapabilityDeclarationID: validated.CapabilityDeclaration.ID,
		ResourceID:              validated.Resource.ID,
		CallerIdentity:          validated.ExecutionContext.Subject,
		Scope:                   validated.ExecutionContext.Scope,
		Operation:               request.Operation,
		Payload:                 append([]byte(nil), request.Payload...),
	}
	if err := descriptor.Validate(); err != nil {
		_ = owner.resources.ReleaseExecution(validated.Resource.ID, allocationID)
		return allocationNotGranted(execution.AllocationReasonInvalidRequest, err)
	}
	allocation := execution.ExecutionAllocation{
		ID:               allocationID,
		Lifecycle:        execution.AllocationRunning,
		CurrentOccupancy: execution.CurrentOccupancyNotEstablished,
		OwnershipFence:   ownershipFence,
		Descriptor:       descriptor,
	}
	if err := allocation.Validate(); err != nil {
		_ = owner.resources.ReleaseExecution(validated.Resource.ID, allocationID)
		return allocationNotGranted(execution.AllocationReasonInvalidRequest, err)
	}
	owner.allocations[allocation.ID] = &executionAllocationRecord{
		allocation: allocation.Clone(),
	}
	return execution.TryAllocateResult{Granted: true, Allocation: allocation.Clone()}, nil
}

// DispatchExecution accepts only an allocation identity. The frozen
// descriptor is the sole source of Resource, Capability, operation, and input
// for the Provider handoff.
func (kernel *Kernel) DispatchExecution(allocationID execution.ExecutionAllocationID) (execution.ExecutionObservation, error) {
	if kernel == nil || kernel.ownership == nil || kernel.Capabilities == nil || kernel.Resources == nil {
		return execution.ExecutionObservation{}, execution.ErrAllocationNotFound
	}
	if err := allocationID.Validate(); err != nil {
		return execution.ExecutionObservation{}, err
	}

	owner := kernel.ownership
	owner.mu.Lock()
	record, exists := owner.allocations[allocationID]
	if !exists {
		owner.mu.Unlock()
		return execution.ExecutionObservation{}, execution.ErrAllocationNotFound
	}
	if record.dispatchClaimed {
		if record.dispatchCompleted {
			observation := record.observation.Clone()
			owner.mu.Unlock()
			return observation, nil
		}
		owner.mu.Unlock()
		return execution.ExecutionObservation{}, execution.ErrAlreadyDispatched
	}
	if record.allocation.Lifecycle != execution.AllocationRunning {
		owner.mu.Unlock()
		return execution.ExecutionObservation{}, execution.ErrAllocationNotRunning
	}
	record.dispatchClaimed = true
	descriptor := record.allocation.Descriptor.Clone()
	owner.mu.Unlock()

	observation, dispatchErr, providerCrossed := kernel.dispatchExecutionDescriptor(allocationID, descriptor)

	owner.mu.Lock()
	var releaseErr error
	if current, stillPresent := owner.allocations[allocationID]; stillPresent {
		current.observation = observation.Clone()
		current.dispatchCompleted = true
		current.providerCrossed = providerCrossed
		if observation.Occupancy == execution.OccupancyConclusionEnded {
			_, releaseErr = owner.commitEndedAllocationLocked(current, nil)
		} else if observation.Occupancy == execution.OccupancyConclusionUnknown {
			current.allocation.CurrentOccupancy = execution.CurrentOccupancyUnknown
		}
	}
	owner.mu.Unlock()
	if releaseErr != nil {
		if dispatchErr == nil {
			dispatchErr = releaseErr
		} else {
			dispatchErr = errors.Join(dispatchErr, releaseErr)
		}
	}
	return observation, dispatchErr
}

func (kernel *Kernel) dispatchExecutionDescriptor(allocationID execution.ExecutionAllocationID, descriptor execution.ExecutionDescriptor) (execution.ExecutionObservation, error, bool) {
	preCallFailure := func(err error) (execution.ExecutionObservation, error, bool) {
		if err == nil {
			err = execution.ErrInvalidExecutionAllocation
		}
		return execution.ExecutionObservation{
			AllocationID: allocationID,
			InvocationID: descriptor.InvocationID,
			Outcome:      execution.ExecutionOutcomeFailed,
			Occupancy:    execution.OccupancyConclusionEnded,
			Reason:       err.Error(),
		}, err, false
	}

	if err := descriptor.Validate(); err != nil {
		return preCallFailure(err)
	}
	validated, err := kernel.Capabilities.ValidateHandleRequest(capability.HandleValidationRequest{
		ExecutionContextID: descriptor.ExecutionContextID,
		CapabilityHandleID: descriptor.CapabilityHandleID,
		CallerIdentity:     descriptor.CallerIdentity,
		Scope:              descriptor.Scope,
	})
	if err != nil {
		return preCallFailure(fmt.Errorf("dispatch authority validation: %w", err))
	}
	if err := validateDescriptorBinding(descriptor, validated); err != nil {
		return preCallFailure(err)
	}

	currentResource, err := kernel.Resources.GetResource(descriptor.ResourceID)
	if err != nil {
		return preCallFailure(fmt.Errorf("dispatch resource lookup: %w", err))
	}
	if !currentResource.IsAvailable() {
		return preCallFailure(execution.ErrResourceUnavailable)
	}
	claimed, err := kernel.Resources.ExecutionOccupancyForAllocation(descriptor.ResourceID, allocationID)
	if err != nil {
		return preCallFailure(fmt.Errorf("dispatch resource occupancy: %w", err))
	}
	if !claimed {
		return preCallFailure(execution.ErrOccupancyMismatch)
	}

	operation := capability.ProviderOperation{
		InvocationID:            descriptor.InvocationID,
		Operation:               descriptor.Operation,
		Payload:                 append([]byte(nil), descriptor.Payload...),
		CapabilityDeclarationID: descriptor.CapabilityDeclarationID,
		CapabilityInstanceID:    descriptor.CapabilityInstanceID,
		ResourceID:              descriptor.ResourceID,
	}
	if err := operation.Validate(); err != nil {
		return preCallFailure(err)
	}

	// Provider resolution is deliberately a dispatch-time last-mile concern.
	// Allocation freezes the execution binding, but does not freeze provider
	// routing or lifecycle. Snapshot the currently configured bounded seam only
	// after the allocation, authority, Resource, and operation checks pass and
	// immediately before the provider method boundary.
	provider := kernel.providerSnapshot()
	if isNilCapabilityProvider(provider) {
		return preCallFailure(execution.ErrProviderUnavailable)
	}

	if observable, supportsObservation := provider.(capability.ObservableCapabilityProvider); supportsObservation {
		// The observable seam is crossed at the method call. From this point,
		// a returned error or malformed observation cannot prove that the
		// provider did not start or retain a side effect.
		candidate, providerErr := observable.InvokeWithObservation(operation.Clone())
		observation, observationErr := canonicalProviderObservation(candidate, allocationID, descriptor)
		if observationErr != nil {
			// Once the optional observation-capable seam has been crossed, an
			// invalid or absent observation cannot prove that the side effect
			// stopped. Preserve uncertainty instead of manufacturing FAILED.
			observation = unknownProviderObservation(allocationID, descriptor, observationErr.Error())
			if providerErr != nil {
				return observation, providerErr, true
			}
			return observation, observationErr, true
		}
		if observation.Reason == "" && providerErr != nil {
			observation.Reason = providerErr.Error()
		}
		return observation, providerErr, true
	}

	providerErr := provider.Invoke(operation.Clone())
	if providerErr != nil {
		// The legacy error-only seam does not prove whether a side effect
		// happened or stopped. Preserve the safety invariant by retaining the
		// slot until a later reconciliation-capable boundary resolves it.
		return unknownProviderObservation(allocationID, descriptor, providerErr.Error()), providerErr, true
	}
	return execution.ExecutionObservation{
		AllocationID: allocationID,
		InvocationID: descriptor.InvocationID,
		Outcome:      execution.ExecutionOutcomeSuccess,
		Occupancy:    execution.OccupancyConclusionEnded,
		Reason:       "provider completed",
	}, nil, true
}

// executionProviderCrossed reports whether the completed dispatch actually
// crossed the Provider method boundary. It is an internal bridge for the
// legacy LocalRuntime audit adapter; it is not an execution-status API.
func (kernel *Kernel) executionProviderCrossed(allocationID execution.ExecutionAllocationID) bool {
	if kernel == nil || kernel.ownership == nil {
		return false
	}
	kernel.ownership.mu.Lock()
	record, exists := kernel.ownership.allocations[allocationID]
	crossed := exists && record.dispatchCompleted && record.providerCrossed
	kernel.ownership.mu.Unlock()
	return crossed
}

// ReleaseExecution explicitly abandons an allocation that has not crossed
// the Provider boundary, or completes a known-ended allocation. A post-
// Provider UNKNOWN allocation remains held until ResolveExecutionOccupancy.
func (kernel *Kernel) ReleaseExecution(allocationID execution.ExecutionAllocationID, releaseReason string) (execution.ReleaseResult, error) {
	result := execution.ReleaseResult{AllocationID: allocationID, Reason: releaseReason}
	if kernel == nil || kernel.ownership == nil || kernel.Resources == nil {
		result.Status = execution.ReleaseStatusAllocationNotFound
		return result, execution.ErrAllocationNotFound
	}
	if err := allocationID.Validate(); err != nil {
		result.Status = execution.ReleaseStatusNotAllowed
		return result, err
	}
	if !utf8.ValidString(releaseReason) || strings.ContainsRune(releaseReason, '\x00') {
		result.Status = execution.ReleaseStatusNotAllowed
		return result, execution.ErrReleaseNotAllowed
	}

	owner := kernel.ownership
	owner.mu.Lock()
	defer owner.mu.Unlock()
	record, exists := owner.allocations[allocationID]
	if !exists {
		result.Status = execution.ReleaseStatusAllocationNotFound
		return result, execution.ErrAllocationNotFound
	}
	if record.allocation.Lifecycle == execution.AllocationReleased {
		result.Status = execution.ReleaseStatusAlreadyReleased
		return result, nil
	}
	if record.allocation.Lifecycle != execution.AllocationRunning {
		result.Status = execution.ReleaseStatusNotAllowed
		return result, execution.ErrReleaseNotAllowed
	}
	if record.dispatchClaimed && !record.dispatchCompleted {
		result.Status = execution.ReleaseStatusNotAllowed
		return result, execution.ErrReleaseNotAllowed
	}
	if record.allocation.CurrentOccupancy == execution.CurrentOccupancyUnknown ||
		record.dispatchCompleted && record.observation.Occupancy == execution.OccupancyConclusionUnknown {
		result.Status = execution.ReleaseStatusNotAllowed
		return result, execution.ErrReleaseNotAllowed
	}
	if record.allocation.CurrentOccupancy == execution.CurrentOccupancyEnded {
		result.Status = execution.ReleaseStatusNotAllowed
		return result, fmt.Errorf("%w: RUNNING allocation cannot already be ENDED", execution.ErrExecutionOwnershipInvariant)
	}
	if _, err := owner.commitEndedAllocationLocked(record, nil); err != nil {
		result.Status = execution.ReleaseStatusNotAllowed
		return result, err
	}
	result.Status = execution.ReleaseStatusReleased
	return result, nil
}

// commitEndedAllocationLocked performs the bounded local transition while
// executionOwnershipManager's lock is held. ResourceManager holds its own
// claim lock across the callback, so an accepted transition cannot expose a
// released allocation with a held exact claim. When resolutionEvent is
// non-nil, its immutable EventRecord is published inside the same callback;
// a publication error leaves both the allocation and claim unchanged.
func (owner *executionOwnershipManager) commitEndedAllocationLocked(
	record *executionAllocationRecord,
	resolutionEvent *event.EventRecord,
) (event.EventRecord, error) {
	if owner == nil || owner.resources == nil || record == nil {
		return event.EventRecord{}, execution.ErrExecutionOwnershipInvariant
	}
	if record.allocation.Lifecycle == execution.AllocationReleased {
		if record.allocation.CurrentOccupancy != execution.CurrentOccupancyEnded {
			return event.EventRecord{}, fmt.Errorf("%w: released allocation is not ENDED", execution.ErrExecutionOwnershipInvariant)
		}
		claimed, err := owner.resources.ExecutionOccupancyForAllocation(record.allocation.Descriptor.ResourceID, record.allocation.ID)
		if err != nil {
			return event.EventRecord{}, fmt.Errorf("%w: verify released allocation claim: %v", execution.ErrExecutionOwnershipInvariant, err)
		}
		if claimed {
			return event.EventRecord{}, fmt.Errorf("%w: released allocation still owns an exact claim", execution.ErrExecutionOwnershipInvariant)
		}
		return event.EventRecord{}, nil
	}
	if record.allocation.Lifecycle != execution.AllocationRunning {
		return event.EventRecord{}, fmt.Errorf("%w: unsupported lifecycle %q", execution.ErrExecutionOwnershipInvariant, record.allocation.Lifecycle)
	}
	if record.allocation.CurrentOccupancy == execution.CurrentOccupancyEnded {
		return event.EventRecord{}, fmt.Errorf("%w: running allocation is already ENDED", execution.ErrExecutionOwnershipInvariant)
	}

	previous := record.allocation.Clone()
	var published event.EventRecord
	err := owner.resources.ReleaseExecutionWithCommit(
		record.allocation.Descriptor.ResourceID,
		record.allocation.ID,
		func() error {
			next := previous.Clone()
			next.CurrentOccupancy = execution.CurrentOccupancyEnded
			next.Lifecycle = execution.AllocationReleased
			if err := next.Validate(); err != nil {
				return fmt.Errorf("%w: validate ended allocation: %v", execution.ErrExecutionOwnershipInvariant, err)
			}
			record.allocation = next
			if resolutionEvent != nil {
				if owner.events == nil {
					return fmt.Errorf("%w: event manager is unavailable", execution.ErrExecutionOwnershipInvariant)
				}
				var err error
				published, err = owner.events.PublishAndReturn(resolutionEvent.Clone())
				if err != nil {
					return err
				}
			}
			return nil
		},
	)
	if err != nil {
		record.allocation = previous
		if errors.Is(err, resource.ErrExecutionOccupancyMismatch) || errors.Is(err, resource.ErrResourceNotFound) {
			return event.EventRecord{}, fmt.Errorf("%w: exact Resource claim is missing or cannot be released: %v", execution.ErrExecutionOwnershipInvariant, err)
		}
		return event.EventRecord{}, err
	}
	return published, nil
}

// GetExecutionResolutionAuthority returns the Resource-side manager-issued
// authority needed by the bounded same-fence resolution input. The authority
// is not derived from a CapabilityHandle or Provider.
func (kernel *Kernel) GetExecutionResolutionAuthority(resourceID ResourceID) (resource.ExecutionResolutionAuthority, error) {
	if kernel == nil || kernel.Resources == nil {
		return resource.ExecutionResolutionAuthority{}, execution.ErrResourceNotFound
	}
	return kernel.Resources.GetExecutionResolutionAuthority(resourceID)
}

// GetOccupancyResolutionAuthority is a naming-compatible alias for
// GetExecutionResolutionAuthority.
func (kernel *Kernel) GetOccupancyResolutionAuthority(resourceID ResourceID) (resource.ExecutionResolutionAuthority, error) {
	return kernel.GetExecutionResolutionAuthority(resourceID)
}

// ResolveExecutionOccupancy accepts one authoritative ENDED conclusion for
// one exact RUNNING/UNKNOWN allocation. The Resource manager-issued authority
// and its fence are checked under the ownership lock; cross-fence and
// capability/provider-derived authority are not accepted.
func (kernel *Kernel) ResolveExecutionOccupancy(request execution.ExecutionOccupancyResolutionRequest) (execution.ExecutionResolutionResult, error) {
	result := execution.ExecutionResolutionResult{
		AllocationID: request.AllocationID,
		Status:       execution.ResolutionStatusRejected,
	}
	if kernel == nil || kernel.ownership == nil || kernel.Resources == nil {
		result.Reason = execution.ErrInvalidExecutionResolution.Error()
		return result, execution.ErrInvalidExecutionResolution
	}
	request = request.Clone()
	if err := request.Validate(); err != nil {
		result.Reason = err.Error()
		return result, err
	}

	owner := kernel.ownership
	owner.mu.Lock()
	defer owner.mu.Unlock()
	record, exists := owner.allocations[request.AllocationID]
	if !exists {
		result.Status = execution.ResolutionStatusAllocationNotFound
		result.Reason = execution.ErrAllocationNotFound.Error()
		return result, execution.ErrAllocationNotFound
	}
	if record.allocation.Descriptor.ResourceID != request.ResourceID ||
		record.allocation.ID != request.AllocationID {
		result.Reason = execution.ErrResolutionBindingMismatch.Error()
		return result, execution.ErrResolutionBindingMismatch
	}
	if record.allocation.OwnershipFence != request.Authority.Fence {
		result.Reason = resource.ErrOwnershipFenceMismatch.Error()
		return result, resource.ErrOwnershipFenceMismatch
	}
	if err := owner.resources.ValidateExecutionResolutionAuthority(request.ResourceID, request.Authority); err != nil {
		result.Reason = err.Error()
		return result, err
	}

	if record.allocation.Lifecycle == execution.AllocationReleased {
		if record.allocation.CurrentOccupancy != execution.CurrentOccupancyEnded {
			result.Status = execution.ResolutionStatusInvariantViolation
			result.Reason = execution.ErrExecutionOwnershipInvariant.Error()
			return result, fmt.Errorf("%w: released allocation is not ENDED", execution.ErrExecutionOwnershipInvariant)
		}
		claimed, err := owner.resources.ExecutionOccupancyForAllocation(request.ResourceID, request.AllocationID)
		if err != nil {
			result.Status = execution.ResolutionStatusInvariantViolation
			result.Reason = err.Error()
			return result, fmt.Errorf("%w: verify already-resolved claim: %v", execution.ErrExecutionOwnershipInvariant, err)
		}
		if claimed {
			result.Status = execution.ResolutionStatusInvariantViolation
			result.Reason = execution.ErrExecutionOwnershipInvariant.Error()
			return result, fmt.Errorf("%w: already-released allocation still owns an exact claim", execution.ErrExecutionOwnershipInvariant)
		}
		if !record.dispatchCompleted || record.observation.Occupancy != execution.OccupancyConclusionUnknown {
			result.Status = execution.ResolutionStatusRejected
			result.Reason = execution.ErrResolutionNotAllowed.Error()
			return result, fmt.Errorf("%w: terminal allocation has no accepted UNKNOWN resolution history", execution.ErrResolutionNotAllowed)
		}
		result.Status = execution.ResolutionStatusAlreadyResolved
		result.Reason = "equivalent occupancy resolution already accepted"
		return result, nil
	}
	if record.allocation.Lifecycle != execution.AllocationRunning {
		result.Reason = execution.ErrResolutionNotAllowed.Error()
		return result, fmt.Errorf("%w: allocation lifecycle is %s", execution.ErrResolutionNotAllowed, record.allocation.Lifecycle)
	}
	if record.allocation.CurrentOccupancy == execution.CurrentOccupancyNotEstablished {
		result.Reason = execution.ErrResolutionNotAllowed.Error()
		return result, fmt.Errorf("%w: allocation has not crossed the Provider boundary", execution.ErrResolutionNotAllowed)
	}
	if record.allocation.CurrentOccupancy != execution.CurrentOccupancyUnknown {
		result.Reason = execution.ErrResolutionNotAllowed.Error()
		return result, fmt.Errorf("%w: current occupancy is %s", execution.ErrResolutionNotAllowed, record.allocation.CurrentOccupancy)
	}
	claimed, err := owner.resources.ExecutionOccupancyForAllocation(request.ResourceID, request.AllocationID)
	if err != nil {
		result.Status = execution.ResolutionStatusInvariantViolation
		result.Reason = err.Error()
		return result, fmt.Errorf("%w: verify exact Resource claim: %v", execution.ErrExecutionOwnershipInvariant, err)
	}
	if !claimed {
		result.Status = execution.ResolutionStatusInvariantViolation
		result.Reason = execution.ErrExecutionOwnershipInvariant.Error()
		return result, fmt.Errorf("%w: RUNNING/UNKNOWN allocation has no exact Resource claim", execution.ErrExecutionOwnershipInvariant)
	}

	resolutionEvent, err := newExecutionOccupancyResolutionEvent(request, record.allocation)
	if err != nil {
		result.Reason = err.Error()
		return result, err
	}
	published, err := owner.commitEndedAllocationLocked(record, &resolutionEvent)
	if err != nil {
		if errors.Is(err, execution.ErrExecutionOwnershipInvariant) {
			result.Status = execution.ResolutionStatusInvariantViolation
		}
		result.Reason = err.Error()
		return result, err
	}
	result.Status = execution.ResolutionStatusResolved
	result.EventID = published.ID
	result.Reason = "authoritative occupancy resolved to ENDED"
	return result, nil
}

func newExecutionOccupancyResolutionEvent(
	request execution.ExecutionOccupancyResolutionRequest,
	allocation execution.ExecutionAllocation,
) (event.EventRecord, error) {
	reason := strings.TrimSpace(request.Evidence.Reason)
	if reason == "" {
		reason = "authoritative Resource-side occupancy evidence"
	}
	resourceReference := identity.ObjectReference{
		Kind: identity.ObjectKindResource,
		ID:   request.ResourceID.String(),
	}
	cause := fmt.Sprintf(
		"authoritative execution occupancy resolution: allocation=%s resource=%s fence=%s evidence=%s",
		allocation.ID.String(), request.ResourceID.String(), allocation.OwnershipFence.String(), reason,
	)
	result := fmt.Sprintf(
		"ENDED; allocation=%s; resource=%s; fence=%s",
		allocation.ID.String(), request.ResourceID.String(), allocation.OwnershipFence.String(),
	)
	return event.NewEventRecord(
		resourceReference,
		event.ExecutionOccupancyResolved,
		resourceReference,
		cause,
		result,
	)
}

func (kernel *Kernel) GetExecutionAllocation(allocationID execution.ExecutionAllocationID) (execution.ExecutionAllocation, error) {
	if kernel == nil || kernel.ownership == nil {
		return execution.ExecutionAllocation{}, execution.ErrAllocationNotFound
	}
	if err := allocationID.Validate(); err != nil {
		return execution.ExecutionAllocation{}, err
	}
	kernel.ownership.mu.Lock()
	record, exists := kernel.ownership.allocations[allocationID]
	if !exists {
		kernel.ownership.mu.Unlock()
		return execution.ExecutionAllocation{}, execution.ErrAllocationNotFound
	}
	allocation := record.allocation.Clone()
	kernel.ownership.mu.Unlock()
	return allocation, nil
}

// GetExecutionObservation returns the immutable observation captured by a
// completed dispatch. An in-flight allocation has no observation yet.
func (kernel *Kernel) GetExecutionObservation(allocationID execution.ExecutionAllocationID) (execution.ExecutionObservation, error) {
	if kernel == nil || kernel.ownership == nil {
		return execution.ExecutionObservation{}, execution.ErrAllocationNotFound
	}
	if err := allocationID.Validate(); err != nil {
		return execution.ExecutionObservation{}, err
	}
	kernel.ownership.mu.Lock()
	record, exists := kernel.ownership.allocations[allocationID]
	if !exists {
		kernel.ownership.mu.Unlock()
		return execution.ExecutionObservation{}, execution.ErrAllocationNotFound
	}
	if !record.dispatchCompleted {
		kernel.ownership.mu.Unlock()
		return execution.ExecutionObservation{}, execution.ErrAllocationNotRunning
	}
	observation := record.observation.Clone()
	kernel.ownership.mu.Unlock()
	return observation, nil
}

func (kernel *Kernel) ListExecutionAllocations() []execution.ExecutionAllocation {
	if kernel == nil || kernel.ownership == nil {
		return nil
	}
	kernel.ownership.mu.Lock()
	allocations := make([]execution.ExecutionAllocation, 0, len(kernel.ownership.allocations))
	for _, record := range kernel.ownership.allocations {
		allocations = append(allocations, record.allocation.Clone())
	}
	kernel.ownership.mu.Unlock()
	sort.Slice(allocations, func(left, right int) bool { return allocations[left].ID < allocations[right].ID })
	return allocations
}

func allocationNotGranted(reason execution.AllocationReason, cause error) (execution.TryAllocateResult, error) {
	result := execution.TryAllocateResult{Granted: false, Reason: reason}
	sentinel := allocationReasonError(reason)
	if cause == nil {
		cause = sentinel
	} else if sentinel != nil {
		cause = errors.Join(sentinel, cause)
	}
	return result, &execution.NotGrantedError{Reason: reason, Cause: cause}
}

func allocationReasonError(reason execution.AllocationReason) error {
	switch reason {
	case execution.AllocationReasonInvalidRequest:
		return execution.ErrInvalidExecutionRequest
	case execution.AllocationReasonAuthorityDenied:
		return execution.ErrAuthorityDenied
	case execution.AllocationReasonResourceNotFound:
		return execution.ErrResourceNotFound
	case execution.AllocationReasonResourceUnavailable:
		return execution.ErrResourceUnavailable
	case execution.AllocationReasonResourceBusy:
		return execution.ErrResourceBusy
	case execution.AllocationReasonExecutionConstraintViolation:
		return execution.ErrExecutionConstraintViolation
	default:
		return nil
	}
}

func allocationReasonForAuthorityError(err error) execution.AllocationReason {
	if errors.Is(err, resource.ErrResourceNotFound) {
		return execution.AllocationReasonResourceNotFound
	}
	if errors.Is(err, resource.ErrResourceRemoved) || errors.Is(err, resource.ErrResourceUnavailable) ||
		errors.Is(err, capability.ErrResourceUnavailable) {
		return execution.AllocationReasonResourceUnavailable
	}
	return execution.AllocationReasonAuthorityDenied
}

func allocationReasonForResourceError(err error) execution.AllocationReason {
	switch {
	case errors.Is(err, resource.ErrResourceNotFound):
		return execution.AllocationReasonResourceNotFound
	case errors.Is(err, resource.ErrResourceUnavailable):
		return execution.AllocationReasonResourceUnavailable
	case errors.Is(err, resource.ErrResourceBusy):
		return execution.AllocationReasonResourceBusy
	default:
		return execution.AllocationReasonInvalidRequest
	}
}

func validateSupportedExecutionConstraint(declaration capability.CapabilityDeclaration, operation string) error {
	if expected, exists := declaration.Constraints[ExecutionOperationConstraintKey]; exists && expected != "" && expected != operation {
		return fmt.Errorf("%w: declaration requires operation %q, request selected %q", execution.ErrExecutionConstraintViolation, expected, operation)
	}
	return nil
}

func validateDescriptorBinding(descriptor execution.ExecutionDescriptor, validated capability.ValidatedHandle) error {
	if descriptor.ExecutionContextID != validated.ExecutionContext.ID ||
		descriptor.CapabilityHandleID != validated.CapabilityHandle.ID ||
		descriptor.CapabilityInstanceID != validated.CapabilityInstance.ID ||
		descriptor.CapabilityDeclarationID != validated.CapabilityDeclaration.ID ||
		descriptor.ResourceID != validated.Resource.ID ||
		descriptor.CallerIdentity != validated.ExecutionContext.Subject ||
		descriptor.Scope != validated.ExecutionContext.Scope {
		return execution.ErrBindingMismatch
	}
	if err := validateSupportedExecutionConstraint(validated.CapabilityDeclaration, descriptor.Operation); err != nil {
		return err
	}
	return nil
}

func canonicalProviderObservation(candidate execution.ExecutionObservation, allocationID execution.ExecutionAllocationID, descriptor execution.ExecutionDescriptor) (execution.ExecutionObservation, error) {
	if candidate.AllocationID == "" {
		candidate.AllocationID = allocationID
	} else if candidate.AllocationID != allocationID {
		return execution.ExecutionObservation{}, execution.ErrBindingMismatch
	}
	if candidate.InvocationID.IsZero() {
		candidate.InvocationID = descriptor.InvocationID
	} else if candidate.InvocationID != descriptor.InvocationID {
		return execution.ExecutionObservation{}, execution.ErrBindingMismatch
	}
	if err := candidate.Validate(); err != nil {
		return execution.ExecutionObservation{}, err
	}
	return candidate.Clone(), nil
}

func unknownProviderObservation(allocationID execution.ExecutionAllocationID, descriptor execution.ExecutionDescriptor, reason string) execution.ExecutionObservation {
	return execution.ExecutionObservation{
		AllocationID: allocationID,
		InvocationID: descriptor.InvocationID,
		Outcome:      execution.ExecutionOutcomeUnknown,
		Occupancy:    execution.OccupancyConclusionUnknown,
		Reason:       reason,
	}
}
