package resource

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"dtm/internal/kernel/identity"
)

type ResourceID = identity.ResourceID
type CapabilityDeclarationID = identity.CapabilityDeclarationID
type ExecutionAllocationID = identity.ExecutionAllocationID

type LifecycleState = identity.LifecycleState
type State = LifecycleState
type AvailabilityState = identity.AvailabilityState
type EvaluationState = identity.EvaluationState

const (
	StateCreated   LifecycleState = identity.LifecycleCreated
	StateActive    LifecycleState = identity.LifecycleActive
	StateSuspended LifecycleState = identity.LifecycleSuspended
	StateRemoved   LifecycleState = identity.LifecycleRemoved

	AvailabilityAvailable   AvailabilityState = identity.AvailabilityAvailable
	AvailabilityDegraded    AvailabilityState = identity.AvailabilityDegraded
	AvailabilityUnavailable AvailabilityState = identity.AvailabilityUnavailable

	ResourceStateCreated   = StateCreated
	ResourceStateActive    = StateActive
	ResourceStateSuspended = StateSuspended
	ResourceStateRemoved   = StateRemoved
)

var (
	ErrInvalidResource              = errors.New("invalid kernel resource")
	ErrInvalidState                 = errors.New("invalid resource state")
	ErrInvalidTransition            = errors.New("invalid resource state transition")
	ErrResourceNotFound             = errors.New("resource not found")
	ErrResourceExists               = errors.New("resource already exists")
	ErrResourceRemoved              = errors.New("resource has been removed")
	ErrResourceUnavailable          = errors.New("resource is unavailable")
	ErrResourceBusy                 = errors.New("resource execution capacity is busy")
	ErrExecutionOccupancyMismatch   = errors.New("resource execution occupancy mismatch")
	ErrInvalidOwnershipFence        = errors.New("invalid resource execution-ownership fence")
	ErrInvalidResolutionAuthority   = errors.New("invalid execution-occupancy resolution authority")
	ErrResolutionAuthorityDenied    = errors.New("execution-occupancy resolution authority denied")
	ErrResolutionAuthorityMismatch  = errors.New("execution-occupancy resolution authority binding mismatch")
	ErrOwnershipFenceMismatch       = errors.New("execution-occupancy ownership fence mismatch")
	ErrCapabilityDeclarationExists  = errors.New("capability declaration is already attached")
	ErrCapabilityDeclarationMissing = errors.New("capability declaration reference is missing")
)

// OwnershipFence is a manager-issued value identifying one local
// Resource execution-ownership incarnation. It is deliberately distinct
// from Resource generation, Node generation, registration identity, and
// Provider state. The value is opaque to callers; possession of its printed
// form is not resolution authority.
type OwnershipFence string

func (fence OwnershipFence) Validate() error {
	if strings.TrimSpace(string(fence)) == "" || strings.ContainsRune(string(fence), '\x00') || !utf8.ValidString(string(fence)) {
		return ErrInvalidOwnershipFence
	}
	return nil
}

func (fence OwnershipFence) String() string {
	return string(fence)
}

func (fence OwnershipFence) IsZero() bool {
	return fence == ""
}

func newOwnershipFence() (OwnershipFence, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate ownership fence: %w", err)
	}
	return OwnershipFence("ownership-fence-" + hex.EncodeToString(value[:])), nil
}

func newResolutionAuthorityToken() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate resolution authority: %w", err)
	}
	return "resolution-authority-" + hex.EncodeToString(value[:]), nil
}

// ExecutionResolutionAuthority is a manager-issued, in-process capability
// for one Resource execution-ownership incarnation. Fence is intentionally
// visible for exact binding and diagnostics, while the manager-issued token
// remains package-private so ResourceID + fence knowledge cannot forge this
// authority.
type ExecutionResolutionAuthority struct {
	ResourceID ResourceID
	Fence      OwnershipFence
	token      string
}

func (authority ExecutionResolutionAuthority) Validate() error {
	if err := authority.ResourceID.Validate(); err != nil {
		return fmt.Errorf("%w: resource: %v", ErrInvalidResolutionAuthority, err)
	}
	if err := authority.Fence.Validate(); err != nil {
		return fmt.Errorf("%w: fence: %v", ErrInvalidResolutionAuthority, err)
	}
	if strings.TrimSpace(authority.token) == "" || strings.ContainsRune(authority.token, '\x00') || !utf8.ValidString(authority.token) {
		return fmt.Errorf("%w: manager-issued token is required", ErrResolutionAuthorityDenied)
	}
	return nil
}

func (authority ExecutionResolutionAuthority) Clone() ExecutionResolutionAuthority {
	return authority
}

func (authority ExecutionResolutionAuthority) IsZero() bool {
	return authority.ResourceID == "" && authority.Fence.IsZero() && authority.token == ""
}

// ResolutionAuthorityFence exposes the exact fence carried by the authority
// without implying that the fence is sufficient authority by itself.
func (authority ExecutionResolutionAuthority) ResolutionAuthorityFence() OwnershipFence {
	return authority.Fence
}

type Resource struct {
	ID                       ResourceID
	Scope                    string
	CapabilityDeclarationIDs []CapabilityDeclarationID
	State                    LifecycleState
	Availability             AvailabilityState
	// CurrentOwnershipFence is the dedicated local execution-ownership
	// incarnation fence. Phase 2 has no API that advances or replaces it.
	CurrentOwnershipFence OwnershipFence
	// ExecutionCapacity is the number of unweighted execution slots owned by
	// this Resource. Zero is the compatibility encoding for the Phase 1
	// default of one slot; positive values opt into a larger fixed capacity.
	ExecutionCapacity int
}

func NewResource(
	id ResourceID,
	scope string,
	declarationIDs []CapabilityDeclarationID,
	state LifecycleState,
) (Resource, error) {
	return NewResourceWithExecutionCapacity(id, scope, declarationIDs, state, 0)
}

// NewResourceWithExecutionCapacity is the explicit constructor for the
// fixed, unweighted Resource execution capacity. A zero capacity retains the
// compatibility default of one slot.
func NewResourceWithExecutionCapacity(
	id ResourceID,
	scope string,
	declarationIDs []CapabilityDeclarationID,
	state LifecycleState,
	executionCapacity int,
) (Resource, error) {
	fence, err := newOwnershipFence()
	if err != nil {
		return Resource{}, err
	}
	resource := Resource{
		ID:                       id,
		Scope:                    scope,
		CapabilityDeclarationIDs: append([]CapabilityDeclarationID(nil), declarationIDs...),
		State:                    state,
		Availability:             availabilityForState(state),
		CurrentOwnershipFence:    fence,
		ExecutionCapacity:        executionCapacity,
	}
	if err := resource.Validate(); err != nil {
		return Resource{}, err
	}
	return resource, nil
}

func (resource Resource) Validate() error {
	if err := resource.ID.Validate(); err != nil {
		return fmt.Errorf("%w: identity: %v", ErrInvalidResource, err)
	}
	if strings.TrimSpace(resource.Scope) == "" || strings.ContainsRune(resource.Scope, '\x00') {
		return fmt.Errorf("%w: scope is required", ErrInvalidResource)
	}
	if !isValidState(resource.State) {
		return fmt.Errorf("%w: %q", ErrInvalidState, resource.State)
	}
	if err := resource.Availability.Validate(); err != nil {
		return fmt.Errorf("%w: availability: %v", ErrInvalidResource, err)
	}
	if err := resource.CurrentOwnershipFence.Validate(); err != nil {
		return fmt.Errorf("%w: ownership fence: %v", ErrInvalidResource, err)
	}
	if resource.ExecutionCapacity < 0 {
		return fmt.Errorf("%w: execution capacity cannot be negative", ErrInvalidResource)
	}
	seen := make(map[CapabilityDeclarationID]struct{}, len(resource.CapabilityDeclarationIDs))
	for _, declarationID := range resource.CapabilityDeclarationIDs {
		if err := declarationID.Validate(); err != nil {
			return fmt.Errorf("%w: declaration reference: %v", ErrInvalidResource, err)
		}
		if _, exists := seen[declarationID]; exists {
			return fmt.Errorf("%w: duplicate declaration reference %q", ErrInvalidResource, declarationID)
		}
		seen[declarationID] = struct{}{}
	}
	return nil
}

func (resource Resource) Clone() Resource {
	clone := resource
	clone.CapabilityDeclarationIDs = append([]CapabilityDeclarationID(nil), resource.CapabilityDeclarationIDs...)
	return clone
}

func (resource Resource) LifecycleState() LifecycleState {
	return resource.State
}

func (resource Resource) IsAvailable() bool {
	return (resource.State == StateCreated || resource.State == StateActive) && resource.Availability != AvailabilityUnavailable
}

// EffectiveExecutionCapacity returns the fixed, unweighted number of
// execution slots owned by the Resource. Zero preserves old Resource
// literals/constructors while carrying the Phase 1 default of one.
func (resource Resource) EffectiveExecutionCapacity() int {
	if resource.ExecutionCapacity == 0 {
		return 1
	}
	return resource.ExecutionCapacity
}

func (resource Resource) HasCapabilityDeclaration(id CapabilityDeclarationID) bool {
	for _, current := range resource.CapabilityDeclarationIDs {
		if current == id {
			return true
		}
	}
	return false
}

func (resource Resource) Transition(next LifecycleState) (Resource, error) {
	if !isValidState(next) {
		return Resource{}, fmt.Errorf("%w: %q", ErrInvalidState, next)
	}
	if next == resource.State {
		return resource.Clone(), nil
	}
	if !canTransition(resource.State, next) {
		return Resource{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, resource.State, next)
	}
	updated := resource.Clone()
	updated.State = next
	updated.Availability = availabilityForState(next)
	return updated, nil
}

func isValidState(state LifecycleState) bool {
	switch state {
	case StateCreated, StateActive, StateSuspended, StateRemoved:
		return true
	default:
		return false
	}
}

func canTransition(current, next LifecycleState) bool {
	switch current {
	case StateCreated:
		return next == StateActive || next == StateSuspended || next == StateRemoved
	case StateActive:
		return next == StateSuspended || next == StateRemoved
	case StateSuspended:
		return next == StateActive || next == StateRemoved
	case StateRemoved:
		return false
	default:
		return false
	}
}

func availabilityForState(state LifecycleState) AvailabilityState {
	switch state {
	case StateCreated, StateActive:
		return AvailabilityAvailable
	case StateSuspended, StateRemoved:
		return AvailabilityUnavailable
	default:
		return AvailabilityUnavailable
	}
}

func sortResources(resources []Resource) {
	sort.Slice(resources, func(left, right int) bool {
		return resources[left].ID < resources[right].ID
	})
}
