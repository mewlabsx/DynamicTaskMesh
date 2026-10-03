package resource

import (
	"fmt"
	"strings"
	"sync"

	"dtm/internal/kernel/event"
	"dtm/internal/kernel/identity"
)

type Manager struct {
	mu                        sync.RWMutex
	resources                 map[ResourceID]Resource
	executionClaims           map[ResourceID]map[identity.ExecutionAllocationID]struct{}
	resolutionAuthorityTokens map[ResourceID]string
	events                    *event.Manager
	publish                   func(event.EventRecord) error
}

type ResourceManager = Manager

func NewManager(auditEvents ...*event.Manager) *Manager {
	var events *event.Manager
	if len(auditEvents) > 0 {
		events = auditEvents[0]
	}
	if events == nil {
		events = event.NewManager()
	}
	return &Manager{
		resources:                 make(map[ResourceID]Resource),
		executionClaims:           make(map[ResourceID]map[identity.ExecutionAllocationID]struct{}),
		resolutionAuthorityTokens: make(map[ResourceID]string),
		events:                    events,
		publish:                   events.Publish,
	}
}

func NewResourceManager(auditEvents ...*event.Manager) *Manager {
	return NewManager(auditEvents...)
}

func (manager *Manager) EventManager() *event.Manager {
	return manager.events
}

// CreateResource creates a resource in CREATED state. A local scope is used
// when no scope is supplied so the object is always valid without inventing a
// locator or transport identity.
func (manager *Manager) CreateResource(scope ...string) (Resource, error) {
	return manager.CreateResourceWithCapacity(0, scope...)
}

// CreateResourceWithCapacity creates a Resource with a fixed number of
// unweighted execution slots. Zero preserves the legacy default of one.
func (manager *Manager) CreateResourceWithCapacity(executionCapacity int, scope ...string) (Resource, error) {
	if len(scope) > 1 {
		return Resource{}, fmt.Errorf("%w: only one scope is allowed", ErrInvalidResource)
	}
	selectedScope := "local"
	if len(scope) == 1 {
		selectedScope = strings.TrimSpace(scope[0])
	}
	if selectedScope == "" {
		return Resource{}, fmt.Errorf("%w: scope is required", ErrInvalidResource)
	}

	for {
		id, err := identity.NewResourceID()
		if err != nil {
			return Resource{}, err
		}
		created, err := NewResourceWithExecutionCapacity(id, selectedScope, nil, StateCreated, executionCapacity)
		if err != nil {
			return Resource{}, err
		}
		authorityToken, err := newResolutionAuthorityToken()
		if err != nil {
			return Resource{}, err
		}

		manager.mu.Lock()
		if _, exists := manager.resources[id]; !exists {
			manager.resources[id] = created.Clone()
			manager.resolutionAuthorityTokens[id] = authorityToken
			if err := manager.publishAudit(
				event.ResourceCreated,
				identity.ObjectReference{Kind: identity.ObjectKindResource, ID: created.ID.String()},
				identity.ObjectReference{Kind: identity.ObjectKindResource, ID: created.ID.String()},
				"resource manager create",
				"created",
			); err != nil {
				delete(manager.resources, id)
				delete(manager.resolutionAuthorityTokens, id)
				manager.mu.Unlock()
				return Resource{}, err
			}
			manager.mu.Unlock()
			return created, nil
		}
		manager.mu.Unlock()
	}
}

// CreateResourceWithID creates a Resource with an already authoritative
// identity. It is a Kernel-internal composition seam for an admission bridge,
// not a User Space ABI. The ordinary CreateResource path remains the
// random-identity constructor used by existing local callers.
func (manager *Manager) CreateResourceWithID(id ResourceID, scope string) (Resource, error) {
	return manager.CreateResourceWithIDAndCapacity(id, scope, 0)
}

// CreateResourceWithIDAndCapacity is the authoritative-identity variant of
// CreateResourceWithCapacity used by bounded admission composition.
func (manager *Manager) CreateResourceWithIDAndCapacity(id ResourceID, scope string, executionCapacity int) (Resource, error) {
	if manager == nil {
		return Resource{}, ErrInvalidResource
	}
	if err := id.Validate(); err != nil {
		return Resource{}, err
	}
	selectedScope := strings.TrimSpace(scope)
	if selectedScope == "" {
		return Resource{}, fmt.Errorf("%w: scope is required", ErrInvalidResource)
	}
	created, err := NewResourceWithExecutionCapacity(id, selectedScope, nil, StateCreated, executionCapacity)
	if err != nil {
		return Resource{}, err
	}
	authorityToken, err := newResolutionAuthorityToken()
	if err != nil {
		return Resource{}, err
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	if current, exists := manager.resources[id]; exists {
		if current.ID != id {
			return Resource{}, fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
		}
		return Resource{}, ErrResourceExists
	}
	manager.resources[id] = created.Clone()
	manager.resolutionAuthorityTokens[id] = authorityToken
	if err := manager.publishAudit(
		event.ResourceCreated,
		identity.ObjectReference{Kind: identity.ObjectKindResource, ID: created.ID.String()},
		identity.ObjectReference{Kind: identity.ObjectKindResource, ID: created.ID.String()},
		"resource manager create with authoritative identity",
		"created",
	); err != nil {
		delete(manager.resources, id)
		delete(manager.resolutionAuthorityTokens, id)
		return Resource{}, err
	}
	return created, nil
}

func (manager *Manager) GetResource(id ResourceID) (Resource, error) {
	if err := id.Validate(); err != nil {
		return Resource{}, err
	}
	manager.mu.RLock()
	resource, exists := manager.resources[id]
	manager.mu.RUnlock()
	if !exists {
		return Resource{}, ErrResourceNotFound
	}
	if resource.ID != id {
		return Resource{}, fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
	}
	return resource.Clone(), nil
}

// GetExecutionResolutionAuthority returns the Resource-manager-issued local
// authority for the current execution-ownership incarnation. The returned
// value is a transition input snapshot, not a Kernel Object or a public
// security credential.
func (manager *Manager) GetExecutionResolutionAuthority(id ResourceID) (ExecutionResolutionAuthority, error) {
	if manager == nil {
		return ExecutionResolutionAuthority{}, ErrInvalidResource
	}
	if err := id.Validate(); err != nil {
		return ExecutionResolutionAuthority{}, err
	}
	manager.mu.RLock()
	current, exists := manager.resources[id]
	token := manager.resolutionAuthorityTokens[id]
	manager.mu.RUnlock()
	if !exists {
		return ExecutionResolutionAuthority{}, ErrResourceNotFound
	}
	if current.ID != id {
		return ExecutionResolutionAuthority{}, fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
	}
	authority := ExecutionResolutionAuthority{ResourceID: id, Fence: current.CurrentOwnershipFence, token: token}
	if err := authority.Validate(); err != nil {
		return ExecutionResolutionAuthority{}, fmt.Errorf("%w: %v", ErrResolutionAuthorityDenied, err)
	}
	return authority, nil
}

// GetOccupancyResolutionAuthority is a naming-compatible alias for the
// Resource execution-ownership authority snapshot.
func (manager *Manager) GetOccupancyResolutionAuthority(id ResourceID) (ExecutionResolutionAuthority, error) {
	return manager.GetExecutionResolutionAuthority(id)
}

// ValidateExecutionResolutionAuthority verifies both the manager-issued
// opaque token and the exact current Resource/fence binding. A known fence
// without this Resource-side token is insufficient authority.
func (manager *Manager) ValidateExecutionResolutionAuthority(id ResourceID, authority ExecutionResolutionAuthority) error {
	if manager == nil {
		return ErrInvalidResource
	}
	if err := id.Validate(); err != nil {
		return err
	}
	if err := authority.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrResolutionAuthorityDenied, err)
	}
	if authority.ResourceID != id {
		return ErrResolutionAuthorityMismatch
	}
	manager.mu.RLock()
	current, exists := manager.resources[id]
	expectedToken := manager.resolutionAuthorityTokens[id]
	manager.mu.RUnlock()
	if !exists {
		return ErrResourceNotFound
	}
	if current.ID != id {
		return fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
	}
	if authority.Fence != current.CurrentOwnershipFence {
		return ErrOwnershipFenceMismatch
	}
	if expectedToken == "" || authority.token != expectedToken {
		return ErrResolutionAuthorityDenied
	}
	return nil
}

// TryAcquireExecution is a low-level Kernel coordination primitive. It
// atomically checks the authoritative Resource state and consumes one
// unweighted execution slot. It deliberately owns occupancy at the Resource
// level so different CapabilityInstances cannot bypass mutual exclusivity on
// the same Resource. Normal execution paths must use the Kernel facade; this
// Manager method is not a User Space ABI and does not create an
// ExecutionAllocation record by itself.
func (manager *Manager) TryAcquireExecution(id ResourceID, allocationID identity.ExecutionAllocationID) (bool, error) {
	acquired, _, err := manager.TryAcquireExecutionWithFence(id, allocationID)
	return acquired, err
}

// TryAcquireExecutionWithFence is the allocation boundary variant that
// returns the same Resource-owned fence snapshot that guarded the claim.
// Normal callers should continue to use the Kernel allocation facade.
func (manager *Manager) TryAcquireExecutionWithFence(id ResourceID, allocationID identity.ExecutionAllocationID) (bool, OwnershipFence, error) {
	if manager == nil {
		return false, "", ErrInvalidResource
	}
	if err := id.Validate(); err != nil {
		return false, "", err
	}
	if err := allocationID.Validate(); err != nil {
		return false, "", err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()

	current, exists := manager.resources[id]
	if !exists {
		return false, "", ErrResourceNotFound
	}
	if current.ID != id {
		return false, "", fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
	}
	if err := current.Validate(); err != nil {
		return false, "", err
	}
	if !current.IsAvailable() {
		return false, "", ErrResourceUnavailable
	}
	claims := manager.executionClaims[id]
	if claims == nil {
		claims = make(map[identity.ExecutionAllocationID]struct{})
		manager.executionClaims[id] = claims
	}
	if _, exists := claims[allocationID]; exists {
		return false, "", ErrResourceBusy
	}
	occupancy := len(claims)
	if occupancy >= current.EffectiveExecutionCapacity() {
		return false, "", ErrResourceBusy
	}
	claims[allocationID] = struct{}{}
	return true, current.CurrentOwnershipFence, nil
}

// ReleaseExecution is the matching low-level Kernel coordination primitive for
// TryAcquireExecution. It returns exactly one previously acquired Resource
// slot. Lifecycle and Availability are intentionally untouched; releasing an
// allocation never makes an unavailable Resource executable. Normal execution
// paths must use the Kernel facade; this method is not a User Space ABI.
func (manager *Manager) ReleaseExecution(id ResourceID, allocationID identity.ExecutionAllocationID) error {
	return manager.ReleaseExecutionWithCommit(id, allocationID, nil)
}

// ReleaseExecutionWithCommit is the narrow local ownership primitive used by
// the Kernel facade when an allocation state transition and exact claim
// removal must share one bounded consistency operation. It removes the exact
// claim while holding the Resource lock, runs commit before releasing that
// lock, and restores the claim if commit fails; it is not a general
// transaction manager or User Space ABI.
func (manager *Manager) ReleaseExecutionWithCommit(id ResourceID, allocationID identity.ExecutionAllocationID, commit func() error) error {
	if manager == nil {
		return ErrInvalidResource
	}
	if err := id.Validate(); err != nil {
		return err
	}
	if err := allocationID.Validate(); err != nil {
		return err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()

	current, exists := manager.resources[id]
	if !exists {
		return ErrResourceNotFound
	}
	if current.ID != id {
		return fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
	}
	claims := manager.executionClaims[id]
	if len(claims) == 0 {
		return ErrExecutionOccupancyMismatch
	}
	if _, exists := claims[allocationID]; !exists {
		return ErrExecutionOccupancyMismatch
	}
	delete(claims, allocationID)
	if commit != nil {
		if err := commit(); err != nil {
			claims[allocationID] = struct{}{}
			return err
		}
	}
	return nil
}

// ExecutionOccupancyForAllocation verifies that one exact allocation owns a
// live claim on the Resource. Gate B uses this last-mile check instead of
// treating any occupancy on the Resource as proof for a different allocation.
func (manager *Manager) ExecutionOccupancyForAllocation(id ResourceID, allocationID identity.ExecutionAllocationID) (bool, error) {
	if manager == nil {
		return false, ErrInvalidResource
	}
	if err := id.Validate(); err != nil {
		return false, err
	}
	if err := allocationID.Validate(); err != nil {
		return false, err
	}
	manager.mu.RLock()
	current, exists := manager.resources[id]
	claims := manager.executionClaims[id]
	_, claimed := claims[allocationID]
	manager.mu.RUnlock()
	if !exists {
		return false, ErrResourceNotFound
	}
	if current.ID != id {
		return false, fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
	}
	return claimed, nil
}

// ExecutionOccupancy returns the number of active allocations consuming the
// Resource's execution slots.
func (manager *Manager) ExecutionOccupancy(id ResourceID) (int, error) {
	if manager == nil {
		return 0, ErrInvalidResource
	}
	if err := id.Validate(); err != nil {
		return 0, err
	}
	manager.mu.RLock()
	current, exists := manager.resources[id]
	occupancy := len(manager.executionClaims[id])
	manager.mu.RUnlock()
	if !exists {
		return 0, ErrResourceNotFound
	}
	if current.ID != id {
		return 0, fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
	}
	return occupancy, nil
}

// AvailableExecutionCapacity returns the number of currently unoccupied
// slots. It is an observation only and is not a reservation.
func (manager *Manager) AvailableExecutionCapacity(id ResourceID) (int, error) {
	if manager == nil {
		return 0, ErrInvalidResource
	}
	if err := id.Validate(); err != nil {
		return 0, err
	}
	manager.mu.RLock()
	current, exists := manager.resources[id]
	occupancy := len(manager.executionClaims[id])
	manager.mu.RUnlock()
	if !exists {
		return 0, ErrResourceNotFound
	}
	if current.ID != id {
		return 0, fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
	}
	available := current.EffectiveExecutionCapacity() - occupancy
	if available < 0 {
		return 0, ErrExecutionOccupancyMismatch
	}
	return available, nil
}

// UpdateState is a Kernel-internal lifecycle mutation path, not a User Space
// ABI. Its state change and mandatory audit publication are rollback-coupled.
func (manager *Manager) UpdateState(id ResourceID, next LifecycleState) error {
	if err := id.Validate(); err != nil {
		return err
	}
	manager.mu.Lock()
	current, exists := manager.resources[id]
	if !exists {
		manager.mu.Unlock()
		return ErrResourceNotFound
	}
	if current.ID != id {
		manager.mu.Unlock()
		return fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
	}
	updated, err := current.Transition(next)
	if err != nil {
		manager.mu.Unlock()
		return err
	}
	manager.resources[id] = updated
	if current.State != next {
		eventType := event.ResourceStateUpdated
		result := string(next)
		if next == StateRemoved {
			eventType = event.ResourceRemoved
			result = "removed"
		}
		if err := manager.publishAudit(
			eventType,
			identity.ObjectReference{Kind: identity.ObjectKindResource, ID: id.String()},
			identity.ObjectReference{Kind: identity.ObjectKindResource, ID: id.String()},
			"resource manager state transition",
			result,
		); err != nil {
			manager.resources[id] = current
			manager.mu.Unlock()
			return err
		}
	}
	manager.mu.Unlock()
	return nil
}

func (manager *Manager) RemoveResource(id ResourceID) error {
	return manager.UpdateState(id, StateRemoved)
}

// UpdateAvailability is a Kernel-internal availability mutation path, not a
// User Space ABI. Its state change and mandatory audit publication are
// rollback-coupled.
func (manager *Manager) UpdateAvailability(id ResourceID, availability AvailabilityState) error {
	if err := id.Validate(); err != nil {
		return err
	}
	if err := availability.Validate(); err != nil {
		return err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	current, exists := manager.resources[id]
	if !exists {
		return ErrResourceNotFound
	}
	if current.ID != id {
		return fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
	}
	if current.State == StateRemoved {
		return ErrResourceRemoved
	}
	if current.Availability == availability {
		return nil
	}
	previous := current
	current.Availability = availability
	manager.resources[id] = current
	if err := manager.publishAudit(
		event.ResourceAvailabilityUpdated,
		identity.ObjectReference{Kind: identity.ObjectKindResource, ID: id.String()},
		identity.ObjectReference{Kind: identity.ObjectKindResource, ID: id.String()},
		"resource manager availability update",
		string(availability),
	); err != nil {
		manager.resources[id] = previous
		return err
	}
	return nil
}

// AttachCapabilityDeclaration is called by the Capability Manager after a
// declaration has been validated. It is intentionally the only mutation path
// for the declaration reference list. It is a Kernel-internal mutation API,
// not a User Space authorization or ABI operation.
func (manager *Manager) AttachCapabilityDeclaration(resourceID ResourceID, declarationID CapabilityDeclarationID) error {
	if err := resourceID.Validate(); err != nil {
		return err
	}
	if err := declarationID.Validate(); err != nil {
		return err
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	current, exists := manager.resources[resourceID]
	if !exists {
		return ErrResourceNotFound
	}
	if current.ID != resourceID {
		return fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
	}
	if current.State == StateRemoved {
		return ErrResourceRemoved
	}
	if current.HasCapabilityDeclaration(declarationID) {
		return ErrCapabilityDeclarationExists
	}
	current.CapabilityDeclarationIDs = append(current.CapabilityDeclarationIDs, declarationID)
	manager.resources[resourceID] = current
	return nil
}

// DetachCapabilityDeclaration is an internal rollback path used when a
// capability declaration mutation cannot publish its mandatory audit event.
// It is not a user-space authorization or ABI operation.
func (manager *Manager) DetachCapabilityDeclaration(resourceID ResourceID, declarationID CapabilityDeclarationID) error {
	if err := resourceID.Validate(); err != nil {
		return err
	}
	if err := declarationID.Validate(); err != nil {
		return err
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	current, exists := manager.resources[resourceID]
	if !exists {
		return ErrResourceNotFound
	}
	if current.ID != resourceID {
		return fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidResource)
	}
	updated := current.Clone()
	for index, currentID := range updated.CapabilityDeclarationIDs {
		if currentID == declarationID {
			updated.CapabilityDeclarationIDs = append(updated.CapabilityDeclarationIDs[:index], updated.CapabilityDeclarationIDs[index+1:]...)
			manager.resources[resourceID] = updated
			return nil
		}
	}
	return ErrCapabilityDeclarationMissing
}

func (manager *Manager) ListResources() []Resource {
	manager.mu.RLock()
	resources := make([]Resource, 0, len(manager.resources))
	for _, current := range manager.resources {
		resources = append(resources, current.Clone())
	}
	manager.mu.RUnlock()
	sortResources(resources)
	return resources
}

func (manager *Manager) publishAudit(
	eventType string,
	source, affected identity.ObjectReference,
	cause, result string,
) error {
	record, err := event.NewEventRecord(source, eventType, affected, cause, result)
	if err != nil {
		return err
	}
	return manager.publish(record)
}
