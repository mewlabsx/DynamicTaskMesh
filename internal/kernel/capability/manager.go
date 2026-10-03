package capability

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"dtm/internal/kernel/event"
	"dtm/internal/kernel/execution"
	"dtm/internal/kernel/identity"
	"dtm/internal/kernel/resource"
)

type Manager struct {
	mu                      sync.RWMutex
	resources               *resource.Manager
	executions              *execution.Manager
	events                  *event.Manager
	publish                 func(event.EventRecord) error
	beforeInstanceStateLock func()
	declarations            map[CapabilityDeclarationID]CapabilityDeclaration
	instances               map[CapabilityInstanceID]CapabilityInstance
	handles                 map[CapabilityHandleID]CapabilityHandle
}

type CapabilityManager = Manager

func NewManager(resources *resource.Manager, dependencies ...any) *Manager {
	var executionManager *execution.Manager
	var events *event.Manager
	for _, dependency := range dependencies {
		switch value := dependency.(type) {
		case *execution.Manager:
			executionManager = value
		case *event.Manager:
			events = value
		}
	}
	if events == nil {
		events = event.NewManager()
	}
	return &Manager{
		resources:    resources,
		executions:   executionManager,
		events:       events,
		publish:      events.Publish,
		declarations: make(map[CapabilityDeclarationID]CapabilityDeclaration),
		instances:    make(map[CapabilityInstanceID]CapabilityInstance),
		handles:      make(map[CapabilityHandleID]CapabilityHandle),
	}
}

func NewCapabilityManager(resources *resource.Manager, dependencies ...any) *Manager {
	return NewManager(resources, dependencies...)
}

func (manager *Manager) EventManager() *event.Manager {
	return manager.events
}

func (manager *Manager) RegisterDeclaration(spec DeclarationSpec) (CapabilityDeclaration, error) {
	if manager.resources == nil {
		return CapabilityDeclaration{}, ErrInvalidDependency
	}
	resourceSnapshot, err := manager.resources.GetResource(spec.ResourceID)
	if err != nil {
		return CapabilityDeclaration{}, fmt.Errorf("validate declaration resource: %w", err)
	}
	if resourceSnapshot.State == resource.StateRemoved {
		return CapabilityDeclaration{}, resource.ErrResourceRemoved
	}

	id, err := identity.NewCapabilityDeclarationID()
	if err != nil {
		return CapabilityDeclaration{}, err
	}
	declaration, err := NewCapabilityDeclaration(
		id,
		spec.ResourceID,
		spec.Name,
		spec.Version,
		spec.InputMetadata,
		spec.OutputMetadata,
		spec.Constraints,
	)
	if err != nil {
		return CapabilityDeclaration{}, err
	}

	if err := manager.resources.AttachCapabilityDeclaration(declaration.ResourceID, declaration.ID); err != nil {
		return CapabilityDeclaration{}, fmt.Errorf("attach declaration to resource: %w", err)
	}
	manager.mu.Lock()
	manager.declarations[declaration.ID] = declaration.Clone()
	if err := manager.publishAudit(
		event.CapabilityDeclarationRegistered,
		identity.ObjectReference{Kind: identity.ObjectKindResource, ID: declaration.ResourceID.String()},
		identity.ObjectReference{Kind: identity.ObjectKindCapabilityDeclaration, ID: declaration.ID.String()},
		"capability manager register",
		"registered",
	); err != nil {
		delete(manager.declarations, declaration.ID)
		manager.mu.Unlock()
		if rollbackErr := manager.resources.DetachCapabilityDeclaration(declaration.ResourceID, declaration.ID); rollbackErr != nil {
			return CapabilityDeclaration{}, fmt.Errorf("%w; rollback declaration attachment: %v", err, rollbackErr)
		}
		return CapabilityDeclaration{}, err
	}
	manager.mu.Unlock()
	return declaration, nil
}

func (manager *Manager) RegisterDeclarationFor(
	resourceID resource.ResourceID,
	name, version string,
) (CapabilityDeclaration, error) {
	return manager.RegisterDeclaration(DeclarationSpec{
		ResourceID: resourceID,
		Name:       name,
		Version:    version,
	})
}

func (manager *Manager) GetDeclaration(id CapabilityDeclarationID) (CapabilityDeclaration, error) {
	if err := id.Validate(); err != nil {
		return CapabilityDeclaration{}, err
	}
	manager.mu.RLock()
	declaration, exists := manager.declarations[id]
	manager.mu.RUnlock()
	if !exists {
		return CapabilityDeclaration{}, ErrDeclarationNotFound
	}
	if declaration.ID != id {
		return CapabilityDeclaration{}, fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidDeclaration)
	}
	return declaration.Clone(), nil
}

func (manager *Manager) CreateInstance(declarationID CapabilityDeclarationID) (CapabilityInstance, error) {
	declaration, err := manager.GetDeclaration(declarationID)
	if err != nil {
		return CapabilityInstance{}, err
	}
	if manager.resources == nil {
		return CapabilityInstance{}, ErrInvalidDependency
	}
	resourceSnapshot, err := manager.resources.GetResource(declaration.ResourceID)
	if err != nil {
		return CapabilityInstance{}, fmt.Errorf("validate instance provider: %w", err)
	}
	if resourceSnapshot.State == resource.StateRemoved {
		return CapabilityInstance{}, resource.ErrResourceRemoved
	}

	id, err := identity.NewCapabilityInstanceID()
	if err != nil {
		return CapabilityInstance{}, err
	}
	instance, err := NewCapabilityInstance(
		id,
		declaration.ID,
		declaration.ResourceID,
		InstanceStateCreated,
	)
	if err != nil {
		return CapabilityInstance{}, err
	}
	manager.mu.Lock()
	manager.instances[instance.ID] = instance
	if err := manager.publishAudit(
		event.CapabilityInstanceCreated,
		identity.ObjectReference{Kind: identity.ObjectKindCapabilityDeclaration, ID: declaration.ID.String()},
		identity.ObjectReference{Kind: identity.ObjectKindCapabilityInstance, ID: instance.ID.String()},
		"capability manager create",
		"created",
	); err != nil {
		delete(manager.instances, instance.ID)
		manager.mu.Unlock()
		return CapabilityInstance{}, err
	}
	manager.mu.Unlock()
	return instance, nil
}

func (manager *Manager) GetInstance(id CapabilityInstanceID) (CapabilityInstance, error) {
	if err := id.Validate(); err != nil {
		return CapabilityInstance{}, err
	}
	manager.mu.RLock()
	instance, exists := manager.instances[id]
	manager.mu.RUnlock()
	if !exists {
		return CapabilityInstance{}, ErrInstanceNotFound
	}
	if instance.ID != id {
		return CapabilityInstance{}, fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidInstance)
	}
	return instance.Clone(), nil
}

// UpdateInstanceState is a Kernel-internal lifecycle mutation path, not a
// User Space ABI. Its state change and mandatory audit publication are
// rollback-coupled.
func (manager *Manager) UpdateInstanceState(id CapabilityInstanceID, next InstanceState) error {
	if err := id.Validate(); err != nil {
		return err
	}
	switch next {
	case InstanceStateCreated, InstanceStateActive, InstanceStateSuspended, InstanceStateRevoked, InstanceStateDestroyed:
	default:
		return fmt.Errorf("%w: %q", ErrInvalidInstance, next)
	}

	if manager.beforeInstanceStateLock != nil {
		manager.beforeInstanceStateLock()
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()

	current, exists := manager.instances[id]
	if !exists {
		return ErrInstanceNotFound
	}
	if current.ID != id {
		return fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidInstance)
	}
	if current.State == next {
		return nil
	}
	if current.State == InstanceStateRevoked || current.State == InstanceStateDestroyed {
		return ErrInstanceRevoked
	}
	if next == InstanceStateCreated {
		return fmt.Errorf("%w: %q", ErrInvalidInstance, next)
	}
	if next == InstanceStateActive && manager.resources != nil {
		provider, providerErr := manager.resources.GetResource(current.ProviderResourceID)
		if providerErr != nil {
			return fmt.Errorf("validate instance provider: %w", providerErr)
		}
		if !provider.IsAvailable() {
			return ErrInstanceUnavailable
		}
	}
	previous := current.Clone()
	updated := current.Clone()
	updated.State = next
	updated.Availability = availabilityForInstanceState(next)
	manager.instances[id] = updated

	if previous.State != updated.State {
		eventType := event.CapabilityInstanceStateUpdated
		result := string(next)
		if next == InstanceStateRevoked {
			eventType = event.CapabilityInstanceRevoked
			result = "revoked"
		} else if next == InstanceStateDestroyed {
			eventType = event.CapabilityInstanceDestroyed
			result = "destroyed"
		}
		if err := manager.publishAudit(
			eventType,
			identity.ObjectReference{Kind: identity.ObjectKindCapabilityDeclaration, ID: updated.DeclarationID.String()},
			identity.ObjectReference{Kind: identity.ObjectKindCapabilityInstance, ID: updated.ID.String()},
			"capability manager state transition",
			result,
		); err != nil {
			manager.instances[id] = previous
			return err
		}
	}
	return nil
}

func (manager *Manager) CreateHandle(
	subjectID ExecutionContextID,
	targetID CapabilityInstanceID,
	permissions PermissionSet,
	scope ...string,
) (CapabilityHandle, error) {
	if manager.executions == nil {
		return CapabilityHandle{}, ErrInvalidDependency
	}
	context, err := manager.executions.GetContext(subjectID)
	if err != nil {
		return CapabilityHandle{}, fmt.Errorf("validate handle subject: %w", err)
	}
	if !context.IsUsable() {
		if context.State == execution.StateTerminated {
			return CapabilityHandle{}, execution.ErrContextTerminated
		}
		return CapabilityHandle{}, ErrContextUnavailable
	}
	instance, err := manager.GetInstance(targetID)
	if err != nil {
		return CapabilityHandle{}, err
	}
	if !instance.IsAvailable() {
		switch instance.State {
		case InstanceStateRevoked:
			return CapabilityHandle{}, ErrInstanceRevoked
		case InstanceStateDestroyed:
			return CapabilityHandle{}, ErrInstanceUnavailable
		default:
			return CapabilityHandle{}, ErrInstanceUnavailable
		}
	}
	if err := manager.validateProviderAvailability(instance); err != nil {
		return CapabilityHandle{}, err
	}

	selectedScope := strings.TrimSpace(context.Scope)
	if len(scope) > 1 {
		return CapabilityHandle{}, fmt.Errorf("%w: only one scope is allowed", ErrInvalidHandle)
	}
	if len(scope) == 1 {
		requestedScope := strings.TrimSpace(scope[0])
		if requestedScope == "" {
			return CapabilityHandle{}, ErrInvalidHandle
		}
		if requestedScope != selectedScope {
			return CapabilityHandle{}, ErrScopeMismatch
		}
		selectedScope = requestedScope
	}
	if selectedScope == "" {
		return CapabilityHandle{}, ErrInvalidHandle
	}

	id, err := identity.NewCapabilityHandleID()
	if err != nil {
		return CapabilityHandle{}, err
	}
	handle, err := NewCapabilityHandle(
		id,
		subjectID,
		targetID,
		permissions,
		selectedScope,
		HandleStateCreated,
	)
	if err != nil {
		return CapabilityHandle{}, err
	}
	manager.mu.Lock()
	manager.handles[handle.ID] = handle.Clone()
	if err := manager.publishAudit(
		event.CapabilityHandleCreated,
		identity.ObjectReference{Kind: identity.ObjectKindExecutionContext, ID: subjectID.String()},
		identity.ObjectReference{Kind: identity.ObjectKindCapabilityHandle, ID: handle.ID.String()},
		"capability manager create",
		"created",
	); err != nil {
		delete(manager.handles, handle.ID)
		manager.mu.Unlock()
		return CapabilityHandle{}, err
	}
	manager.mu.Unlock()
	return handle, nil
}

func (manager *Manager) GetHandle(id CapabilityHandleID) (CapabilityHandle, error) {
	if err := id.Validate(); err != nil {
		return CapabilityHandle{}, err
	}
	manager.mu.RLock()
	handle, exists := manager.handles[id]
	manager.mu.RUnlock()
	if !exists {
		return CapabilityHandle{}, ErrHandleNotFound
	}
	if handle.ID != id {
		return CapabilityHandle{}, fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidHandle)
	}
	return handle.Clone(), nil
}

// ValidateHandle is the single structural/boundary validation path for handle
// use. It checks the handle lifecycle, subject context, target instance, and
// current provider Resource state; it is not complete authorization or a User
// Space ABI.
func (manager *Manager) ValidateHandle(id CapabilityHandleID) error {
	if manager == nil {
		return ErrInvalidDependency
	}
	handle, err := manager.GetHandle(id)
	if err != nil {
		return err
	}
	// Preserve the pre-K2-C compatibility path's handle-state precedence when
	// a legacy caller presents more than one invalid record at once. The full
	// deterministic K2-C chain remains authoritative after this precheck.
	if err := validateHandleLifecycleAvailability(handle); err != nil {
		return err
	}
	_, err = manager.ValidateHandleRequest(HandleValidationRequest{
		ExecutionContextID: handle.SubjectExecutionContextID,
		CapabilityHandleID: id,
	})
	return err
}

// Invoke is the K0.5 syscall-shaped boundary for a capability. It performs
// validation and emits an audit fact only; it never executes Payload or calls
// a device adapter.
func (manager *Manager) Invoke(
	handleID CapabilityHandleID,
	request InvocationRequest,
) (*InvocationResult, error) {
	if err := request.Validate(); err != nil {
		if auditErr := manager.auditInvocation(handleID, nil, false, err.Error()); auditErr != nil {
			return nil, errors.Join(err, auditErr)
		}
		return nil, err
	}
	handle, err := manager.GetHandle(handleID)
	if err != nil {
		if auditErr := manager.auditInvocation(handleID, nil, false, err.Error()); auditErr != nil {
			return nil, errors.Join(err, auditErr)
		}
		return nil, err
	}
	if err := manager.ValidateHandle(handleID); err != nil {
		result := &InvocationResult{Accepted: false, Reason: err.Error()}
		if auditErr := manager.auditInvocation(handleID, &handle, false, result.Reason); auditErr != nil {
			return nil, auditErr
		}
		return result, nil
	}
	result := &InvocationResult{Accepted: true, Reason: "accepted"}
	if err := manager.auditInvocation(handleID, &handle, true, result.Reason); err != nil {
		return nil, err
	}
	return result, nil
}

// ActivateHandle is a Kernel-internal Handle lifecycle mutation path, not a
// User Space ABI. Its state change and mandatory audit publication are
// rollback-coupled.
func (manager *Manager) ActivateHandle(id CapabilityHandleID) error {
	if err := manager.ValidateHandle(id); err != nil {
		return err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	handle, exists := manager.handles[id]
	if !exists {
		return ErrHandleNotFound
	}
	if handle.ID != id {
		return fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidHandle)
	}
	if handle.State == HandleStateRevoked || handle.State == HandleStateExpired || handle.State == HandleStateDestroyed {
		return ErrInvalidHandle
	}
	handleSnapshot := handle
	handle.State = HandleStateActive
	handle.Availability = AvailabilityAvailable
	manager.handles[id] = handle
	if err := manager.publishAudit(
		event.CapabilityHandleActivated,
		identity.ObjectReference{Kind: identity.ObjectKindExecutionContext, ID: handle.SubjectExecutionContextID.String()},
		identity.ObjectReference{Kind: identity.ObjectKindCapabilityHandle, ID: handle.ID.String()},
		"capability manager activate",
		"active",
	); err != nil {
		manager.handles[id] = handleSnapshot
		return err
	}
	return nil
}

// RevokeHandle is a Kernel-internal Handle lifecycle mutation path, not a
// User Space ABI. Its state change and mandatory audit publication are
// rollback-coupled.
func (manager *Manager) RevokeHandle(id CapabilityHandleID) error {
	if err := id.Validate(); err != nil {
		return err
	}
	manager.mu.Lock()
	handle, exists := manager.handles[id]
	if !exists {
		manager.mu.Unlock()
		return ErrHandleNotFound
	}
	if handle.ID != id {
		manager.mu.Unlock()
		return fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidHandle)
	}
	if handle.State == HandleStateDestroyed {
		manager.mu.Unlock()
		return ErrHandleDestroyed
	}
	if handle.State == HandleStateRevoked {
		manager.mu.Unlock()
		return nil
	}
	handleSnapshot := handle
	handle.State = HandleStateRevoked
	handle.Availability = AvailabilityUnavailable
	manager.handles[id] = handle
	if err := manager.publishAudit(
		event.CapabilityHandleRevoked,
		identity.ObjectReference{Kind: identity.ObjectKindExecutionContext, ID: handle.SubjectExecutionContextID.String()},
		identity.ObjectReference{Kind: identity.ObjectKindCapabilityHandle, ID: handle.ID.String()},
		"capability manager revoke",
		"revoked",
	); err != nil {
		manager.handles[id] = handleSnapshot
		manager.mu.Unlock()
		return err
	}
	manager.mu.Unlock()
	return nil
}

// ReleaseHandle is a Kernel-internal Handle lifecycle mutation path, not a
// User Space ABI. Its state change and mandatory audit publication are
// rollback-coupled.
func (manager *Manager) ReleaseHandle(id CapabilityHandleID) error {
	if err := id.Validate(); err != nil {
		return err
	}
	manager.mu.Lock()
	handle, exists := manager.handles[id]
	if !exists {
		manager.mu.Unlock()
		return ErrHandleNotFound
	}
	if handle.ID != id {
		manager.mu.Unlock()
		return fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidHandle)
	}
	if handle.State == HandleStateDestroyed {
		manager.mu.Unlock()
		return nil
	}
	handleSnapshot := handle
	handle.State = HandleStateDestroyed
	handle.Availability = AvailabilityUnavailable
	manager.handles[id] = handle
	if err := manager.publishAudit(
		event.CapabilityHandleReleased,
		identity.ObjectReference{Kind: identity.ObjectKindExecutionContext, ID: handle.SubjectExecutionContextID.String()},
		identity.ObjectReference{Kind: identity.ObjectKindCapabilityHandle, ID: handle.ID.String()},
		"capability manager release",
		"released",
	); err != nil {
		manager.handles[id] = handleSnapshot
		manager.mu.Unlock()
		return err
	}
	manager.mu.Unlock()
	return nil
}

func (manager *Manager) validateProviderAvailability(instance CapabilityInstance) error {
	if manager.resources == nil {
		return ErrInvalidDependency
	}
	provider, err := manager.resources.GetResource(instance.ProviderResourceID)
	if err != nil || !provider.IsAvailable() {
		return ErrInstanceUnavailable
	}
	return nil
}

func (manager *Manager) ListDeclarations() []CapabilityDeclaration {
	manager.mu.RLock()
	declarations := make([]CapabilityDeclaration, 0, len(manager.declarations))
	for _, declaration := range manager.declarations {
		declarations = append(declarations, declaration.Clone())
	}
	manager.mu.RUnlock()
	sort.Slice(declarations, func(left, right int) bool { return declarations[left].ID < declarations[right].ID })
	return declarations
}

func (manager *Manager) ListInstances() []CapabilityInstance {
	manager.mu.RLock()
	instances := make([]CapabilityInstance, 0, len(manager.instances))
	for _, instance := range manager.instances {
		instances = append(instances, instance.Clone())
	}
	manager.mu.RUnlock()
	sort.Slice(instances, func(left, right int) bool { return instances[left].ID < instances[right].ID })
	return instances
}

func (manager *Manager) ListHandles() []CapabilityHandle {
	manager.mu.RLock()
	handles := make([]CapabilityHandle, 0, len(manager.handles))
	for _, handle := range manager.handles {
		handles = append(handles, handle.Clone())
	}
	manager.mu.RUnlock()
	sort.Slice(handles, func(left, right int) bool { return handles[left].ID < handles[right].ID })
	return handles
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

func (manager *Manager) auditInvocation(
	handleID CapabilityHandleID,
	handle *CapabilityHandle,
	accepted bool,
	reason string,
) error {
	source := identity.ObjectReference{Kind: identity.ObjectKindCapabilityHandle, ID: handleID.String()}
	if handle != nil {
		source = identity.ObjectReference{Kind: identity.ObjectKindExecutionContext, ID: handle.SubjectExecutionContextID.String()}
	}
	eventType := event.CapabilityHandleInvokeRejected
	if accepted {
		eventType = event.CapabilityHandleInvokeAccepted
	}
	return manager.publishAudit(
		eventType,
		source,
		identity.ObjectReference{Kind: identity.ObjectKindCapabilityHandle, ID: handleID.String()},
		"capability manager invoke",
		reason,
	)
}
