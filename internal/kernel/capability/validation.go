package capability

import (
	"fmt"
	"strings"

	"dtm/internal/kernel/execution"
	"dtm/internal/kernel/resource"
)

// HandleValidationRequest identifies the caller and the handle that the
// caller wants to use. CallerIdentity is an optional semantic consistency
// check; it is not an authentication proof or a complete authorization
// decision. Scope, when supplied, is compared byte-for-byte with the
// authoritative ExecutionContext scope.
type HandleValidationRequest struct {
	ExecutionContextID ExecutionContextID
	CapabilityHandleID CapabilityHandleID
	CallerIdentity     string
	Scope              string
}

func (request HandleValidationRequest) Validate() error {
	if err := request.ExecutionContextID.Validate(); err != nil {
		return fmt.Errorf("%w: identity: %v", execution.ErrInvalidContext, err)
	}
	if err := request.CapabilityHandleID.Validate(); err != nil {
		return fmt.Errorf("%w: handle identity: %v", ErrInvalidHandle, err)
	}
	if strings.ContainsRune(request.CallerIdentity, '\x00') {
		return fmt.Errorf("%w: invalid caller identity", ErrCallerIdentityMismatch)
	}
	if request.Scope != "" && (strings.TrimSpace(request.Scope) == "" || strings.ContainsRune(request.Scope, '\x00')) {
		return fmt.Errorf("%w: invalid request scope", ErrScopeMismatch)
	}
	return nil
}

// ValidatedHandle is an immutable-by-convention snapshot of the records that
// establish a usable capability authority. It is a validation result, not a
// new Kernel Object and not a replacement for the authoritative manager
// records.
type ValidatedHandle struct {
	ExecutionContext      execution.ExecutionContext
	CapabilityHandle      CapabilityHandle
	CapabilityInstance    CapabilityInstance
	CapabilityDeclaration CapabilityDeclaration
	Resource              resource.Resource
}

func (validated ValidatedHandle) Clone() ValidatedHandle {
	validated.ExecutionContext = validated.ExecutionContext.Clone()
	validated.CapabilityHandle = validated.CapabilityHandle.Clone()
	validated.CapabilityInstance = validated.CapabilityInstance.Clone()
	validated.CapabilityDeclaration = validated.CapabilityDeclaration.Clone()
	validated.Resource = validated.Resource.Clone()
	return validated
}

// HandleValidator is the reusable, side-effect-free K2-C validation facade.
// The Manager remains the owner of the authoritative records; this facade
// only orchestrates their deterministic checks.
type HandleValidator struct {
	manager *Manager
}

func NewHandleValidator(manager *Manager) *HandleValidator {
	return &HandleValidator{manager: manager}
}

func (validator *HandleValidator) Validate(request HandleValidationRequest) (ValidatedHandle, error) {
	if validator == nil || validator.manager == nil {
		return ValidatedHandle{}, ErrInvalidDependency
	}
	return validator.manager.validateHandleRequest(request)
}

// ValidateHandleRequest is the authoritative K2-C validation entry point.
// It performs checks only. It does not mutate lifecycle or availability,
// publish events, create/renew handles, retry, activate, migrate, or
// reconcile records.
func (manager *Manager) ValidateHandleRequest(request HandleValidationRequest) (ValidatedHandle, error) {
	return manager.validateHandleRequest(request)
}

type capabilityChainSnapshot struct {
	handle           CapabilityHandle
	instance         CapabilityInstance
	declaration      CapabilityDeclaration
	handleFound      bool
	instanceFound    bool
	declarationFound bool
}

// snapshotCapabilityChain takes one CapabilityManager read snapshot so map
// iteration or an intermediate manager mutation cannot choose the first error
// or change the handle/instance/declaration chain halfway through this local
// check. Cross-manager atomicity with ExecutionManager and ResourceManager is
// intentionally not introduced here.
func (manager *Manager) snapshotCapabilityChain(handleID CapabilityHandleID) capabilityChainSnapshot {
	manager.mu.RLock()
	declaration, declarationFound := CapabilityDeclaration{}, false
	instance, instanceFound := CapabilityInstance{}, false
	handle, handleFound := CapabilityHandle{}, false
	if manager.handles != nil {
		current, exists := manager.handles[handleID]
		if exists {
			handle, handleFound = current.Clone(), true
		}
	}
	if handleFound && manager.instances != nil {
		current, exists := manager.instances[handle.TargetCapabilityInstanceID]
		if exists {
			instance, instanceFound = current.Clone(), true
		}
	}
	if instanceFound && manager.declarations != nil {
		current, exists := manager.declarations[instance.DeclarationID]
		if exists {
			declaration, declarationFound = current.Clone(), true
		}
	}
	manager.mu.RUnlock()
	return capabilityChainSnapshot{
		handle:           handle,
		instance:         instance,
		declaration:      declaration,
		handleFound:      handleFound,
		instanceFound:    instanceFound,
		declarationFound: declarationFound,
	}
}

func (manager *Manager) validateHandleRequest(request HandleValidationRequest) (ValidatedHandle, error) {
	if manager == nil {
		return ValidatedHandle{}, ErrInvalidDependency
	}
	if err := request.Validate(); err != nil {
		return ValidatedHandle{}, err
	}
	if manager.executions == nil || manager.resources == nil {
		return ValidatedHandle{}, ErrInvalidDependency
	}

	// 1. Resolve and validate the caller's execution context first.
	context, err := manager.executionContextForHandleRequest(request)
	if err != nil {
		return ValidatedHandle{}, err
	}

	// 2. Resolve the authoritative Handle -> Instance -> Declaration chain.
	chain := manager.snapshotCapabilityChain(request.CapabilityHandleID)
	if !chain.handleFound {
		return ValidatedHandle{}, ErrHandleNotFound
	}
	handle := chain.handle
	if err := handle.Validate(); err != nil {
		return ValidatedHandle{}, err
	}
	if handle.ID != request.CapabilityHandleID {
		return ValidatedHandle{}, ErrRelationshipMismatch
	}
	if err := validateHandleLifecycleAvailability(handle); err != nil {
		return ValidatedHandle{}, err
	}

	// 3. Bind the handle to this exact context and exact scope. Equal scopes
	// do not authorize a different ExecutionContext.
	if handle.SubjectExecutionContextID != request.ExecutionContextID {
		return ValidatedHandle{}, ErrContextMismatch
	}
	if handle.Scope != context.Scope {
		return ValidatedHandle{}, ErrScopeMismatch
	}

	// 4. Validate the target instance and its lifecycle/availability.
	if !chain.instanceFound {
		return ValidatedHandle{}, ErrInstanceNotFound
	}
	instance := chain.instance
	if err := instance.Validate(); err != nil {
		return ValidatedHandle{}, err
	}
	if instance.ID != handle.TargetCapabilityInstanceID {
		return ValidatedHandle{}, ErrRelationshipMismatch
	}
	switch instance.State {
	case InstanceStateRevoked:
		return ValidatedHandle{}, ErrInstanceRevoked
	case InstanceStateSuspended, InstanceStateDestroyed:
		return ValidatedHandle{}, ErrInstanceUnavailable
	case InstanceStateCreated, InstanceStateActive:
		if !instance.IsAvailable() {
			return ValidatedHandle{}, ErrInstanceUnavailable
		}
	default:
		return ValidatedHandle{}, ErrInvalidInstance
	}

	// 5. Validate the declaration relationship before consulting the provider
	// Resource. A capability instance cannot redirect a declaration to another
	// Resource.
	if !chain.declarationFound {
		return ValidatedHandle{}, ErrDeclarationNotFound
	}
	declaration := chain.declaration
	if err := declaration.Validate(); err != nil {
		return ValidatedHandle{}, err
	}
	if instance.DeclarationID != declaration.ID || declaration.ResourceID != instance.ProviderResourceID {
		return ValidatedHandle{}, ErrRelationshipMismatch
	}

	// 6. Resolve and validate the provider Resource and its explicit
	// declaration attachment. Availability is checked independently from every
	// lifecycle record and never rewrites an instance or handle state.
	providerResource, err := manager.resources.GetResource(instance.ProviderResourceID)
	if err != nil {
		return ValidatedHandle{}, wrapInstanceUnavailable(err)
	}
	if err := providerResource.Validate(); err != nil {
		return ValidatedHandle{}, fmt.Errorf("%w: provider resource: %v", ErrRelationshipMismatch, err)
	}
	if providerResource.ID != instance.ProviderResourceID || !providerResource.HasCapabilityDeclaration(declaration.ID) {
		return ValidatedHandle{}, ErrRelationshipMismatch
	}
	if providerResource.State == resource.StateRemoved {
		return ValidatedHandle{}, wrapInstanceUnavailable(resource.ErrResourceRemoved)
	}
	if !providerResource.IsAvailable() {
		return ValidatedHandle{}, wrapInstanceUnavailable(ErrResourceUnavailable)
	}

	return ValidatedHandle{
		ExecutionContext:      context.Clone(),
		CapabilityHandle:      handle.Clone(),
		CapabilityInstance:    instance.Clone(),
		CapabilityDeclaration: declaration.Clone(),
		Resource:              providerResource.Clone(),
	}, nil
}

func (manager *Manager) executionContextForHandleRequest(request HandleValidationRequest) (execution.ExecutionContext, error) {
	context, err := manager.executions.GetContext(request.ExecutionContextID)
	if err != nil {
		return execution.ExecutionContext{}, fmt.Errorf("validate handle context: %w", err)
	}
	if err := context.Validate(); err != nil {
		return execution.ExecutionContext{}, err
	}
	if !context.IsUsable() {
		if context.State == execution.StateTerminated {
			return execution.ExecutionContext{}, execution.ErrContextTerminated
		}
		return execution.ExecutionContext{}, ErrContextUnavailable
	}
	if request.CallerIdentity != "" && request.CallerIdentity != context.Subject {
		return execution.ExecutionContext{}, ErrCallerIdentityMismatch
	}
	if request.Scope != "" && request.Scope != context.Scope {
		return execution.ExecutionContext{}, ErrScopeMismatch
	}
	return context, nil
}

func wrapInstanceUnavailable(cause error) error {
	if cause == nil {
		return ErrInstanceUnavailable
	}
	return fmt.Errorf("%w: %w", ErrInstanceUnavailable, cause)
}

func validateHandleLifecycleAvailability(handle CapabilityHandle) error {
	switch handle.State {
	case HandleStateRevoked:
		return ErrHandleRevoked
	case HandleStateExpired:
		return ErrHandleExpired
	case HandleStateDestroyed:
		return ErrHandleDestroyed
	case HandleStateSuspended:
		return ErrInvalidHandle
	case HandleStateCreated, HandleStateActive:
		if handle.Availability == AvailabilityUnavailable {
			return ErrInvalidHandle
		}
		return nil
	default:
		return ErrInvalidHandle
	}
}

// ProviderOperation is the bounded internal handoff that may cross from the
// validated Kernel boundary to a provider. It contains only authoritative
// identity references and the structurally validated operation; it carries no
// mutable manager, context authority, caller-controlled locator, retry, or
// delegation state.
type ProviderOperation struct {
	InvocationID            execution.InvocationID
	Operation               string
	Payload                 []byte
	CapabilityDeclarationID CapabilityDeclarationID
	CapabilityInstanceID    CapabilityInstanceID
	ResourceID              ResourceID
}

func (operation ProviderOperation) Validate() error {
	if !operation.InvocationID.IsZero() {
		if err := operation.InvocationID.Validate(); err != nil {
			return fmt.Errorf("%w: invocation identity: %v", ErrInvalidInvocation, err)
		}
	}
	if err := operation.CapabilityDeclarationID.Validate(); err != nil {
		return fmt.Errorf("%w: declaration identity: %v", ErrInvalidInvocation, err)
	}
	if err := operation.CapabilityInstanceID.Validate(); err != nil {
		return fmt.Errorf("%w: instance identity: %v", ErrInvalidInvocation, err)
	}
	if err := operation.ResourceID.Validate(); err != nil {
		return fmt.Errorf("%w: resource identity: %v", ErrInvalidInvocation, err)
	}
	return (InvocationRequest{Operation: operation.Operation, Payload: operation.Payload}).Validate()
}

func (operation ProviderOperation) Clone() ProviderOperation {
	operation.Payload = append([]byte(nil), operation.Payload...)
	return operation
}

// ObservableCapabilityProvider is the optional Phase 1 extension to the
// existing Provider seam. Providers that implement only CapabilityProvider
// retain the legacy synchronous error contract; providers implementing this
// interface can report orthogonal outcome and occupancy certainty.
type ObservableCapabilityProvider interface {
	InvokeWithObservation(operation ProviderOperation) (execution.ExecutionObservation, error)
}

type CapabilityExecutionProvider = ObservableCapabilityProvider

// CapabilityProvider is intentionally a minimal internal seam. K2-C does
// not define provider result mapping, event policy, remote trust, retries, or
// a complete authorization system.
type CapabilityProvider interface {
	Invoke(operation ProviderOperation) error
}

type Provider = CapabilityProvider

// InvokeWithProvider validates the complete local authority chain before the
// first provider call. A validation failure cannot call the provider. A
// provider error is returned once and is never retried or translated here.
func (manager *Manager) InvokeWithProvider(
	validation HandleValidationRequest,
	request InvocationRequest,
	provider CapabilityProvider,
) error {
	// Establish the caller's authority before inspecting the requested
	// operation. This fixes deterministic precedence for a request that has
	// both an invalid authority reference and an invalid operation, and keeps
	// the provider behind the complete validation barrier.
	validated, err := manager.ValidateHandleRequest(validation)
	if err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return err
	}
	if provider == nil {
		return ErrInvalidDependency
	}
	operation := ProviderOperation{
		Operation:               request.Operation,
		Payload:                 append([]byte(nil), request.Payload...),
		CapabilityDeclarationID: validated.CapabilityDeclaration.ID,
		CapabilityInstanceID:    validated.CapabilityInstance.ID,
		ResourceID:              validated.Resource.ID,
	}
	if err := operation.Validate(); err != nil {
		return err
	}
	return provider.Invoke(operation.Clone())
}
