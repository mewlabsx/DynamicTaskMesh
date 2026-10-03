package capability

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"dtm/internal/kernel/identity"
)

type ResourceID = identity.ResourceID
type CapabilityDeclarationID = identity.CapabilityDeclarationID
type CapabilityInstanceID = identity.CapabilityInstanceID
type CapabilityHandleID = identity.CapabilityHandleID
type ExecutionContextID = identity.ExecutionContextID
type AvailabilityState = identity.AvailabilityState
type EvaluationState = identity.EvaluationState

type InstanceState string
type HandleState string

// PermissionSet 仅承载权限元数据并执行结构性校验。
// K0.5-R1 不在此处实现完整授权、策略解释或正式权限命名空间。
type PermissionSet = []string

const (
	InstanceStateCreated   InstanceState = "CREATED"
	InstanceStateActive    InstanceState = "ACTIVE"
	InstanceStateSuspended InstanceState = "SUSPENDED"
	InstanceStateRevoked   InstanceState = "REVOKED"
	InstanceStateDestroyed InstanceState = "DESTROYED"

	HandleStateCreated   HandleState = "CREATED"
	HandleStateActive    HandleState = "ACTIVE"
	HandleStateSuspended HandleState = "SUSPENDED"
	HandleStateExpired   HandleState = "EXPIRED"
	HandleStateRevoked   HandleState = "REVOKED"
	HandleStateDestroyed HandleState = "DESTROYED"

	AvailabilityAvailable   AvailabilityState = identity.AvailabilityAvailable
	AvailabilityDegraded    AvailabilityState = identity.AvailabilityDegraded
	AvailabilityUnavailable AvailabilityState = identity.AvailabilityUnavailable

	CapabilityInstanceStateCreated   = InstanceStateCreated
	CapabilityInstanceStateActive    = InstanceStateActive
	CapabilityInstanceStateSuspended = InstanceStateSuspended
	CapabilityInstanceStateRevoked   = InstanceStateRevoked
	CapabilityInstanceStateDestroyed = InstanceStateDestroyed
	CapabilityHandleStateCreated     = HandleStateCreated
	CapabilityHandleStateActive      = HandleStateActive
	CapabilityHandleStateSuspended   = HandleStateSuspended
	CapabilityHandleStateExpired     = HandleStateExpired
	CapabilityHandleStateRevoked     = HandleStateRevoked
	CapabilityHandleStateDestroyed   = HandleStateDestroyed
)

var (
	ErrInvalidDeclaration     = errors.New("invalid capability declaration")
	ErrDeclarationNotFound    = errors.New("capability declaration not found")
	ErrInvalidInstance        = errors.New("invalid capability instance")
	ErrInstanceNotFound       = errors.New("capability instance not found")
	ErrInstanceUnavailable    = errors.New("capability instance is unavailable")
	ErrInstanceRevoked        = errors.New("capability instance is revoked")
	ErrInvalidHandle          = errors.New("invalid capability handle")
	ErrHandleNotFound         = errors.New("capability handle not found")
	ErrHandleRevoked          = errors.New("capability handle is revoked")
	ErrHandleExpired          = errors.New("capability handle is expired")
	ErrHandleDestroyed        = errors.New("capability handle is destroyed")
	ErrInvalidPermission      = errors.New("invalid capability permission set")
	ErrScopeMismatch          = errors.New("capability handle scope does not match execution context scope")
	ErrContextMismatch        = errors.New("execution context does not own capability handle")
	ErrCallerIdentityMismatch = errors.New("caller identity does not match execution context subject")
	ErrRelationshipMismatch   = errors.New("capability relationship mismatch")
	ErrResourceUnavailable    = errors.New("provider resource is unavailable")
	ErrInvalidDependency      = errors.New("invalid capability manager dependency")
	ErrContextUnavailable     = errors.New("execution context is unavailable")
	ErrInvalidInvocation      = errors.New("invalid capability invocation")
)

type DeclarationSpec struct {
	ResourceID     ResourceID
	Name           string
	Version        string
	InputMetadata  map[string]string
	OutputMetadata map[string]string
	Constraints    map[string]string
}

type CapabilityDeclarationSpec = DeclarationSpec

type CapabilityDeclaration struct {
	ID             CapabilityDeclarationID
	ResourceID     ResourceID
	Name           string
	Version        string
	InputMetadata  map[string]string
	OutputMetadata map[string]string
	Constraints    map[string]string
}

func NewCapabilityDeclaration(
	id CapabilityDeclarationID,
	resourceID ResourceID,
	name, version string,
	inputMetadata, outputMetadata, constraints map[string]string,
) (CapabilityDeclaration, error) {
	declaration := CapabilityDeclaration{
		ID:             id,
		ResourceID:     resourceID,
		Name:           name,
		Version:        version,
		InputMetadata:  cloneStringMap(inputMetadata),
		OutputMetadata: cloneStringMap(outputMetadata),
		Constraints:    cloneStringMap(constraints),
	}
	if err := declaration.Validate(); err != nil {
		return CapabilityDeclaration{}, err
	}
	return declaration, nil
}

func (declaration CapabilityDeclaration) Validate() error {
	if err := declaration.ID.Validate(); err != nil {
		return fmt.Errorf("%w: identity: %v", ErrInvalidDeclaration, err)
	}
	if err := declaration.ResourceID.Validate(); err != nil {
		return fmt.Errorf("%w: resource identity: %v", ErrInvalidDeclaration, err)
	}
	if strings.TrimSpace(declaration.Name) == "" || strings.ContainsRune(declaration.Name, '\x00') {
		return fmt.Errorf("%w: capability name is required", ErrInvalidDeclaration)
	}
	if strings.TrimSpace(declaration.Version) == "" || strings.ContainsRune(declaration.Version, '\x00') {
		return fmt.Errorf("%w: version is required", ErrInvalidDeclaration)
	}
	if err := validateMetadata(declaration.InputMetadata); err != nil {
		return fmt.Errorf("%w: input metadata: %v", ErrInvalidDeclaration, err)
	}
	if err := validateMetadata(declaration.OutputMetadata); err != nil {
		return fmt.Errorf("%w: output metadata: %v", ErrInvalidDeclaration, err)
	}
	if err := validateMetadata(declaration.Constraints); err != nil {
		return fmt.Errorf("%w: constraints: %v", ErrInvalidDeclaration, err)
	}
	return nil
}

func (declaration CapabilityDeclaration) Clone() CapabilityDeclaration {
	clone := declaration
	clone.InputMetadata = cloneStringMap(declaration.InputMetadata)
	clone.OutputMetadata = cloneStringMap(declaration.OutputMetadata)
	clone.Constraints = cloneStringMap(declaration.Constraints)
	return clone
}

type CapabilityInstance struct {
	ID                 CapabilityInstanceID
	DeclarationID      CapabilityDeclarationID
	ProviderResourceID ResourceID
	State              InstanceState
	Availability       AvailabilityState
}

func NewCapabilityInstance(
	id CapabilityInstanceID,
	declarationID CapabilityDeclarationID,
	providerResourceID ResourceID,
	state InstanceState,
) (CapabilityInstance, error) {
	instance := CapabilityInstance{
		ID:                 id,
		DeclarationID:      declarationID,
		ProviderResourceID: providerResourceID,
		State:              state,
		Availability:       availabilityForInstanceState(state),
	}
	if err := instance.Validate(); err != nil {
		return CapabilityInstance{}, err
	}
	return instance, nil
}

func (instance CapabilityInstance) Validate() error {
	if err := instance.ID.Validate(); err != nil {
		return fmt.Errorf("%w: identity: %v", ErrInvalidInstance, err)
	}
	if err := instance.DeclarationID.Validate(); err != nil {
		return fmt.Errorf("%w: declaration identity: %v", ErrInvalidInstance, err)
	}
	if err := instance.ProviderResourceID.Validate(); err != nil {
		return fmt.Errorf("%w: provider identity: %v", ErrInvalidInstance, err)
	}
	if !isValidInstanceState(instance.State) {
		return fmt.Errorf("%w: state %q", ErrInvalidInstance, instance.State)
	}
	if err := instance.Availability.Validate(); err != nil {
		return fmt.Errorf("%w: availability: %v", ErrInvalidInstance, err)
	}
	return nil
}

func (instance CapabilityInstance) IsAvailable() bool {
	return (instance.State == InstanceStateCreated || instance.State == InstanceStateActive) &&
		instance.Availability != AvailabilityUnavailable
}

func (instance CapabilityInstance) Clone() CapabilityInstance {
	return instance
}

type CapabilityHandle struct {
	ID                         CapabilityHandleID
	SubjectExecutionContextID  ExecutionContextID
	TargetCapabilityInstanceID CapabilityInstanceID
	Permissions                PermissionSet
	Scope                      string
	State                      HandleState
	Availability               AvailabilityState
}

func NewCapabilityHandle(
	id CapabilityHandleID,
	subjectID ExecutionContextID,
	targetID CapabilityInstanceID,
	permissions PermissionSet,
	scope string,
	state HandleState,
) (CapabilityHandle, error) {
	handle := CapabilityHandle{
		ID:                         id,
		SubjectExecutionContextID:  subjectID,
		TargetCapabilityInstanceID: targetID,
		Permissions:                normalizePermissions(permissions),
		Scope:                      scope,
		State:                      state,
		Availability:               availabilityForHandleState(state),
	}
	if err := handle.Validate(); err != nil {
		return CapabilityHandle{}, err
	}
	return handle, nil
}

func (handle CapabilityHandle) Validate() error {
	if err := handle.ID.Validate(); err != nil {
		return fmt.Errorf("%w: identity: %v", ErrInvalidHandle, err)
	}
	if err := handle.SubjectExecutionContextID.Validate(); err != nil {
		return fmt.Errorf("%w: subject context: %v", ErrInvalidHandle, err)
	}
	if err := handle.TargetCapabilityInstanceID.Validate(); err != nil {
		return fmt.Errorf("%w: target instance: %v", ErrInvalidHandle, err)
	}
	if len(handle.Permissions) == 0 {
		return fmt.Errorf("%w: at least one permission is required", ErrInvalidPermission)
	}
	seen := make(map[string]struct{}, len(handle.Permissions))
	for _, permission := range handle.Permissions {
		if !utf8.ValidString(permission) || strings.TrimSpace(permission) == "" || strings.ContainsRune(permission, '\x00') {
			return fmt.Errorf("%w: blank permission", ErrInvalidPermission)
		}
		if _, exists := seen[permission]; exists {
			return fmt.Errorf("%w: duplicate permission %q", ErrInvalidPermission, permission)
		}
		seen[permission] = struct{}{}
	}
	if strings.TrimSpace(handle.Scope) == "" || strings.ContainsRune(handle.Scope, '\x00') {
		return fmt.Errorf("%w: scope is required", ErrInvalidHandle)
	}
	if !isValidHandleState(handle.State) {
		return fmt.Errorf("%w: state %q", ErrInvalidHandle, handle.State)
	}
	if err := handle.Availability.Validate(); err != nil {
		return fmt.Errorf("%w: availability: %v", ErrInvalidHandle, err)
	}
	return nil
}

func (handle CapabilityHandle) Clone() CapabilityHandle {
	clone := handle
	clone.Permissions = append(PermissionSet(nil), handle.Permissions...)
	return clone
}

func (handle CapabilityHandle) IsUsable() bool {
	return (handle.State == HandleStateCreated || handle.State == HandleStateActive) &&
		handle.Availability != AvailabilityUnavailable
}

func isValidInstanceState(state InstanceState) bool {
	switch state {
	case InstanceStateCreated, InstanceStateActive, InstanceStateSuspended, InstanceStateRevoked, InstanceStateDestroyed:
		return true
	default:
		return false
	}
}

func isValidHandleState(state HandleState) bool {
	switch state {
	case HandleStateCreated, HandleStateActive, HandleStateSuspended, HandleStateExpired, HandleStateRevoked, HandleStateDestroyed:
		return true
	default:
		return false
	}
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func validateMetadata(values map[string]string) error {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := values[key]
		if strings.TrimSpace(key) == "" || strings.ContainsRune(key, '\x00') || strings.ContainsRune(value, '\x00') {
			return errors.New("metadata keys and values must be non-empty and NUL-free")
		}
	}
	return nil
}

func normalizePermissions(values PermissionSet) PermissionSet {
	if values == nil {
		return nil
	}
	clone := append(PermissionSet(nil), values...)
	sort.Strings(clone)
	return clone
}

func availabilityForInstanceState(state InstanceState) AvailabilityState {
	switch state {
	case InstanceStateCreated, InstanceStateActive:
		return AvailabilityAvailable
	default:
		return AvailabilityUnavailable
	}
}

func availabilityForHandleState(state HandleState) AvailabilityState {
	switch state {
	case HandleStateCreated, HandleStateActive:
		return AvailabilityAvailable
	default:
		return AvailabilityUnavailable
	}
}

type InvocationRequest struct {
	Operation string
	Payload   []byte
}

func (request InvocationRequest) Validate() error {
	if strings.TrimSpace(request.Operation) == "" || strings.ContainsRune(request.Operation, '\x00') {
		return fmt.Errorf("%w: operation is required", ErrInvalidInvocation)
	}
	return nil
}

func (request InvocationRequest) Clone() InvocationRequest {
	request.Payload = append([]byte(nil), request.Payload...)
	return request
}

type InvocationResult struct {
	Accepted bool
	Reason   string
}
