package kernel

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	"dtm/internal/kernel/capability"
	"dtm/internal/kernel/event"
	"dtm/internal/kernel/execution"
	"dtm/internal/kernel/identity"
	"dtm/internal/kernel/resource"
	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
)

const (
	localRuntimeResourceScope         = "local"
	localRuntimeDeclarationVersion    = "world-a.v1"
	localRuntimeOperationConstraint   = "dtm.k2e.operation_id"
	localRuntimeIdempotencyConstraint = "dtm.k2e.idempotency_mode"
)

var (
	ErrInvalidLocalRuntime       = errors.New("invalid local kernel runtime")
	ErrInvalidResourceEvidence   = errors.New("invalid runtime resource evidence")
	ErrStaleResourceEvidence     = errors.New("stale runtime resource evidence")
	ErrResourceAdmissionConflict = errors.New("resource admission conflicts with existing kernel resource")
	ErrResourceNotAdmitted       = errors.New("resource was not admitted by the local kernel runtime")
	ErrOperationNotDeclared      = errors.New("operation is not declared by the admitted resource")
)

// ResourceAdmission is a transient composition result, not a Kernel Object.
// Evidence preserves the exact World A record that established the local
// Kernel chain; Resource, Declarations, and Instances are authoritative
// manager snapshots created from that record. Declarations and Instances are
// paired by index and are created one-per-declared provider operation.
type ResourceAdmission struct {
	Evidence     resourcedirectory.ResourceRecordView
	Resource     Resource
	Declarations []CapabilityDeclaration
	Instances    []CapabilityInstance
}

func (admission ResourceAdmission) Clone() ResourceAdmission {
	clone := admission
	clone.Evidence = admission.Evidence.Clone()
	clone.Resource = admission.Resource.Clone()
	clone.Declarations = make([]CapabilityDeclaration, len(admission.Declarations))
	for index := range admission.Declarations {
		clone.Declarations[index] = admission.Declarations[index].Clone()
	}
	clone.Instances = make([]CapabilityInstance, len(admission.Instances))
	for index := range admission.Instances {
		clone.Instances[index] = admission.Instances[index].Clone()
	}
	return clone
}

// CapabilityForOperation returns the declaration/instance pair produced for
// one operation in the authoritative World A descriptor.
func (admission ResourceAdmission) CapabilityForOperation(operation string) (CapabilityDeclaration, CapabilityInstance, error) {
	for index, declaration := range admission.Declarations {
		if declaration.Constraints[localRuntimeOperationConstraint] != operation {
			continue
		}
		if index >= len(admission.Instances) {
			return CapabilityDeclaration{}, CapabilityInstance{}, ErrResourceNotAdmitted
		}
		return declaration.Clone(), admission.Instances[index].Clone(), nil
	}
	return CapabilityDeclaration{}, CapabilityInstance{}, ErrOperationNotDeclared
}

// LocalRuntime is the bounded Gate A convergence facade. It coordinates the
// existing Kernel managers and one injected local provider; it does not own a
// new object store, transport, persistence layer, authorization policy, or
// autonomous loop.
type LocalRuntime struct {
	kernel        *Kernel
	resources     *resource.Manager
	capabilities  *capability.Manager
	executions    *execution.Manager
	events        *event.Manager
	directory     *resourcedirectory.Directory
	provider      capability.CapabilityProvider
	resourceScope string

	mu         sync.RWMutex
	admissions map[ResourceID]ResourceAdmission
}

// NewLocalRuntime composes one bounded local Gate A runtime. The directory is
// the existing World A authority source; the provider is an internal local
// seam and is not a transport or a public syscall ABI.
func NewLocalRuntime(
	kernel *Kernel,
	directory *resourcedirectory.Directory,
	provider capability.CapabilityProvider,
) (*LocalRuntime, error) {
	if kernel == nil || kernel.Resources == nil || kernel.Capabilities == nil ||
		kernel.Executions == nil || kernel.Events == nil || directory == nil || isNilCapabilityProvider(provider) {
		return nil, ErrInvalidLocalRuntime
	}
	if err := kernel.SetProvider(&localRuntimeProviderAdapter{provider: provider}); err != nil {
		return nil, fmt.Errorf("configure local provider: %w", err)
	}
	return &LocalRuntime{
		kernel:        kernel,
		resources:     kernel.Resources,
		capabilities:  kernel.Capabilities,
		executions:    kernel.Executions,
		events:        kernel.Events,
		directory:     directory,
		provider:      provider,
		resourceScope: localRuntimeResourceScope,
		admissions:    make(map[ResourceID]ResourceAdmission),
	}, nil
}

// EventManager exposes the existing immutable audit owner for read-only local
// demo evidence. It does not make EventRecord publication a caller operation.
func (runtime *LocalRuntime) EventManager() *event.Manager {
	if runtime == nil {
		return nil
	}
	return runtime.events
}

// AdmitResourceEvidence accepts one current authoritative World A record and
// creates the Kernel Resource/Declaration/Instance chain. The incoming value
// is checked against a fresh Directory lookup so an old generation,
// registration, publication, or eligibility snapshot cannot be admitted.
func (runtime *LocalRuntime) AdmitResourceEvidence(evidence resourcedirectory.ResourceRecordView) (ResourceAdmission, error) {
	if runtime == nil || runtime.resources == nil || runtime.capabilities == nil || runtime.directory == nil {
		return ResourceAdmission{}, ErrInvalidLocalRuntime
	}
	candidate := evidence.Clone()
	if err := validateLocalResourceEvidence(candidate); err != nil {
		return ResourceAdmission{}, err
	}

	current, err := runtime.directory.GetByID(candidate.Descriptor.ID)
	if err != nil {
		return ResourceAdmission{}, fmt.Errorf("%w: authoritative lookup: %v", ErrInvalidResourceEvidence, err)
	}
	if err := validateLocalResourceEvidence(current); err != nil {
		return ResourceAdmission{}, fmt.Errorf("%w: current record: %v", ErrInvalidResourceEvidence, err)
	}
	if !sameLocalResourceEvidence(candidate, current) {
		return ResourceAdmission{}, ErrStaleResourceEvidence
	}

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if existing, exists := runtime.admissions[ResourceID(candidate.Descriptor.ID)]; exists {
		if sameLocalResourceEvidence(existing.Evidence, current) {
			return existing.Clone(), nil
		}
		return ResourceAdmission{}, ErrStaleResourceEvidence
	}

	// Re-read under the local admission lock. This is a bounded freshness
	// check, not a cross-manager transaction or a claim of TOCTOU closure.
	current, err = runtime.directory.GetByID(candidate.Descriptor.ID)
	if err != nil {
		return ResourceAdmission{}, fmt.Errorf("%w: authoritative lookup: %v", ErrInvalidResourceEvidence, err)
	}
	if err := validateLocalResourceEvidence(current); err != nil {
		return ResourceAdmission{}, fmt.Errorf("%w: current record: %v", ErrInvalidResourceEvidence, err)
	}
	if !sameLocalResourceEvidence(candidate, current) {
		return ResourceAdmission{}, ErrStaleResourceEvidence
	}
	candidate = current.Clone()

	if _, err := runtime.resources.GetResource(ResourceID(candidate.Descriptor.ID)); err == nil {
		return ResourceAdmission{}, ErrResourceAdmissionConflict
	} else if !errors.Is(err, resource.ErrResourceNotFound) {
		return ResourceAdmission{}, fmt.Errorf("check existing Kernel resource: %w", err)
	}

	resourceObject, err := runtime.resources.CreateResourceWithID(
		ResourceID(candidate.Descriptor.ID), runtime.resourceScope,
	)
	if err != nil {
		if errors.Is(err, resource.ErrResourceExists) {
			return ResourceAdmission{}, ErrResourceAdmissionConflict
		}
		return ResourceAdmission{}, fmt.Errorf("create Kernel resource: %w", err)
	}

	declarations := make([]CapabilityDeclaration, 0, len(candidate.Descriptor.Operations))
	instances := make([]CapabilityInstance, 0, len(candidate.Descriptor.Operations))
	for _, operation := range candidate.Descriptor.Operations {
		declaration, err := runtime.capabilities.RegisterDeclaration(localDeclarationSpec(
			ResourceID(candidate.Descriptor.ID), candidate.Descriptor, operation,
		))
		if err != nil {
			return ResourceAdmission{}, fmt.Errorf("admit declaration %q: %w", operation.ID, err)
		}
		instance, err := runtime.capabilities.CreateInstance(declaration.ID)
		if err != nil {
			return ResourceAdmission{}, fmt.Errorf("admit instance for declaration %q: %w", declaration.ID, err)
		}
		declarations = append(declarations, declaration.Clone())
		instances = append(instances, instance.Clone())
	}
	resourceObject, err = runtime.resources.GetResource(ResourceID(candidate.Descriptor.ID))
	if err != nil {
		return ResourceAdmission{}, fmt.Errorf("refresh admitted Kernel resource: %w", err)
	}

	admission := ResourceAdmission{
		Evidence:     candidate.Clone(),
		Resource:     resourceObject.Clone(),
		Declarations: declarations,
		Instances:    instances,
	}
	runtime.admissions[resourceObject.ID] = admission.Clone()
	return admission.Clone(), nil
}

// AdmitResource is the concise alias used by the local demo. It accepts the
// same authoritative evidence and has no different semantics.
func (runtime *LocalRuntime) AdmitResource(evidence resourcedirectory.ResourceRecordView) (ResourceAdmission, error) {
	return runtime.AdmitResourceEvidence(evidence)
}

// GetAdmission returns a defensive snapshot of a completed local admission.
// It is evidence/query state, not an authority-bearing object.
func (runtime *LocalRuntime) GetAdmission(id ResourceID) (ResourceAdmission, error) {
	if runtime == nil {
		return ResourceAdmission{}, ErrInvalidLocalRuntime
	}
	if err := id.Validate(); err != nil {
		return ResourceAdmission{}, err
	}
	runtime.mu.RLock()
	admission, exists := runtime.admissions[id]
	runtime.mu.RUnlock()
	if !exists {
		return ResourceAdmission{}, ErrResourceNotAdmitted
	}
	return admission.Clone(), nil
}

func (runtime *LocalRuntime) ListAdmissions() []ResourceAdmission {
	if runtime == nil {
		return nil
	}
	runtime.mu.RLock()
	admissions := make([]ResourceAdmission, 0, len(runtime.admissions))
	for _, admission := range runtime.admissions {
		admissions = append(admissions, admission.Clone())
	}
	runtime.mu.RUnlock()
	sort.Slice(admissions, func(left, right int) bool {
		return admissions[left].Resource.ID < admissions[right].Resource.ID
	})
	return admissions
}

// CreateExecutionContext is an explicit authority-boundary setup operation.
// Resource admission never calls it implicitly.
func (runtime *LocalRuntime) CreateExecutionContext(subject string, scope ...string) (ExecutionContext, error) {
	if runtime == nil || runtime.executions == nil {
		return ExecutionContext{}, ErrInvalidLocalRuntime
	}
	return runtime.executions.CreateContext(subject, scope...)
}

// CreateCapabilityHandle is an explicit authority issuance operation. The
// caller must supply the context, instance, PermissionSet metadata, and an
// optional exact Scope; admission does not mint a Handle.
func (runtime *LocalRuntime) CreateCapabilityHandle(
	contextID ExecutionContextID,
	instanceID CapabilityInstanceID,
	permissions capability.PermissionSet,
	scope ...string,
) (CapabilityHandle, error) {
	if runtime == nil || runtime.capabilities == nil {
		return CapabilityHandle{}, ErrInvalidLocalRuntime
	}
	return runtime.capabilities.CreateHandle(contextID, instanceID, permissions, scope...)
}

type LocalInvocationRequest struct {
	InvocationID       execution.InvocationID
	RootTaskRef        string
	ChildTaskRef       string
	ExecutionContextID ExecutionContextID
	CapabilityHandleID CapabilityHandleID
	CallerIdentity     string
	Scope              string
	Operation          string
	Payload            []byte
}

func (request LocalInvocationRequest) Clone() LocalInvocationRequest {
	request.Payload = append([]byte(nil), request.Payload...)
	return request
}

type LocalInvocationStatus string

const (
	LocalInvocationRejected  LocalInvocationStatus = "REJECTED"
	LocalInvocationCompleted LocalInvocationStatus = "COMPLETED"
	LocalInvocationFailed    LocalInvocationStatus = "FAILED"
	LocalInvocationUnknown   LocalInvocationStatus = "UNKNOWN"
)

type LocalInvocationResult struct {
	InvocationID            execution.InvocationID
	AllocationID            execution.ExecutionAllocationID
	Status                  LocalInvocationStatus
	Reason                  string
	EventID                 EventID
	ExecutionContextID      ExecutionContextID
	CapabilityHandleID      CapabilityHandleID
	CapabilityInstanceID    CapabilityInstanceID
	CapabilityDeclarationID CapabilityDeclarationID
	ResourceID              ResourceID
	Operation               string
	Observation             execution.ExecutionObservation
}

// Invoke is the one canonical local semantic Kernel entry point. It first
// validates the caller/context/Handle chain, then checks the admitted
// operation/evidence, then uses Gate B's bounded allocation/dispatch/release
// path to cross the existing ProviderOperation seam. It does not add task
// scheduling, retry, or autonomous policy.
func (runtime *LocalRuntime) Invoke(request LocalInvocationRequest) (LocalInvocationResult, error) {
	if runtime == nil || runtime.kernel == nil || runtime.capabilities == nil || runtime.directory == nil || runtime.events == nil || isNilCapabilityProvider(runtime.provider) {
		return LocalInvocationResult{}, ErrInvalidLocalRuntime
	}
	request = request.Clone()
	authorityRequest := capability.HandleValidationRequest{
		ExecutionContextID: request.ExecutionContextID,
		CapabilityHandleID: request.CapabilityHandleID,
		CallerIdentity:     request.CallerIdentity,
		Scope:              request.Scope,
	}
	validated, err := runtime.capabilities.ValidateHandleRequest(authorityRequest)
	if err != nil {
		return runtime.rejectInvocation(request, err, nil)
	}

	admission, err := runtime.admissionForValidated(validated)
	if err != nil {
		return runtime.rejectInvocation(request, err, &validated)
	}
	currentEvidence, err := runtime.currentAdmissionEvidence(admission)
	if err != nil {
		return runtime.rejectInvocation(request, err, &validated)
	}

	// This is intentionally after the complete authority validation above.
	// Invalid authority therefore wins over a malformed operation.
	invocationRequest := capability.InvocationRequest{
		Operation: request.Operation,
		Payload:   append([]byte(nil), request.Payload...),
	}
	if err := invocationRequest.Validate(); err != nil {
		return runtime.rejectInvocation(request, err, &validated)
	}
	if err := validateAdmittedOperation(admission, currentEvidence, validated, request.Operation); err != nil {
		return runtime.rejectInvocation(request, err, &validated)
	}

	allocationResult, allocationErr := runtime.kernel.TryAllocateExecution(execution.ExecutionRequest{
		InvocationID:       request.InvocationID,
		RootTaskRef:        request.RootTaskRef,
		ChildTaskRef:       request.ChildTaskRef,
		ExecutionContextID: request.ExecutionContextID,
		CapabilityHandleID: request.CapabilityHandleID,
		CallerIdentity:     request.CallerIdentity,
		Scope:              request.Scope,
		Operation:          request.Operation,
		Payload:            append([]byte(nil), request.Payload...),
	})
	if allocationErr != nil || !allocationResult.Granted {
		if allocationErr == nil {
			allocationErr = fmt.Errorf("execution allocation not granted: %s", allocationResult.Reason)
		}
		return runtime.rejectInvocation(request, allocationErr, &validated)
	}

	allocation := allocationResult.Allocation.Clone()
	observation, dispatchErr := runtime.kernel.DispatchExecution(allocation.ID)
	providerCrossed := runtime.kernel.executionProviderCrossed(allocation.ID)
	status := localInvocationStatusForObservation(observation)
	resultReason := observation.Reason
	if resultReason == "" && dispatchErr != nil {
		resultReason = dispatchErr.Error()
	}
	if resultReason == "" {
		resultReason = strings.ToLower(string(observation.Outcome))
	}
	result := localInvocationResult(request, status, resultReason, &validated)
	result.InvocationID = allocation.Descriptor.InvocationID
	result.AllocationID = allocation.ID
	result.Observation = observation.Clone()
	if observation.Occupancy == execution.OccupancyConclusionEnded {
		if _, releaseErr := runtime.kernel.ReleaseExecution(allocation.ID, "local invocation completed"); releaseErr != nil {
			if result.Reason == "" {
				result.Reason = releaseErr.Error()
			} else {
				result.Reason = fmt.Sprintf("%s; release: %v", result.Reason, releaseErr)
			}
		}
	}
	eventID, auditErr := runtime.publishInvocationAudit(request, &validated, status, providerCrossed)
	if auditErr != nil {
		// The provider outcome is already known. Preserve it alongside the
		// audit error so a caller cannot mistake a post-side-effect audit
		// failure for a pre-dispatch rejection or an empty result.
		result.Reason = fmt.Sprintf("%s; audit: %v", result.Reason, auditErr)
		return result, auditErr
	}
	result.EventID = eventID
	return result, nil
}

func localInvocationStatusForObservation(observation execution.ExecutionObservation) LocalInvocationStatus {
	switch observation.Outcome {
	case execution.ExecutionOutcomeSuccess:
		return LocalInvocationCompleted
	case execution.ExecutionOutcomeUnknown:
		return LocalInvocationUnknown
	default:
		return LocalInvocationFailed
	}
}

func (runtime *LocalRuntime) rejectInvocation(
	request LocalInvocationRequest,
	reason error,
	validated *capability.ValidatedHandle,
) (LocalInvocationResult, error) {
	if reason == nil {
		reason = ErrInvalidLocalRuntime
	}
	result := localInvocationResult(request, LocalInvocationRejected, reason.Error(), validated)
	eventID, auditErr := runtime.publishInvocationAudit(request, validated, LocalInvocationRejected, false)
	if auditErr != nil {
		return LocalInvocationResult{}, auditErr
	}
	result.EventID = eventID
	return result, nil
}

func localInvocationResult(
	request LocalInvocationRequest,
	status LocalInvocationStatus,
	reason string,
	validated *capability.ValidatedHandle,
) LocalInvocationResult {
	result := LocalInvocationResult{
		InvocationID:       request.InvocationID,
		Status:             status,
		Reason:             reason,
		ExecutionContextID: request.ExecutionContextID,
		CapabilityHandleID: request.CapabilityHandleID,
		Operation:          request.Operation,
	}
	if validated != nil {
		result.ExecutionContextID = validated.ExecutionContext.ID
		result.CapabilityHandleID = validated.CapabilityHandle.ID
		result.CapabilityInstanceID = validated.CapabilityInstance.ID
		result.CapabilityDeclarationID = validated.CapabilityDeclaration.ID
		result.ResourceID = validated.Resource.ID
	}
	return result
}

func (runtime *LocalRuntime) admissionForValidated(validated capability.ValidatedHandle) (ResourceAdmission, error) {
	runtime.mu.RLock()
	admission, exists := runtime.admissions[ResourceID(validated.Resource.ID)]
	runtime.mu.RUnlock()
	if !exists {
		return ResourceAdmission{}, ErrResourceNotAdmitted
	}
	if admission.Resource.ID != validated.Resource.ID ||
		ResourceID(admission.Evidence.Descriptor.ID) != validated.Resource.ID {
		return ResourceAdmission{}, capability.ErrRelationshipMismatch
	}
	if err := admission.Resource.Validate(); err != nil ||
		len(admission.Declarations) != len(admission.Instances) ||
		len(admission.Resource.CapabilityDeclarationIDs) != len(admission.Declarations) {
		return ResourceAdmission{}, capability.ErrRelationshipMismatch
	}
	declarationIDs := make(map[CapabilityDeclarationID]struct{}, len(admission.Declarations))
	instanceIDs := make(map[CapabilityInstanceID]struct{}, len(admission.Instances))
	for index, declaration := range admission.Declarations {
		instance := admission.Instances[index]
		if declaration.Validate() != nil || instance.Validate() != nil ||
			!admission.Resource.HasCapabilityDeclaration(declaration.ID) ||
			declaration.ResourceID != admission.Resource.ID ||
			instance.DeclarationID != declaration.ID ||
			instance.ProviderResourceID != admission.Resource.ID {
			return ResourceAdmission{}, capability.ErrRelationshipMismatch
		}
		if _, exists := declarationIDs[declaration.ID]; exists {
			return ResourceAdmission{}, capability.ErrRelationshipMismatch
		}
		if _, exists := instanceIDs[instance.ID]; exists {
			return ResourceAdmission{}, capability.ErrRelationshipMismatch
		}
		declarationIDs[declaration.ID] = struct{}{}
		instanceIDs[instance.ID] = struct{}{}
	}
	for _, declarationID := range admission.Resource.CapabilityDeclarationIDs {
		if _, exists := declarationIDs[declarationID]; !exists {
			return ResourceAdmission{}, capability.ErrRelationshipMismatch
		}
	}
	if _, exists := instanceIDs[validated.CapabilityInstance.ID]; !exists {
		return ResourceAdmission{}, ErrResourceNotAdmitted
	}
	for index, instance := range admission.Instances {
		if instance.ID != validated.CapabilityInstance.ID {
			continue
		}
		if admission.Declarations[index].ID != validated.CapabilityDeclaration.ID {
			return ResourceAdmission{}, capability.ErrRelationshipMismatch
		}
		return admission.Clone(), nil
	}
	return ResourceAdmission{}, ErrResourceNotAdmitted
}

func (runtime *LocalRuntime) currentAdmissionEvidence(admission ResourceAdmission) (resourcedirectory.ResourceRecordView, error) {
	current, err := runtime.directory.GetByID(admission.Evidence.Descriptor.ID)
	if err != nil {
		return resourcedirectory.ResourceRecordView{}, fmt.Errorf("%w: %v", ErrStaleResourceEvidence, err)
	}
	if err := validateLocalResourceEvidence(current); err != nil {
		return resourcedirectory.ResourceRecordView{}, fmt.Errorf("%w: %v", ErrStaleResourceEvidence, err)
	}
	if !sameLocalResourceEvidence(admission.Evidence, current) {
		return resourcedirectory.ResourceRecordView{}, ErrStaleResourceEvidence
	}
	return current, nil
}

func validateAdmittedOperation(
	admission ResourceAdmission,
	evidence resourcedirectory.ResourceRecordView,
	validated capability.ValidatedHandle,
	operation string,
) error {
	operationFound := false
	for _, declared := range evidence.Descriptor.Operations {
		if declared.ID.String() == operation {
			operationFound = true
			break
		}
	}
	if !operationFound {
		return ErrOperationNotDeclared
	}
	for index, instance := range admission.Instances {
		if instance.ID != validated.CapabilityInstance.ID {
			continue
		}
		if index >= len(admission.Declarations) {
			return ErrResourceNotAdmitted
		}
		declaration := admission.Declarations[index]
		if declaration.ID != validated.CapabilityDeclaration.ID || declaration.ResourceID != validated.Resource.ID ||
			declaration.Constraints[localRuntimeOperationConstraint] != operation {
			return capability.ErrRelationshipMismatch
		}
		return nil
	}
	return ErrResourceNotAdmitted
}

func (runtime *LocalRuntime) publishInvocationAudit(
	request LocalInvocationRequest,
	validated *capability.ValidatedHandle,
	status LocalInvocationStatus,
	providerCalled bool,
) (EventID, error) {
	if err := request.CapabilityHandleID.Validate(); err != nil {
		// A malformed Handle reference cannot form a valid affected-object
		// EventRecord. Preserve the rejection without fabricating an audit fact.
		return "", nil
	}
	affected := identity.ObjectReference{
		Kind: identity.ObjectKindCapabilityHandle,
		ID:   request.CapabilityHandleID.String(),
	}
	// Before validation, the requested context is caller-supplied and may not
	// exist. Attribute a rejected request to its structurally valid Handle
	// reference until an authoritative context snapshot is available.
	source := affected
	if validated != nil {
		source = identity.ObjectReference{
			Kind: identity.ObjectKindExecutionContext,
			ID:   validated.ExecutionContext.ID.String(),
		}
	}
	eventType := event.CapabilityHandleInvokeRejected
	result := "rejected"
	if providerCalled {
		eventType = event.CapabilityHandleInvokeAccepted
		switch status {
		case LocalInvocationCompleted:
			result = "completed"
		case LocalInvocationFailed:
			result = "failed"
		case LocalInvocationUnknown:
			result = "unknown"
		}
	}
	record, err := event.NewEventRecord(
		source,
		eventType,
		affected,
		"kernel local runtime invocation",
		result,
	)
	if err != nil {
		return "", err
	}
	stored, err := runtime.events.PublishAndReturn(record)
	if err != nil {
		return "", err
	}
	return stored.ID, nil
}

// localRuntimeProviderAdapter makes the legacy K2 local seam's synchronous
// contract explicit for Gate B: a returned error is a definite terminal
// failure for this in-process adapter. If a caller already supplies the
// observation-capable extension, its explicit uncertainty is preserved.
type localRuntimeProviderAdapter struct {
	provider capability.CapabilityProvider
}

func (adapter *localRuntimeProviderAdapter) Invoke(operation capability.ProviderOperation) error {
	return adapter.provider.Invoke(operation)
}

func (adapter *localRuntimeProviderAdapter) InvokeWithObservation(operation capability.ProviderOperation) (execution.ExecutionObservation, error) {
	if observable, ok := adapter.provider.(capability.ObservableCapabilityProvider); ok {
		return observable.InvokeWithObservation(operation)
	}
	err := adapter.provider.Invoke(operation)
	observation := execution.ExecutionObservation{
		Outcome:   execution.ExecutionOutcomeSuccess,
		Occupancy: execution.OccupancyConclusionEnded,
		Reason:    "provider completed",
	}
	if err != nil {
		observation.Outcome = execution.ExecutionOutcomeFailed
		observation.Reason = err.Error()
	}
	return observation, err
}

func localDeclarationSpec(
	resourceID ResourceID,
	descriptor model.ResourceDescriptor,
	operation model.OperationDescriptor,
) capability.DeclarationSpec {
	inputMetadata := map[string]string{}
	if operation.InputSchemaID != "" {
		inputMetadata["schema_id"] = operation.InputSchemaID
	}
	outputMetadata := map[string]string{}
	if operation.OutputSchemaID != "" {
		outputMetadata["schema_id"] = operation.OutputSchemaID
	}
	return capability.DeclarationSpec{
		ResourceID:     resourceID,
		Name:           fmt.Sprintf("%s.%s", descriptor.Type, operation.ID),
		Version:        localRuntimeDeclarationVersion,
		InputMetadata:  inputMetadata,
		OutputMetadata: outputMetadata,
		Constraints: map[string]string{
			localRuntimeOperationConstraint:   operation.ID.String(),
			localRuntimeIdempotencyConstraint: string(operation.IdempotencyMode),
		},
	}
}

func validateLocalResourceEvidence(evidence resourcedirectory.ResourceRecordView) error {
	if err := evidence.Descriptor.Validate(); err != nil {
		return fmt.Errorf("%w: descriptor: %v", ErrInvalidResourceEvidence, err)
	}
	if _, err := model.NewResourceRef(
		evidence.Descriptor.ID,
		evidence.Descriptor.Generation,
		evidence.Descriptor.OwnerNodeID,
		evidence.NodeGeneration,
		evidence.RegistrationID,
	); err != nil {
		return fmt.Errorf("%w: resource fence: %v", ErrInvalidResourceEvidence, err)
	}
	if evidence.PublicationState != resourcedirectory.PublicationStatePublished {
		return fmt.Errorf("%w: publication state %q is not published", ErrInvalidResourceEvidence, evidence.PublicationState)
	}
	if !evidence.Eligible || evidence.IneligibleReason != resourcedirectory.IneligibleReasonNone {
		return fmt.Errorf("%w: resource is not currently eligible", ErrInvalidResourceEvidence)
	}
	return nil
}

func sameLocalResourceEvidence(left, right resourcedirectory.ResourceRecordView) bool {
	if left.NodeGeneration != right.NodeGeneration || left.RegistrationID != right.RegistrationID ||
		left.PublicationState != right.PublicationState || left.Eligible != right.Eligible ||
		left.IneligibleReason != right.IneligibleReason {
		return false
	}
	return sameResourceDescriptor(left.Descriptor, right.Descriptor)
}

func sameResourceDescriptor(left, right model.ResourceDescriptor) bool {
	if left.ID != right.ID || left.Kind != right.Kind || left.Type != right.Type ||
		left.OwnerNodeID != right.OwnerNodeID || left.Generation != right.Generation {
		return false
	}
	leftOperations := left.CanonicalOperations()
	rightOperations := right.CanonicalOperations()
	if len(leftOperations) != len(rightOperations) {
		return false
	}
	for index := range leftOperations {
		if leftOperations[index] != rightOperations[index] {
			return false
		}
	}
	if len(left.Attributes) != len(right.Attributes) {
		return false
	}
	for key, value := range left.Attributes {
		rightValue, exists := right.Attributes[key]
		if !exists || rightValue != value {
			return false
		}
	}
	return true
}

func isNilCapabilityProvider(provider capability.CapabilityProvider) bool {
	if provider == nil {
		return true
	}
	value := reflect.ValueOf(provider)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
