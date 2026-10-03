package kernel_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"dtm/internal/kernel"
	"dtm/internal/kernel/capability"
	"dtm/internal/kernel/event"
	"dtm/internal/kernel/identity"
	"dtm/internal/kernel/resource"
	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/resourcedirectory"
)

const (
	k2eNodeID         model.NodeID = "k2e-node"
	k2eRegistrationID              = "k2e-registration-1"
	k2eSubject                     = "k2e-caller"
)

type k2eProvider struct {
	calls      int
	operations []capability.ProviderOperation
	err        error
}

func (provider *k2eProvider) Invoke(operation capability.ProviderOperation) error {
	provider.calls++
	provider.operations = append(provider.operations, operation.Clone())
	return provider.err
}

type k2eFixture struct {
	kernel     *kernel.Kernel
	directory  *resourcedirectory.Directory
	runtime    *kernel.LocalRuntime
	provider   *k2eProvider
	descriptor model.ResourceDescriptor
	evidence   resourcedirectory.ResourceRecordView
	admission  kernel.ResourceAdmission
	context    kernel.ExecutionContext
	handle     kernel.CapabilityHandle
}

func newK2ERuntimeFixture(t *testing.T) *k2eFixture {
	t.Helper()

	descriptor, err := model.AdaptLegacyCapability(k2eNodeID, "temperature_sensor", 1)
	if err != nil {
		t.Fatalf("AdaptLegacyCapability() error = %v", err)
	}
	directory := resourcedirectory.New()
	snapshot, err := resourcedirectory.NewNodeResourceSnapshot(
		k2eNodeID,
		1,
		k2eRegistrationID,
		[]model.ResourceDescriptor{descriptor},
	)
	if err != nil {
		t.Fatalf("NewNodeResourceSnapshot() error = %v", err)
	}
	if _, err := directory.ApplyNodeSnapshot(snapshot); err != nil {
		t.Fatalf("ApplyNodeSnapshot() error = %v", err)
	}
	lifecycle, err := resourcedirectory.NewNodeLifecycleView(
		k2eNodeID,
		1,
		k2eRegistrationID,
		node.StatusActive,
		true,
		true,
	)
	if err != nil {
		t.Fatalf("NewNodeLifecycleView() error = %v", err)
	}
	if err := directory.ActivateNodeLifecycle(lifecycle); err != nil {
		t.Fatalf("ActivateNodeLifecycle() error = %v", err)
	}
	evidence, err := directory.GetByID(descriptor.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	k := kernel.New()
	provider := &k2eProvider{}
	runtime, err := kernel.NewLocalRuntime(k, directory, provider)
	if err != nil {
		t.Fatalf("NewLocalRuntime() error = %v", err)
	}
	return &k2eFixture{
		kernel:     k,
		directory:  directory,
		runtime:    runtime,
		provider:   provider,
		descriptor: descriptor,
		evidence:   evidence,
	}
}

func newK2EFixture(t *testing.T) *k2eFixture {
	t.Helper()
	fixture := newK2ERuntimeFixture(t)
	admission, err := fixture.runtime.AdmitResourceEvidence(fixture.evidence)
	if err != nil {
		t.Fatalf("AdmitResourceEvidence() error = %v", err)
	}
	context, err := fixture.runtime.CreateExecutionContext(k2eSubject, "local")
	if err != nil {
		t.Fatalf("CreateExecutionContext() error = %v", err)
	}
	if len(admission.Instances) != 1 {
		t.Fatalf("admission.Instances length = %d, want 1", len(admission.Instances))
	}
	handle, err := fixture.runtime.CreateCapabilityHandle(
		context.ID,
		admission.Instances[0].ID,
		capability.PermissionSet{"k2e.invoke"},
		context.Scope,
	)
	if err != nil {
		t.Fatalf("CreateCapabilityHandle() error = %v", err)
	}
	fixture.admission = admission
	fixture.context = context
	fixture.handle = handle
	return fixture
}

func (fixture *k2eFixture) request(operation string) kernel.LocalInvocationRequest {
	return kernel.LocalInvocationRequest{
		ExecutionContextID: fixture.context.ID,
		CapabilityHandleID: fixture.handle.ID,
		CallerIdentity:     fixture.context.Subject,
		Scope:              fixture.context.Scope,
		Operation:          operation,
		Payload:            []byte("k2e-payload"),
	}
}

func TestK2EAdmissionBuildsKernelChainFromWorldAEvidence(t *testing.T) {
	fixture := newK2ERuntimeFixture(t)
	admission, err := fixture.runtime.AdmitResourceEvidence(fixture.evidence)
	if err != nil {
		t.Fatalf("AdmitResourceEvidence() error = %v", err)
	}
	if admission.Resource.ID != kernel.ResourceID(fixture.descriptor.ID) {
		t.Fatalf("admitted Resource.ID = %q, want %q", admission.Resource.ID, fixture.descriptor.ID)
	}
	if !reflect.DeepEqual(admission.Evidence, fixture.evidence) {
		t.Fatalf("admission did not preserve the authoritative World A evidence")
	}
	if len(admission.Declarations) != 1 || len(admission.Instances) != 1 {
		t.Fatalf("admission chain lengths = declarations %d, instances %d; want 1, 1", len(admission.Declarations), len(admission.Instances))
	}
	declaration := admission.Declarations[0]
	instance := admission.Instances[0]
	if declaration.ResourceID != admission.Resource.ID {
		t.Fatalf("declaration ResourceID = %q, want %q", declaration.ResourceID, admission.Resource.ID)
	}
	if instance.DeclarationID != declaration.ID || instance.ProviderResourceID != admission.Resource.ID {
		t.Fatalf("instance relationship = declaration %q/resource %q, want declaration %q/resource %q", instance.DeclarationID, instance.ProviderResourceID, declaration.ID, admission.Resource.ID)
	}
	if declaration.Constraints["dtm.k2e.operation_id"] != "read_temperature" {
		t.Fatalf("operation constraint = %q, want read_temperature", declaration.Constraints["dtm.k2e.operation_id"])
	}
	if got := len(fixture.kernel.Capabilities.ListHandles()); got != 0 {
		t.Fatalf("admission created %d handles; Resource admission must not mint authority", got)
	}
	if got := len(fixture.runtime.ListAdmissions()); got != 1 {
		t.Fatalf("ListAdmissions() length = %d, want 1", got)
	}
	repeated, err := fixture.runtime.AdmitResourceEvidence(fixture.evidence)
	if err != nil {
		t.Fatalf("repeated AdmitResourceEvidence() error = %v", err)
	}
	if !reflect.DeepEqual(admission, repeated) {
		t.Fatal("repeated identical evidence did not converge to the same admission snapshot")
	}
	if got := len(fixture.kernel.Capabilities.ListDeclarations()); got != 1 {
		t.Fatalf("repeated admission created %d declarations, want 1", got)
	}
	if got := len(fixture.kernel.Capabilities.ListInstances()); got != 1 {
		t.Fatalf("repeated admission created %d instances, want 1", got)
	}
}

func TestK2EAdmissionRejectsStaleWorldAEvidence(t *testing.T) {
	fixture := newK2ERuntimeFixture(t)
	nextSnapshot, err := resourcedirectory.NewNodeResourceSnapshot(
		k2eNodeID,
		2,
		"k2e-registration-2",
		[]model.ResourceDescriptor{fixture.descriptor},
	)
	if err != nil {
		t.Fatalf("NewNodeResourceSnapshot() error = %v", err)
	}
	if _, err := fixture.directory.ApplyNodeSnapshot(nextSnapshot); err != nil {
		t.Fatalf("ApplyNodeSnapshot(next) error = %v", err)
	}
	lifecycle, err := resourcedirectory.NewNodeLifecycleView(k2eNodeID, 2, "k2e-registration-2", node.StatusActive, true, true)
	if err != nil {
		t.Fatalf("NewNodeLifecycleView(next) error = %v", err)
	}
	if err := fixture.directory.ActivateNodeLifecycle(lifecycle); err != nil {
		t.Fatalf("ActivateNodeLifecycle(next) error = %v", err)
	}
	if _, err := fixture.runtime.AdmitResourceEvidence(fixture.evidence); !errors.Is(err, kernel.ErrStaleResourceEvidence) {
		t.Fatalf("AdmitResourceEvidence(stale) error = %v, want ErrStaleResourceEvidence", err)
	}
	if got := len(fixture.kernel.Resources.ListResources()); got != 0 {
		t.Fatalf("stale admission created %d Kernel resources", got)
	}
}

func TestK2EAdmissionRejectsIneligibleEvidence(t *testing.T) {
	fixture := newK2ERuntimeFixture(t)
	fixture.directory.QuiesceNode(k2eNodeID, resourcedirectory.IneligibleReasonNodeLifecycleUnavailable)
	ineligible, err := fixture.directory.GetByID(fixture.descriptor.ID)
	if err != nil {
		t.Fatalf("GetByID(ineligible) error = %v", err)
	}
	if _, err := fixture.runtime.AdmitResourceEvidence(ineligible); !errors.Is(err, kernel.ErrInvalidResourceEvidence) {
		t.Fatalf("AdmitResourceEvidence(ineligible) error = %v, want ErrInvalidResourceEvidence", err)
	}
	if got := len(fixture.kernel.Resources.ListResources()); got != 0 {
		t.Fatalf("ineligible admission created %d Kernel resources", got)
	}
}

func TestK2EAdmissionDoesNotGrantAuthority(t *testing.T) {
	fixture := newK2ERuntimeFixture(t)
	if _, err := fixture.runtime.AdmitResourceEvidence(fixture.evidence); err != nil {
		t.Fatalf("AdmitResourceEvidence() error = %v", err)
	}
	if got := len(fixture.kernel.Capabilities.ListHandles()); got != 0 {
		t.Fatalf("admission created %d handles; want 0", got)
	}
	context, err := fixture.runtime.CreateExecutionContext(k2eSubject, "local")
	if err != nil {
		t.Fatalf("CreateExecutionContext() error = %v", err)
	}
	result, err := fixture.runtime.Invoke(kernel.LocalInvocationRequest{
		ExecutionContextID: context.ID,
		CapabilityHandleID: capability.CapabilityHandleID("missing-handle"),
		CallerIdentity:     context.Subject,
		Scope:              context.Scope,
		Operation:          "read_temperature",
	})
	if err != nil {
		t.Fatalf("Invoke(without handle) error = %v", err)
	}
	if result.Status != kernel.LocalInvocationRejected || fixture.provider.calls != 0 {
		t.Fatalf("Invoke(without handle) = status %q/calls %d, want REJECTED/0", result.Status, fixture.provider.calls)
	}
}

func TestK2EAuthorityValidationPrecedesOperationValidation(t *testing.T) {
	fixture := newK2ERuntimeFixture(t)
	context, err := fixture.runtime.CreateExecutionContext(k2eSubject, "local")
	if err != nil {
		t.Fatalf("CreateExecutionContext() error = %v", err)
	}
	result, err := fixture.runtime.Invoke(kernel.LocalInvocationRequest{
		ExecutionContextID: context.ID,
		CapabilityHandleID: capability.CapabilityHandleID("missing-handle"),
		CallerIdentity:     context.Subject,
		Scope:              context.Scope,
		Operation:          "\x00invalid-operation",
	})
	if err != nil {
		t.Fatalf("Invoke(invalid authority and operation) error = %v", err)
	}
	if result.Status != kernel.LocalInvocationRejected || fixture.provider.calls != 0 {
		t.Fatalf("Invoke(invalid authority and operation) = status %q/calls %d, want REJECTED/0", result.Status, fixture.provider.calls)
	}
	if strings.Contains(result.Reason, "invalid capability invocation") {
		t.Fatalf("operation validation ran before authority validation: reason = %q", result.Reason)
	}
	if !strings.Contains(result.Reason, "handle") {
		t.Fatalf("authority-first rejection reason = %q, want handle failure", result.Reason)
	}
}

func TestK2EValidInvocationCallsProviderOnceAndAudits(t *testing.T) {
	fixture := newK2EFixture(t)
	result, err := fixture.runtime.Invoke(fixture.request("read_temperature"))
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}
	if result.Status != kernel.LocalInvocationCompleted {
		t.Fatalf("Invoke() status = %q, want COMPLETED; reason = %q", result.Status, result.Reason)
	}
	if fixture.provider.calls != 1 {
		t.Fatalf("provider calls = %d, want exactly 1", fixture.provider.calls)
	}
	if len(fixture.provider.operations) != 1 || fixture.provider.operations[0].Operation != "read_temperature" {
		t.Fatalf("provider operations = %#v, want one read_temperature operation", fixture.provider.operations)
	}
	if result.EventID == "" {
		t.Fatal("Invoke() returned an empty audit EventID")
	}
	records := fixture.runtime.EventManager().Query(event.QueryFilter{
		EventType: event.CapabilityHandleInvokeAccepted,
		AffectedObject: identity.ObjectReference{
			Kind: identity.ObjectKindCapabilityHandle,
			ID:   fixture.handle.ID.String(),
		},
	})
	if len(records) != 1 {
		t.Fatalf("accepted invocation audit records = %d, want 1", len(records))
	}
	record := records[0]
	if record.ID != result.EventID || record.Result != "completed" {
		t.Fatalf("audit = id %q/result %q, want id %q/result completed", record.ID, record.Result, result.EventID)
	}
	if record.SourceObject != (identity.ObjectReference{Kind: identity.ObjectKindExecutionContext, ID: fixture.context.ID.String()}) {
		t.Fatalf("audit source = %#v, want execution context %q", record.SourceObject, fixture.context.ID)
	}
}

func TestK2EProviderFailureIsDeterministicAndNotRetried(t *testing.T) {
	fixture := newK2EFixture(t)
	fixture.provider.err = errors.New("controlled provider failure")
	result, err := fixture.runtime.Invoke(fixture.request("read_temperature"))
	if err != nil {
		t.Fatalf("Invoke(provider failure) error = %v", err)
	}
	if result.Status != kernel.LocalInvocationFailed || fixture.provider.calls != 1 {
		t.Fatalf("Invoke(provider failure) = status %q/calls %d, want FAILED/1", result.Status, fixture.provider.calls)
	}
	if !strings.Contains(result.Reason, "controlled provider failure") {
		t.Fatalf("failure reason = %q, want provider error", result.Reason)
	}
	records := fixture.runtime.EventManager().Query(event.QueryFilter{
		EventType: event.CapabilityHandleInvokeAccepted,
		AffectedObject: identity.ObjectReference{
			Kind: identity.ObjectKindCapabilityHandle,
			ID:   fixture.handle.ID.String(),
		},
	})
	if len(records) != 1 || records[0].Result != "failed" {
		t.Fatalf("provider failure audit = %#v, want one accepted/failed record", records)
	}
}

func TestK2EAuthorityFailuresNeverCallProvider(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*k2eFixture) kernel.LocalInvocationRequest
	}{
		{
			name: "wrong context",
			mutate: func(fixture *k2eFixture) kernel.LocalInvocationRequest {
				other, err := fixture.runtime.CreateExecutionContext("other-caller", "local")
				if err != nil {
					panic(err)
				}
				request := fixture.request("read_temperature")
				request.ExecutionContextID = other.ID
				request.CallerIdentity = other.Subject
				return request
			},
		},
		{
			name: "scope mismatch",
			mutate: func(fixture *k2eFixture) kernel.LocalInvocationRequest {
				request := fixture.request("read_temperature")
				request.Scope = "other-scope"
				return request
			},
		},
		{
			name: "revoked handle",
			mutate: func(fixture *k2eFixture) kernel.LocalInvocationRequest {
				if err := fixture.kernel.Capabilities.RevokeHandle(fixture.handle.ID); err != nil {
					panic(err)
				}
				return fixture.request("read_temperature")
			},
		},
		{
			name: "destroyed handle",
			mutate: func(fixture *k2eFixture) kernel.LocalInvocationRequest {
				if err := fixture.kernel.Capabilities.ReleaseHandle(fixture.handle.ID); err != nil {
					panic(err)
				}
				return fixture.request("read_temperature")
			},
		},
		{
			name: "revoked instance",
			mutate: func(fixture *k2eFixture) kernel.LocalInvocationRequest {
				if err := fixture.kernel.Capabilities.UpdateInstanceState(fixture.admission.Instances[0].ID, capability.InstanceStateRevoked); err != nil {
					panic(err)
				}
				return fixture.request("read_temperature")
			},
		},
		{
			name: "destroyed instance",
			mutate: func(fixture *k2eFixture) kernel.LocalInvocationRequest {
				if err := fixture.kernel.Capabilities.UpdateInstanceState(fixture.admission.Instances[0].ID, capability.InstanceStateDestroyed); err != nil {
					panic(err)
				}
				return fixture.request("read_temperature")
			},
		},
		{
			name: "suspended instance",
			mutate: func(fixture *k2eFixture) kernel.LocalInvocationRequest {
				if err := fixture.kernel.Capabilities.UpdateInstanceState(fixture.admission.Instances[0].ID, capability.InstanceStateSuspended); err != nil {
					panic(err)
				}
				return fixture.request("read_temperature")
			},
		},
		{
			name: "unavailable resource",
			mutate: func(fixture *k2eFixture) kernel.LocalInvocationRequest {
				if err := fixture.kernel.Resources.UpdateAvailability(fixture.admission.Resource.ID, resource.AvailabilityUnavailable); err != nil {
					panic(err)
				}
				return fixture.request("read_temperature")
			},
		},
		{
			name: "terminated context",
			mutate: func(fixture *k2eFixture) kernel.LocalInvocationRequest {
				if err := fixture.kernel.Executions.TerminateContext(fixture.context.ID); err != nil {
					panic(err)
				}
				return fixture.request("read_temperature")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newK2EFixture(t)
			request := test.mutate(fixture)
			result, err := fixture.runtime.Invoke(request)
			if err != nil {
				t.Fatalf("Invoke() error = %v", err)
			}
			if result.Status != kernel.LocalInvocationRejected {
				t.Fatalf("Invoke() status = %q, want REJECTED; reason = %q", result.Status, result.Reason)
			}
			if fixture.provider.calls != 0 {
				t.Fatalf("provider calls = %d, want 0 after authority failure", fixture.provider.calls)
			}
		})
	}
}

func TestK2EOperationAndResourceIdentityCannotBypassAuthority(t *testing.T) {
	fixture := newK2EFixture(t)
	request := fixture.request("set_target_temperature")
	result, err := fixture.runtime.Invoke(request)
	if err != nil {
		t.Fatalf("Invoke(undeclared operation) error = %v", err)
	}
	if result.Status != kernel.LocalInvocationRejected || fixture.provider.calls != 0 {
		t.Fatalf("undeclared operation = status %q/calls %d, want REJECTED/0", result.Status, fixture.provider.calls)
	}

	resourceAsHandle := fixture.request("read_temperature")
	resourceAsHandle.CapabilityHandleID = capability.CapabilityHandleID(fixture.admission.Resource.ID)
	result, err = fixture.runtime.Invoke(resourceAsHandle)
	if err != nil {
		t.Fatalf("Invoke(resource as handle) error = %v", err)
	}
	if result.Status != kernel.LocalInvocationRejected || fixture.provider.calls != 0 {
		t.Fatalf("resource-as-handle = status %q/calls %d, want REJECTED/0", result.Status, fixture.provider.calls)
	}
}

func TestK2EForeignValidChainIsNotAdmitted(t *testing.T) {
	fixture := newK2EFixture(t)
	foreignResource, err := fixture.kernel.Resources.CreateResource("local")
	if err != nil {
		t.Fatalf("CreateResource(foreign) error = %v", err)
	}
	foreignDeclaration, err := fixture.kernel.Capabilities.RegisterDeclarationFor(foreignResource.ID, "foreign.operation", "v1")
	if err != nil {
		t.Fatalf("RegisterDeclarationFor(foreign) error = %v", err)
	}
	foreignInstance, err := fixture.kernel.Capabilities.CreateInstance(foreignDeclaration.ID)
	if err != nil {
		t.Fatalf("CreateInstance(foreign) error = %v", err)
	}
	foreignContext, err := fixture.runtime.CreateExecutionContext("foreign-caller", "local")
	if err != nil {
		t.Fatalf("CreateExecutionContext(foreign) error = %v", err)
	}
	foreignHandle, err := fixture.runtime.CreateCapabilityHandle(foreignContext.ID, foreignInstance.ID, capability.PermissionSet{"foreign.invoke"}, foreignContext.Scope)
	if err != nil {
		t.Fatalf("CreateCapabilityHandle(foreign) error = %v", err)
	}
	result, err := fixture.runtime.Invoke(kernel.LocalInvocationRequest{
		ExecutionContextID: foreignContext.ID,
		CapabilityHandleID: foreignHandle.ID,
		CallerIdentity:     foreignContext.Subject,
		Scope:              foreignContext.Scope,
		Operation:          "foreign.operation",
	})
	if err != nil {
		t.Fatalf("Invoke(foreign chain) error = %v", err)
	}
	if result.Status != kernel.LocalInvocationRejected || fixture.provider.calls != 0 {
		t.Fatalf("foreign chain = status %q/calls %d, want REJECTED/0", result.Status, fixture.provider.calls)
	}
}
