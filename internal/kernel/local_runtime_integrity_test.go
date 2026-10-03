package kernel

import (
	"testing"

	"dtm/internal/kernel/capability"
	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/resourcedirectory"
)

type localRuntimeIntegrityProvider struct {
	calls int
}

func (provider *localRuntimeIntegrityProvider) Invoke(capability.ProviderOperation) error {
	provider.calls++
	return nil
}

func TestLocalRuntimeRejectsCorruptedAdmissionRelationship(t *testing.T) {
	const (
		nodeID         model.NodeID = "k2e-integrity-node"
		registrationID              = "k2e-integrity-registration"
	)

	descriptor, err := model.AdaptLegacyCapability(nodeID, "temperature_sensor", 1)
	if err != nil {
		t.Fatalf("AdaptLegacyCapability() error = %v", err)
	}
	directory := resourcedirectory.New()
	snapshot, err := resourcedirectory.NewNodeResourceSnapshot(nodeID, 1, registrationID, []model.ResourceDescriptor{descriptor})
	if err != nil {
		t.Fatalf("NewNodeResourceSnapshot() error = %v", err)
	}
	if _, err := directory.ApplyNodeSnapshot(snapshot); err != nil {
		t.Fatalf("ApplyNodeSnapshot() error = %v", err)
	}
	lifecycle, err := resourcedirectory.NewNodeLifecycleView(nodeID, 1, registrationID, node.StatusActive, true, true)
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
	provider := &localRuntimeIntegrityProvider{}
	k := New()
	runtime, err := NewLocalRuntime(k, directory, provider)
	if err != nil {
		t.Fatalf("NewLocalRuntime() error = %v", err)
	}
	admission, err := runtime.AdmitResourceEvidence(evidence)
	if err != nil {
		t.Fatalf("AdmitResourceEvidence() error = %v", err)
	}
	context, err := runtime.CreateExecutionContext("integrity-caller", "local")
	if err != nil {
		t.Fatalf("CreateExecutionContext() error = %v", err)
	}
	handle, err := runtime.CreateCapabilityHandle(context.ID, admission.Instances[0].ID, capability.PermissionSet{"k2e.invoke"}, context.Scope)
	if err != nil {
		t.Fatalf("CreateCapabilityHandle() error = %v", err)
	}

	runtime.mu.Lock()
	corrupted := runtime.admissions[ResourceID(descriptor.ID)].Clone()
	corrupted.Declarations[0].ResourceID = ResourceID("foreign-resource")
	runtime.admissions[ResourceID(descriptor.ID)] = corrupted
	runtime.mu.Unlock()

	result, err := runtime.Invoke(LocalInvocationRequest{
		ExecutionContextID: context.ID,
		CapabilityHandleID: handle.ID,
		CallerIdentity:     context.Subject,
		Scope:              context.Scope,
		Operation:          "read_temperature",
	})
	if err != nil {
		t.Fatalf("Invoke(corrupted admission) error = %v", err)
	}
	if result.Status != LocalInvocationRejected {
		t.Fatalf("Invoke(corrupted admission) status = %q, want REJECTED", result.Status)
	}
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0 after relationship corruption", provider.calls)
	}

	runtime.mu.Lock()
	duplicated := admission.Clone()
	duplicated.Declarations = append(duplicated.Declarations, duplicated.Declarations[0].Clone())
	duplicated.Instances = append(duplicated.Instances, duplicated.Instances[0].Clone())
	runtime.admissions[ResourceID(descriptor.ID)] = duplicated
	runtime.mu.Unlock()

	result, err = runtime.Invoke(LocalInvocationRequest{
		ExecutionContextID: context.ID,
		CapabilityHandleID: handle.ID,
		CallerIdentity:     context.Subject,
		Scope:              context.Scope,
		Operation:          "read_temperature",
	})
	if err != nil {
		t.Fatalf("Invoke(duplicated admission) error = %v", err)
	}
	if result.Status != LocalInvocationRejected {
		t.Fatalf("Invoke(duplicated admission) status = %q, want REJECTED", result.Status)
	}
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0 after duplicated admission relationship", provider.calls)
	}
}
