package kernel_test

import (
	"errors"
	"testing"

	"dtm/internal/kernel/capability"
	"dtm/internal/kernel/event"
	"dtm/internal/kernel/execution"
	"dtm/internal/kernel/identity"
	"dtm/internal/kernel/resource"
)

func TestFreezeInvariantLifecycleAvailabilityAndEventReference(t *testing.T) {
	if err := identity.LifecycleState(identity.AvailabilityUnavailable).Validate(); err == nil {
		t.Fatal("UNAVAILABLE was accepted as a lifecycle state")
	}
	if _, err := resource.NewResource(resource.ResourceID("resource-1"), "scope-a", nil, resource.LifecycleState("UNAVAILABLE")); !errors.Is(err, resource.ErrInvalidState) {
		t.Fatalf("resource lifecycle UNAVAILABLE error = %v, want ErrInvalidState", err)
	}
	if _, err := resource.NewResource(resource.ResourceID("resource-1"), "scope-a", nil, resource.LifecycleState("TERMINATED")); !errors.Is(err, resource.ErrInvalidState) {
		t.Fatalf("resource lifecycle TERMINATED error = %v, want ErrInvalidState", err)
	}

	for _, kind := range []identity.ObjectKind{
		identity.ObjectKindResource,
		identity.ObjectKindCapabilityDeclaration,
		identity.ObjectKindCapabilityInstance,
		identity.ObjectKindCapabilityHandle,
		identity.ObjectKindExecutionContext,
		identity.ObjectKindEventRecord,
	} {
		reference, err := identity.NewObjectReference(kind, "object-1")
		if err != nil {
			t.Fatalf("NewObjectReference(%s) error = %v", kind, err)
		}
		if err := reference.Validate(); err != nil {
			t.Fatalf("ObjectReference(%s).Validate() error = %v", kind, err)
		}
	}

	record, err := event.NewEventRecord(
		identity.ObjectReference{Kind: identity.ObjectKindResource, ID: "resource-1"},
		event.ResourceCreated,
		identity.ObjectReference{Kind: identity.ObjectKindEventRecord, ID: "event-1"},
		"freeze invariant",
		"recorded",
	)
	if err != nil {
		t.Fatalf("NewEventRecord() error = %v", err)
	}
	if err := record.Validate(); err != nil {
		t.Fatalf("EventRecord.Validate() error = %v", err)
	}
}

func TestFreezeInvariantProviderAvailabilityDoesNotMutateInstanceSnapshots(t *testing.T) {
	resources := resource.NewManager()
	executions := execution.NewManager()
	events := event.NewManager()
	capabilities := capability.NewManager(resources, executions, events)
	resourceObject, err := resources.CreateResource("scope-a")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	declaration, err := capabilities.RegisterDeclarationFor(resourceObject.ID, "temperature.read", "1.0")
	if err != nil {
		t.Fatalf("RegisterDeclarationFor() error = %v", err)
	}
	instance, err := capabilities.CreateInstance(declaration.ID)
	if err != nil {
		t.Fatalf("CreateInstance() error = %v", err)
	}
	if err := capabilities.UpdateInstanceState(instance.ID, capability.InstanceStateActive); err != nil {
		t.Fatalf("UpdateInstanceState(ACTIVE) error = %v", err)
	}

	before, err := capabilities.GetInstance(instance.ID)
	if err != nil {
		t.Fatalf("GetInstance(before) error = %v", err)
	}
	if err := resources.UpdateAvailability(resourceObject.ID, resource.AvailabilityUnavailable); err != nil {
		t.Fatalf("UpdateAvailability(unavailable) error = %v", err)
	}
	got, err := capabilities.GetInstance(instance.ID)
	if err != nil {
		t.Fatalf("GetInstance(unavailable) error = %v", err)
	}
	listed := capabilities.ListInstances()
	if len(listed) != 1 {
		t.Fatalf("ListInstances() count = %d, want 1", len(listed))
	}
	if got.State != before.State || got.Availability != before.Availability || listed[0].State != before.State || listed[0].Availability != before.Availability {
		t.Fatalf("provider availability changed lifecycle snapshot: before=%#v got=%#v listed=%#v", before, got, listed[0])
	}

	listed[0].State = capability.InstanceStateRevoked
	if afterMutation, err := capabilities.GetInstance(instance.ID); err != nil {
		t.Fatalf("GetInstance(after returned snapshot mutation) error = %v", err)
	} else if afterMutation.State != before.State {
		t.Fatalf("returned snapshot mutated manager state: %#v", afterMutation)
	}

	if err := resources.UpdateAvailability(resourceObject.ID, resource.AvailabilityAvailable); err != nil {
		t.Fatalf("UpdateAvailability(available) error = %v", err)
	}
	afterRecovery, err := capabilities.GetInstance(instance.ID)
	if err != nil {
		t.Fatalf("GetInstance(after recovery) error = %v", err)
	}
	if afterRecovery.State != before.State || afterRecovery.Availability != before.Availability {
		t.Fatalf("provider recovery changed lifecycle snapshot: before=%#v after=%#v", before, afterRecovery)
	}
}

func TestFreezeInvariantHandleScopeIsBoundToExecutionContext(t *testing.T) {
	resources := resource.NewManager()
	executions := execution.NewManager()
	capabilities := capability.NewManager(resources, executions)
	resourceObject, err := resources.CreateResource("scope-a")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	declaration, err := capabilities.RegisterDeclarationFor(resourceObject.ID, "temperature.read", "1.0")
	if err != nil {
		t.Fatalf("RegisterDeclarationFor() error = %v", err)
	}
	instance, err := capabilities.CreateInstance(declaration.ID)
	if err != nil {
		t.Fatalf("CreateInstance() error = %v", err)
	}
	context, err := executions.CreateContext("runtime-a", "scope-a")
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}
	if _, err := capabilities.CreateHandle(context.ID, instance.ID, capability.PermissionSet{"test-grant"}, "scope-b"); !errors.Is(err, capability.ErrScopeMismatch) {
		t.Fatalf("CreateHandle(mismatched scope) error = %v, want ErrScopeMismatch", err)
	}
	if handle, err := capabilities.CreateHandle(context.ID, instance.ID, capability.PermissionSet{"test-grant"}, "scope-a"); err != nil || handle.Scope != context.Scope {
		t.Fatalf("CreateHandle(matching scope) = %#v, %v", handle, err)
	}
}
