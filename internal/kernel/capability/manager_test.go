package capability

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"dtm/internal/kernel/event"
	"dtm/internal/kernel/execution"
	"dtm/internal/kernel/resource"
)

type capabilityFixture struct {
	resources   *resource.Manager
	executions  *execution.Manager
	manager     *Manager
	resource    resource.Resource
	declaration CapabilityDeclaration
	instance    CapabilityInstance
	context     execution.ExecutionContext
	handle      CapabilityHandle
}

func newCapabilityFixture(t *testing.T) capabilityFixture {
	t.Helper()
	resources := resource.NewManager()
	executions := execution.NewManager()
	manager := NewManager(resources, executions)
	resourceObject, err := resources.CreateResource("scope-a")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	declaration, err := manager.RegisterDeclaration(DeclarationSpec{
		ResourceID:     resourceObject.ID,
		Name:           "temperature.read",
		Version:        "1.0",
		InputMetadata:  map[string]string{"schema": "none"},
		OutputMetadata: map[string]string{"type": "celsius"},
		Constraints:    map[string]string{"read-only": "true"},
	})
	if err != nil {
		t.Fatalf("RegisterDeclaration() error = %v", err)
	}
	instance, err := manager.CreateInstance(declaration.ID)
	if err != nil {
		t.Fatalf("CreateInstance() error = %v", err)
	}
	context, err := executions.CreateContext("runtime-a", "scope-a")
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}
	handle, err := manager.CreateHandle(context.ID, instance.ID, PermissionSet{"test-grant"})
	if err != nil {
		t.Fatalf("CreateHandle() error = %v", err)
	}
	return capabilityFixture{
		resources: resources, executions: executions, manager: manager,
		resource: resourceObject, declaration: declaration, instance: instance,
		context: context, handle: handle,
	}
}

func TestRegisterDeclarationAndCreateInstancePreserveOwnership(t *testing.T) {
	fixture := newCapabilityFixture(t)
	resourceObject, err := fixture.resources.GetResource(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetResource() error = %v", err)
	}
	if !resourceObject.HasCapabilityDeclaration(fixture.declaration.ID) {
		t.Fatalf("resource does not reference declaration %q", fixture.declaration.ID)
	}
	if fixture.instance.ProviderResourceID != fixture.resource.ID {
		t.Fatalf("instance provider = %q, want %q", fixture.instance.ProviderResourceID, fixture.resource.ID)
	}

	if _, err := fixture.manager.RegisterDeclaration(DeclarationSpec{
		ResourceID: resource.ResourceID("missing-resource"),
		Name:       "camera.capture",
		Version:    "1.0",
	}); !errors.Is(err, resource.ErrResourceNotFound) {
		t.Fatalf("missing provider registration error = %v, want ErrResourceNotFound", err)
	}
}

func TestHandleRevokeIsTerminal(t *testing.T) {
	fixture := newCapabilityFixture(t)
	if err := fixture.manager.ValidateHandle(fixture.handle.ID); err != nil {
		t.Fatalf("ValidateHandle(before revoke) error = %v", err)
	}
	if err := fixture.manager.RevokeHandle(fixture.handle.ID); err != nil {
		t.Fatalf("RevokeHandle() error = %v", err)
	}
	if err := fixture.manager.ValidateHandle(fixture.handle.ID); !errors.Is(err, ErrHandleRevoked) {
		t.Fatalf("ValidateHandle(after revoke) error = %v, want ErrHandleRevoked", err)
	}
	if err := fixture.manager.ActivateHandle(fixture.handle.ID); !errors.Is(err, ErrHandleRevoked) {
		t.Fatalf("ActivateHandle(after revoke) error = %v, want ErrHandleRevoked", err)
	}
}

func TestTerminatedContextCannotCreateOrValidateHandle(t *testing.T) {
	fixture := newCapabilityFixture(t)
	if err := fixture.executions.TerminateContext(fixture.context.ID); err != nil {
		t.Fatalf("TerminateContext() error = %v", err)
	}
	if _, err := fixture.manager.CreateHandle(fixture.context.ID, fixture.instance.ID, PermissionSet{"test-grant"}); !errors.Is(err, execution.ErrContextTerminated) {
		t.Fatalf("CreateHandle(terminated context) error = %v, want ErrContextTerminated", err)
	}
	if err := fixture.manager.ValidateHandle(fixture.handle.ID); !errors.Is(err, execution.ErrContextTerminated) {
		t.Fatalf("ValidateHandle(terminated context) error = %v, want ErrContextTerminated", err)
	}
}

func TestResourceRemovalDoesNotRewriteCapabilityLifecycle(t *testing.T) {
	fixture := newCapabilityFixture(t)
	if err := fixture.resources.RemoveResource(fixture.resource.ID); err != nil {
		t.Fatalf("RemoveResource() error = %v", err)
	}
	instance, err := fixture.manager.GetInstance(fixture.instance.ID)
	if err != nil {
		t.Fatalf("GetInstance(after removal) error = %v", err)
	}
	if instance.State != fixture.instance.State || instance.Availability != fixture.instance.Availability {
		t.Fatalf("instance snapshot changed after resource removal: got=%#v want=%#v", instance, fixture.instance)
	}
	if err := fixture.manager.ValidateHandle(fixture.handle.ID); !errors.Is(err, ErrInstanceUnavailable) {
		t.Fatalf("ValidateHandle(after resource removal) error = %v, want ErrInstanceUnavailable", err)
	}
}

func TestProviderAvailabilityDoesNotRewriteInstanceLifecycle(t *testing.T) {
	tests := []struct {
		name  string
		state InstanceState
	}{
		{name: "created", state: InstanceStateCreated},
		{name: "active", state: InstanceStateActive},
		{name: "suspended", state: InstanceStateSuspended},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCapabilityFixture(t)
			if test.state != InstanceStateCreated {
				if err := fixture.manager.UpdateInstanceState(fixture.instance.ID, test.state); err != nil {
					t.Fatalf("UpdateInstanceState(%q) error = %v", test.state, err)
				}
			}
			before, err := fixture.manager.GetInstance(fixture.instance.ID)
			if err != nil {
				t.Fatalf("GetInstance(before) error = %v", err)
			}
			if err := fixture.resources.UpdateAvailability(fixture.resource.ID, resource.AvailabilityUnavailable); err != nil {
				t.Fatalf("UpdateAvailability(unavailable) error = %v", err)
			}
			got, err := fixture.manager.GetInstance(fixture.instance.ID)
			if err != nil {
				t.Fatalf("GetInstance(unavailable) error = %v", err)
			}
			listed := fixture.manager.ListInstances()
			if len(listed) != 1 {
				t.Fatalf("ListInstances() count = %d, want 1", len(listed))
			}
			if got.State != before.State || got.Availability != before.Availability || listed[0].State != before.State || listed[0].Availability != before.Availability {
				t.Fatalf("provider availability rewrote instance: before=%#v got=%#v listed=%#v", before, got, listed[0])
			}
			if err := fixture.resources.UpdateAvailability(fixture.resource.ID, resource.AvailabilityAvailable); err != nil {
				t.Fatalf("UpdateAvailability(available) error = %v", err)
			}
			after, err := fixture.manager.GetInstance(fixture.instance.ID)
			if err != nil {
				t.Fatalf("GetInstance(after recovery) error = %v", err)
			}
			if after.State != before.State || after.Availability != before.Availability {
				t.Fatalf("provider recovery rewrote instance: before=%#v after=%#v", before, after)
			}
		})
	}
}

func TestCreateHandleRequiresMatchingScope(t *testing.T) {
	fixture := newCapabilityFixture(t)
	if _, err := fixture.manager.CreateHandle(fixture.context.ID, fixture.instance.ID, PermissionSet{"test-grant"}, "scope-b"); !errors.Is(err, ErrScopeMismatch) {
		t.Fatalf("CreateHandle(mismatched scope) error = %v, want ErrScopeMismatch", err)
	}
	if handle, err := fixture.manager.CreateHandle(fixture.context.ID, fixture.instance.ID, PermissionSet{"test-grant"}, "scope-a"); err != nil || handle.Scope != "scope-a" {
		t.Fatalf("CreateHandle(matching scope) = %#v, %v", handle, err)
	}
}

func TestPermissionSetValidatesStructureAndClones(t *testing.T) {
	fixture := newCapabilityFixture(t)
	permissions := PermissionSet{"zeta", "alpha"}
	handle, err := fixture.manager.CreateHandle(fixture.context.ID, fixture.instance.ID, permissions)
	if err != nil {
		t.Fatalf("CreateHandle(valid permissions) error = %v", err)
	}
	permissions[0] = "mutated-after-create"
	if len(handle.Permissions) != 2 || handle.Permissions[0] != "alpha" || handle.Permissions[1] != "zeta" {
		t.Fatalf("normalized permissions = %#v, want deterministic sorted snapshot", handle.Permissions)
	}
	handle.Permissions[0] = "mutated-snapshot"
	stored, err := fixture.manager.GetHandle(handle.ID)
	if err != nil {
		t.Fatalf("GetHandle() error = %v", err)
	}
	if stored.Permissions[0] != "alpha" {
		t.Fatalf("manager-owned permissions changed through returned snapshot: %#v", stored.Permissions)
	}

	for _, permissions := range []PermissionSet{{""}, {"alpha", "alpha"}, {"alpha\x00beta"}} {
		if _, err := fixture.manager.CreateHandle(fixture.context.ID, fixture.instance.ID, permissions); !errors.Is(err, ErrInvalidPermission) {
			t.Fatalf("CreateHandle(%#v) error = %v, want ErrInvalidPermission", permissions, err)
		}
	}
}

func TestMandatoryAuditFailureRollsBackCapabilityMutations(t *testing.T) {
	fixture := newCapabilityFixture(t)
	auditErr := errors.New("injected capability audit failure")
	fixture.manager.publish = func(event.EventRecord) error { return auditErr }

	if _, err := fixture.manager.CreateInstance(fixture.declaration.ID); !errors.Is(err, auditErr) {
		t.Fatalf("CreateInstance(audit failure) error = %v, want injected error", err)
	}
	if instances := fixture.manager.ListInstances(); len(instances) != 1 {
		t.Fatalf("CreateInstance(audit failure) instances = %d, want existing fixture instance only", len(instances))
	}

	if _, err := fixture.manager.CreateHandle(fixture.context.ID, fixture.instance.ID, PermissionSet{"test-grant"}); !errors.Is(err, auditErr) {
		t.Fatalf("CreateHandle(audit failure) error = %v, want injected error", err)
	}
	if handles := fixture.manager.ListHandles(); len(handles) != 1 {
		t.Fatalf("CreateHandle(audit failure) handles = %d, want existing fixture handle only", len(handles))
	}

	fixture.manager.publish = fixture.manager.events.Publish
	if err := fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateActive); err != nil {
		t.Fatalf("UpdateInstanceState(ACTIVE) error = %v", err)
	}
	fixture.manager.publish = func(event.EventRecord) error { return auditErr }
	if err := fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateDestroyed); !errors.Is(err, auditErr) {
		t.Fatalf("UpdateInstanceState(DESTROYED) error = %v, want injected error", err)
	}
	instance, err := fixture.manager.GetInstance(fixture.instance.ID)
	if err != nil {
		t.Fatalf("GetInstance(after destroy rollback) error = %v", err)
	}
	if instance.State != InstanceStateActive {
		t.Fatalf("instance state after destroy audit rollback = %q, want ACTIVE", instance.State)
	}

	if err := fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateRevoked); !errors.Is(err, auditErr) {
		t.Fatalf("UpdateInstanceState(REVOKED) error = %v, want injected error", err)
	}
	instance, err = fixture.manager.GetInstance(fixture.instance.ID)
	if err != nil {
		t.Fatalf("GetInstance(after rollback) error = %v", err)
	}
	if instance.State != InstanceStateActive {
		t.Fatalf("instance state after audit rollback = %q, want ACTIVE", instance.State)
	}

	if _, err := fixture.manager.Invoke(fixture.handle.ID, InvocationRequest{Operation: "read"}); !errors.Is(err, auditErr) {
		t.Fatalf("Invoke(audit failure) error = %v, want injected error", err)
	}
}

func TestUpdateInstanceStateRollbackUsesAuthoritativeLockSnapshot(t *testing.T) {
	fixture := newCapabilityFixture(t)
	auditErr := errors.New("injected suspended-state audit failure")
	operationStarted := make(chan struct{})
	allowFirstOperation := make(chan struct{})
	var hookCalls int32
	fixture.manager.beforeInstanceStateLock = func() {
		if atomic.AddInt32(&hookCalls, 1) == 1 {
			close(operationStarted)
			<-allowFirstOperation
		}
	}
	fixture.manager.publish = func(record event.EventRecord) error {
		if record.EventType == event.CapabilityInstanceStateUpdated && record.Result == string(InstanceStateSuspended) {
			return auditErr
		}
		return fixture.manager.events.Publish(record)
	}

	firstResult := make(chan error, 1)
	go func() {
		firstResult <- fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateSuspended)
	}()
	select {
	case <-operationStarted:
	case <-time.After(2 * time.Second):
		close(allowFirstOperation)
		t.Fatal("first state operation did not reach synchronization barrier")
	}

	if err := fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateActive); err != nil {
		t.Fatalf("successful concurrent UpdateInstanceState(ACTIVE) error = %v", err)
	}
	close(allowFirstOperation)
	if err := <-firstResult; !errors.Is(err, auditErr) {
		t.Fatalf("failed concurrent UpdateInstanceState(SUSPENDED) error = %v, want injected audit error", err)
	}

	instance, err := fixture.manager.GetInstance(fixture.instance.ID)
	if err != nil {
		t.Fatalf("GetInstance(after concurrent rollback) error = %v", err)
	}
	if instance.State != InstanceStateActive {
		t.Fatalf("concurrent successful state was overwritten by stale rollback: got=%q want=%q", instance.State, InstanceStateActive)
	}
}

func TestUpdateInstanceStateSameStateIsNoOp(t *testing.T) {
	fixture := newCapabilityFixture(t)
	countStateEvents := func() int {
		return len(fixture.manager.EventManager().Query(event.QueryFilter{EventType: event.CapabilityInstanceStateUpdated}))
	}

	if err := fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateCreated); err != nil {
		t.Fatalf("same-state UpdateInstanceState(CREATED) error = %v", err)
	}
	if got := countStateEvents(); got != 0 {
		t.Fatalf("same-state CREATED update emitted %d lifecycle events, want 0", got)
	}
	if err := fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateActive); err != nil {
		t.Fatalf("UpdateInstanceState(ACTIVE) error = %v", err)
	}
	if got := countStateEvents(); got != 1 {
		t.Fatalf("ACTIVE transition emitted %d lifecycle events, want 1", got)
	}
	if err := fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateActive); err != nil {
		t.Fatalf("same-state UpdateInstanceState(ACTIVE) error = %v", err)
	}
	if got := countStateEvents(); got != 1 {
		t.Fatalf("same-state ACTIVE update emitted %d lifecycle events, want 1 total", got)
	}
}

func TestMandatoryAuditFailureRollsBackHandleMutations(t *testing.T) {
	fixture := newCapabilityFixture(t)
	auditErr := errors.New("injected handle audit failure")
	fixture.manager.publish = func(event.EventRecord) error { return auditErr }
	if err := fixture.manager.ActivateHandle(fixture.handle.ID); !errors.Is(err, auditErr) {
		t.Fatalf("ActivateHandle(audit failure) error = %v, want injected error", err)
	}
	handle, err := fixture.manager.GetHandle(fixture.handle.ID)
	if err != nil {
		t.Fatalf("GetHandle(after activate rollback) error = %v", err)
	}
	if handle.State != HandleStateCreated {
		t.Fatalf("handle state after activate audit rollback = %q, want CREATED", handle.State)
	}

	fixture.manager.publish = fixture.manager.events.Publish
	if err := fixture.manager.ActivateHandle(fixture.handle.ID); err != nil {
		t.Fatalf("ActivateHandle() error = %v", err)
	}
	fixture.manager.publish = func(event.EventRecord) error { return auditErr }
	if err := fixture.manager.ReleaseHandle(fixture.handle.ID); !errors.Is(err, auditErr) {
		t.Fatalf("ReleaseHandle(audit failure) error = %v, want injected error", err)
	}
	handle, err = fixture.manager.GetHandle(fixture.handle.ID)
	if err != nil {
		t.Fatalf("GetHandle(after release rollback) error = %v", err)
	}
	if handle.State != HandleStateActive {
		t.Fatalf("handle state after release audit rollback = %q, want ACTIVE", handle.State)
	}
}

func TestRegisterDeclarationAuditFailureRollsBackResourceAttachment(t *testing.T) {
	resources := resource.NewManager()
	events := event.NewManager()
	manager := NewManager(resources, events)
	resourceObject, err := resources.CreateResource("scope-a")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	auditErr := errors.New("injected declaration audit failure")
	manager.publish = func(event.EventRecord) error { return auditErr }
	declaration, err := manager.RegisterDeclaration(DeclarationSpec{
		ResourceID: resourceObject.ID,
		Name:       "temperature.read",
		Version:    "1.0",
	})
	if !errors.Is(err, auditErr) {
		t.Fatalf("RegisterDeclaration(audit failure) error = %v, want injected error", err)
	}
	if declaration.ID != "" {
		t.Fatalf("RegisterDeclaration(audit failure) declaration = %#v, want zero", declaration)
	}
	if _, err := manager.GetDeclaration(CapabilityDeclarationID("capability-declaration-missing")); !errors.Is(err, ErrDeclarationNotFound) {
		t.Fatalf("GetDeclaration(after rollback) error = %v, want ErrDeclarationNotFound", err)
	}
	resourceSnapshot, err := resources.GetResource(resourceObject.ID)
	if err != nil {
		t.Fatalf("GetResource(after rollback) error = %v", err)
	}
	if len(resourceSnapshot.CapabilityDeclarationIDs) != 0 {
		t.Fatalf("resource declaration references after rollback = %#v, want empty", resourceSnapshot.CapabilityDeclarationIDs)
	}
}

func TestManagerRejectsForgedInstanceAndCopiesMutableFields(t *testing.T) {
	fixture := newCapabilityFixture(t)
	declaration, err := fixture.manager.GetDeclaration(fixture.declaration.ID)
	if err != nil {
		t.Fatalf("GetDeclaration() error = %v", err)
	}
	declaration.InputMetadata["schema"] = "forged"
	storedDeclaration, err := fixture.manager.GetDeclaration(fixture.declaration.ID)
	if err != nil {
		t.Fatalf("GetDeclaration(second) error = %v", err)
	}
	if storedDeclaration.InputMetadata["schema"] != "none" {
		t.Fatalf("stored declaration metadata changed through snapshot: %#v", storedDeclaration.InputMetadata)
	}

	if _, err := fixture.manager.CreateInstance(CapabilityDeclarationID("unknown-declaration")); !errors.Is(err, ErrDeclarationNotFound) {
		t.Fatalf("forged instance creation error = %v, want ErrDeclarationNotFound", err)
	}
}

func TestInvokeAcceptsOnlyValidatedHandle(t *testing.T) {
	tests := []struct {
		name           string
		prepare        func(*capabilityFixture) error
		wantError      error
		wantAccepted   bool
		wantAuditEvent string
	}{
		{
			name:           "valid handle",
			wantAccepted:   true,
			wantAuditEvent: event.CapabilityHandleInvokeAccepted,
		},
		{
			name: "revoked handle",
			prepare: func(fixture *capabilityFixture) error {
				return fixture.manager.RevokeHandle(fixture.handle.ID)
			},
			wantAccepted:   false,
			wantAuditEvent: event.CapabilityHandleInvokeRejected,
		},
		{
			name: "released handle",
			prepare: func(fixture *capabilityFixture) error {
				return fixture.manager.ReleaseHandle(fixture.handle.ID)
			},
			wantAccepted:   false,
			wantAuditEvent: event.CapabilityHandleInvokeRejected,
		},
		{
			name: "expired handle",
			prepare: func(fixture *capabilityFixture) error {
				fixture.manager.mu.Lock()
				handle := fixture.manager.handles[fixture.handle.ID]
				handle.State = HandleStateExpired
				handle.Availability = AvailabilityUnavailable
				fixture.manager.handles[fixture.handle.ID] = handle
				fixture.manager.mu.Unlock()
				return nil
			},
			wantAccepted:   false,
			wantAuditEvent: event.CapabilityHandleInvokeRejected,
		},
		{
			name: "destroyed instance",
			prepare: func(fixture *capabilityFixture) error {
				return fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateDestroyed)
			},
			wantAccepted:   false,
			wantAuditEvent: event.CapabilityHandleInvokeRejected,
		},
		{
			name: "terminated context",
			prepare: func(fixture *capabilityFixture) error {
				return fixture.executions.TerminateContext(fixture.context.ID)
			},
			wantAccepted:   false,
			wantAuditEvent: event.CapabilityHandleInvokeRejected,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCapabilityFixture(t)
			if test.prepare != nil {
				if err := test.prepare(&fixture); err != nil {
					t.Fatalf("prepare() error = %v", err)
				}
			}
			result, err := fixture.manager.Invoke(fixture.handle.ID, InvocationRequest{Operation: "read"})
			if err != nil {
				t.Fatalf("Invoke() error = %v", err)
			}
			if result == nil || result.Accepted != test.wantAccepted {
				t.Fatalf("Invoke() result = %#v, want accepted=%t", result, test.wantAccepted)
			}
			records := fixture.manager.EventManager().Query(event.QueryFilter{EventType: test.wantAuditEvent})
			if len(records) != 1 {
				t.Fatalf("audit records = %d, want 1: %#v", len(records), records)
			}
		})
	}
}

func TestInvokeUnknownHandleReturnsErrorAndAuditRejection(t *testing.T) {
	fixture := newCapabilityFixture(t)
	unknownID := CapabilityHandleID("unknown-handle")
	result, err := fixture.manager.Invoke(unknownID, InvocationRequest{Operation: "read"})
	if !errors.Is(err, ErrHandleNotFound) {
		t.Fatalf("Invoke(unknown) error = %v, want ErrHandleNotFound", err)
	}
	if result != nil {
		t.Fatalf("Invoke(unknown) result = %#v, want nil", result)
	}
	records := fixture.manager.EventManager().Query(event.QueryFilter{EventType: event.CapabilityHandleInvokeRejected})
	if len(records) != 1 {
		t.Fatalf("unknown-handle audit records = %d, want 1", len(records))
	}
}

func TestK2CLegacyInvokePreservesOperationValidationPrecedence(t *testing.T) {
	fixture := newCapabilityFixture(t)
	result, err := fixture.manager.Invoke(
		CapabilityHandleID("unknown-handle"),
		InvocationRequest{Operation: "\x00"},
	)
	if !errors.Is(err, ErrInvalidInvocation) {
		t.Fatalf("legacy Invoke() error = %v, want ErrInvalidInvocation", err)
	}
	if result != nil {
		t.Fatalf("legacy Invoke() result = %#v, want nil", result)
	}
	if records := fixture.manager.EventManager().Query(event.QueryFilter{EventType: event.CapabilityHandleInvokeRejected}); len(records) != 1 {
		t.Fatalf("legacy invalid-operation audit records = %d, want 1", len(records))
	}
}
