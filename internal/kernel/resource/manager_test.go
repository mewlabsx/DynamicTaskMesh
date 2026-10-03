package resource

import (
	"errors"
	"testing"

	"dtm/internal/kernel/event"
)

func TestManagerCreatesAndProtectsResourceSnapshots(t *testing.T) {
	manager := NewManager()
	created, err := manager.CreateResource("zone-a")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	if created.State != StateCreated {
		t.Fatalf("created resource state = %q, want %q", created.State, StateCreated)
	}
	if created.Scope != "zone-a" {
		t.Fatalf("created resource scope = %q, want zone-a", created.Scope)
	}

	created.State = StateRemoved
	created.Scope = "changed-outside-manager"
	got, err := manager.GetResource(created.ID)
	if err != nil {
		t.Fatalf("GetResource() error = %v", err)
	}
	if got.State != StateCreated || got.Scope != "zone-a" {
		t.Fatalf("manager state changed through returned value: %#v", got)
	}
}

func TestManagerLifecycleAndRemovalAreManagerControlled(t *testing.T) {
	manager := NewManager()
	created, err := manager.CreateResource()
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	if err := manager.UpdateState(created.ID, LifecycleState("TERMINATED")); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("UpdateState(TERMINATED) error = %v, want ErrInvalidState", err)
	}
	if err := manager.UpdateState(created.ID, StateActive); err != nil {
		t.Fatalf("UpdateState(ACTIVE) error = %v", err)
	}
	if err := manager.UpdateState(created.ID, StateSuspended); err != nil {
		t.Fatalf("UpdateState(SUSPENDED) error = %v", err)
	}
	if err := manager.UpdateState(created.ID, StateActive); err != nil {
		t.Fatalf("UpdateState(ACTIVE after SUSPENDED) error = %v", err)
	}
	if err := manager.RemoveResource(created.ID); err != nil {
		t.Fatalf("RemoveResource() error = %v", err)
	}
	if err := manager.UpdateState(created.ID, StateActive); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("UpdateState after removal error = %v, want ErrInvalidTransition", err)
	}
	removed, err := manager.GetResource(created.ID)
	if err != nil {
		t.Fatalf("GetResource(removed) error = %v", err)
	}
	if removed.State != StateRemoved {
		t.Fatalf("removed resource state = %q, want %q", removed.State, StateRemoved)
	}
}

func TestManagerAttachesUniqueCapabilityDeclarationReferences(t *testing.T) {
	manager := NewManager()
	created, err := manager.CreateResource("local")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	declarationID := CapabilityDeclarationID("declaration-1")
	if err := manager.AttachCapabilityDeclaration(created.ID, declarationID); err != nil {
		t.Fatalf("AttachCapabilityDeclaration() error = %v", err)
	}
	if err := manager.AttachCapabilityDeclaration(created.ID, declarationID); !errors.Is(err, ErrCapabilityDeclarationExists) {
		t.Fatalf("duplicate attach error = %v, want ErrCapabilityDeclarationExists", err)
	}
	got, err := manager.GetResource(created.ID)
	if err != nil {
		t.Fatalf("GetResource() error = %v", err)
	}
	if !got.HasCapabilityDeclaration(declarationID) {
		t.Fatalf("resource does not reference attached declaration: %#v", got.CapabilityDeclarationIDs)
	}
}

func TestManagerPublishesResourceAuditEvents(t *testing.T) {
	manager := NewManager()
	created, err := manager.CreateResource("audit-scope")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	if records := manager.EventManager().Query(event.QueryFilter{EventType: event.ResourceCreated}); len(records) != 1 {
		t.Fatalf("created audit records = %d, want 1", len(records))
	}
	if err := manager.RemoveResource(created.ID); err != nil {
		t.Fatalf("RemoveResource() error = %v", err)
	}
	if records := manager.EventManager().Query(event.QueryFilter{EventType: event.ResourceRemoved}); len(records) != 1 {
		t.Fatalf("removed audit records = %d, want 1", len(records))
	}
}

func TestUnavailableIsAvailabilityNotLifecycle(t *testing.T) {
	if _, err := NewResource(ResourceID("resource-1"), "local", nil, LifecycleState("UNAVAILABLE")); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("NewResource(UNAVAILABLE lifecycle) error = %v, want ErrInvalidState", err)
	}
}

func TestResourceRejectsTerminatedLifecycle(t *testing.T) {
	if _, err := NewResource(ResourceID("resource-1"), "local", nil, LifecycleState("TERMINATED")); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("NewResource(TERMINATED) error = %v, want ErrInvalidState", err)
	}
}

func TestMandatoryAuditFailureRollsBackResourceMutations(t *testing.T) {
	manager := NewManager()
	auditErr := errors.New("injected resource audit failure")
	manager.publish = func(event.EventRecord) error { return auditErr }
	if _, err := manager.CreateResource("audit-scope"); !errors.Is(err, auditErr) {
		t.Fatalf("CreateResource(audit failure) error = %v, want injected error", err)
	}
	if resources := manager.ListResources(); len(resources) != 0 {
		t.Fatalf("CreateResource(audit failure) resources = %d, want 0", len(resources))
	}

	manager.publish = manager.events.Publish
	created, err := manager.CreateResource("audit-scope")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	manager.publish = func(event.EventRecord) error { return auditErr }
	if err := manager.UpdateState(created.ID, StateActive); !errors.Is(err, auditErr) {
		t.Fatalf("UpdateState(audit failure) error = %v, want injected error", err)
	}
	got, err := manager.GetResource(created.ID)
	if err != nil {
		t.Fatalf("GetResource(after state rollback) error = %v", err)
	}
	if got.State != StateCreated {
		t.Fatalf("resource state after state audit rollback = %q, want CREATED", got.State)
	}

	manager.publish = manager.events.Publish
	if err := manager.UpdateState(created.ID, StateActive); err != nil {
		t.Fatalf("UpdateState(ACTIVE) error = %v", err)
	}
	manager.publish = func(event.EventRecord) error { return auditErr }
	if err := manager.UpdateAvailability(created.ID, AvailabilityUnavailable); !errors.Is(err, auditErr) {
		t.Fatalf("UpdateAvailability(audit failure) error = %v, want injected error", err)
	}
	got, err = manager.GetResource(created.ID)
	if err != nil {
		t.Fatalf("GetResource(after availability rollback) error = %v", err)
	}
	if got.Availability != AvailabilityAvailable {
		t.Fatalf("resource availability after audit rollback = %q, want AVAILABLE", got.Availability)
	}

	if err := manager.RemoveResource(created.ID); !errors.Is(err, auditErr) {
		t.Fatalf("RemoveResource(audit failure) error = %v, want injected error", err)
	}
	got, err = manager.GetResource(created.ID)
	if err != nil {
		t.Fatalf("GetResource(after rollback) error = %v", err)
	}
	if got.State != StateActive {
		t.Fatalf("resource state after remove audit rollback = %q, want ACTIVE", got.State)
	}
}
