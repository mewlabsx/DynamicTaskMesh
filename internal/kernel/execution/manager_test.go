package execution

import (
	"errors"
	"testing"

	"dtm/internal/kernel/event"
)

func TestManagerCreatesContextAndTerminatesIt(t *testing.T) {
	manager := NewManager()
	created, err := manager.CreateContext("runtime-a", "scope-a")
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}
	if created.State != StateCreated {
		t.Fatalf("created context state = %q, want %q", created.State, StateCreated)
	}
	if err := manager.UpdateState(created.ID, StateActive); err != nil {
		t.Fatalf("UpdateState(ACTIVE) error = %v", err)
	}
	if err := manager.TerminateContext(created.ID); err != nil {
		t.Fatalf("TerminateContext() error = %v", err)
	}
	terminated, err := manager.GetContext(created.ID)
	if err != nil {
		t.Fatalf("GetContext() error = %v", err)
	}
	if terminated.State != StateTerminated {
		t.Fatalf("terminated context state = %q, want %q", terminated.State, StateTerminated)
	}
	if terminated.IsUsable() {
		t.Fatal("terminated context reported usable")
	}
	if err := manager.UpdateState(created.ID, StateActive); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("reactivation error = %v, want ErrInvalidTransition", err)
	}
}

func TestManagerRejectsInvalidContext(t *testing.T) {
	manager := NewManager()
	for _, subject := range []string{"", " ", "runtime\x00a"} {
		if _, err := manager.CreateContext(subject); !errors.Is(err, ErrInvalidContext) {
			t.Fatalf("CreateContext(%q) error = %v, want ErrInvalidContext", subject, err)
		}
	}
}

func TestManagerPublishesExecutionAuditEvents(t *testing.T) {
	manager := NewManager()
	created, err := manager.CreateContext("runtime-audit", "scope-audit")
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}
	if records := manager.EventManager().Query(event.QueryFilter{EventType: event.ExecutionContextCreated}); len(records) != 1 {
		t.Fatalf("created audit records = %d, want 1", len(records))
	}
	if err := manager.TerminateContext(created.ID); err != nil {
		t.Fatalf("TerminateContext() error = %v", err)
	}
	if records := manager.EventManager().Query(event.QueryFilter{EventType: event.ExecutionContextTerminated}); len(records) != 1 {
		t.Fatalf("terminated audit records = %d, want 1", len(records))
	}
}

func TestMandatoryAuditFailureRollsBackExecutionMutations(t *testing.T) {
	manager := NewManager()
	auditErr := errors.New("injected execution audit failure")
	manager.publish = func(event.EventRecord) error { return auditErr }
	if _, err := manager.CreateContext("runtime-audit", "scope-audit"); !errors.Is(err, auditErr) {
		t.Fatalf("CreateContext(audit failure) error = %v, want injected error", err)
	}
	if contexts := manager.contexts; len(contexts) != 0 {
		t.Fatalf("CreateContext(audit failure) contexts = %d, want 0", len(contexts))
	}

	manager.publish = manager.events.Publish
	created, err := manager.CreateContext("runtime-audit", "scope-audit")
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}
	manager.publish = func(event.EventRecord) error { return auditErr }
	if err := manager.UpdateState(created.ID, StateActive); !errors.Is(err, auditErr) {
		t.Fatalf("UpdateState(audit failure) error = %v, want injected error", err)
	}
	got, err := manager.GetContext(created.ID)
	if err != nil {
		t.Fatalf("GetContext(after state rollback) error = %v", err)
	}
	if got.State != StateCreated {
		t.Fatalf("context state after state audit rollback = %q, want CREATED", got.State)
	}

	manager.publish = manager.events.Publish
	if err := manager.UpdateState(created.ID, StateActive); err != nil {
		t.Fatalf("UpdateState(ACTIVE) error = %v", err)
	}
	manager.publish = func(event.EventRecord) error { return auditErr }
	if err := manager.TerminateContext(created.ID); !errors.Is(err, auditErr) {
		t.Fatalf("TerminateContext(audit failure) error = %v, want injected error", err)
	}
	got, err = manager.GetContext(created.ID)
	if err != nil {
		t.Fatalf("GetContext(after rollback) error = %v", err)
	}
	if got.State != StateActive {
		t.Fatalf("context state after terminate audit rollback = %q, want ACTIVE", got.State)
	}
}
