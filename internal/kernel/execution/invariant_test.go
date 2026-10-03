package execution

import (
	"errors"
	"testing"
)

func TestInvariant_StoredContextIdentityMustMatchLookupKey(t *testing.T) {
	manager := NewManager()
	context, err := manager.CreateContext("runtime-a", "scope-a")
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}

	manager.mu.Lock()
	stored := manager.contexts[context.ID].Clone()
	stored.ID = ExecutionContextID("context-other")
	manager.contexts[context.ID] = stored
	manager.mu.Unlock()

	if _, err := manager.GetContext(context.ID); !errors.Is(err, ErrInvalidContext) {
		t.Fatalf("GetContext() error = %v, want ErrInvalidContext", err)
	}
}

func TestInvariant_StoredContextIdentityGuardProtectsMutation(t *testing.T) {
	manager := NewManager()
	context, err := manager.CreateContext("runtime-a", "scope-a")
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}

	manager.mu.Lock()
	stored := manager.contexts[context.ID].Clone()
	stored.ID = ExecutionContextID("context-other")
	manager.contexts[context.ID] = stored
	manager.mu.Unlock()

	if err := manager.UpdateState(context.ID, StateActive); !errors.Is(err, ErrInvalidContext) {
		t.Fatalf("UpdateState() error = %v, want ErrInvalidContext", err)
	}
}
