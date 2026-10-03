package kernel_test

import (
	"errors"
	"reflect"
	"testing"

	"dtm/internal/kernel/execution"
	"dtm/internal/kernel/identity"
	"dtm/internal/model"
	"dtm/internal/task"
)

func TestInvariant_ObjectKindsRemainDistinctForSameID(t *testing.T) {
	const sharedID = "shared-object-id"
	kinds := []identity.ObjectKind{
		identity.ObjectKindResource,
		identity.ObjectKindCapabilityDeclaration,
		identity.ObjectKindCapabilityInstance,
		identity.ObjectKindCapabilityHandle,
		identity.ObjectKindExecutionContext,
		identity.ObjectKindEventRecord,
	}
	references := make([]identity.ObjectReference, 0, len(kinds))
	for _, kind := range kinds {
		reference, err := identity.NewObjectReference(kind, sharedID)
		if err != nil {
			t.Fatalf("NewObjectReference(%s) error = %v", kind, err)
		}
		references = append(references, reference)
	}
	for left := range references {
		for right := left + 1; right < len(references); right++ {
			if references[left] == references[right] {
				t.Fatalf("object references collapsed distinct kinds: %#v and %#v", references[left], references[right])
			}
		}
	}

	identityTypes := []any{
		identity.ResourceID(sharedID),
		identity.CapabilityDeclarationID(sharedID),
		identity.CapabilityInstanceID(sharedID),
		identity.CapabilityHandleID(sharedID),
		identity.ExecutionContextID(sharedID),
		identity.EventID(sharedID),
	}
	for left := range identityTypes {
		for right := left + 1; right < len(identityTypes); right++ {
			if reflect.TypeOf(identityTypes[left]) == reflect.TypeOf(identityTypes[right]) {
				t.Fatalf("identity types collapsed: %T and %T", identityTypes[left], identityTypes[right])
			}
		}
	}
}

func TestInvariant_ExecutionContextIsNotTask(t *testing.T) {
	context, err := execution.NewContext(
		execution.ExecutionContextID("shared-id"),
		"runtime-a",
		"scope-a",
		execution.StateCreated,
	)
	if err != nil {
		t.Fatalf("NewContext() error = %v", err)
	}
	legacyTask, err := task.New(model.TaskID("shared-id"), "cool environment", nil, nil)
	if err != nil {
		t.Fatalf("task.New() error = %v", err)
	}
	if reflect.TypeOf(context) == reflect.TypeOf(legacyTask) {
		t.Fatal("ExecutionContext and Task unexpectedly share an object type")
	}
	if reflect.TypeOf(context.ID) == reflect.TypeOf(legacyTask.ID) {
		t.Fatal("ExecutionContext ID and Task ID unexpectedly share an identity type")
	}
}

func TestInvariant_ObjectKindIntentUnsupportedInK2(t *testing.T) {
	_, err := identity.NewObjectReference(identity.ObjectKindIntent, "intent-1")
	if !errors.Is(err, identity.ErrInvalidObjectReference) {
		t.Fatalf("NewObjectReference(ObjectKindIntent) error = %v, want ErrInvalidObjectReference", err)
	}
}
