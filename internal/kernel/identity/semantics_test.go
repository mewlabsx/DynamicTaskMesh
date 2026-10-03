package identity

import (
	"errors"
	"testing"
)

func TestObjectReferenceValidation(t *testing.T) {
	tests := []struct {
		name      string
		reference ObjectReference
		wantError bool
	}{
		{
			name:      "empty id",
			reference: ObjectReference{Kind: ObjectKindResource},
			wantError: true,
		},
		{
			name:      "unknown kind",
			reference: ObjectReference{Kind: ObjectKindUnknown, ID: "resource-1"},
			wantError: true,
		},
		{
			name:      "out of range kind",
			reference: ObjectReference{Kind: ObjectKind(255), ID: "resource-1"},
			wantError: true,
		},
		{
			name:      "valid resource",
			reference: ObjectReference{Kind: ObjectKindResource, ID: "resource-1"},
		},
		{
			name:      "valid capability declaration",
			reference: ObjectReference{Kind: ObjectKindCapabilityDeclaration, ID: "declaration-1"},
		},
		{
			name:      "valid capability instance",
			reference: ObjectReference{Kind: ObjectKindCapabilityInstance, ID: "instance-1"},
		},
		{
			name:      "valid capability handle",
			reference: ObjectReference{Kind: ObjectKindCapabilityHandle, ID: "handle-1"},
		},
		{
			name:      "valid execution context",
			reference: ObjectReference{Kind: ObjectKindExecutionContext, ID: "context-1"},
		},
		{
			name:      "valid event record",
			reference: ObjectReference{Kind: ObjectKindEventRecord, ID: "event-1"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.reference.Validate()
			if test.wantError && err == nil {
				t.Fatal("Validate() error = nil")
			}
			if !test.wantError && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
		})
	}
}

func TestNewObjectReferenceRejectsInvalidKind(t *testing.T) {
	if _, err := NewObjectReference(ObjectKindUnknown, "resource-1"); err == nil {
		t.Fatal("NewObjectReference() error = nil")
	} else if !errors.Is(err, ErrInvalidObjectReference) {
		t.Fatalf("NewObjectReference() error = %v, want ErrInvalidObjectReference", err)
	}
}

func TestObjectReferenceRejectsReservedIntentKind(t *testing.T) {
	reference := ObjectReference{Kind: ObjectKindIntent, ID: "intent-1"}
	if err := reference.Validate(); !errors.Is(err, ErrInvalidObjectReference) {
		t.Fatalf("Intent ObjectReference.Validate() error = %v, want ErrInvalidObjectReference", err)
	}
}

func TestSemanticStateVocabulariesAreSeparate(t *testing.T) {
	if LifecycleActive == LifecycleRemoved {
		t.Fatal("lifecycle states unexpectedly equal")
	}
	if AvailabilityAvailable == AvailabilityUnavailable {
		t.Fatal("availability states unexpectedly equal")
	}
	if EvaluationUnknown == "" {
		t.Fatal("evaluation vocabulary is empty")
	}
}
