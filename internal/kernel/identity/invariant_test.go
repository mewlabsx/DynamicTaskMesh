package identity

import (
	"errors"
	"reflect"
	"testing"
)

func TestInvariant_LifecycleAvailabilityAndEvaluationTypesRemainSeparate(t *testing.T) {
	if reflect.TypeOf(LifecycleState("ACTIVE")) == reflect.TypeOf(AvailabilityState("AVAILABLE")) {
		t.Fatal("LifecycleState and AvailabilityState unexpectedly share a type")
	}
	if reflect.TypeOf(LifecycleState("ACTIVE")) == reflect.TypeOf(EvaluationState("UNKNOWN")) {
		t.Fatal("LifecycleState and EvaluationState unexpectedly share a type")
	}
	if reflect.TypeOf(AvailabilityState("AVAILABLE")) == reflect.TypeOf(EvaluationState("UNKNOWN")) {
		t.Fatal("AvailabilityState and EvaluationState unexpectedly share a type")
	}
	if err := AvailabilityUnavailable.Validate(); err != nil {
		t.Fatalf("AvailabilityUnavailable.Validate() error = %v", err)
	}
	if err := EvaluationUnknown.Validate(); err != nil {
		t.Fatalf("EvaluationUnknown.Validate() error = %v", err)
	}
	if err := LifecycleState(AvailabilityUnavailable).Validate(); err == nil {
		t.Fatal("UNAVAILABLE was accepted as LifecycleState")
	}
}

func TestInvariant_ReservedIntentReferenceRejectionIsDeterministic(t *testing.T) {
	reference := ObjectReference{Kind: ObjectKindIntent, ID: "intent-1"}
	firstErr := reference.Validate()
	if !errors.Is(firstErr, ErrInvalidObjectReference) {
		t.Fatalf("first ObjectReference.Validate() error = %v, want ErrInvalidObjectReference", firstErr)
	}
	expectedText := firstErr.Error()
	for attempt := 1; attempt < 32; attempt++ {
		err := reference.Validate()
		if !errors.Is(err, ErrInvalidObjectReference) {
			t.Fatalf("attempt %d: ObjectReference.Validate() error = %v, want ErrInvalidObjectReference", attempt, err)
		}
		if err.Error() != expectedText {
			t.Fatalf("attempt %d: ObjectReference.Validate() error = %v, want %q", attempt, err, expectedText)
		}
	}
}
