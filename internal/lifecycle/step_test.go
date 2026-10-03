package lifecycle

import (
	"errors"
	"testing"
)

func TestStepLifecycleAllowsExecutionSequence(t *testing.T) {
	lifecycle, err := NewStep("step-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []StepState{
		StepStateMapped,
		StepStateDispatched,
		StepStateRunning,
		StepStateSuccess,
	} {
		if err := lifecycle.Transition(next); err != nil {
			t.Fatalf("Transition(%q) error = %v", next, err)
		}
	}
}

func TestStepLifecycleAllowsRetryAndRemap(t *testing.T) {
	lifecycle, err := RestoreStep("step-1", StepStateRunning)
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []StepState{
		StepStateFailed,
		StepStateRetrying,
		StepStateRemapped,
		StepStateRunning,
		StepStateSuccess,
	} {
		if err := lifecycle.Transition(next); err != nil {
			t.Fatalf("Transition(%q) error = %v", next, err)
		}
	}
}

func TestStepLifecycleRejectsInvalidIdentityStateAndTransition(t *testing.T) {
	if _, err := NewStep(""); !errors.Is(err, ErrInvalidStepTransition) {
		t.Fatalf("NewStep(blank) error = %v", err)
	}
	if _, err := RestoreStep("step-1", StepState("unknown")); !errors.Is(err, ErrInvalidStepTransition) {
		t.Fatalf("RestoreStep(unknown) error = %v", err)
	}
	lifecycle, err := NewStep("step-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := lifecycle.Transition(StepStateRunning); !errors.Is(err, ErrInvalidStepTransition) {
		t.Fatalf("Transition(running) error = %v", err)
	}
}

func TestStepSuccessAndCancelledAreTerminal(t *testing.T) {
	for _, state := range []StepState{StepStateSuccess, StepStateCancelled} {
		lifecycle, err := RestoreStep("step-1", state)
		if err != nil {
			t.Fatal(err)
		}
		if err := lifecycle.Transition(StepStateFailed); !errors.Is(err, ErrInvalidStepTransition) {
			t.Fatalf("Transition(failed) from %q error = %v", state, err)
		}
	}
}
