package task

import (
	"errors"
	"testing"

	"dtm/internal/model"
)

func TestNewRejectsEmptyID(t *testing.T) {
	for _, id := range []model.TaskID{"", " \t\n "} {
		_, err := New(id, "cool_environment", nil, nil)

		if !errors.Is(err, ErrInvalidTask) {
			t.Fatalf("New(%q, ...) error = %v, want %v", id, err, ErrInvalidTask)
		}
	}
}

func TestNewRejectsEmptyIntent(t *testing.T) {
	for _, intent := range []string{"", " \t\n "} {
		_, err := New("task-1", intent, nil, nil)

		if !errors.Is(err, ErrInvalidTask) {
			t.Fatalf("New(..., %q, ...) error = %v, want %v", intent, err, ErrInvalidTask)
		}
	}
}

func TestNewRejectsDuplicateRequirements(t *testing.T) {
	requirements := []model.Capability{"temperature_sensor", "temperature_sensor"}

	_, err := New("task-1", "cool_environment", requirements, nil)

	if !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("New(...) error = %v, want %v", err, ErrInvalidTask)
	}
}

func TestNewRejectsBlankRequirement(t *testing.T) {
	for _, requirement := range []model.Capability{"", " \t\n "} {
		_, err := New("task-1", "cool_environment", []model.Capability{requirement}, nil)

		if !errors.Is(err, ErrInvalidTask) {
			t.Fatalf("New(..., requirement %q, ...) error = %v, want %v", requirement, err, ErrInvalidTask)
		}
	}
}

func TestNewAllowsNoRequirementsBeforePlanning(t *testing.T) {
	created, err := New("task-1", "cool_environment", nil, nil)

	if err != nil {
		t.Fatalf("New(...) returned unexpected error: %v", err)
	}
	if len(created.Requirements) != 0 {
		t.Fatalf("len(Task.Requirements) = %d, want 0", len(created.Requirements))
	}
}

func TestNewCopiesConstraintsAndRequirements(t *testing.T) {
	requirements := []model.Capability{"temperature_sensor", "cooling_control"}
	constraints := Constraints{"latency": "100ms"}

	created, err := New("task-1", "cool_environment", requirements, constraints)
	if err != nil {
		t.Fatalf("New(...) returned unexpected error: %v", err)
	}

	requirements[0] = "mutated_capability"
	constraints["latency"] = "5s"
	constraints["location"] = "caller-owned"

	if got := created.Requirements[0]; got != "temperature_sensor" {
		t.Fatalf("Task.Requirements[0] = %q after caller mutation, want %q", got, "temperature_sensor")
	}
	if got := created.Constraints["latency"]; got != "100ms" {
		t.Fatalf("Task.Constraints[latency] = %q after caller mutation, want %q", got, "100ms")
	}
	if _, exists := created.Constraints["location"]; exists {
		t.Fatal("Task.Constraints unexpectedly contains key added to caller-owned map")
	}
}
