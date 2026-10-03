package model

import (
	"errors"
	"testing"
)

func TestNewCapabilityRejectsBlankName(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "whitespace", value: " \t\n "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewCapability(tt.value)

			if !errors.Is(err, ErrInvalidCapability) {
				t.Fatalf("NewCapability(%q) error = %v, want %v", tt.value, err, ErrInvalidCapability)
			}
		})
	}
}

func TestNewCapabilityPreservesSuppliedNonblankName(t *testing.T) {
	const supplied = "  cool_environment  "

	capability, err := NewCapability(supplied)
	if err != nil {
		t.Fatalf("NewCapability(%q) returned unexpected error: %v", supplied, err)
	}
	if got := capability.String(); got != supplied {
		t.Fatalf("Capability.String() = %q, want %q", got, supplied)
	}
}

func TestSharedIdentifiersAreNamedStringTypes(t *testing.T) {
	var taskID TaskID = "task-1"
	var nodeID NodeID = "node-1"
	var stepID StepID = "step-1"

	if string(taskID) != "task-1" {
		t.Fatalf("TaskID = %q, want %q", taskID, "task-1")
	}
	if string(nodeID) != "node-1" {
		t.Fatalf("NodeID = %q, want %q", nodeID, "node-1")
	}
	if string(stepID) != "step-1" {
		t.Fatalf("StepID = %q, want %q", stepID, "step-1")
	}
}
