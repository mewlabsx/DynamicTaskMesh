package planner

import (
	"errors"
	"reflect"
	"testing"

	"dtm/internal/model"
	"dtm/internal/task"
)

func TestPlanCoolEnvironment(t *testing.T) {
	t.Parallel()

	input, err := task.New(
		"task-1",
		"cool_environment",
		nil,
		task.Constraints{"target_temperature": "26"},
	)
	if err != nil {
		t.Fatalf("task.New() error = %v", err)
	}

	got, err := New().Plan(input)
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}

	want := Plan{
		TaskID: "task-1",
		Steps: []Step{
			{
				ID:              "step-1",
				Capability:      "temperature_sensor",
				IdempotencyMode: model.IdempotencyIdempotent,
				Inputs:          map[string]string{"operation": "read_temperature"},
			},
			{
				ID:              "step-2",
				Capability:      "cooling_control",
				IdempotencyMode: model.IdempotencyNonIdempotent,
				Inputs:          map[string]string{"target_temperature": "26"},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Plan() = %#v, want %#v", got, want)
	}
}

func TestPlanAcceptsExactlyMatchingRequirements(t *testing.T) {
	t.Parallel()

	input, err := task.New(
		"task-1",
		"cool_environment",
		[]model.Capability{"temperature_sensor", "cooling_control"},
		task.Constraints{"target_temperature": "26"},
	)
	if err != nil {
		t.Fatalf("task.New() error = %v", err)
	}

	if _, err := New().Plan(input); err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
}

func TestPlanRejectsConflictingRequirements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		requirements []model.Capability
	}{
		{name: "wrong value", requirements: []model.Capability{"temperature_sensor", "heating_control"}},
		{name: "wrong order", requirements: []model.Capability{"cooling_control", "temperature_sensor"}},
		{name: "missing value", requirements: []model.Capability{"temperature_sensor"}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			input, err := task.New(
				"task-1",
				"cool_environment",
				tt.requirements,
				task.Constraints{"target_temperature": "26"},
			)
			if err != nil {
				t.Fatalf("task.New() error = %v", err)
			}

			_, err = New().Plan(input)
			if !errors.Is(err, ErrRequirementConflict) {
				t.Fatalf("Plan() error = %v, want %v", err, ErrRequirementConflict)
			}
		})
	}
}

func TestPlanRejectsUnknownIntentWithDistinctError(t *testing.T) {
	t.Parallel()

	input, err := task.New("task-1", "warm_environment", nil, nil)
	if err != nil {
		t.Fatalf("task.New() error = %v", err)
	}

	_, err = New().Plan(input)
	if !errors.Is(err, ErrUnknownIntent) {
		t.Fatalf("Plan() error = %v, want %v", err, ErrUnknownIntent)
	}
	if errors.Is(err, ErrRequirementConflict) {
		t.Fatalf("Plan() error = %v, must be distinct from %v", err, ErrRequirementConflict)
	}
}

func TestPlanReturnsIndependentStepSlices(t *testing.T) {
	t.Parallel()

	input, err := task.New(
		"task-1",
		"cool_environment",
		nil,
		task.Constraints{"target_temperature": "26"},
	)
	if err != nil {
		t.Fatalf("task.New() error = %v", err)
	}
	planner := New()

	first, err := planner.Plan(input)
	if err != nil {
		t.Fatalf("first Plan() error = %v", err)
	}
	first.Steps[0].Capability = "tampered"

	second, err := planner.Plan(input)
	if err != nil {
		t.Fatalf("second Plan() error = %v", err)
	}
	if got, want := second.Steps[0].Capability, model.Capability("temperature_sensor"); got != want {
		t.Fatalf("second Plan() capability = %q, want %q", got, want)
	}
}

func TestPlanCopiesStepInputs(t *testing.T) {
	t.Parallel()

	input, err := task.New(
		"task-1",
		"cool_environment",
		nil,
		task.Constraints{"target_temperature": "26"},
	)
	if err != nil {
		t.Fatalf("task.New() error = %v", err)
	}

	planner := New()
	first, err := planner.Plan(input)
	if err != nil {
		t.Fatalf("first Plan() error = %v", err)
	}

	input.Constraints["target_temperature"] = "18"
	if got := first.Steps[1].Inputs["target_temperature"]; got != "26" {
		t.Fatalf("first cooling target after task mutation = %q, want %q", got, "26")
	}

	first.Steps[0].Inputs["operation"] = "tampered"
	first.Steps[1].Inputs["target_temperature"] = "12"

	secondInput, err := task.New(
		"task-2",
		"cool_environment",
		nil,
		task.Constraints{"target_temperature": "26"},
	)
	if err != nil {
		t.Fatalf("task.New() error = %v", err)
	}
	second, err := planner.Plan(secondInput)
	if err != nil {
		t.Fatalf("second Plan() error = %v", err)
	}
	if got := second.Steps[0].Inputs["operation"]; got != "read_temperature" {
		t.Fatalf("second sensor operation = %q, want %q", got, "read_temperature")
	}
	if got := second.Steps[1].Inputs["target_temperature"]; got != "26" {
		t.Fatalf("second cooling target = %q, want %q", got, "26")
	}
}

func TestPlanRejectsInvalidTargetTemperature(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		constraints task.Constraints
	}{
		{name: "missing", constraints: nil},
		{name: "not numeric", constraints: task.Constraints{"target_temperature": "cool"}},
		{name: "NaN", constraints: task.Constraints{"target_temperature": "NaN"}},
		{name: "positive infinity", constraints: task.Constraints{"target_temperature": "+Inf"}},
		{name: "negative infinity", constraints: task.Constraints{"target_temperature": "-Inf"}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			input, err := task.New("task-1", "cool_environment", nil, tt.constraints)
			if err != nil {
				t.Fatalf("task.New() error = %v", err)
			}

			_, err = New().Plan(input)
			if !errors.Is(err, ErrRequirementConflict) {
				t.Fatalf("Plan() error = %v, want %v", err, ErrRequirementConflict)
			}
		})
	}
}
