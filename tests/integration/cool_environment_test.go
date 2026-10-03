package integration_test

import (
	"context"
	"testing"

	"dtm/internal/capability"
	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/planner"
	"dtm/internal/runtime"
	"dtm/internal/task"
)

type successfulExecutor struct{}

func (successfulExecutor) Execute(
	_ context.Context,
	_ model.TaskID,
	step mapper.MappedStep,
) (execution.StepResult, error) {
	return execution.NewStepResult(
		step.ID,
		step.NodeID,
		execution.StatusSucceeded,
		map[string]any{"capability": step.Capability.String()},
		"",
	)
}

func TestCoolEnvironmentFlowsThroughTheMesh(t *testing.T) {
	input, err := task.New(
		"task-cool-1",
		"cool_environment",
		[]model.Capability{"temperature_sensor", "cooling_control"},
		task.Constraints{"target_temperature": "26"},
	)
	if err != nil {
		t.Fatalf("task.New() error = %v", err)
	}
	life, err := lifecycle.New(input.ID)
	if err != nil {
		t.Fatalf("lifecycle.New() error = %v", err)
	}

	plan, err := planner.New().Plan(input)
	if err != nil {
		t.Fatalf("Planner.Plan() error = %v", err)
	}
	if err := life.Transition(lifecycle.StatePlanned); err != nil {
		t.Fatalf("transition to planned: %v", err)
	}

	registry := capability.NewRegistry()
	sensor, _ := node.New("sensor-a", []model.Capability{"temperature_sensor"}, node.StatusOnline)
	cooler, _ := node.New("cooler-a", []model.Capability{"cooling_control"}, node.StatusOnline)
	registry.Register(sensor)
	registry.Register(cooler)

	meshMapper, err := mapper.New(registry)
	if err != nil {
		t.Fatalf("mapper.New() error = %v", err)
	}
	mapped, err := meshMapper.Map(plan)
	if err != nil {
		t.Fatalf("Mapper.Map() error = %v", err)
	}
	if err := life.Transition(lifecycle.StateMapped); err != nil {
		t.Fatalf("transition to mapped: %v", err)
	}

	meshRuntime, err := runtime.New(successfulExecutor{})
	if err != nil {
		t.Fatalf("runtime.New() error = %v", err)
	}
	if err := life.Transition(lifecycle.StateDispatched); err != nil {
		t.Fatalf("transition to dispatched: %v", err)
	}
	if err := life.Transition(lifecycle.StateRunning); err != nil {
		t.Fatalf("transition to running: %v", err)
	}
	result, err := meshRuntime.Execute(context.Background(), mapped)
	if err != nil {
		t.Fatalf("Runtime.Execute() error = %v", err)
	}
	if err := life.Transition(lifecycle.StateSucceeded); err != nil {
		t.Fatalf("transition to succeeded: %v", err)
	}

	if life.State() != lifecycle.StateSucceeded {
		t.Fatalf("lifecycle state = %q, want %q", life.State(), lifecycle.StateSucceeded)
	}
	if len(result.StepResults) != 2 {
		t.Fatalf("step result count = %d, want 2", len(result.StepResults))
	}
	if result.StepResults[0].StepID != "step-1" || result.StepResults[0].NodeID != "sensor-a" {
		t.Fatalf("first result = %+v, want step-1 on sensor-a", result.StepResults[0])
	}
	if result.StepResults[1].StepID != "step-2" || result.StepResults[1].NodeID != "cooler-a" {
		t.Fatalf("second result = %+v, want step-2 on cooler-a", result.StepResults[1])
	}
}
