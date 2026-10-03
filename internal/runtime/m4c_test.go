package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
)

type mappingValidatorFunc func(mapper.MappedStep) error

func (function mappingValidatorFunc) Validate(step mapper.MappedStep) error {
	return function(step)
}

func completeResourceStep() mapper.MappedStep {
	return mapper.MappedStep{
		ID: "step-1", Capability: "temperature_sensor", NodeID: "node-a",
		ResourceRef: model.ResourceRef{
			ResourceID: "resource-a", ResourceGeneration: 1, OwnerNodeID: "node-a",
			OwnerNodeGeneration: 1, RegistrationID: "registration-a",
		},
	}
}

func TestExecuteStaleResourceMappingRemapsBeforeAttempt(t *testing.T) {
	step := completeResourceStep()
	plan := mappedPlan("task-stale", step)
	validationCalls := 0
	executorCalls := 0
	var attempts []StepAttempt
	instance, err := New(
		attemptExecutorFunc(func(_ context.Context, _ model.TaskID, step mapper.MappedStep, attempt StepAttempt) (execution.StepResult, error) {
			executorCalls++
			attempts = append(attempts, attempt)
			return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, nil, "")
		}),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 1}),
		WithRemapping(remapperFunc(func(step mapper.MappedStep, excluded map[model.NodeID]struct{}) (mapper.MappedStep, error) {
			if len(excluded) != 0 {
				t.Fatalf("stale remap excluded nodes = %v, want none", excluded)
			}
			step.NodeID = "node-b"
			step.ResourceRef.OwnerNodeID = "node-b"
			step.ResourceRef.ResourceID = "resource-b"
			return step, nil
		}), RemappingPolicy{MaxRemap: 1}),
		WithMappingValidator(mappingValidatorFunc(func(step mapper.MappedStep) error {
			validationCalls++
			if validationCalls == 1 {
				return mapper.ErrStaleResourceMapping
			}
			if step.NodeID != "node-b" || step.ResourceRef.ResourceID != "resource-b" {
				t.Fatalf("validated remapped step = %+v", step)
			}
			return nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	var progress []Progress
	result, err := instance.ExecuteWithObserver(context.Background(), plan, func(_ context.Context, event Progress) error {
		progress = append(progress, event)
		return nil
	})
	if err != nil || result.Status != execution.StatusSucceeded {
		t.Fatalf("ExecuteWithObserver() = %#v, %v", result, err)
	}
	if executorCalls != 1 || !reflect.DeepEqual(attempts, []StepAttempt{{Number: 1, IdempotencyKey: IdempotencyKey(plan.TaskID, step.ID)}}) {
		t.Fatalf("executor calls=%d attempts=%v, want one attempt #1", executorCalls, attempts)
	}
	var states []ProgressState
	for _, event := range progress {
		states = append(states, event.State)
		switch event.State {
		case ProgressRemapped:
			if event.ResourceRef.ResourceID != "resource-b" || event.MappingValidated {
				t.Fatalf("remap evidence = %+v, want new unvalidated ResourceRef", event)
			}
		case ProgressAttemptStarted, ProgressAttemptSucceeded:
			if event.NodeID != "node-b" || event.Capability != "temperature_sensor" ||
				event.ResourceRef.ResourceID != "resource-b" || event.ResourceRef.OwnerNodeID != "node-b" ||
				!event.MappingValidated {
				t.Fatalf("validated execution evidence = %+v", event)
			}
		}
	}
	want := []ProgressState{ProgressRemapped, ProgressAttemptStarted, ProgressAttemptSucceeded}
	if !reflect.DeepEqual(states, want) {
		t.Fatalf("progress states=%v, want %v", states, want)
	}
}

func TestExecuteInvalidResourceMappingFailsBeforeAttemptOrRemap(t *testing.T) {
	executorCalls, remapCalls := 0, 0
	instance, err := New(
		executorFunc(func(context.Context, mapper.MappedStep) (execution.StepResult, error) {
			executorCalls++
			return execution.StepResult{}, nil
		}),
		WithRemapping(remapperFunc(func(step mapper.MappedStep, _ map[model.NodeID]struct{}) (mapper.MappedStep, error) {
			remapCalls++
			return step, nil
		}), RemappingPolicy{MaxRemap: 2}),
		WithMappingValidator(mappingValidatorFunc(func(mapper.MappedStep) error {
			return mapper.ErrInvalidResourceMapping
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = instance.Execute(context.Background(), mappedPlan("task-invalid-resource", completeResourceStep()))
	if !errors.Is(err, ErrInvalidResourceMapping) || executorCalls != 0 || remapCalls != 0 {
		t.Fatalf("Execute() error=%v executorCalls=%d remapCalls=%d", err, executorCalls, remapCalls)
	}
}

func TestExecuteRevalidatesResourceMappingBeforeEveryRetryAttempt(t *testing.T) {
	validationCalls := 0
	executorCalls := 0
	instance, err := New(
		attemptExecutorFunc(func(_ context.Context, _ model.TaskID, step mapper.MappedStep, attempt StepAttempt) (execution.StepResult, error) {
			executorCalls++
			if attempt.Number == 1 {
				return execution.StepResult{}, fmt.Errorf("%w: transient failure", ErrRetryableExecution)
			}
			return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, nil, "")
		}),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 2}),
		WithMappingValidator(mappingValidatorFunc(func(mapper.MappedStep) error {
			validationCalls++
			if validationCalls == 2 {
				return mapper.ErrStaleResourceMapping
			}
			return nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = instance.Execute(context.Background(), mappedPlan("task-retry-fence", completeResourceStep()))
	if !errors.Is(err, ErrStaleResourceMapping) || validationCalls != 2 || executorCalls != 1 {
		t.Fatalf("Execute() error=%v validationCalls=%d executorCalls=%d", err, validationCalls, executorCalls)
	}
}

func TestExecuteStaleResourceMappingStopsAtMaxRemapWithoutAttempt(t *testing.T) {
	validationCalls := 0
	remapCalls := 0
	executorCalls := 0
	instance, err := New(
		executorFunc(func(context.Context, mapper.MappedStep) (execution.StepResult, error) {
			executorCalls++
			return execution.StepResult{}, nil
		}),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 2}),
		WithRemapping(remapperFunc(func(step mapper.MappedStep, excluded map[model.NodeID]struct{}) (mapper.MappedStep, error) {
			remapCalls++
			if len(excluded) != 0 {
				t.Fatalf("stale remap excluded nodes = %v, want none", excluded)
			}
			step.NodeID = model.NodeID(fmt.Sprintf("node-remap-%d", remapCalls))
			step.ResourceRef.OwnerNodeID = step.NodeID
			return step, nil
		}), RemappingPolicy{MaxRemap: 2}),
		WithMappingValidator(mappingValidatorFunc(func(mapper.MappedStep) error {
			validationCalls++
			return mapper.ErrStaleResourceMapping
		})),
	)
	if err != nil {
		t.Fatal(err)
	}

	var progress []Progress
	_, err = instance.ExecuteWithObserver(context.Background(), mappedPlan("task-max-remap", completeResourceStep()), func(_ context.Context, event Progress) error {
		progress = append(progress, event)
		return nil
	})
	if !errors.Is(err, ErrStaleResourceMapping) || validationCalls != 3 || remapCalls != 2 || executorCalls != 0 {
		t.Fatalf("ExecuteWithObserver() error=%v validationCalls=%d remapCalls=%d executorCalls=%d", err, validationCalls, remapCalls, executorCalls)
	}
	for _, event := range progress {
		if event.State != ProgressRemapped || event.Attempt != 0 {
			t.Fatalf("progress=%+v, want remap-only events with unconsumed attempt", progress)
		}
	}
}
