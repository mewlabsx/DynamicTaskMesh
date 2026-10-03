package application

import (
	"context"
	"errors"
	"strings"
	"testing"

	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/planner"
	meshruntime "dtm/internal/runtime"
	"dtm/internal/task"
)

type resourceRecoveryFunc func(context.Context, mapper.MappedStep) (mapper.MappedStep, bool, error)

func (function resourceRecoveryFunc) RecoverResourceMapping(ctx context.Context, step mapper.MappedStep) (mapper.MappedStep, bool, error) {
	return function(ctx, step)
}

func recoveredResourceRef(nodeID model.NodeID) model.ResourceRef {
	return model.ResourceRef{
		ResourceID: "resource-recovered", ResourceGeneration: 2, OwnerNodeID: nodeID,
		OwnerNodeGeneration: 3, RegistrationID: "registration-recovered",
	}
}

func runningRecoverableRecord(inputID model.TaskID, step mapper.MappedStep) TaskRecord {
	return TaskRecord{
		Task: inputTask(inputID), State: lifecycle.StateRunning, Version: 3,
		Steps: []ExecutionStepRecord{{
			ID: step.ID, Position: 0, Capability: step.Capability,
			IdempotencyMode: model.IdempotencyIdempotent, NodeID: step.NodeID,
			Inputs: cloneStringMap(step.Inputs), State: lifecycle.StepStateRunning,
			MaxAttempts: 3, Version: 2,
		}},
	}
}

func inputTask(id model.TaskID) task.Task {
	return task.Task{ID: id, Intent: "measure_temperature"}
}

func TestRecoverResourceMappingBuildsCompleteRuntimeRef(t *testing.T) {
	input, _, mapped, _ := validPipeline(t)
	repository := &recordingTaskRepository{latest: map[model.TaskID]TaskRecord{
		input.ID: runningRecoverableRecord(input.ID, mapped.Steps[0]),
	}}
	var gotPlan mapper.MappedPlan
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, nil }),
		observableRunFunc(func(_ context.Context, plan mapper.MappedPlan, _ meshruntime.Observer) (execution.Result, error) {
			gotPlan = plan
			result, err := execution.NewStepResult(plan.Steps[0].ID, plan.Steps[0].NodeID, execution.StatusSucceeded, nil, "")
			if err != nil {
				return execution.Result{}, err
			}
			return execution.NewResult(plan.TaskID, execution.StatusSucceeded, []execution.StepResult{result}, "")
		}),
		WithTaskRepository(repository),
		WithResourceRecoveryMapper(resourceRecoveryFunc(func(_ context.Context, step mapper.MappedStep) (mapper.MappedStep, bool, error) {
			step.ResourceRef = recoveredResourceRef(step.NodeID)
			return step, false, nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(context.Background(), input.ID); err != nil {
		t.Fatal(err)
	}
	if len(gotPlan.Steps) != 1 || gotPlan.Steps[0].ResourceRef != recoveredResourceRef(mapped.Steps[0].NodeID) {
		t.Fatalf("recovered runtime plan = %#v, want complete ResourceRef", gotPlan)
	}
}

func TestRecoverResourceRemapReusesPersistedResolutionBeforeExecution(t *testing.T) {
	input, _, mapped, _ := validPipeline(t)
	repository := &recordingTaskRepository{latest: map[model.TaskID]TaskRecord{
		input.ID: runningRecoverableRecord(input.ID, mapped.Steps[0]),
	}}
	calls := 0
	var gotPlan mapper.MappedPlan
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, nil }),
		observableRunFunc(func(_ context.Context, plan mapper.MappedPlan, _ meshruntime.Observer) (execution.Result, error) {
			gotPlan = plan
			result, err := execution.NewStepResult(plan.Steps[0].ID, plan.Steps[0].NodeID, execution.StatusSucceeded, nil, "")
			if err != nil {
				return execution.Result{}, err
			}
			return execution.NewResult(plan.TaskID, execution.StatusSucceeded, []execution.StepResult{result}, "")
		}),
		WithTaskRepository(repository),
		WithResourceRecoveryMapper(resourceRecoveryFunc(func(_ context.Context, step mapper.MappedStep) (mapper.MappedStep, bool, error) {
			calls++
			nodeID := model.NodeID("node-b")
			if calls > 1 {
				nodeID = "node-c"
			}
			step.NodeID = nodeID
			step.ResourceRef = recoveredResourceRef(nodeID)
			return step, true, nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(context.Background(), input.ID); err != nil {
		t.Fatal(err)
	}
	got, err := repository.Find(context.Background(), input.ID)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || got.Steps[0].NodeID != "node-b" || gotPlan.Steps[0].NodeID != "node-b" || gotPlan.Steps[0].ResourceRef.OwnerNodeID != "node-b" {
		t.Fatalf("resolution calls=%d persisted=%#v runtime=%#v", calls, got, gotPlan)
	}
}

func TestRecoverResourcePendingPreservesUnderlyingCause(t *testing.T) {
	input, _, mapped, _ := validPipeline(t)
	repository := &recordingTaskRepository{latest: map[model.TaskID]TaskRecord{
		input.ID: runningRecoverableRecord(input.ID, mapped.Steps[0]),
	}}
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, nil }),
		unusedRunner(t),
		WithTaskRepository(repository),
		WithResourceRecoveryMapper(resourceRecoveryFunc(func(context.Context, mapper.MappedStep) (mapper.MappedStep, bool, error) {
			return mapper.MappedStep{}, false, mapper.ErrResourceCandidateSourceNotReady
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = service.Recover(context.Background(), input.ID)
	if !errors.Is(err, ErrRecoveryPending) || !errors.Is(err, mapper.ErrResourceCandidateSourceNotReady) {
		t.Fatalf("Recover() error = %v, want pending and source-not-ready", err)
	}
	got, _ := repository.Find(context.Background(), input.ID)
	if got.State != lifecycle.StateRunning || len(repository.records) != 0 {
		t.Fatalf("pending recovery mutated task: %#v snapshots=%d", got, len(repository.records))
	}
}

func TestRecoverUnsupportedResourceCapabilityTerminalizesTask(t *testing.T) {
	input, _, mapped, _ := validPipeline(t)
	repository := &recordingTaskRepository{latest: map[model.TaskID]TaskRecord{
		input.ID: runningRecoverableRecord(input.ID, mapped.Steps[0]),
	}}
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, nil }),
		unusedRunner(t),
		WithTaskRepository(repository),
		WithResourceRecoveryMapper(resourceRecoveryFunc(func(context.Context, mapper.MappedStep) (mapper.MappedStep, bool, error) {
			return mapper.MappedStep{}, false, model.ErrUnsupportedLegacyCapability
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(context.Background(), input.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := repository.Find(context.Background(), input.ID)
	if got.State != lifecycle.StateFailed || got.FailureCode != RecoveryResourceMappingFailureCode || !strings.Contains(got.Error, model.ErrUnsupportedLegacyCapability.Error()) {
		t.Fatalf("terminal recovery record = %#v", got)
	}
}
