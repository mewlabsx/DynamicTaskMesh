package application

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/planner"
	meshruntime "dtm/internal/runtime"
	"dtm/internal/storage"
	"dtm/internal/task"
)

var (
	errFactory = errors.New("factory failed")
	errPlan    = errors.New("planning failed")
	errMap     = errors.New("mapping failed")
	errRun     = errors.New("runtime failed")
)

type planFunc func(task.Task) (planner.Plan, error)

func (fn planFunc) Plan(input task.Task) (planner.Plan, error) {
	return fn(input)
}

type mapFunc func(planner.Plan) (mapper.MappedPlan, error)

func (fn mapFunc) Map(input planner.Plan) (mapper.MappedPlan, error) {
	return fn(input)
}

type runFunc func(context.Context, mapper.MappedPlan) (execution.Result, error)

func (fn runFunc) Execute(ctx context.Context, input mapper.MappedPlan) (execution.Result, error) {
	return fn(ctx, input)
}

type observableRunFunc func(
	context.Context,
	mapper.MappedPlan,
	meshruntime.Observer,
) (execution.Result, error)

func (fn observableRunFunc) Execute(
	ctx context.Context,
	input mapper.MappedPlan,
) (execution.Result, error) {
	return fn(ctx, input, nil)
}

type recoveryEligibilityFunc func(model.NodeID) bool

func (function recoveryEligibilityFunc) Eligible(nodeID model.NodeID) bool {
	return function(nodeID)
}

func (fn observableRunFunc) ExecuteWithObserver(
	ctx context.Context,
	input mapper.MappedPlan,
	observer meshruntime.Observer,
) (execution.Result, error) {
	return fn(ctx, input, observer)
}

func TestM5ResponseLostRetryReturnsOriginalTask(t *testing.T) {
	first, plan, mapped, result := validPipeline(t)
	retry, err := task.New("task-retry", first.Intent, first.Requirements, first.Constraints)
	if err != nil {
		t.Fatal(err)
	}
	repository := &recordingTaskRepository{}
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(input task.Task) (planner.Plan, error) {
			copy := plan
			copy.TaskID = input.ID
			return copy, nil
		}),
		mapFunc(func(input planner.Plan) (mapper.MappedPlan, error) {
			copy := mapped
			copy.TaskID = input.TaskID
			return copy, nil
		}),
		runFunc(func(_ context.Context, input mapper.MappedPlan) (execution.Result, error) {
			copy := result
			copy.TaskID = input.TaskID
			return copy, nil
		}),
		WithTaskRepository(repository),
	)
	if err != nil {
		t.Fatal(err)
	}
	firstOutcome, deduplicated, err := service.SubmitSubmission(context.Background(), first, "submission-1", "v1:same")
	if err != nil || deduplicated {
		t.Fatalf("first submission = (%#v, %t, %v)", firstOutcome, deduplicated, err)
	}
	retryOutcome, deduplicated, err := service.SubmitSubmission(context.Background(), retry, "submission-1", "v1:same")
	if err != nil {
		t.Fatal(err)
	}
	if !deduplicated {
		t.Fatal("retry was not reported as deduplicated")
	}
	if retryOutcome.TaskID != first.ID {
		t.Fatalf("retry task ID = %q, want %q", retryOutcome.TaskID, first.ID)
	}
	if len(repository.latest) != 1 {
		t.Fatalf("response-lost retry persisted %d tasks, want one", len(repository.latest))
	}
}

func TestSubmitSucceedsInLifecycleOrder(t *testing.T) {
	input, plan, mapped, result := validPipeline(t)
	var calls []string
	var created *lifecycle.Lifecycle

	service, err := NewTaskService(
		func(id model.TaskID) (*lifecycle.Lifecycle, error) {
			calls = append(calls, "create")
			value, newErr := lifecycle.New(id)
			created = value
			return value, newErr
		},
		planFunc(func(got task.Task) (planner.Plan, error) {
			calls = append(calls, "plan")
			if !reflect.DeepEqual(got, input) {
				t.Fatalf("planned task = %#v, want %#v", got, input)
			}
			if created.State() != lifecycle.StateCreated {
				t.Fatalf("state during plan = %q", created.State())
			}
			return plan, nil
		}),
		mapFunc(func(got planner.Plan) (mapper.MappedPlan, error) {
			calls = append(calls, "map")
			if !reflect.DeepEqual(got, plan) {
				t.Fatalf("mapped plan = %#v, want %#v", got, plan)
			}
			if created.State() != lifecycle.StatePlanned {
				t.Fatalf("state during map = %q", created.State())
			}
			return mapped, nil
		}),
		runFunc(func(_ context.Context, got mapper.MappedPlan) (execution.Result, error) {
			calls = append(calls, "run")
			if !reflect.DeepEqual(got, mapped) {
				t.Fatalf("executed plan = %#v, want %#v", got, mapped)
			}
			if created.State() != lifecycle.StateRunning {
				t.Fatalf("state during run = %q", created.State())
			}
			return result, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	outcome, err := service.Submit(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"create", "plan", "map", "run"}) {
		t.Fatalf("calls = %v", calls)
	}
	if outcome.TaskID != input.ID || outcome.State != lifecycle.StateSucceeded {
		t.Fatalf("outcome = %#v", outcome)
	}
	if outcome.Execution == nil || !reflect.DeepEqual(*outcome.Execution, result) {
		t.Fatalf("execution = %#v, want %#v", outcome.Execution, result)
	}
	if created.State() != lifecycle.StateSucceeded {
		t.Fatalf("final lifecycle state = %q", created.State())
	}
}

func TestSubmitPersistsEachLifecycleMilestoneAndExecutionResult(t *testing.T) {
	input, plan, mapped, result := validPipeline(t)
	repository := &recordingTaskRepository{}
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapped, nil }),
		observableRunFunc(func(ctx context.Context, _ mapper.MappedPlan, observer meshruntime.Observer) (execution.Result, error) {
			if err := observer(ctx, meshruntime.Progress{TaskID: input.ID, StepID: "step-1", NodeID: "sensor-1", Attempt: 1, State: meshruntime.ProgressAttemptStarted}); err != nil {
				return execution.Result{}, err
			}
			stepResult := result.StepResults[0]
			if err := observer(ctx, meshruntime.Progress{TaskID: input.ID, StepID: "step-1", NodeID: "sensor-1", Attempt: 1, State: meshruntime.ProgressAttemptSucceeded, Result: &stepResult}); err != nil {
				return execution.Result{}, err
			}
			return result, nil
		}),
		WithTaskRepository(repository),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	var gotStates []lifecycle.State
	var gotStepStates []lifecycle.StepState
	for _, persisted := range repository.records {
		if len(gotStates) == 0 || gotStates[len(gotStates)-1] != persisted.State {
			gotStates = append(gotStates, persisted.State)
		}
		if len(persisted.Steps) == 1 && (len(gotStepStates) == 0 || gotStepStates[len(gotStepStates)-1] != persisted.Steps[0].State) {
			gotStepStates = append(gotStepStates, persisted.Steps[0].State)
		}
	}
	wantStates := []lifecycle.State{lifecycle.StateCreated, lifecycle.StatePlanning, lifecycle.StateMapped, lifecycle.StateDispatched, lifecycle.StateRunning, lifecycle.StateSuccess}
	if !reflect.DeepEqual(gotStates, wantStates) {
		t.Fatalf("persisted task states = %v, want %v", gotStates, wantStates)
	}
	wantStepStates := []lifecycle.StepState{lifecycle.StepStateCreated, lifecycle.StepStateMapped, lifecycle.StepStateDispatched, lifecycle.StepStateRunning, lifecycle.StepStateSuccess}
	if !reflect.DeepEqual(gotStepStates, wantStepStates) {
		t.Fatalf("persisted step states = %v, want %v", gotStepStates, wantStepStates)
	}
	final := repository.records[len(repository.records)-1]
	if final.ExecutionStatus != execution.StatusSucceeded ||
		len(final.Steps) != 1 ||
		final.Steps[0].State != lifecycle.StepStateSuccess ||
		final.Steps[0].Status != execution.StatusSucceeded ||
		!reflect.DeepEqual(final.Steps[0].Output, result.StepResults[0].Output) {
		t.Fatalf("final persisted record = %#v", final)
	}
}

func TestSubmitEmitsAndPersistsExactResourceRefEvidence(t *testing.T) {
	input, plan, mapped, result := validPipeline(t)
	ref := model.ResourceRef{
		ResourceID: "sensor-resource", ResourceGeneration: 7, OwnerNodeID: "sensor-1",
		OwnerNodeGeneration: 3, RegistrationID: "registration-sensor-3",
	}
	mapped.Steps[0].ResourceRef = ref
	repository := &recordingTaskRepository{}
	observed := make([]TaskResourceEvidence, 0, 3)
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapped, nil }),
		observableRunFunc(func(ctx context.Context, _ mapper.MappedPlan, observer meshruntime.Observer) (execution.Result, error) {
			stepResult := result.StepResults[0]
			for _, progress := range []meshruntime.Progress{
				{TaskID: input.ID, StepID: "step-1", Capability: "temperature_sensor", NodeID: "sensor-1", ResourceRef: ref, MappingValidated: true, Attempt: 1, State: meshruntime.ProgressAttemptStarted},
				{TaskID: input.ID, StepID: "step-1", Capability: "temperature_sensor", NodeID: "sensor-1", ResourceRef: ref, MappingValidated: true, Attempt: 1, State: meshruntime.ProgressAttemptSucceeded, Result: &stepResult},
			} {
				if err := observer(ctx, progress); err != nil {
					return execution.Result{}, err
				}
			}
			return result, nil
		}),
		WithTaskRepository(repository),
		WithTaskResourceEvidenceObserver(func(evidence TaskResourceEvidence) {
			observed = append(observed, evidence)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if len(observed) != 3 || observed[0].Stage != "selected" ||
		observed[1].Stage != string(meshruntime.ProgressAttemptStarted) ||
		observed[2].Stage != string(meshruntime.ProgressAttemptSucceeded) {
		t.Fatalf("resource evidence stages = %#v", observed)
	}
	for _, evidence := range observed {
		if evidence.ResourceRef != ref || evidence.NodeID != ref.OwnerNodeID || evidence.Capability != "temperature_sensor" {
			t.Fatalf("resource evidence lost exact mapped ResourceRef: %+v", evidence)
		}
	}
	if !observed[1].MappingValidated || !observed[2].MappingValidated ||
		observed[2].ActualExecutionNode != ref.OwnerNodeID || observed[2].ExecutionStatus != execution.StatusSucceeded {
		t.Fatalf("execution evidence = %+v", observed[1:])
	}
	if len(repository.resourceEvidenceEvents) != 3 {
		t.Fatalf("persisted resource evidence events = %#v", repository.resourceEvidenceEvents)
	}
	for _, event := range repository.resourceEvidenceEvents {
		resourceRef, ok := event.Detail["resource_ref"].(map[string]any)
		if !ok || resourceRef["resource_id"] != string(ref.ResourceID) ||
			resourceRef["registration_id"] != ref.RegistrationID || event.Detail["mapping_source"] != "authority_resource_directory" {
			t.Fatalf("persisted event detail = %#v", event.Detail)
		}
	}
}

func TestPersistAggregateProgressDoesNotMutateMemoryBeforeRepositorySuccess(t *testing.T) {
	input, _, mapped, _ := validPipeline(t)
	failure := errors.New("aggregate progress failed")
	tests := []struct {
		name      string
		taskState lifecycle.State
		stepState lifecycle.StepState
		progress  meshruntime.Progress
	}{
		{name: "retry", taskState: lifecycle.StateRunning, stepState: lifecycle.StepStateFailed, progress: meshruntime.Progress{TaskID: input.ID, StepID: mapped.Steps[0].ID, NodeID: mapped.Steps[0].NodeID, State: meshruntime.ProgressRetrying}},
		{name: "dispatch", taskState: lifecycle.StateRetrying, stepState: lifecycle.StepStateRetrying, progress: meshruntime.Progress{TaskID: input.ID, StepID: mapped.Steps[0].ID, NodeID: mapped.Steps[0].NodeID, State: meshruntime.ProgressDispatched}},
		{name: "remap", taskState: lifecycle.StateRetrying, stepState: lifecycle.StepStateRetrying, progress: meshruntime.Progress{TaskID: input.ID, StepID: mapped.Steps[0].ID, NodeID: "sensor-2", PreviousNode: mapped.Steps[0].NodeID, State: meshruntime.ProgressRemapped}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			record := TaskRecord{Task: input, State: test.taskState, Version: 10, Steps: []ExecutionStepRecord{{ID: mapped.Steps[0].ID, Capability: mapped.Steps[0].Capability, NodeID: mapped.Steps[0].NodeID, Inputs: mapped.Steps[0].Inputs, State: test.stepState, Status: execution.StatusFailed, Error: "offline", FailureCode: "execution_failed", Version: 20}}}
			repository := &recordingTaskRepository{latest: map[model.TaskID]TaskRecord{input.ID: cloneTaskRecord(record)}, progressErr: failure}
			service, err := NewTaskService(lifecycle.New, planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, nil }), mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, nil }), runFunc(func(context.Context, mapper.MappedPlan) (execution.Result, error) { return execution.Result{}, nil }), WithTaskRepository(repository))
			if err != nil {
				t.Fatal(err)
			}
			machine, err := lifecycle.Restore(input.ID, test.taskState)
			if err != nil {
				t.Fatal(err)
			}
			before := cloneTaskRecord(record)
			err = service.persistProgress(context.Background(), machine, &record, map[model.ExecutionID]int64{}, test.progress)
			if !errors.Is(err, ErrPersistence) || !errors.Is(err, failure) {
				t.Fatalf("persistProgress() error = %v", err)
			}
			if !reflect.DeepEqual(record, before) || machine.State() != test.taskState || !reflect.DeepEqual(repository.latest[input.ID], before) || len(repository.records) != 0 {
				t.Fatalf("repository failure mutated state: machine=%s before=%#v memory=%#v persisted=%#v snapshots=%d", machine.State(), before, record, repository.latest[input.ID], len(repository.records))
			}
		})
	}
}

func TestPersistAggregateProgressSynchronizesMemoryAndRepositoryVersions(t *testing.T) {
	input, _, mapped, _ := validPipeline(t)
	record := TaskRecord{Task: input, State: lifecycle.StateRetrying, Version: 10, Steps: []ExecutionStepRecord{{ID: mapped.Steps[0].ID, Capability: mapped.Steps[0].Capability, NodeID: mapped.Steps[0].NodeID, Inputs: mapped.Steps[0].Inputs, State: lifecycle.StepStateRetrying, Version: 20}}}
	repository := &recordingTaskRepository{latest: map[model.TaskID]TaskRecord{input.ID: cloneTaskRecord(record)}}
	service, err := NewTaskService(lifecycle.New, planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, nil }), mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, nil }), runFunc(func(context.Context, mapper.MappedPlan) (execution.Result, error) { return execution.Result{}, nil }), WithTaskRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	service.now = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }
	machine, err := lifecycle.Restore(input.ID, record.State)
	if err != nil {
		t.Fatal(err)
	}
	progress := meshruntime.Progress{TaskID: input.ID, StepID: mapped.Steps[0].ID, NodeID: "sensor-2", PreviousNode: mapped.Steps[0].NodeID, State: meshruntime.ProgressRemapped}
	if err := service.persistProgress(context.Background(), machine, &record, map[model.ExecutionID]int64{}, progress); err != nil {
		t.Fatal(err)
	}
	persisted := repository.latest[input.ID]
	if record.State != lifecycle.StateRemapped || record.Version != 11 || record.Steps[0].State != lifecycle.StepStateRemapped || record.Steps[0].Version != 21 || record.Steps[0].NodeID != "sensor-2" || machine.State() != lifecycle.StateRemapped || !reflect.DeepEqual(record, persisted) {
		t.Fatalf("aggregate progress memory/database mismatch: machine=%s memory=%#v persisted=%#v", machine.State(), record, persisted)
	}
	if err := service.persistProgress(context.Background(), machine, &record, map[model.ExecutionID]int64{}, progress); err != nil {
		t.Fatal(err)
	}
	if len(repository.records) != 1 || record.Version != 11 || record.Steps[0].Version != 21 {
		t.Fatalf("duplicate progress wrote a second snapshot: records=%d task_version=%d step_version=%d", len(repository.records), record.Version, record.Steps[0].Version)
	}
}

func TestSubmitPersistsRetryAndRemapProgress(t *testing.T) {
	input, plan, mapped, _ := validPipeline(t)
	repository := &recordingTaskRepository{}
	runner := observableRunFunc(func(
		ctx context.Context,
		_ mapper.MappedPlan,
		observer meshruntime.Observer,
	) (execution.Result, error) {
		failedStep, _ := execution.NewStepResult(
			"step-1", "sensor-1", execution.StatusFailed, nil, "offline",
		)
		step, _ := execution.NewStepResult(
			"step-1", "sensor-2", execution.StatusSucceeded, nil, "",
		)
		attemptFailure := errors.New("offline")
		for _, event := range []meshruntime.Progress{
			{TaskID: input.ID, StepID: "step-1", NodeID: "sensor-1", Attempt: 1, State: meshruntime.ProgressAttemptStarted},
			{TaskID: input.ID, StepID: "step-1", NodeID: "sensor-1", Attempt: 1, State: meshruntime.ProgressAttemptFailed, Result: &failedStep, Failure: attemptFailure},
			{TaskID: input.ID, StepID: "step-1", NodeID: "sensor-1", State: meshruntime.ProgressRetrying},
			{TaskID: input.ID, StepID: "step-1", NodeID: "sensor-2", PreviousNode: "sensor-1", State: meshruntime.ProgressRemapped},
			{TaskID: input.ID, StepID: "step-1", NodeID: "sensor-2", State: meshruntime.ProgressRunning},
			{TaskID: input.ID, StepID: "step-1", NodeID: "sensor-2", Attempt: 2, State: meshruntime.ProgressAttemptStarted},
			{TaskID: input.ID, StepID: "step-1", NodeID: "sensor-2", Attempt: 2, State: meshruntime.ProgressAttemptSucceeded, Result: &step},
		} {
			if err := observer(ctx, event); err != nil {
				return execution.Result{}, err
			}
		}
		return execution.NewResult(input.ID, execution.StatusSucceeded, []execution.StepResult{step}, "")
	})
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapped, nil }),
		runner,
		WithTaskRepository(repository),
	)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.Submit(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	var gotStates []lifecycle.State
	for _, record := range repository.records {
		if record.State == lifecycle.StateRetrying ||
			record.State == lifecycle.StateRemapped {
			if len(gotStates) == 0 || gotStates[len(gotStates)-1] != record.State {
				gotStates = append(gotStates, record.State)
			}
		}
	}
	if !reflect.DeepEqual(gotStates, []lifecycle.State{
		lifecycle.StateRetrying,
		lifecycle.StateRemapped,
	}) {
		t.Fatalf("persisted recovery states = %v", gotStates)
	}
	final := repository.records[len(repository.records)-1]
	if final.State != lifecycle.StateSuccess ||
		final.Steps[0].NodeID != "sensor-2" ||
		final.Steps[0].State != lifecycle.StepStateSuccess {
		t.Fatalf("final record = %#v", final)
	}
}

func TestNewTaskServiceRejectsNilRepositoryOption(t *testing.T) {
	input, plan, mapped, result := validPipeline(t)
	_ = input
	var repository *recordingTaskRepository
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapped, nil }),
		runFunc(func(context.Context, mapper.MappedPlan) (execution.Result, error) { return result, nil }),
		WithTaskRepository(repository),
	)
	if service != nil || !errors.Is(err, ErrInvalidTaskRepository) {
		t.Fatalf("NewTaskService() = %#v, %v", service, err)
	}
}

func TestNewTaskServiceRejectsNilDependencies(t *testing.T) {
	validFactory := LifecycleFactory(lifecycle.New)
	validPlanner := planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, nil })
	validMapper := mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, nil })
	validRunner := runFunc(func(context.Context, mapper.MappedPlan) (execution.Result, error) {
		return execution.Result{}, nil
	})

	var typedNilPlanner *nilPlanner
	var typedNilMapper *nilMapper
	var typedNilRunner *nilRunner

	tests := []struct {
		name    string
		factory LifecycleFactory
		planner PlanPort
		mapper  MapPort
		runner  RunPort
		want    error
	}{
		{"nil factory", nil, validPlanner, validMapper, validRunner, ErrInvalidLifecycleFactory},
		{"nil planner", validFactory, nil, validMapper, validRunner, ErrInvalidPlanner},
		{"typed nil planner", validFactory, typedNilPlanner, validMapper, validRunner, ErrInvalidPlanner},
		{"nil mapper", validFactory, validPlanner, nil, validRunner, ErrInvalidMapper},
		{"typed nil mapper", validFactory, validPlanner, typedNilMapper, validRunner, ErrInvalidMapper},
		{"nil runner", validFactory, validPlanner, validMapper, nil, ErrInvalidRunner},
		{"typed nil runner", validFactory, validPlanner, validMapper, typedNilRunner, ErrInvalidRunner},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, err := NewTaskService(test.factory, test.planner, test.mapper, test.runner)
			if service != nil {
				t.Fatalf("service = %#v", service)
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

type nilPlanner struct{}

func (*nilPlanner) Plan(task.Task) (planner.Plan, error) { return planner.Plan{}, nil }

type nilMapper struct{}

func (*nilMapper) Map(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, nil }

type nilRunner struct{}

func (*nilRunner) Execute(context.Context, mapper.MappedPlan) (execution.Result, error) {
	return execution.Result{}, nil
}

type recordingTaskRepository struct {
	noopTaskRepository
	records                []TaskRecord
	latest                 map[model.TaskID]TaskRecord
	executions             map[model.ExecutionID]storage.Execution
	progressErr            error
	events                 []storage.TaskEvent
	resourceEvidenceEvents []storage.TaskEvent
	submissions            map[string]struct {
		fingerprint string
		taskID      model.TaskID
	}
}

func (repository *recordingTaskRepository) snapshot(record TaskRecord) {
	record = cloneTaskRecord(record)
	repository.records = append(repository.records, record)
	if repository.latest == nil {
		repository.latest = make(map[model.TaskID]TaskRecord)
	}
	repository.latest[record.Task.ID] = cloneTaskRecord(record)
}

// Create and Save remain test helpers for recovery fixtures from the pre-M2 API.
func (repository *recordingTaskRepository) Create(_ context.Context, record TaskRecord) error {
	repository.snapshot(record)
	return nil
}

func (repository *recordingTaskRepository) Save(_ context.Context, record TaskRecord) error {
	repository.snapshot(record)
	return nil
}

func (repository *recordingTaskRepository) CreateTask(_ context.Context, record storage.Task) error {
	if _, exists := repository.latest[record.ID]; exists {
		return storage.ErrAlreadyExists
	}
	repository.snapshot(storageTaskToRecord(record))
	return nil
}

func (repository *recordingTaskRepository) CreateTaskSubmission(
	_ context.Context,
	request storage.CreateTaskSubmissionRequest,
) (storage.CreateTaskSubmissionResult, error) {
	if request.IdempotencyKey != "" {
		if bound, exists := repository.submissions[request.IdempotencyKey]; exists {
			if bound.fingerprint != request.RequestFingerprint {
				return storage.CreateTaskSubmissionResult{}, storage.ErrIdempotencyConflict
			}
			record := taskRecordToStorage(repository.latest[bound.taskID])
			return storage.CreateTaskSubmissionResult{Task: record, Deduplicated: true}, nil
		}
	}
	if _, exists := repository.latest[request.Task.ID]; exists {
		return storage.CreateTaskSubmissionResult{}, storage.ErrAlreadyExists
	}
	repository.snapshot(storageTaskToRecord(request.Task))
	if request.IdempotencyKey != "" {
		if repository.submissions == nil {
			repository.submissions = make(map[string]struct {
				fingerprint string
				taskID      model.TaskID
			})
		}
		repository.submissions[request.IdempotencyKey] = struct {
			fingerprint string
			taskID      model.TaskID
		}{request.RequestFingerprint, request.Task.ID}
	}
	return storage.CreateTaskSubmissionResult{Task: request.Task, Created: true}, nil
}

func (repository *recordingTaskRepository) GetTask(_ context.Context, taskID model.TaskID) (storage.Task, error) {
	record, exists := repository.latest[taskID]
	if !exists {
		return storage.Task{}, storage.ErrNotFound
	}
	return taskRecordToStorage(record), nil
}

func (repository *recordingTaskRepository) RecordTaskPlan(
	_ context.Context, taskID model.TaskID, _ lifecycle.State, version int64,
	steps []storage.TaskStep, _ storage.TaskEvent,
) (int64, error) {
	record := repository.latest[taskID]
	record.State = lifecycle.StatePlanning
	record.Version = version + 1
	record.Steps = make([]ExecutionStepRecord, 0, len(steps))
	for _, step := range steps {
		converted := storageTaskToRecord(storage.Task{Steps: []storage.TaskStep{step}})
		record.Steps = append(record.Steps, converted.Steps...)
	}
	repository.snapshot(record)
	return record.Version, nil
}

func (repository *recordingTaskRepository) RecordStepAssignment(
	_ context.Context, taskID model.TaskID, stepID model.StepID, _ lifecycle.StepState,
	version int64, nodeID model.NodeID, event storage.TaskEvent,
) (int64, error) {
	if event.Detail != nil {
		repository.resourceEvidenceEvents = append(repository.resourceEvidenceEvents, event)
	}
	record := repository.latest[taskID]
	index := findStep(record.Steps, stepID)
	record.Steps[index].NodeID = nodeID
	record.Steps[index].State = lifecycle.StepStateMapped
	record.Steps[index].Version = version + 1
	repository.snapshot(record)
	return version + 1, nil
}

func (repository *recordingTaskRepository) RecordTaskStepProgress(
	_ context.Context,
	request storage.RecordTaskStepProgressRequest,
) (int64, int64, error) {
	if repository.progressErr != nil {
		return 0, 0, repository.progressErr
	}
	record := repository.latest[request.TaskID]
	index := findStep(record.Steps, request.StepID)
	step := &record.Steps[index]
	record.State, record.Version, record.UpdatedAt = request.NewTaskState, request.ExpectedTaskVersion+1, request.UpdatedAt
	step.State, step.Version, step.UpdatedAt = request.NewStepState, request.ExpectedStepVersion+1, request.UpdatedAt
	step.Status, step.Output, step.Error, step.FailureCode, step.CompletedAt = "", nil, "", "", nil
	if request.NewNodeID != nil {
		step.NodeID = *request.NewNodeID
	}
	repository.snapshot(record)
	return record.Version, step.Version, nil
}
func (repository *recordingTaskRepository) UpdateTaskState(
	_ context.Context, taskID model.TaskID, _ lifecycle.State, version int64,
	next lifecycle.State, failureCode, failureMessage string, event storage.TaskEvent,
) (int64, error) {
	record := repository.latest[taskID]
	record.State = next
	record.Version = version + 1
	record.FailureCode = failureCode
	record.Error = failureMessage
	if next == lifecycle.StateSuccess {
		record.ExecutionStatus = execution.StatusSucceeded
	}
	if next == lifecycle.StateFailed {
		record.ExecutionStatus = execution.StatusFailed
		record.ExecutionError = failureMessage
	}
	repository.snapshot(record)
	repository.events = append(repository.events, event)
	return record.Version, nil
}

func (repository *recordingTaskRepository) UpdateStepState(
	_ context.Context, taskID model.TaskID, stepID model.StepID, _ lifecycle.StepState,
	version int64, next lifecycle.StepState, result *execution.StepResult, _ storage.TaskEvent,
) (int64, error) {
	record := repository.latest[taskID]
	index := findStep(record.Steps, stepID)
	step := &record.Steps[index]
	step.State, step.Version = next, version+1
	if next == lifecycle.StepStateRetrying || next == lifecycle.StepStateRemapped {
		step.Status, step.Output, step.Error = "", nil, ""
	}
	if result != nil {
		step.Status, step.Output, step.Error = result.Status, cloneAnyMap(result.Output), result.Error
	}
	repository.snapshot(record)
	return version + 1, nil
}

func (repository *recordingTaskRepository) StartExecution(
	_ context.Context, request storage.StartExecutionRequest,
) (storage.Execution, int64, error) {
	if request.Event.Detail != nil {
		repository.resourceEvidenceEvents = append(repository.resourceEvidenceEvents, request.Event)
	}
	if repository.executions == nil {
		repository.executions = make(map[model.ExecutionID]storage.Execution)
	}
	started := request.StartedAt.UTC()
	attempt := storage.Execution{ID: request.ExecutionID, RequestID: string(request.ExecutionID), TaskID: request.TaskID, StepID: request.StepID,
		AttemptNo: request.AttemptNo, NodeID: request.NodeID, State: storage.ExecutionStateStarted,
		Request: request.Request, StartedAt: &started, CreatedAt: started, UpdatedAt: started, Version: 1}
	repository.executions[attempt.ID] = attempt
	record := repository.latest[request.TaskID]
	index := findStep(record.Steps, request.StepID)
	record.Steps[index].State = lifecycle.StepStateRunning
	record.Steps[index].NodeID = request.NodeID
	record.Steps[index].AttemptCount = request.AttemptNo
	record.Steps[index].Version = request.ExpectedStepVersion + 1
	repository.snapshot(record)
	return attempt, request.ExpectedStepVersion + 1, nil
}

func (repository *recordingTaskRepository) CompleteExecution(
	_ context.Context, request storage.CompleteExecutionRequest,
) (int64, int64, error) {
	if request.Event.Detail != nil {
		repository.resourceEvidenceEvents = append(repository.resourceEvidenceEvents, request.Event)
	}
	attempt := repository.executions[request.ExecutionID]
	attempt.State, attempt.Result = request.NewExecutionState, request.Result
	attempt.FailureCode, attempt.FailureMessage = request.FailureCode, request.FailureMessage
	completed := request.CompletedAt.UTC()
	attempt.CompletedAt = &completed
	attempt.UpdatedAt = completed
	attempt.Version = request.ExpectedExecutionVersion + 1
	repository.executions[attempt.ID] = attempt
	record := repository.latest[attempt.TaskID]
	index := findStep(record.Steps, attempt.StepID)
	step := &record.Steps[index]
	step.State, step.Version = request.NewStepState, request.ExpectedStepVersion+1
	step.CompletedAt = &completed
	step.FailureCode, step.Error = request.FailureCode, request.FailureMessage
	if request.Result != nil {
		step.Status, step.Output, step.Error = request.Result.Status, cloneAnyMap(request.Result.Output), request.Result.Error
	}
	repository.snapshot(record)
	return attempt.Version, step.Version, nil
}

func (repository *recordingTaskRepository) GetExecutions(_ context.Context, taskID model.TaskID, stepID model.StepID) ([]storage.Execution, error) {
	attempts := make([]storage.Execution, 0)
	for _, attempt := range repository.executions {
		if attempt.TaskID == taskID && attempt.StepID == stepID {
			attempts = append(attempts, attempt)
		}
	}
	return attempts, nil
}

func (repository *recordingTaskRepository) ReplaceTaskForRecovery(_ context.Context, record storage.Task) error {
	repository.snapshot(storageTaskToRecord(record))
	return nil
}

func (repository *recordingTaskRepository) Find(_ context.Context, taskID model.TaskID) (TaskRecord, error) {
	record, exists := repository.latest[taskID]
	if !exists {
		return TaskRecord{}, ErrTaskNotFound
	}
	return record, nil
}

func (*recordingTaskRepository) RecoverInterrupted(context.Context, string) (int64, error) {
	return 0, nil
}

func cloneTaskRecord(record TaskRecord) TaskRecord {
	record.Steps = append([]ExecutionStepRecord(nil), record.Steps...)
	return record
}

func TestSubmitReturnsZeroOutcomeWhenLifecycleCreationFails(t *testing.T) {
	input, _, _, _ := validPipeline(t)
	service := mustService(t,
		func(model.TaskID) (*lifecycle.Lifecycle, error) { return nil, errFactory },
		planFunc(func(task.Task) (planner.Plan, error) {
			t.Fatal("planner must not be called")
			return planner.Plan{}, nil
		}),
		unusedMapper(t),
		unusedRunner(t),
	)

	outcome, err := service.Submit(context.Background(), input)
	if !errors.Is(err, errFactory) {
		t.Fatalf("error = %v", err)
	}
	if !reflect.DeepEqual(outcome, Outcome{}) {
		t.Fatalf("outcome = %#v, want zero value", outcome)
	}
}

func TestRunAcceptedPersistsFailureWhenCoreContextIsAlreadyCanceled(t *testing.T) {
	input, _, _, _ := validPipeline(t)
	repository := &recordingTaskRepository{}
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) {
			t.Fatal("planner must not run after Core cancellation")
			return planner.Plan{}, nil
		}),
		unusedMapper(t),
		unusedRunner(t),
		WithTaskRepository(repository),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Accept(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	outcome, err := service.RunAccepted(ctx, input)
	if !errors.Is(err, context.Canceled) || outcome.State != lifecycle.StateFailed {
		t.Fatalf("RunAccepted() = %#v, %v", outcome, err)
	}
	record, err := repository.Find(context.Background(), input.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != lifecycle.StateFailed ||
		!strings.Contains(record.Error, context.Canceled.Error()) {
		t.Fatalf("persisted record = %#v", record)
	}
}

func TestRecoverResumesPersistedRunningTask(t *testing.T) {
	input, _, mapped, _ := validPipeline(t)
	repository := &recordingTaskRepository{
		latest: map[model.TaskID]TaskRecord{
			input.ID: {
				Task:  input,
				State: lifecycle.StateRunning,
				Steps: []ExecutionStepRecord{{
					ID: mapped.Steps[0].ID, Position: 0,
					Capability: mapped.Steps[0].Capability,
					NodeID:     mapped.Steps[0].NodeID,
					Inputs:     mapped.Steps[0].Inputs,
					State:      lifecycle.StepStateRunning,
				}},
			},
		},
	}
	runner := observableRunFunc(func(
		_ context.Context,
		plan mapper.MappedPlan,
		_ meshruntime.Observer,
	) (execution.Result, error) {
		if !reflect.DeepEqual(plan, mapped) {
			t.Fatalf("recovered plan = %#v, want %#v", plan, mapped)
		}
		step, _ := execution.NewStepResult(
			"step-1", "sensor-1", execution.StatusSucceeded, nil, "",
		)
		return execution.NewResult(input.ID, execution.StatusSucceeded, []execution.StepResult{step}, "")
	})
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, nil }),
		runner,
		WithTaskRepository(repository),
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
	if got.State != lifecycle.StateSuccess ||
		got.ExecutionStatus != execution.StatusSucceeded ||
		got.Steps[0].State != lifecycle.StepStateSuccess {
		t.Fatalf("recovered record = %#v", got)
	}
}

func TestRecoverWithStartedExecutionIsConservativeAndIdempotent(t *testing.T) {
	input, _, mapped, _ := validPipeline(t)
	startedAt := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	executionID := model.ExecutionID("task-1-step-1-attempt-1")
	repository := &recordingTaskRepository{
		latest: map[model.TaskID]TaskRecord{
			input.ID: {
				Task: input, State: lifecycle.StateRunning,
				Steps: []ExecutionStepRecord{{
					ID: mapped.Steps[0].ID, Position: 0, Capability: mapped.Steps[0].Capability,
					NodeID: mapped.Steps[0].NodeID, Inputs: mapped.Steps[0].Inputs,
					State: lifecycle.StepStateRunning, AttemptCount: 1, MaxAttempts: 2, Version: 4,
				}},
			},
		},
		executions: map[model.ExecutionID]storage.Execution{
			executionID: {ID: executionID, RequestID: string(executionID), TaskID: input.ID, StepID: mapped.Steps[0].ID, AttemptNo: 1, NodeID: mapped.Steps[0].NodeID, State: storage.ExecutionStateStarted, StartedAt: &startedAt, Version: 1},
		},
	}
	runnerCalls := 0
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, nil }),
		observableRunFunc(func(context.Context, mapper.MappedPlan, meshruntime.Observer) (execution.Result, error) {
			runnerCalls++
			return execution.Result{}, nil
		}),
		WithTaskRepository(repository),
	)
	if err != nil {
		t.Fatal(err)
	}
	for call := 1; call <= 2; call++ {
		err := service.Recover(context.Background(), input.ID)
		if !errors.Is(err, ErrRecoveryPending) || errors.Is(err, storage.ErrAlreadyExists) {
			t.Fatalf("Recover() call %d error = %v", call, err)
		}
	}
	if runnerCalls != 0 || len(repository.records) != 0 || len(repository.executions) != 1 {
		t.Fatalf("recovery side effects: runner=%d snapshots=%d executions=%d", runnerCalls, len(repository.records), len(repository.executions))
	}
	if repository.executions[executionID].State != storage.ExecutionStateStarted {
		t.Fatalf("started execution history changed: %#v", repository.executions[executionID])
	}
}

func TestRecoverWaitsForAssignedNodeReregistration(t *testing.T) {
	input, _, mapped, _ := validPipeline(t)
	repository := &recordingTaskRepository{latest: map[model.TaskID]TaskRecord{
		input.ID: {
			Task: input, State: lifecycle.StateRetrying, Version: 5,
			Steps: []ExecutionStepRecord{{
				ID: mapped.Steps[0].ID, Position: 0, Capability: mapped.Steps[0].Capability,
				IdempotencyMode: model.IdempotencyIdempotent, NodeID: mapped.Steps[0].NodeID,
				Inputs: mapped.Steps[0].Inputs, State: lifecycle.StepStateRetrying,
				AttemptCount: 1, MaxAttempts: 3, Version: 5,
			}},
		},
	}}
	eligible := false
	runnerCalls := 0
	runner := runFunc(func(_ context.Context, plan mapper.MappedPlan) (execution.Result, error) {
		runnerCalls++
		step, err := execution.NewStepResult(plan.Steps[0].ID, plan.Steps[0].NodeID, execution.StatusSucceeded, nil, "")
		if err != nil {
			return execution.Result{}, err
		}
		return execution.NewResult(input.ID, execution.StatusSucceeded, []execution.StepResult{step}, "")
	})
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, nil }),
		runner,
		WithTaskRepository(repository),
		WithRecoveryEligibility(recoveryEligibilityFunc(func(nodeID model.NodeID) bool {
			return eligible && nodeID == mapped.Steps[0].NodeID
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(context.Background(), input.ID); !errors.Is(err, ErrRecoveryPending) || !strings.Contains(err.Error(), RecoveryWaitingForNodeCode) {
		t.Fatalf("recovery before re-registration error = %v", err)
	}
	if runnerCalls != 0 || len(repository.records) != 0 {
		t.Fatalf("recovery ran before node gate: runner=%d records=%d", runnerCalls, len(repository.records))
	}
	eligible = true
	if err := service.Recover(context.Background(), input.ID); err != nil {
		t.Fatal(err)
	}
	if runnerCalls != 1 {
		t.Fatalf("runner calls after re-registration = %d", runnerCalls)
	}
}

func TestRecoverLeavesRetryableTaskForLaterScan(t *testing.T) {
	input, _, mapped, _ := validPipeline(t)
	repository := &recordingTaskRepository{
		latest: map[model.TaskID]TaskRecord{
			input.ID: {
				Task:  input,
				State: lifecycle.StateRunning,
				Steps: []ExecutionStepRecord{{
					ID: mapped.Steps[0].ID, Position: 0,
					Capability: mapped.Steps[0].Capability,
					NodeID:     mapped.Steps[0].NodeID,
					State:      lifecycle.StepStateRunning,
				}},
			},
		},
	}
	runner := observableRunFunc(func(
		ctx context.Context,
		_ mapper.MappedPlan,
		observer meshruntime.Observer,
	) (execution.Result, error) {
		if err := observer(ctx, meshruntime.Progress{
			TaskID: input.ID, StepID: "step-1",
			NodeID: "sensor-1", State: meshruntime.ProgressRetrying,
		}); err != nil {
			return execution.Result{}, err
		}
		return execution.Result{}, meshruntime.ErrRetryableExecution
	})
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, nil }),
		runner,
		WithTaskRepository(repository),
	)
	if err != nil {
		t.Fatal(err)
	}

	if err := service.Recover(context.Background(), input.ID); !errors.Is(err, meshruntime.ErrRetryableExecution) {
		t.Fatalf("Recover() error = %v", err)
	}
	got, _ := repository.Find(context.Background(), input.ID)
	if got.State != lifecycle.StateRetrying ||
		got.Steps[0].State != lifecycle.StepStateRetrying {
		t.Fatalf("retryable record = %#v", got)
	}
}

func TestRecoverCreatedDeterministicPlannerFailureBecomesTerminal(t *testing.T) {
	input, _, _, _ := validPipeline(t)
	input.Intent = "unsupported_intent"
	repository := &recordingTaskRepository{latest: map[model.TaskID]TaskRecord{
		input.ID: {Task: input, State: lifecycle.StateCreated, Version: 1},
	}}
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, planner.ErrUnknownIntent }),
		unusedMapper(t),
		unusedRunner(t),
		WithTaskRepository(repository),
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
	if got.State != lifecycle.StateFailed || got.FailureCode != RecoveryPlannerFailureCode ||
		!strings.Contains(got.Error, planner.ErrUnknownIntent.Error()) {
		t.Fatalf("terminalized record = %#v", got)
	}
	if len(repository.events) != 1 || repository.events[0].Type != "recovery_created_planner_failed" ||
		repository.events[0].FromState != string(lifecycle.StateCreated) ||
		repository.events[0].ToState != string(lifecycle.StateFailed) {
		t.Fatalf("terminalization events = %#v", repository.events)
	}
	if err := service.Recover(context.Background(), input.ID); err != nil {
		t.Fatal(err)
	}
	if len(repository.events) != 1 {
		t.Fatalf("terminal task was processed twice: events=%#v", repository.events)
	}
}

func TestRecoverCreatedTemporaryPlannerFailureRemainsPending(t *testing.T) {
	input, _, _, _ := validPipeline(t)
	repository := &recordingTaskRepository{latest: map[model.TaskID]TaskRecord{
		input.ID: {Task: input, State: lifecycle.StateCreated, Version: 1},
	}}
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, storage.ErrUnavailable }),
		unusedMapper(t),
		unusedRunner(t),
		WithTaskRepository(repository),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(context.Background(), input.ID); !errors.Is(err, ErrRecoveryPending) {
		t.Fatalf("Recover() error = %v", err)
	}
	got, _ := repository.Find(context.Background(), input.ID)
	if got.State != lifecycle.StateCreated || len(repository.events) != 0 {
		t.Fatalf("temporary failure mutated task: record=%#v events=%#v", got, repository.events)
	}
}

func TestRecoverCanceledContextDoesNotMutateCreatedTask(t *testing.T) {
	input, _, _, _ := validPipeline(t)
	repository := &recordingTaskRepository{latest: map[model.TaskID]TaskRecord{
		input.ID: {Task: input, State: lifecycle.StateCreated, Version: 1},
	}}
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) {
			t.Fatal("planner called after recovery cancellation")
			return planner.Plan{}, nil
		}),
		unusedMapper(t),
		unusedRunner(t),
		WithTaskRepository(repository),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := service.Recover(ctx, input.ID); !errors.Is(err, ErrRecoveryPending) {
		t.Fatalf("Recover() error = %v", err)
	}
	got, _ := repository.Find(context.Background(), input.ID)
	if got.State != lifecycle.StateCreated || len(repository.events) != 0 {
		t.Fatalf("canceled recovery mutated task: record=%#v events=%#v", got, repository.events)
	}
}

func TestRecoverMappedTaskRemapsStaleNodeWithoutDuplicateStepOrExecution(t *testing.T) {
	input, _, mapped, _ := validPipeline(t)
	step := mapped.Steps[0]
	repository := &recordingTaskRepository{latest: map[model.TaskID]TaskRecord{
		input.ID: {
			Task: input, State: lifecycle.StateMapped, Version: 3,
			Steps: []ExecutionStepRecord{{
				ID: step.ID, Position: 0, Capability: step.Capability,
				IdempotencyMode: model.IdempotencyIdempotent, NodeID: "node-a", Inputs: step.Inputs,
				State: lifecycle.StepStateMapped, MaxAttempts: 3, Version: 2,
			}},
		},
	}}
	runner := observableRunFunc(func(ctx context.Context, plan mapper.MappedPlan, observer meshruntime.Observer) (execution.Result, error) {
		if len(plan.Steps) != 1 || plan.Steps[0].NodeID != "node-b" {
			t.Fatalf("recovered plan = %#v", plan)
		}
		if err := observer(ctx, meshruntime.Progress{TaskID: input.ID, StepID: step.ID, NodeID: "node-b", Attempt: 1, State: meshruntime.ProgressAttemptStarted}); err != nil {
			return execution.Result{}, err
		}
		stepResult, err := execution.NewStepResult(step.ID, "node-b", execution.StatusSucceeded, map[string]any{"ok": true}, "")
		if err != nil {
			return execution.Result{}, err
		}
		if err := observer(ctx, meshruntime.Progress{TaskID: input.ID, StepID: step.ID, NodeID: "node-b", Attempt: 1, State: meshruntime.ProgressAttemptSucceeded, Result: &stepResult}); err != nil {
			return execution.Result{}, err
		}
		return execution.NewResult(input.ID, execution.StatusSucceeded, []execution.StepResult{stepResult}, "")
	})
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, nil }),
		mapFunc(func(plan planner.Plan) (mapper.MappedPlan, error) {
			return mapper.MappedPlan{TaskID: plan.TaskID, Steps: []mapper.MappedStep{{
				ID: plan.Steps[0].ID, Capability: plan.Steps[0].Capability,
				IdempotencyMode: plan.Steps[0].IdempotencyMode, NodeID: "node-b", Inputs: plan.Steps[0].Inputs,
			}}}, nil
		}),
		runner,
		WithTaskRepository(repository),
		WithRecoveryEligibility(recoveryEligibilityFunc(func(nodeID model.NodeID) bool { return nodeID == "node-b" })),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(context.Background(), input.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := repository.Find(context.Background(), input.ID)
	if got.State != lifecycle.StateSuccess || len(got.Steps) != 1 || got.Steps[0].NodeID != "node-b" ||
		got.Steps[0].State != lifecycle.StepStateSuccess || len(repository.executions) != 1 {
		t.Fatalf("mapped recovery result=%#v executions=%#v", got, repository.executions)
	}
}

func TestSubmitFailsWhenPlannerFailsAndSkipsLaterPorts(t *testing.T) {
	input, _, _, _ := validPipeline(t)
	var created *lifecycle.Lifecycle
	service := mustService(t,
		recordLifecycle(&created),
		planFunc(func(task.Task) (planner.Plan, error) { return planner.Plan{}, errPlan }),
		unusedMapper(t),
		unusedRunner(t),
	)

	outcome, err := service.Submit(context.Background(), input)
	assertFailedOutcome(t, outcome, input.ID, nil)
	if !errors.Is(err, errPlan) {
		t.Fatalf("error = %v", err)
	}
	if created.State() != lifecycle.StateFailed {
		t.Fatalf("lifecycle state = %q", created.State())
	}
}

func TestSubmitFailsWhenMapperFailsAndSkipsRunner(t *testing.T) {
	input, plan, _, _ := validPipeline(t)
	var created *lifecycle.Lifecycle
	service := mustService(t,
		recordLifecycle(&created),
		planFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapper.MappedPlan{}, errMap }),
		unusedRunner(t),
	)

	outcome, err := service.Submit(context.Background(), input)
	assertFailedOutcome(t, outcome, input.ID, nil)
	if !errors.Is(err, errMap) {
		t.Fatalf("error = %v", err)
	}
	if created.State() != lifecycle.StateFailed {
		t.Fatalf("lifecycle state = %q", created.State())
	}
}

func TestSubmitPreservesRuntimePartialResult(t *testing.T) {
	input, plan, mapped, _ := validPipeline(t)
	partial := failedExecution(t, input.ID)
	var created *lifecycle.Lifecycle
	service := mustService(t,
		recordLifecycle(&created),
		planFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapped, nil }),
		runFunc(func(context.Context, mapper.MappedPlan) (execution.Result, error) {
			return partial, errRun
		}),
	)

	outcome, err := service.Submit(context.Background(), input)
	assertFailedOutcome(t, outcome, input.ID, &partial)
	if !errors.Is(err, errRun) {
		t.Fatalf("error = %v", err)
	}
	if created.State() != lifecycle.StateFailed {
		t.Fatalf("lifecycle state = %q", created.State())
	}
}

func TestSubmitOmitsEmptyRuntimeResultOnFailure(t *testing.T) {
	input, plan, mapped, _ := validPipeline(t)
	service := mustService(t, LifecycleFactory(lifecycle.New),
		planFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapped, nil }),
		runFunc(func(context.Context, mapper.MappedPlan) (execution.Result, error) {
			return execution.Result{}, errRun
		}),
	)

	outcome, err := service.Submit(context.Background(), input)
	assertFailedOutcome(t, outcome, input.ID, nil)
	if !errors.Is(err, errRun) {
		t.Fatalf("error = %v", err)
	}
}

func TestSubmitFailsOnCanceledContextBeforePlanning(t *testing.T) {
	input, _, _, _ := validPipeline(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var created *lifecycle.Lifecycle
	service := mustService(t, recordLifecycle(&created),
		planFunc(func(task.Task) (planner.Plan, error) {
			t.Fatal("planner must not be called after cancellation")
			return planner.Plan{}, nil
		}),
		unusedMapper(t),
		unusedRunner(t),
	)

	outcome, err := service.Submit(ctx, input)
	assertFailedOutcome(t, outcome, input.ID, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
	if created.State() != lifecycle.StateFailed {
		t.Fatalf("lifecycle state = %q", created.State())
	}
}

func TestSubmitPersistsFailureAfterContextCancellation(t *testing.T) {
	input, _, _, _ := validPipeline(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	repository := &recordingTaskRepository{}
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(task.Task) (planner.Plan, error) {
			t.Fatal("planner must not be called after cancellation")
			return planner.Plan{}, nil
		}),
		unusedMapper(t),
		unusedRunner(t),
		WithTaskRepository(repository),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(ctx, input); !errors.Is(err, context.Canceled) {
		t.Fatalf("Submit() error = %v, want context cancellation", err)
	}
	if len(repository.records) != 2 ||
		repository.records[0].State != lifecycle.StateCreated ||
		repository.records[1].State != lifecycle.StateFailed ||
		!strings.Contains(repository.records[1].Error, context.Canceled.Error()) {
		t.Fatalf("persisted records = %#v", repository.records)
	}
}

func TestSubmitStopsAfterContextCancellationDuringPlanning(t *testing.T) {
	input, plan, _, _ := validPipeline(t)
	ctx, cancel := context.WithCancel(context.Background())
	service := mustService(t, LifecycleFactory(lifecycle.New),
		planFunc(func(task.Task) (planner.Plan, error) {
			cancel()
			return plan, nil
		}),
		unusedMapper(t),
		unusedRunner(t),
	)

	outcome, err := service.Submit(ctx, input)
	assertFailedOutcome(t, outcome, input.ID, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestSubmitStopsAfterContextCancellationDuringMapping(t *testing.T) {
	input, plan, mapped, _ := validPipeline(t)
	ctx, cancel := context.WithCancel(context.Background())
	service := mustService(t, LifecycleFactory(lifecycle.New),
		planFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) {
			cancel()
			return mapped, nil
		}),
		unusedRunner(t),
	)

	outcome, err := service.Submit(ctx, input)
	assertFailedOutcome(t, outcome, input.ID, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestSubmitPreservesRuntimeResultWhenContextIsCanceledDuringExecution(t *testing.T) {
	input, plan, mapped, _ := validPipeline(t)
	partial := failedExecution(t, input.ID)
	ctx, cancel := context.WithCancel(context.Background())
	service := mustService(t, LifecycleFactory(lifecycle.New),
		planFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
		mapFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapped, nil }),
		runFunc(func(got context.Context, _ mapper.MappedPlan) (execution.Result, error) {
			if got != ctx {
				t.Fatal("runner received a different context")
			}
			cancel()
			return partial, nil
		}),
	)

	outcome, err := service.Submit(ctx, input)
	assertFailedOutcome(t, outcome, input.ID, &partial)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v", err)
	}
}

func TestSubmitReportsActualStateWhenTransitionToFailedFails(t *testing.T) {
	input, _, _, _ := validPipeline(t)
	var created *lifecycle.Lifecycle
	service := mustService(t,
		recordLifecycle(&created),
		planFunc(func(task.Task) (planner.Plan, error) {
			for _, state := range []lifecycle.State{
				lifecycle.StatePlanning,
				lifecycle.StateMapped,
				lifecycle.StateDispatched,
				lifecycle.StateRunning,
				lifecycle.StateSuccess,
			} {
				if err := created.Transition(state); err != nil {
					t.Fatalf("advance lifecycle to %q: %v", state, err)
				}
			}
			return planner.Plan{}, errPlan
		}),
		unusedMapper(t),
		unusedRunner(t),
	)

	outcome, err := service.Submit(context.Background(), input)
	if outcome.TaskID != input.ID || outcome.State != lifecycle.StateSucceeded {
		t.Fatalf("outcome = %#v, want actual succeeded state", outcome)
	}
	if outcome.Execution != nil {
		t.Fatalf("execution = %#v, want nil", outcome.Execution)
	}
	if !errors.Is(err, errPlan) {
		t.Fatalf("error = %v, want original planner error", err)
	}
	if !errors.Is(err, lifecycle.ErrInvalidTransition) {
		t.Fatalf("error = %v, want failed-transition error", err)
	}
}

func TestSubmitReportsLifecycleTransitionFailure(t *testing.T) {
	input, plan, _, _ := validPipeline(t)
	transitionErr := lifecycle.ErrInvalidTransition
	service := mustService(t,
		func(id model.TaskID) (*lifecycle.Lifecycle, error) {
			value, err := lifecycle.New(id)
			if err != nil {
				return nil, err
			}
			if err := value.Transition(lifecycle.StateFailed); err != nil {
				return nil, err
			}
			return value, nil
		},
		planFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
		unusedMapper(t),
		unusedRunner(t),
	)

	outcome, err := service.Submit(context.Background(), input)
	assertFailedOutcome(t, outcome, input.ID, nil)
	if !errors.Is(err, transitionErr) {
		t.Fatalf("error = %v", err)
	}
}

func mustService(
	t *testing.T,
	factory LifecycleFactory,
	planner PlanPort,
	mapper MapPort,
	runner RunPort,
) *TaskService {
	t.Helper()
	service, err := NewTaskService(factory, planner, mapper, runner)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func recordLifecycle(target **lifecycle.Lifecycle) LifecycleFactory {
	return func(id model.TaskID) (*lifecycle.Lifecycle, error) {
		value, err := lifecycle.New(id)
		*target = value
		return value, err
	}
}

func unusedMapper(t *testing.T) MapPort {
	t.Helper()
	return mapFunc(func(planner.Plan) (mapper.MappedPlan, error) {
		t.Fatal("mapper must not be called")
		return mapper.MappedPlan{}, nil
	})
}

func unusedRunner(t *testing.T) RunPort {
	t.Helper()
	return runFunc(func(context.Context, mapper.MappedPlan) (execution.Result, error) {
		t.Fatal("runner must not be called")
		return execution.Result{}, nil
	})
}

func assertFailedOutcome(
	t *testing.T,
	got Outcome,
	taskID model.TaskID,
	executionResult *execution.Result,
) {
	t.Helper()
	if got.TaskID != taskID || got.State != lifecycle.StateFailed {
		t.Fatalf("outcome = %#v", got)
	}
	if !reflect.DeepEqual(got.Execution, executionResult) {
		t.Fatalf("execution = %#v, want %#v", got.Execution, executionResult)
	}
}

func validPipeline(t *testing.T) (task.Task, planner.Plan, mapper.MappedPlan, execution.Result) {
	t.Helper()
	input, err := task.New(
		"task-1",
		"cool_environment",
		[]model.Capability{"temperature_sensor"},
		task.Constraints{"target_temperature": "26"},
	)
	if err != nil {
		t.Fatal(err)
	}
	plan := planner.Plan{
		TaskID: input.ID,
		Steps: []planner.Step{{
			ID:         "step-1",
			Capability: "temperature_sensor",
			Inputs:     map[string]string{"operation": "read_temperature"},
		}},
	}
	mapped := mapper.MappedPlan{
		TaskID: input.ID,
		Steps: []mapper.MappedStep{{
			ID:         "step-1",
			Capability: "temperature_sensor",
			NodeID:     "sensor-1",
			Inputs:     map[string]string{"operation": "read_temperature"},
		}},
	}
	step, err := execution.NewStepResult(
		"step-1",
		"sensor-1",
		execution.StatusSucceeded,
		map[string]any{"temperature": 30.0},
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := execution.NewResult(input.ID, execution.StatusSucceeded, []execution.StepResult{step}, "")
	if err != nil {
		t.Fatal(err)
	}
	return input, plan, mapped, result
}

func failedExecution(t *testing.T, taskID model.TaskID) execution.Result {
	t.Helper()
	step, err := execution.NewStepResult(
		"step-1",
		"sensor-1",
		execution.StatusFailed,
		map[string]any{"temperature": 30.0},
		errRun.Error(),
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := execution.NewResult(taskID, execution.StatusFailed, []execution.StepResult{step}, errRun.Error())
	if err != nil {
		t.Fatal(err)
	}
	return result
}
