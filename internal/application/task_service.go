package application

import (
	"context"
	"errors"
	"fmt"
	"reflect"
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
	ErrInvalidLifecycleFactory         = errors.New("invalid lifecycle factory")
	ErrInvalidPlanner                  = errors.New("invalid planner")
	ErrInvalidMapper                   = errors.New("invalid mapper")
	ErrInvalidRunner                   = errors.New("invalid runner")
	ErrInvalidResourceRecoveryMapper   = errors.New("invalid resource recovery mapper")
	ErrInvalidResourceEvidenceObserver = errors.New("invalid resource evidence observer")
	ErrNilLifecycle                    = errors.New("lifecycle factory returned nil")
)

type Outcome struct {
	TaskID    model.TaskID
	State     lifecycle.State
	Execution *execution.Result
	Error     string
}

type PlanPort interface {
	Plan(task.Task) (planner.Plan, error)
}

type MapPort interface {
	Map(planner.Plan) (mapper.MappedPlan, error)
}

type RunPort interface {
	Execute(context.Context, mapper.MappedPlan) (execution.Result, error)
}

type ObservableRunPort interface {
	ExecuteWithObserver(context.Context, mapper.MappedPlan, meshruntime.Observer) (execution.Result, error)
}

// ResourceRecoveryPort reconstructs a complete ResourceRef from the
// persisted Capability + NodeID pair. The bool is true when a fresh global
// candidate was selected and the new NodeID must be persisted as a remap.
type ResourceRecoveryPort interface {
	RecoverResourceMapping(context.Context, mapper.MappedStep) (mapper.MappedStep, bool, error)
}

type recoveryResolutionSet map[model.StepID]mapper.MappedStep

type attemptLimitProvider interface {
	MaxExecutionAttempts() int
}

type LifecycleFactory func(model.TaskID) (*lifecycle.Lifecycle, error)

type TaskService struct {
	factory          LifecycleFactory
	planner          PlanPort
	mapper           MapPort
	runner           RunPort
	repository       TaskRepository
	eligibility      RecoveryEligibility
	resourceRecovery ResourceRecoveryPort
	resourceEvidence TaskResourceEvidenceObserver
	now              func() time.Time
	afterAccept      func(TaskRecord)
}

type TaskServiceOption func(*TaskService) error

func WithTaskRepository(repository TaskRepository) TaskServiceOption {
	return func(service *TaskService) error {
		if isNilPort(repository) {
			return ErrInvalidTaskRepository
		}
		service.repository = repository
		return nil
	}
}

func WithRecoveryEligibility(eligibility RecoveryEligibility) TaskServiceOption {
	return func(service *TaskService) error {
		if isNilPort(eligibility) {
			return ErrInvalidMapper
		}
		service.eligibility = eligibility
		return nil
	}
}

// WithResourceRecoveryMapper injects the Resource-mode restart resolver.
// Legacy TaskServices leave this option unset and retain their v0.3 recovery
// behavior.
func WithResourceRecoveryMapper(recovery ResourceRecoveryPort) TaskServiceOption {
	return func(service *TaskService) error {
		if isNilPort(recovery) {
			return ErrInvalidResourceRecoveryMapper
		}
		service.resourceRecovery = recovery
		return nil
	}
}

func WithAfterTaskAcceptedHook(hook func(TaskRecord)) TaskServiceOption {
	return func(service *TaskService) error {
		if hook == nil {
			return ErrInvalidTaskRepository
		}
		service.afterAccept = hook
		return nil
	}
}

// WithTaskResourceEvidenceObserver observes exact ResourceRef selection and
// execution progress. The callback is informational and cannot alter task
// execution or authority decisions.
func WithTaskResourceEvidenceObserver(observer TaskResourceEvidenceObserver) TaskServiceOption {
	return func(service *TaskService) error {
		if observer == nil {
			return ErrInvalidResourceEvidenceObserver
		}
		service.resourceEvidence = observer
		return nil
	}
}

func NewTaskService(
	factory LifecycleFactory,
	planner PlanPort,
	mapper MapPort,
	runner RunPort,
	options ...TaskServiceOption,
) (*TaskService, error) {
	if factory == nil {
		return nil, ErrInvalidLifecycleFactory
	}
	if isNilPort(planner) {
		return nil, ErrInvalidPlanner
	}
	if isNilPort(mapper) {
		return nil, ErrInvalidMapper
	}
	if isNilPort(runner) {
		return nil, ErrInvalidRunner
	}
	service := &TaskService{
		factory: factory, planner: planner, mapper: mapper, runner: runner,
		repository: noopTaskRepository{}, now: time.Now,
	}
	for _, option := range options {
		if option == nil {
			return nil, ErrInvalidTaskRepository
		}
		if err := option(service); err != nil {
			return nil, err
		}
	}
	return service, nil
}

func (service *TaskService) Submit(ctx context.Context, input task.Task) (Outcome, error) {
	return service.submit(ctx, input, true)
}

func (service *TaskService) Accept(ctx context.Context, input task.Task) error {
	_, _, err := service.AcceptSubmission(ctx, input, "", "")
	return err
}

func (service *TaskService) SubmitSubmission(
	ctx context.Context,
	input task.Task,
	idempotencyKey string,
	requestFingerprint string,
) (Outcome, bool, error) {
	record, deduplicated, err := service.AcceptSubmission(ctx, input, idempotencyKey, requestFingerprint)
	if err != nil {
		return Outcome{}, false, err
	}
	if deduplicated {
		return taskRecordOutcome(record), true, nil
	}
	outcome, err := service.RunAccepted(ctx, input)
	return outcome, false, err
}

func (service *TaskService) AcceptSubmission(
	ctx context.Context,
	input task.Task,
	idempotencyKey string,
	requestFingerprint string,
) (TaskRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return TaskRecord{}, false, err
	}
	record := service.newTaskRecord(input)
	created, err := service.repository.CreateTaskSubmission(context.WithoutCancel(ctx), storage.CreateTaskSubmissionRequest{
		Task: taskRecordToStorage(record),
		Event: storage.TaskEvent{
			TaskID: input.ID, Type: "task_accepted", ToState: string(lifecycle.StateCreated), CreatedAt: record.CreatedAt,
		},
		IdempotencyKey: idempotencyKey, RequestFingerprint: requestFingerprint,
	})
	if err != nil {
		switch {
		case errors.Is(err, storage.ErrIdempotencyConflict):
			return TaskRecord{}, false, ErrIdempotencyConflict
		case errors.Is(err, storage.ErrUnavailable), errors.Is(err, storage.ErrClosed):
			return TaskRecord{}, false, ErrSubmissionUnavailable
		case errors.Is(err, storage.ErrSubmissionBindingCorrupt):
			return TaskRecord{}, false, ErrSubmissionBindingCorrupt
		case errors.Is(err, storage.ErrAlreadyExists):
			return TaskRecord{}, false, mapCreateError(err)
		default:
			return TaskRecord{}, false, ErrSubmissionPersistence
		}
	}
	accepted := storageTaskToRecord(created.Task)
	if created.Created && service.afterAccept != nil {
		service.afterAccept(accepted)
	}
	return accepted, created.Deduplicated, nil
}

func (service *TaskService) RunAccepted(ctx context.Context, input task.Task) (Outcome, error) {
	record, err := service.Find(context.WithoutCancel(ctx), input.ID)
	if err != nil {
		return Outcome{}, fmt.Errorf("find accepted task: %w", err)
	}
	if record.State != lifecycle.StateCreated {
		return Outcome{}, fmt.Errorf("run accepted task %q: state is %q, want %q", input.ID, record.State, lifecycle.StateCreated)
	}
	return service.submit(ctx, input, false)
}

func (service *TaskService) Find(ctx context.Context, taskID model.TaskID) (TaskRecord, error) {
	record, err := service.repository.GetTask(ctx, taskID)
	if errors.Is(err, storage.ErrNotFound) {
		return TaskRecord{}, fmt.Errorf("%w: %s", ErrTaskNotFound, taskID)
	}
	if err != nil {
		return TaskRecord{}, err
	}
	return storageTaskToRecord(record), nil
}

func (service *TaskService) FindRecoverable(ctx context.Context, limit int) ([]TaskRecord, error) {
	records, err := service.repository.FindRecoverableTasks(ctx, limit)
	if err != nil {
		return nil, err
	}
	result := make([]TaskRecord, len(records))
	for index := range records {
		result[index] = storageTaskToRecord(records[index])
	}
	return result, nil
}

type recoverableTaskPageRepository interface {
	FindRecoverableTasksAfter(context.Context, int, *storage.RecoveryCursor, time.Time) ([]storage.Task, error)
}

func (service *TaskService) FindRecoverablePage(
	ctx context.Context,
	limit int,
	after *storage.RecoveryCursor,
	createdBefore time.Time,
) ([]TaskRecord, *storage.RecoveryCursor, error) {
	repository, ok := service.repository.(recoverableTaskPageRepository)
	if !ok {
		records, err := service.FindRecoverable(ctx, limit)
		return records, nil, err
	}
	records, err := repository.FindRecoverableTasksAfter(ctx, limit, after, createdBefore)
	if err != nil {
		return nil, nil, err
	}
	result := make([]TaskRecord, len(records))
	for index := range records {
		result[index] = storageTaskToRecord(records[index])
	}
	if len(records) == 0 || len(records) < limit {
		return result, nil, nil
	}
	last := records[len(records)-1]
	return result, &storage.RecoveryCursor{UpdatedAt: last.UpdatedAt, TaskID: last.ID}, nil
}

func (service *TaskService) Recover(ctx context.Context, taskID model.TaskID) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("%w: recovery context for task %q: %v", ErrRecoveryPending, taskID, err)
	}
	record, err := service.Find(context.WithoutCancel(ctx), taskID)
	if err != nil {
		return fmt.Errorf("find recoverable task %q: %w", taskID, err)
	}
	var recoveryResolutions recoveryResolutionSet
	switch record.State {
	case lifecycle.StateSuccess, lifecycle.StateFailed, lifecycle.StateCancelled:
		return nil
	}
	terminal, err := service.classifyInterruptedExecutions(context.WithoutCancel(ctx), record)
	if err != nil {
		return err
	}
	if terminal {
		return nil
	}
	record, err = service.Find(context.WithoutCancel(ctx), taskID)
	if err != nil {
		return fmt.Errorf("reload classified task %q: %w", taskID, err)
	}
	switch record.State {
	case lifecycle.StateCreated:
		plan, planErr := service.planner.Plan(record.Task)
		if planErr != nil {
			if !isDeterministicRecoveryPlanError(planErr) {
				return fmt.Errorf("%w: recovery preflight plan task %q: %v", ErrRecoveryPending, taskID, planErr)
			}
			taskLifecycle, restoreErr := lifecycle.Restore(record.Task.ID, record.State)
			if restoreErr != nil {
				return fmt.Errorf("restore deterministically invalid task %q: %w", taskID, restoreErr)
			}
			record.FailureCode = RecoveryPlannerFailureCode
			record.Error = planErr.Error()
			if transitionErr := service.transitionTask(
				context.WithoutCancel(ctx), taskLifecycle, &record, lifecycle.StateFailed, "recovery_created_planner_failed",
			); transitionErr != nil {
				return transitionErr
			}
			return nil
		}
		if _, mapErr := service.mapper.Map(plan); mapErr != nil {
			if isTerminalRecoveryMappingError(mapErr) {
				return service.failRecoveryMapping(context.WithoutCancel(ctx), record, mapErr)
			}
			return recoveryMappingError(taskID, "", "map recovery task", mapErr)
		}
		_, runErr := service.RunAccepted(ctx, record.Task)
		return runErr
	case lifecycle.StatePlanning:
		if err := service.resumePlanning(context.WithoutCancel(ctx), &record); err != nil {
			if isTerminalRecoveryMappingError(err) {
				return service.failRecoveryMapping(context.WithoutCancel(ctx), record, err)
			}
			return err
		}
	case lifecycle.StateMapped:
		recoveryResolutions, err = service.ensureRecoveryNodes(context.WithoutCancel(ctx), &record)
		if err != nil {
			if isTerminalRecoveryMappingError(err) {
				return service.failRecoveryMapping(context.WithoutCancel(ctx), record, err)
			}
			return err
		}
		if err := service.resumeMapped(context.WithoutCancel(ctx), &record); err != nil {
			return err
		}
	}
	if recoveryResolutions == nil {
		recoveryResolutions, err = service.ensureRecoveryNodes(context.WithoutCancel(ctx), &record)
		if err != nil {
			if isTerminalRecoveryMappingError(err) {
				return service.failRecoveryMapping(context.WithoutCancel(ctx), record, err)
			}
			return err
		}
	}
	taskLifecycle, err := lifecycle.Restore(record.Task.ID, record.State)
	if err != nil {
		return fmt.Errorf("restore recoverable task %q: %w", taskID, err)
	}
	if err := prepareRecoveredRecord(taskLifecycle, &record); err != nil {
		return err
	}
	now := service.now().UTC()
	record.UpdatedAt = now
	for index := range record.Steps {
		record.Steps[index].UpdatedAt = now
	}
	if err := service.repository.ReplaceTaskForRecovery(context.WithoutCancel(ctx), taskRecordToStorage(record)); err != nil {
		return fmt.Errorf("persist resumed task %q: %w", taskID, err)
	}
	record, err = service.Find(context.WithoutCancel(ctx), taskID)
	if err != nil {
		return fmt.Errorf("reload resumed task %q: %w", taskID, err)
	}
	taskLifecycle, err = lifecycle.Restore(record.Task.ID, record.State)
	if err != nil {
		return err
	}
	plan, err := service.recoverableMappedPlan(record, recoveryResolutions)
	if err != nil {
		if isTerminalRecoveryMappingError(err) {
			return service.failRecoveryMapping(context.WithoutCancel(ctx), record, err)
		}
		return err
	}
	if len(plan.Steps) == 0 {
		return fmt.Errorf("recover task %q: no persisted execution steps", taskID)
	}
	result, err := service.execute(ctx, context.WithoutCancel(ctx), taskLifecycle, &record, plan)
	if err != nil {
		if errors.Is(err, meshruntime.ErrRetryableExecution) {
			return fmt.Errorf("recover task %q: %w", taskID, err)
		}
		_, failure := service.fail(context.WithoutCancel(ctx), taskLifecycle, record, partialResult(result), fmt.Errorf("recover task %q: %w", taskID, err))
		return failure
	}
	if applyRecoveredExecutionResult(&record, result, service.now().UTC()) {
		if err := service.repository.ReplaceTaskForRecovery(context.WithoutCancel(ctx), taskRecordToStorage(record)); err != nil {
			return fmt.Errorf("persist recovered task %q result: %w", taskID, err)
		}
		record, err = service.Find(context.WithoutCancel(ctx), taskID)
		if err != nil {
			return fmt.Errorf("reload recovered task %q result: %w", taskID, err)
		}
	}
	if err := service.transitionTask(context.WithoutCancel(ctx), taskLifecycle, &record, lifecycle.StateSuccess, "task_succeeded"); err != nil {
		return err
	}
	return nil
}

func isDeterministicRecoveryPlanError(err error) bool {
	return errors.Is(err, planner.ErrUnknownIntent) || errors.Is(err, planner.ErrRequirementConflict)
}

func recoveryMappingError(taskID model.TaskID, stepID model.StepID, operation string, err error) error {
	if errors.Is(err, model.ErrUnsupportedLegacyCapability) || errors.Is(err, mapper.ErrInvalidResourceMapping) {
		if stepID != "" {
			return fmt.Errorf("%s %q step %q: %w", operation, taskID, stepID, err)
		}
		return fmt.Errorf("%s %q: %w", operation, taskID, err)
	}
	if stepID != "" {
		return fmt.Errorf("%w: %s: task %q step %q: %w", ErrRecoveryPending, RecoveryWaitingForNodeCode, taskID, stepID, err)
	}
	return fmt.Errorf("%w: %s: task %q: %w", ErrRecoveryPending, RecoveryWaitingForNodeCode, taskID, err)
}

func isTerminalRecoveryMappingError(err error) bool {
	return errors.Is(err, model.ErrUnsupportedLegacyCapability) || errors.Is(err, mapper.ErrInvalidResourceMapping)
}

func (service *TaskService) failRecoveryMapping(ctx context.Context, record TaskRecord, cause error) error {
	taskLifecycle, err := lifecycle.Restore(record.Task.ID, record.State)
	if err != nil {
		return fmt.Errorf("restore terminal recovery task %q: %w", record.Task.ID, err)
	}
	record.FailureCode = RecoveryResourceMappingFailureCode
	record.Error = cause.Error()
	return service.transitionTask(ctx, taskLifecycle, &record, lifecycle.StateFailed, "recovery_resource_mapping_failed")
}

func (service *TaskService) classifyInterruptedExecutions(ctx context.Context, record TaskRecord) (bool, error) {
	resolver, supportsResolution := service.repository.(InterruptedExecutionRepository)
	for _, step := range record.Steps {
		if step.State == lifecycle.StepStateSuccess {
			continue
		}
		attempts, err := service.repository.GetExecutions(ctx, record.Task.ID, step.ID)
		if err != nil {
			return false, fmt.Errorf("inspect task %q step %q executions before recovery: %w", record.Task.ID, step.ID, err)
		}
		for _, attempt := range attempts {
			if attempt.State == storage.ExecutionStateStarted {
				if !supportsResolution {
					return false, fmt.Errorf("%w: task %q step %q execution %q attempt %d remains started", ErrRecoveryPending, record.Task.ID, step.ID, attempt.ID, attempt.AttemptNo)
				}
				resolution, err := resolver.ResolveInterruptedExecution(ctx, storage.ResolveInterruptedExecutionRequest{
					ExecutionID: attempt.ID, ExpectedExecutionVersion: attempt.Version,
					ExpectedTaskVersion: record.Version, ExpectedStepVersion: step.Version,
					ResolvedAt: service.now().UTC(),
				})
				if err != nil {
					return false, fmt.Errorf("classify interrupted execution %q: %w", attempt.ID, err)
				}
				if !resolution.RetryAllowed {
					return true, nil
				}
				return false, nil
			}
		}
	}
	return false, nil
}

func (service *TaskService) ensureRecoveryNodes(ctx context.Context, record *TaskRecord) (recoveryResolutionSet, error) {
	var resolutions recoveryResolutionSet
	if service.resourceRecovery != nil {
		resolutions = make(recoveryResolutionSet)
	}
	if service.eligibility == nil && service.resourceRecovery == nil {
		return resolutions, nil
	}
	for index := range record.Steps {
		step := &record.Steps[index]
		if step.State == lifecycle.StepStateSuccess {
			continue
		}
		if service.resourceRecovery != nil {
			resolved, _, err := service.resourceRecovery.RecoverResourceMapping(ctx, mapper.MappedStep{
				ID: step.ID, Capability: step.Capability, IdempotencyMode: step.IdempotencyMode,
				AttemptOffset: uint32(step.AttemptCount), MaxAttempts: uint32(step.MaxAttempts),
				NodeID: step.NodeID, Inputs: cloneStringMap(step.Inputs),
			})
			if err != nil {
				return nil, recoveryMappingError(record.Task.ID, step.ID, "resolve recovery resource mapping", err)
			}
			if resolved.NodeID == "" {
				return nil, recoveryMappingError(record.Task.ID, step.ID, "resolve recovery resource mapping", mapper.ErrInvalidResourceMapping)
			}
			resolutions[step.ID] = resolved
			// The persistence model intentionally stores only NodeID. A fresh
			// Resource remap on the same Node still rebuilds the full in-memory
			// ResourceRef, but there is no durable Node change to record.
			if resolved.NodeID == step.NodeID {
				continue
			}
			newNodeID := resolved.NodeID
			now := service.now().UTC()
			newTaskState, newStepState := lifecycle.StateRemapped, lifecycle.StepStateRemapped
			kind := storage.TaskStepProgressRemap
			taskEventType, stepEventType := "recovery_task_remapped", "recovery_step_remapped"
			if record.State == lifecycle.StateMapped {
				newTaskState, newStepState = lifecycle.StateMapped, lifecycle.StepStateMapped
				kind = storage.TaskStepProgressRecoveryRemap
				taskEventType, stepEventType = "recovery_mapped_task_revalidated", "recovery_mapped_step_remapped"
			}
			taskEvent := service.taskEvent(record.Task.ID, record.State, newTaskState, taskEventType)
			stepEvent := service.stepEvent(record.Task.ID, step.ID, step.State, newStepState, stepEventType)
			taskVersion, stepVersion, err := service.repository.RecordTaskStepProgress(ctx, storage.RecordTaskStepProgressRequest{
				Kind:   kind,
				TaskID: record.Task.ID, ExpectedTaskState: record.State, ExpectedTaskVersion: record.Version, NewTaskState: newTaskState,
				StepID: step.ID, ExpectedStepState: step.State, ExpectedStepVersion: step.Version, NewStepState: newStepState,
				NewNodeID: &newNodeID, TaskEvent: taskEvent, StepEvent: stepEvent, UpdatedAt: now,
			})
			if err != nil {
				return nil, persistenceError("persist recovery remap", err)
			}
			record.State, record.Version, record.UpdatedAt = newTaskState, taskVersion, now
			step.State, step.NodeID, step.Version, step.UpdatedAt = newStepState, newNodeID, stepVersion, now
			continue
		}
		if service.eligibility == nil {
			continue
		}
		if step.NodeID != "" && service.eligibility.Eligible(step.NodeID) {
			continue
		}
		if record.State != lifecycle.StateMapped && record.State != lifecycle.StateDispatched && record.State != lifecycle.StateRunning && record.State != lifecycle.StateRetrying && record.State != lifecycle.StateRemapped {
			return nil, fmt.Errorf("%w: %s: task %q step %q node %q must re-register with a valid lease and endpoint", ErrRecoveryPending, RecoveryWaitingForNodeCode, record.Task.ID, step.ID, step.NodeID)
		}
		mapped, err := service.mapper.Map(planner.Plan{TaskID: record.Task.ID, Steps: []planner.Step{{
			ID: step.ID, Capability: step.Capability, IdempotencyMode: step.IdempotencyMode, Inputs: cloneStringMap(step.Inputs),
		}}})
		if err != nil || len(mapped.Steps) != 1 || mapped.Steps[0].NodeID == "" || !service.eligibility.Eligible(mapped.Steps[0].NodeID) {
			return nil, fmt.Errorf("%w: %s: task %q step %q node %q must re-register with a valid lease and endpoint", ErrRecoveryPending, RecoveryWaitingForNodeCode, record.Task.ID, step.ID, step.NodeID)
		}
		newNodeID := mapped.Steps[0].NodeID
		now := service.now().UTC()
		newTaskState, newStepState := lifecycle.StateRemapped, lifecycle.StepStateRemapped
		kind := storage.TaskStepProgressRemap
		taskEventType, stepEventType := "recovery_task_remapped", "recovery_step_remapped"
		if record.State == lifecycle.StateMapped {
			newTaskState, newStepState = lifecycle.StateMapped, lifecycle.StepStateMapped
			kind = storage.TaskStepProgressRecoveryRemap
			taskEventType, stepEventType = "recovery_mapped_task_revalidated", "recovery_mapped_step_remapped"
		}
		taskEvent := service.taskEvent(record.Task.ID, record.State, newTaskState, taskEventType)
		stepEvent := service.stepEvent(record.Task.ID, step.ID, step.State, newStepState, stepEventType)
		taskVersion, stepVersion, err := service.repository.RecordTaskStepProgress(ctx, storage.RecordTaskStepProgressRequest{
			Kind:   kind,
			TaskID: record.Task.ID, ExpectedTaskState: record.State, ExpectedTaskVersion: record.Version, NewTaskState: newTaskState,
			StepID: step.ID, ExpectedStepState: step.State, ExpectedStepVersion: step.Version, NewStepState: newStepState,
			NewNodeID: &newNodeID, TaskEvent: taskEvent, StepEvent: stepEvent, UpdatedAt: now,
		})
		if err != nil {
			return nil, persistenceError("persist recovery remap", err)
		}
		record.State, record.Version, record.UpdatedAt = newTaskState, taskVersion, now
		step.State, step.NodeID, step.Version, step.UpdatedAt = newStepState, newNodeID, stepVersion, now
	}
	return resolutions, nil
}

func (service *TaskService) resumePlanning(ctx context.Context, record *TaskRecord) error {
	plan := planner.Plan{TaskID: record.Task.ID}
	for _, step := range record.Steps {
		plan.Steps = append(plan.Steps, planner.Step{ID: step.ID, Capability: step.Capability, IdempotencyMode: step.IdempotencyMode, Inputs: cloneStringMap(step.Inputs)})
	}
	mapped, err := service.mapper.Map(plan)
	if err != nil {
		return recoveryMappingError(record.Task.ID, "", "map planned task", err)
	}
	for _, mappedStep := range mapped.Steps {
		index := findStep(record.Steps, mappedStep.ID)
		if index < 0 {
			return fmt.Errorf("recover planned task %q mapped unknown step %q", record.Task.ID, mappedStep.ID)
		}
		step := &record.Steps[index]
		event := service.stepEvent(record.Task.ID, step.ID, step.State, lifecycle.StepStateMapped, "recovery_step_assigned")
		version, err := service.repository.RecordStepAssignment(ctx, record.Task.ID, step.ID, step.State, step.Version, mappedStep.NodeID, event)
		if err != nil {
			return persistenceError("persist recovery step assignment", err)
		}
		step.NodeID, step.State, step.Version, step.UpdatedAt = mappedStep.NodeID, lifecycle.StepStateMapped, version, event.CreatedAt
	}
	lifecycleState, err := lifecycle.Restore(record.Task.ID, record.State)
	if err != nil {
		return err
	}
	return service.transitionTask(ctx, lifecycleState, record, lifecycle.StateMapped, "recovery_task_mapped")
}

func (service *TaskService) resumeMapped(ctx context.Context, record *TaskRecord) error {
	for index := range record.Steps {
		if record.Steps[index].State == lifecycle.StepStateSuccess {
			continue
		}
		if err := service.transitionStep(ctx, record, index, lifecycle.StepStateDispatched, nil, "recovery_step_dispatched"); err != nil {
			return err
		}
	}
	taskLifecycle, err := lifecycle.Restore(record.Task.ID, record.State)
	if err != nil {
		return err
	}
	return service.transitionTask(ctx, taskLifecycle, record, lifecycle.StateDispatched, "recovery_task_dispatched")
}

func prepareRecoveredRecord(taskLifecycle *lifecycle.Lifecycle, record *TaskRecord) error {
	switch taskLifecycle.State() {
	case lifecycle.StateDispatched:
		if err := taskLifecycle.Transition(lifecycle.StateRunning); err != nil {
			return err
		}
	case lifecycle.StateRetrying:
		if err := taskLifecycle.Transition(lifecycle.StateDispatched); err != nil {
			return err
		}
		if err := taskLifecycle.Transition(lifecycle.StateRunning); err != nil {
			return err
		}
	case lifecycle.StateRemapped:
		if err := taskLifecycle.Transition(lifecycle.StateRunning); err != nil {
			return err
		}
	case lifecycle.StateRunning:
	default:
		return fmt.Errorf("task %q state %q is not recoverable", record.Task.ID, record.State)
	}
	record.State = taskLifecycle.State()
	for index := range record.Steps {
		if record.Steps[index].State == lifecycle.StepStateSuccess {
			continue
		}
		stepLifecycle, err := lifecycle.RestoreStep(record.Steps[index].ID, record.Steps[index].State)
		if err != nil {
			return err
		}
		switch stepLifecycle.State() {
		case lifecycle.StepStateDispatched:
			err = stepLifecycle.Transition(lifecycle.StepStateRunning)
		case lifecycle.StepStateRetrying:
			if err = stepLifecycle.Transition(lifecycle.StepStateDispatched); err == nil {
				err = stepLifecycle.Transition(lifecycle.StepStateRunning)
			}
		case lifecycle.StepStateRemapped:
			err = stepLifecycle.Transition(lifecycle.StepStateRunning)
		case lifecycle.StepStateRunning:
		default:
			err = fmt.Errorf("state %q is not recoverable", stepLifecycle.State())
		}
		if err != nil {
			return fmt.Errorf("resume step %q: %w", record.Steps[index].ID, err)
		}
		record.Steps[index].State = stepLifecycle.State()
	}
	return nil
}

func recoverableMappedPlan(record TaskRecord) mapper.MappedPlan {
	plan := mapper.MappedPlan{TaskID: record.Task.ID}
	for _, step := range record.Steps {
		if step.State == lifecycle.StepStateSuccess {
			continue
		}
		plan.Steps = append(plan.Steps, mapper.MappedStep{
			ID: step.ID, Capability: step.Capability, IdempotencyMode: step.IdempotencyMode, NodeID: step.NodeID,
			AttemptOffset: uint32(step.AttemptCount), MaxAttempts: uint32(step.MaxAttempts), Inputs: cloneStringMap(step.Inputs),
		})
	}
	return plan
}

func (service *TaskService) recoverableMappedPlan(record TaskRecord, resolutions recoveryResolutionSet) (mapper.MappedPlan, error) {
	plan := recoverableMappedPlan(record)
	if service.resourceRecovery == nil {
		return plan, nil
	}
	resolved := make([]mapper.MappedStep, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		mapped, ok := resolutions[step.ID]
		if !ok || mapped.NodeID == "" || mapped.NodeID != step.NodeID || mapped.ResourceRef.OwnerNodeID != mapped.NodeID {
			return mapper.MappedPlan{}, recoveryMappingError(record.Task.ID, step.ID, "reuse recovered resource mapping", mapper.ErrInvalidResourceMapping)
		}
		if err := mapped.ResourceRef.Validate(); err != nil {
			return mapper.MappedPlan{}, recoveryMappingError(
				record.Task.ID,
				step.ID,
				"reuse recovered resource mapping",
				fmt.Errorf("%w: resource ref: %v", mapper.ErrInvalidResourceMapping, err),
			)
		}
		resolved = append(resolved, mapped)
	}
	plan.Steps = resolved
	return plan, nil
}

func (service *TaskService) submit(
	ctx context.Context,
	input task.Task,
	createRecord bool,
) (Outcome, error) {
	taskLifecycle, err := service.factory(input.ID)
	if err != nil {
		return Outcome{}, fmt.Errorf("create lifecycle: %w", err)
	}
	if taskLifecycle == nil {
		return Outcome{}, ErrNilLifecycle
	}
	var record TaskRecord
	if createRecord {
		record = service.newTaskRecord(input)
		_, err := service.repository.CreateTaskSubmission(context.WithoutCancel(ctx), storage.CreateTaskSubmissionRequest{
			Task: taskRecordToStorage(record),
			Event: storage.TaskEvent{
				TaskID: input.ID, Type: "task_accepted", ToState: string(lifecycle.StateCreated), CreatedAt: record.CreatedAt,
			},
		})
		if err != nil {
			return fail(taskLifecycle, input.ID, nil, mapCreateError(fmt.Errorf("create task record: %w", err)))
		}
	} else {
		record, err = service.Find(context.WithoutCancel(ctx), input.ID)
		if err != nil {
			return Outcome{}, err
		}
	}
	persistCtx := context.WithoutCancel(ctx)
	if err := ctx.Err(); err != nil {
		return service.fail(persistCtx, taskLifecycle, record, nil, fmt.Errorf("before planning: %w", err))
	}

	plan, err := service.planner.Plan(input)
	if err != nil {
		return service.fail(persistCtx, taskLifecycle, record, nil, fmt.Errorf("plan task: %w", err))
	}
	record.Steps = service.plannedStepRecords(plan)
	if err := ctx.Err(); err != nil {
		return service.fail(persistCtx, taskLifecycle, record, nil, fmt.Errorf("after planning: %w", err))
	}
	if err := taskLifecycle.Transition(lifecycle.StatePlanning); err != nil {
		return service.fail(persistCtx, taskLifecycle, record, nil, fmt.Errorf("transition to planning: %w", err))
	}
	event := service.taskEvent(record.Task.ID, record.State, lifecycle.StatePlanning, "task_planned")
	newVersion, err := service.repository.RecordTaskPlan(
		persistCtx, record.Task.ID, record.State, record.Version,
		taskStepsToStorage(record.Task.ID, record.Steps), event,
	)
	if err != nil {
		return Outcome{TaskID: input.ID, State: record.State}, persistenceError("persist task plan", err)
	}
	record.State = lifecycle.StatePlanning
	record.Version = newVersion
	record.UpdatedAt = event.CreatedAt

	mappedPlan, err := service.mapper.Map(plan)
	if err != nil {
		return service.fail(persistCtx, taskLifecycle, record, nil, fmt.Errorf("map task: %w", err))
	}
	for _, mappedStep := range mappedPlan.Steps {
		index := findStep(record.Steps, mappedStep.ID)
		if index < 0 {
			return service.fail(persistCtx, taskLifecycle, record, nil, fmt.Errorf("mapped unknown step %q", mappedStep.ID))
		}
		step := &record.Steps[index]
		evidence := selectedResourceEvidence(record.Task.ID, mappedStep)
		stepEvent := service.stepEvent(record.Task.ID, step.ID, step.State, lifecycle.StepStateMapped, "step_assigned")
		stepEvent.Detail = resourceEvidenceDetail(evidence)
		version, err := service.repository.RecordStepAssignment(
			persistCtx, record.Task.ID, step.ID, step.State, step.Version,
			mappedStep.NodeID, stepEvent,
		)
		if err != nil {
			return Outcome{TaskID: input.ID, State: record.State}, persistenceError("persist step assignment", err)
		}
		step.NodeID = mappedStep.NodeID
		step.State = lifecycle.StepStateMapped
		step.Version = version
		step.UpdatedAt = stepEvent.CreatedAt
		service.observeResourceEvidence(evidence)
	}
	if err := service.transitionTask(persistCtx, taskLifecycle, &record, lifecycle.StateMapped, "task_mapped"); err != nil {
		return Outcome{TaskID: input.ID, State: record.State}, err
	}
	for index := range record.Steps {
		if err := service.transitionStep(persistCtx, &record, index, lifecycle.StepStateDispatched, nil, "step_dispatched"); err != nil {
			return Outcome{TaskID: input.ID, State: record.State}, err
		}
	}
	if err := service.transitionTask(persistCtx, taskLifecycle, &record, lifecycle.StateDispatched, "task_dispatched"); err != nil {
		return Outcome{TaskID: input.ID, State: record.State}, err
	}
	if err := service.transitionTask(persistCtx, taskLifecycle, &record, lifecycle.StateRunning, "task_running"); err != nil {
		return Outcome{TaskID: input.ID, State: record.State}, err
	}
	if err := ctx.Err(); err != nil {
		return service.fail(persistCtx, taskLifecycle, record, nil, fmt.Errorf("before execution: %w", err))
	}

	result, err := service.execute(ctx, persistCtx, taskLifecycle, &record, mappedPlan)
	if err != nil {
		if errors.Is(err, ErrPersistence) {
			return Outcome{TaskID: input.ID, State: record.State, Execution: partialResult(result)}, err
		}
		return service.fail(persistCtx, taskLifecycle, record, partialResult(result), fmt.Errorf("execute task: %w", err))
	}
	if err := ctx.Err(); err != nil {
		return service.fail(persistCtx, taskLifecycle, record, partialResult(result), fmt.Errorf("after execution: %w", err))
	}
	if err := service.transitionTask(persistCtx, taskLifecycle, &record, lifecycle.StateSuccess, "task_succeeded"); err != nil {
		return Outcome{TaskID: input.ID, State: record.State, Execution: &result}, err
	}
	applyExecutionResult(&record, &result)
	return Outcome{TaskID: input.ID, State: lifecycle.StateSuccess, Execution: &result}, nil
}

func (service *TaskService) execute(
	ctx context.Context,
	persistCtx context.Context,
	taskLifecycle *lifecycle.Lifecycle,
	record *TaskRecord,
	plan mapper.MappedPlan,
) (execution.Result, error) {
	observable, ok := service.runner.(ObservableRunPort)
	if !ok {
		return service.runner.Execute(ctx, plan)
	}
	executionVersions := make(map[model.ExecutionID]int64)
	return observable.ExecuteWithObserver(ctx, plan, func(_ context.Context, progress meshruntime.Progress) error {
		if err := service.persistProgress(persistCtx, taskLifecycle, record, executionVersions, progress); err != nil {
			return persistenceError("persist runtime progress", err)
		}
		service.observeResourceEvidence(progressResourceEvidence(progress))
		return nil
	})
}

func (service *TaskService) persistProgress(
	ctx context.Context,
	taskLifecycle *lifecycle.Lifecycle,
	record *TaskRecord,
	executionVersions map[model.ExecutionID]int64,
	progress meshruntime.Progress,
) error {
	index := findStep(record.Steps, progress.StepID)
	if index < 0 {
		return fmt.Errorf("runtime progress references unknown step %q", progress.StepID)
	}
	switch progress.State {
	case meshruntime.ProgressAttemptStarted:
		if record.State != lifecycle.StateRunning {
			if err := service.transitionTask(ctx, taskLifecycle, record, lifecycle.StateRunning, "task_running"); err != nil {
				return err
			}
		}
		step := &record.Steps[index]
		event := service.stepEvent(record.Task.ID, step.ID, step.State, lifecycle.StepStateRunning, "execution_started")
		event.Detail = resourceEvidenceDetail(progressResourceEvidence(progress))
		executionID := executionAttemptID(record.Task.ID, step.ID, progress.Attempt)
		startedAt := event.CreatedAt
		attempt, stepVersion, err := service.repository.StartExecution(ctx, storage.StartExecutionRequest{
			ExecutionID: executionID, TaskID: record.Task.ID, StepID: step.ID,
			AttemptNo: int(progress.Attempt), NodeID: progress.NodeID,
			Request:           cloneStringMap(step.Inputs),
			ExpectedStepState: step.State, ExpectedStepVersion: step.Version,
			StartedAt: startedAt, Event: event,
		})
		if err != nil {
			return err
		}
		executionVersions[executionID] = attempt.Version
		step.State = lifecycle.StepStateRunning
		step.NodeID = progress.NodeID
		step.AttemptCount = int(progress.Attempt)
		step.Version = stepVersion
		step.UpdatedAt = startedAt
		if step.StartedAt == nil {
			step.StartedAt = &startedAt
		}
		return nil
	case meshruntime.ProgressAttemptSucceeded,
		meshruntime.ProgressAttemptFailed,
		meshruntime.ProgressAttemptCanceled:
		return service.completeAttempt(ctx, record, index, executionVersions, progress)
	case meshruntime.ProgressRunning:
		if record.State != lifecycle.StateRunning {
			return service.transitionTask(ctx, taskLifecycle, record, lifecycle.StateRunning, "task_running")
		}
		return nil
	case meshruntime.ProgressRetrying, meshruntime.ProgressDispatched, meshruntime.ProgressRemapped:
		return service.recordTaskStepProgress(ctx, taskLifecycle, record, index, progress)
	default:
		return fmt.Errorf("unknown runtime progress state %q", progress.State)
	}
}

func (service *TaskService) recordTaskStepProgress(
	ctx context.Context,
	taskLifecycle *lifecycle.Lifecycle,
	record *TaskRecord,
	stepIndex int,
	progress meshruntime.Progress,
) error {
	taskState, stepState, err := progressLifecycleStates(progress.State)
	if err != nil {
		return err
	}
	step := &record.Steps[stepIndex]
	if record.State == taskState && step.State == stepState {
		if progress.State != meshruntime.ProgressRemapped || step.NodeID == progress.NodeID {
			return nil
		}
		return fmt.Errorf("duplicate remap progress for task %q step %q changes node %q to %q", record.Task.ID, step.ID, step.NodeID, progress.NodeID)
	}
	taskCandidate, err := lifecycle.Restore(record.Task.ID, record.State)
	if err != nil || taskCandidate.Transition(taskState) != nil {
		return fmt.Errorf("task %q cannot apply progress %q from state %q", record.Task.ID, progress.State, record.State)
	}
	stepCandidate, err := lifecycle.RestoreStep(step.ID, step.State)
	if err != nil || stepCandidate.Transition(stepState) != nil {
		return fmt.Errorf("task %q step %q cannot apply progress %q from state %q", record.Task.ID, step.ID, progress.State, step.State)
	}
	kind := storage.TaskStepProgressKind("")
	switch progress.State {
	case meshruntime.ProgressRetrying:
		kind = storage.TaskStepProgressRetry
	case meshruntime.ProgressDispatched:
		kind = storage.TaskStepProgressDispatch
	case meshruntime.ProgressRemapped:
		kind = storage.TaskStepProgressRemap
	}
	updatedAt := service.now().UTC()
	taskEvent := storage.TaskEvent{
		TaskID: record.Task.ID, Type: "task_" + string(progress.State),
		FromState: string(record.State), ToState: string(taskState), CreatedAt: updatedAt,
	}
	stepID := step.ID
	stepEvent := storage.TaskEvent{
		TaskID: record.Task.ID, StepID: &stepID, Type: "step_" + string(progress.State),
		FromState: string(step.State), ToState: string(stepState), CreatedAt: updatedAt,
	}
	stepEvent.Detail = resourceEvidenceDetail(progressResourceEvidence(progress))
	var newNodeID *model.NodeID
	if progress.State == meshruntime.ProgressRemapped {
		nodeID := progress.NodeID
		newNodeID = &nodeID
	}
	newTaskVersion, newStepVersion, err := service.repository.RecordTaskStepProgress(ctx, storage.RecordTaskStepProgressRequest{
		Kind: kind, TaskID: record.Task.ID,
		ExpectedTaskState: record.State, ExpectedTaskVersion: record.Version, NewTaskState: taskState,
		StepID: step.ID, ExpectedStepState: step.State, ExpectedStepVersion: step.Version, NewStepState: stepState,
		NewNodeID: newNodeID, TaskEvent: taskEvent, StepEvent: stepEvent, UpdatedAt: updatedAt,
	})
	if err != nil {
		return persistenceError("record task step "+string(kind)+" progress", err)
	}
	if err := taskLifecycle.Transition(taskState); err != nil {
		return fmt.Errorf("synchronize task %q progress %q after persistence: %w", record.Task.ID, progress.State, err)
	}
	record.State, record.Version, record.UpdatedAt = taskState, newTaskVersion, updatedAt
	record.CompletedAt = nil
	step.State, step.Version, step.UpdatedAt = stepState, newStepVersion, updatedAt
	step.Status, step.Output, step.Error, step.FailureCode, step.CompletedAt = "", nil, "", "", nil
	if newNodeID != nil {
		step.NodeID = *newNodeID
	}
	return nil
}

func (service *TaskService) completeAttempt(
	ctx context.Context,
	record *TaskRecord,
	stepIndex int,
	executionVersions map[model.ExecutionID]int64,
	progress meshruntime.Progress,
) error {
	step := &record.Steps[stepIndex]
	executionID := executionAttemptID(record.Task.ID, step.ID, progress.Attempt)
	executionVersion := executionVersions[executionID]
	if executionVersion < 1 {
		return fmt.Errorf("missing started execution %q", executionID)
	}
	completedAt := service.now().UTC()
	var (
		executionState storage.ExecutionState
		stepState      lifecycle.StepState
		result         *execution.StepResult
		failureCode    string
		failureMessage string
	)
	switch progress.State {
	case meshruntime.ProgressAttemptSucceeded:
		if progress.Result == nil {
			return fmt.Errorf("successful attempt %q has no result", executionID)
		}
		copy := *progress.Result
		result = &copy
		executionState = storage.ExecutionStateSucceeded
		stepState = lifecycle.StepStateSuccess
	case meshruntime.ProgressAttemptCanceled:
		executionState = storage.ExecutionStateCanceled
		stepState = lifecycle.StepStateCancelled
		failureCode = "execution_canceled"
		if progress.Failure != nil {
			failureMessage = progress.Failure.Error()
		}
	default:
		message := "execution failed"
		if progress.Failure != nil {
			message = progress.Failure.Error()
		}
		output := map[string]any(nil)
		if progress.Result != nil {
			output = progress.Result.Output
		}
		failed, err := execution.NewStepResult(step.ID, progress.NodeID, execution.StatusFailed, output, message)
		if err != nil {
			return err
		}
		result = &failed
		executionState = storage.ExecutionStateFailed
		stepState = lifecycle.StepStateFailed
		failureCode = "execution_failed"
		failureMessage = message
	}
	event := service.stepEvent(record.Task.ID, step.ID, step.State, stepState, "execution_completed")
	event.Detail = resourceEvidenceDetail(progressResourceEvidence(progress))
	_, stepVersion, err := service.repository.CompleteExecution(ctx, storage.CompleteExecutionRequest{
		ExecutionID:              executionID,
		ExpectedExecutionState:   storage.ExecutionStateStarted,
		ExpectedExecutionVersion: executionVersion,
		ExpectedStepState:        step.State, ExpectedStepVersion: step.Version,
		NewExecutionState: executionState, NewStepState: stepState,
		Result: result, FailureCode: failureCode, FailureMessage: failureMessage,
		CompletedAt: completedAt, Event: event,
	})
	if err != nil {
		return err
	}
	step.State = stepState
	step.Status = ""
	step.Output = nil
	step.Error = failureMessage
	step.FailureCode = failureCode
	step.CompletedAt = &completedAt
	step.UpdatedAt = completedAt
	step.Version = stepVersion
	if result != nil {
		step.Status = result.Status
		step.Output = cloneAnyMap(result.Output)
		step.Error = result.Error
	}
	return nil
}

func (service *TaskService) observeResourceEvidence(evidence TaskResourceEvidence) {
	if service.resourceEvidence == nil || evidence.ResourceRef == (model.ResourceRef{}) {
		return
	}
	service.resourceEvidence(evidence)
}

func progressLifecycleStates(state meshruntime.ProgressState) (lifecycle.State, lifecycle.StepState, error) {
	switch state {
	case meshruntime.ProgressRetrying:
		return lifecycle.StateRetrying, lifecycle.StepStateRetrying, nil
	case meshruntime.ProgressDispatched:
		return lifecycle.StateDispatched, lifecycle.StepStateDispatched, nil
	case meshruntime.ProgressRunning:
		return lifecycle.StateRunning, lifecycle.StepStateRunning, nil
	case meshruntime.ProgressRemapped:
		return lifecycle.StateRemapped, lifecycle.StepStateRemapped, nil
	default:
		return "", "", fmt.Errorf("unknown runtime progress state %q", state)
	}
}

func (service *TaskService) transitionTask(
	ctx context.Context,
	taskLifecycle *lifecycle.Lifecycle,
	record *TaskRecord,
	next lifecycle.State,
	eventType string,
) error {
	expected := record.State
	if err := taskLifecycle.Transition(next); err != nil {
		return fmt.Errorf("transition task %q to %q: %w", record.Task.ID, next, err)
	}
	event := service.taskEvent(record.Task.ID, expected, next, eventType)
	version, err := service.repository.UpdateTaskState(
		ctx, record.Task.ID, expected, record.Version, next, record.FailureCode, record.Error, event,
	)
	if err != nil {
		return persistenceError("update task state", err)
	}
	record.State = next
	record.Version = version
	record.UpdatedAt = event.CreatedAt
	if next == lifecycle.StateRunning && record.StartedAt == nil {
		started := event.CreatedAt
		record.StartedAt = &started
	}
	switch next {
	case lifecycle.StateSuccess, lifecycle.StateFailed, lifecycle.StateCancelled:
		completed := event.CreatedAt
		record.CompletedAt = &completed
	}
	return nil
}

func (service *TaskService) transitionStep(
	ctx context.Context,
	record *TaskRecord,
	index int,
	next lifecycle.StepState,
	result *execution.StepResult,
	eventType string,
) error {
	step := &record.Steps[index]
	event := service.stepEvent(record.Task.ID, step.ID, step.State, next, eventType)
	version, err := service.repository.UpdateStepState(
		ctx, record.Task.ID, step.ID, step.State, step.Version, next, result, event,
	)
	if err != nil {
		return persistenceError("update step state", err)
	}
	step.State = next
	step.Version = version
	step.UpdatedAt = event.CreatedAt
	if next == lifecycle.StepStateRunning && step.StartedAt == nil {
		started := event.CreatedAt
		step.StartedAt = &started
	}
	switch next {
	case lifecycle.StepStateRetrying, lifecycle.StepStateRemapped:
		step.Status = ""
		step.Output = nil
		step.Error = ""
		step.FailureCode = ""
		step.CompletedAt = nil
	case lifecycle.StepStateSuccess, lifecycle.StepStateFailed, lifecycle.StepStateCancelled:
		completed := event.CreatedAt
		step.CompletedAt = &completed
	}
	return nil
}

func (service *TaskService) fail(
	ctx context.Context,
	taskLifecycle *lifecycle.Lifecycle,
	record TaskRecord,
	result *execution.Result,
	cause error,
) (Outcome, error) {
	if errors.Is(cause, ErrPersistence) {
		return Outcome{TaskID: record.Task.ID, State: record.State, Execution: result}, cause
	}
	outcome, failure := fail(taskLifecycle, record.Task.ID, result, cause)
	record.Error = cause.Error()
	record.FailureCode = "task_failed"
	if outcome.State == lifecycle.StateFailed && record.State != lifecycle.StateFailed {
		if err := service.transitionTask(ctx, taskLifecycle, &record, lifecycle.StateFailed, "task_failed"); err != nil {
			failure = errors.Join(failure, err)
		}
	}
	applyExecutionResult(&record, result)
	return outcome, failure
}

func fail(taskLifecycle *lifecycle.Lifecycle, taskID model.TaskID, result *execution.Result, cause error) (Outcome, error) {
	if err := taskLifecycle.Transition(lifecycle.StateFailed); err != nil {
		cause = errors.Join(cause, fmt.Errorf("transition to failed: %w", err))
	}
	return Outcome{TaskID: taskID, State: taskLifecycle.State(), Execution: result}, cause
}

func (service *TaskService) newTaskRecord(input task.Task) TaskRecord {
	now := service.now().UTC()
	return TaskRecord{
		Task: input, State: lifecycle.StateCreated,
		CreatedAt: now, UpdatedAt: now, Version: 1,
	}
}

func (service *TaskService) plannedStepRecords(plan planner.Plan) []ExecutionStepRecord {
	now := service.now().UTC()
	maxAttempts := 1
	if provider, ok := service.runner.(attemptLimitProvider); ok && provider.MaxExecutionAttempts() > 0 {
		maxAttempts = provider.MaxExecutionAttempts()
	}
	records := make([]ExecutionStepRecord, len(plan.Steps))
	for index, step := range plan.Steps {
		records[index] = ExecutionStepRecord{
			ID: step.ID, Position: index, Capability: step.Capability, IdempotencyMode: step.IdempotencyMode,
			Inputs: cloneStringMap(step.Inputs), State: lifecycle.StepStateCreated,
			MaxAttempts: maxAttempts, CreatedAt: now, UpdatedAt: now, Version: 1,
		}
	}
	return records
}

func (service *TaskService) taskEvent(
	taskID model.TaskID,
	from lifecycle.State,
	to lifecycle.State,
	eventType string,
) storage.TaskEvent {
	return storage.TaskEvent{
		TaskID: taskID, Type: eventType,
		FromState: string(from), ToState: string(to), CreatedAt: service.now().UTC(),
	}
}

func (service *TaskService) stepEvent(
	taskID model.TaskID,
	stepID model.StepID,
	from lifecycle.StepState,
	to lifecycle.StepState,
	eventType string,
) storage.TaskEvent {
	id := stepID
	return storage.TaskEvent{
		TaskID: taskID, StepID: &id, Type: eventType,
		FromState: string(from), ToState: string(to), CreatedAt: service.now().UTC(),
	}
}

func taskRecordToStorage(record TaskRecord) storage.Task {
	result := storage.Task{
		ID: record.Task.ID, Intent: record.Task.Intent,
		Requirements: append([]model.Capability(nil), record.Task.Requirements...),
		Constraints:  record.Task.Constraints,
		State:        record.State, FailureCode: record.FailureCode,
		FailureMessage: record.Error, CreatedAt: record.CreatedAt,
		UpdatedAt: record.UpdatedAt, StartedAt: record.StartedAt,
		CompletedAt: record.CompletedAt, Version: record.Version,
		Steps: taskStepsToStorage(record.Task.ID, record.Steps),
	}
	return result
}

func taskStepsToStorage(taskID model.TaskID, steps []ExecutionStepRecord) []storage.TaskStep {
	result := make([]storage.TaskStep, len(steps))
	for index, step := range steps {
		var assigned *model.NodeID
		if step.NodeID != "" {
			nodeID := step.NodeID
			assigned = &nodeID
		}
		var stepResult *execution.StepResult
		if step.Status != "" {
			value := execution.StepResult{
				StepID: step.ID, NodeID: step.NodeID, Status: step.Status,
				Output: cloneAnyMap(step.Output), Error: step.Error,
			}
			stepResult = &value
		}
		result[index] = storage.TaskStep{
			ID: step.ID, TaskID: taskID, Sequence: step.Position,
			Capability: step.Capability, IdempotencyMode: step.IdempotencyMode, Input: cloneStringMap(step.Inputs),
			State: step.State, AssignedNodeID: assigned,
			AttemptCount: step.AttemptCount, MaxAttempts: step.MaxAttempts,
			FailureCode: step.FailureCode, FailureMessage: step.Error,
			Result: stepResult, CreatedAt: step.CreatedAt, UpdatedAt: step.UpdatedAt,
			StartedAt: step.StartedAt, CompletedAt: step.CompletedAt, Version: step.Version,
		}
	}
	return result
}

func storageTaskToRecord(record storage.Task) TaskRecord {
	input, _ := task.New(record.ID, record.Intent, record.Requirements, record.Constraints)
	result := TaskRecord{
		Task: input, State: record.State, Error: record.FailureMessage,
		FailureCode: record.FailureCode, CreatedAt: record.CreatedAt,
		UpdatedAt: record.UpdatedAt, StartedAt: record.StartedAt,
		CompletedAt: record.CompletedAt, Version: record.Version,
	}
	switch record.State {
	case lifecycle.StateSuccess:
		result.ExecutionStatus = execution.StatusSucceeded
	case lifecycle.StateFailed:
		result.ExecutionStatus = execution.StatusFailed
		result.ExecutionError = record.FailureMessage
	}
	for _, step := range record.Steps {
		item := ExecutionStepRecord{
			ID: step.ID, Position: step.Sequence, Capability: step.Capability, IdempotencyMode: step.IdempotencyMode,
			State: step.State, AttemptCount: step.AttemptCount, MaxAttempts: step.MaxAttempts,
			FailureCode: step.FailureCode, Error: step.FailureMessage,
			Inputs: cloneStringMap(step.Input), CreatedAt: step.CreatedAt,
			UpdatedAt: step.UpdatedAt, StartedAt: step.StartedAt,
			CompletedAt: step.CompletedAt, Version: step.Version,
		}
		if step.AssignedNodeID != nil {
			item.NodeID = *step.AssignedNodeID
		}
		if step.Result != nil {
			item.Status = step.Result.Status
			item.Output = cloneAnyMap(step.Result.Output)
			item.Error = step.Result.Error
		}
		result.Steps = append(result.Steps, item)
	}
	return result
}

func taskRecordOutcome(record TaskRecord) Outcome {
	outcome := Outcome{TaskID: record.Task.ID, State: record.State, Error: record.Error}
	if len(record.Steps) == 0 {
		return outcome
	}
	steps := make([]execution.StepResult, 0, len(record.Steps))
	for _, step := range record.Steps {
		if step.Status == "" {
			continue
		}
		steps = append(steps, execution.StepResult{
			StepID: step.ID, NodeID: step.NodeID, Status: step.Status,
			Output: cloneAnyMap(step.Output), Error: step.Error,
		})
	}
	if len(steps) > 0 {
		outcome.Execution = &execution.Result{
			TaskID: record.Task.ID, Status: record.ExecutionStatus,
			StepResults: steps, Error: record.ExecutionError,
		}
	}
	return outcome
}

func mapCreateError(err error) error {
	if errors.Is(err, storage.ErrAlreadyExists) {
		return fmt.Errorf("%w: %w", ErrTaskAlreadyExists, err)
	}
	return err
}

func persistenceError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrPersistence, operation, err)
}

func executionAttemptID(taskID model.TaskID, stepID model.StepID, attempt uint32) model.ExecutionID {
	return model.ExecutionID(fmt.Sprintf("%d:%s:%d:%s:%d", len(taskID), taskID, len(stepID), stepID, attempt))
}

func findStep(steps []ExecutionStepRecord, stepID model.StepID) int {
	for index := range steps {
		if steps[index].ID == stepID {
			return index
		}
	}
	return -1
}

func applyRecoveredExecutionResult(record *TaskRecord, result execution.Result, completedAt time.Time) bool {
	changed := false
	for _, stepResult := range result.StepResults {
		index := findStep(record.Steps, stepResult.StepID)
		if index < 0 || record.Steps[index].State == lifecycle.StepStateSuccess {
			continue
		}
		step := &record.Steps[index]
		step.State = lifecycle.StepStateSuccess
		step.Status = stepResult.Status
		step.Output = cloneAnyMap(stepResult.Output)
		step.Error = stepResult.Error
		step.UpdatedAt = completedAt
		step.CompletedAt = &completedAt
		changed = true
	}
	return changed
}

func partialResult(result execution.Result) *execution.Result {
	if len(result.StepResults) == 0 {
		return nil
	}
	return &result
}

func applyExecutionResult(record *TaskRecord, result *execution.Result) {
	if result == nil {
		return
	}
	record.ExecutionStatus = result.Status
	record.ExecutionError = result.Error
}

func cloneStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func cloneAnyMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

func isNilPort(port any) bool {
	if port == nil {
		return true
	}
	value := reflect.ValueOf(port)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var _ RecoverableTaskRepository = (*TaskService)(nil)
