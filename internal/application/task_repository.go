package application

import (
	"context"
	"errors"
	"time"

	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/storage"
	"dtm/internal/task"
)

var (
	ErrInvalidTaskRepository           = errors.New("invalid task repository")
	ErrTaskAlreadyExists               = errors.New("task already exists")
	ErrTaskNotFound                    = errors.New("task not found")
	ErrPersistence                     = errors.New("task persistence failed")
	ErrRecoveryPending                 = errors.New("task recovery requires explicit active execution policy")
	ErrIdempotencyConflict             = errors.New("idempotency key is already bound to a different request")
	ErrSubmissionUnavailable           = errors.New("task submission is temporarily unavailable")
	ErrSubmissionBindingCorrupt        = errors.New("task submission binding is corrupt")
	ErrSubmissionPersistence           = errors.New("task submission persistence failed")
	ErrIdempotentSubmissionUnsupported = errors.New("idempotent submission is not supported by the accepted task runner")
)

const (
	RestartInterruptionReason          = "core restarted before task completion"
	RecoveryWaitingForNodeCode         = "RECOVERY_WAITING_FOR_NODE"
	CoreRestartInterruptedCode         = "CORE_RESTART_INTERRUPTED"
	RecoveryNonIdempotentCode          = "RECOVERY_NON_IDEMPOTENT"
	RecoveryIdempotencyUnknownCode     = "RECOVERY_IDEMPOTENCY_UNKNOWN"
	RecoveryRetryExhaustedCode         = "RECOVERY_RETRY_EXHAUSTED"
	RecoveryPlannerFailureCode         = "RECOVERY_PLANNER_DETERMINISTIC_FAILURE"
	RecoveryResourceMappingFailureCode = "RECOVERY_RESOURCE_MAPPING_FAILURE"
)

type InterruptedExecutionRepository interface {
	ResolveInterruptedExecution(context.Context, storage.ResolveInterruptedExecutionRequest) (storage.InterruptedExecutionResolution, error)
}

type RecoveryEligibility interface {
	Eligible(model.NodeID) bool
}

type ExecutionStepRecord struct {
	ID              model.StepID
	Position        int
	Capability      model.Capability
	IdempotencyMode model.IdempotencyMode
	NodeID          model.NodeID
	Inputs          map[string]string
	State           lifecycle.StepState
	Status          execution.Status
	Output          map[string]any
	Error           string
	AttemptCount    int
	MaxAttempts     int
	FailureCode     string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	StartedAt       *time.Time
	CompletedAt     *time.Time
	Version         int64
}

type TaskRecord struct {
	Task            task.Task
	State           lifecycle.State
	Steps           []ExecutionStepRecord
	ExecutionStatus execution.Status
	ExecutionError  string
	Error           string
	FailureCode     string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	StartedAt       *time.Time
	CompletedAt     *time.Time
	Version         int64
}

type TaskRepository interface {
	CreateTask(context.Context, storage.Task) error
	CreateTaskSubmission(context.Context, storage.CreateTaskSubmissionRequest) (storage.CreateTaskSubmissionResult, error)
	RecordTaskPlan(context.Context, model.TaskID, lifecycle.State, int64, []storage.TaskStep, storage.TaskEvent) (int64, error)
	GetTask(context.Context, model.TaskID) (storage.Task, error)
	ListTasks(context.Context, storage.TaskFilter) (storage.TaskPage, error)
	RecordStepAssignment(context.Context, model.TaskID, model.StepID, lifecycle.StepState, int64, model.NodeID, storage.TaskEvent) (int64, error)
	RecordTaskStepProgress(context.Context, storage.RecordTaskStepProgressRequest) (int64, int64, error)
	UpdateTaskState(context.Context, model.TaskID, lifecycle.State, int64, lifecycle.State, string, string, storage.TaskEvent) (int64, error)
	UpdateStepState(context.Context, model.TaskID, model.StepID, lifecycle.StepState, int64, lifecycle.StepState, *execution.StepResult, storage.TaskEvent) (int64, error)
	StartExecution(context.Context, storage.StartExecutionRequest) (storage.Execution, int64, error)
	CompleteExecution(context.Context, storage.CompleteExecutionRequest) (int64, int64, error)
	GetExecution(context.Context, model.ExecutionID) (storage.Execution, error)
	GetExecutions(context.Context, model.TaskID, model.StepID) ([]storage.Execution, error)
	ListExecutionsByTask(context.Context, model.TaskID) ([]storage.Execution, error)
	ReplaceTaskForRecovery(context.Context, storage.Task) error
	FindRecoverableTasks(context.Context, int) ([]storage.Task, error)
}

type noopTaskRepository struct{}

func (noopTaskRepository) CreateTask(context.Context, storage.Task) error { return nil }

func (noopTaskRepository) CreateTaskSubmission(
	_ context.Context,
	request storage.CreateTaskSubmissionRequest,
) (storage.CreateTaskSubmissionResult, error) {
	return storage.CreateTaskSubmissionResult{Task: request.Task, Created: true}, nil
}

func (noopTaskRepository) RecordTaskPlan(
	_ context.Context,
	_ model.TaskID,
	_ lifecycle.State,
	version int64,
	_ []storage.TaskStep,
	_ storage.TaskEvent,
) (int64, error) {
	return version + 1, nil
}

func (noopTaskRepository) GetTask(context.Context, model.TaskID) (storage.Task, error) {
	return storage.Task{}, ErrTaskNotFound
}

func (noopTaskRepository) ListTasks(context.Context, storage.TaskFilter) (storage.TaskPage, error) {
	return storage.TaskPage{Tasks: []storage.Task{}}, nil
}

func (noopTaskRepository) RecordStepAssignment(
	_ context.Context,
	_ model.TaskID,
	_ model.StepID,
	_ lifecycle.StepState,
	version int64,
	_ model.NodeID,
	_ storage.TaskEvent,
) (int64, error) {
	return version + 1, nil
}

func (noopTaskRepository) RecordTaskStepProgress(
	_ context.Context,
	request storage.RecordTaskStepProgressRequest,
) (int64, int64, error) {
	return request.ExpectedTaskVersion + 1, request.ExpectedStepVersion + 1, nil
}
func (noopTaskRepository) UpdateTaskState(
	_ context.Context,
	_ model.TaskID,
	_ lifecycle.State,
	version int64,
	_ lifecycle.State,
	_ string,
	_ string,
	_ storage.TaskEvent,
) (int64, error) {
	return version + 1, nil
}

func (noopTaskRepository) UpdateStepState(
	_ context.Context,
	_ model.TaskID,
	_ model.StepID,
	_ lifecycle.StepState,
	version int64,
	_ lifecycle.StepState,
	_ *execution.StepResult,
	_ storage.TaskEvent,
) (int64, error) {
	return version + 1, nil
}

func (noopTaskRepository) StartExecution(
	_ context.Context,
	request storage.StartExecutionRequest,
) (storage.Execution, int64, error) {
	started := request.StartedAt.UTC()
	return storage.Execution{
		ID: request.ExecutionID, RequestID: string(request.ExecutionID), TaskID: request.TaskID, StepID: request.StepID,
		AttemptNo: request.AttemptNo, NodeID: request.NodeID,
		State: storage.ExecutionStateStarted, Request: request.Request,
		StartedAt: &started, CreatedAt: started, UpdatedAt: started, Version: 1,
	}, request.ExpectedStepVersion + 1, nil
}

func (noopTaskRepository) CompleteExecution(
	_ context.Context,
	request storage.CompleteExecutionRequest,
) (int64, int64, error) {
	return request.ExpectedExecutionVersion + 1, request.ExpectedStepVersion + 1, nil
}

func (noopTaskRepository) GetExecution(context.Context, model.ExecutionID) (storage.Execution, error) {
	return storage.Execution{}, ErrTaskNotFound
}

func (noopTaskRepository) GetExecutions(context.Context, model.TaskID, model.StepID) ([]storage.Execution, error) {
	return nil, nil
}

func (noopTaskRepository) ListExecutionsByTask(context.Context, model.TaskID) ([]storage.Execution, error) {
	return []storage.Execution{}, nil
}

func (noopTaskRepository) ReplaceTaskForRecovery(context.Context, storage.Task) error { return nil }

func (noopTaskRepository) FindRecoverableTasks(context.Context, int) ([]storage.Task, error) {
	return nil, nil
}
