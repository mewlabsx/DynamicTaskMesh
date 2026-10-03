package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/task"
)

var (
	ErrNotFound                   = errors.New("storage record not found")
	ErrInvalidArgument            = errors.New("storage invalid argument")
	ErrAlreadyExists              = errors.New("storage record already exists")
	ErrConflict                   = errors.New("storage state conflict")
	ErrUnavailable                = errors.New("storage unavailable")
	ErrClosed                     = errors.New("storage closed")
	ErrStoragePath                = errors.New("storage path unavailable")
	ErrStorageUnavailable         = ErrUnavailable
	ErrStorageReadOnly            = errors.New("storage is read-only")
	ErrStorageFull                = errors.New("storage is full")
	ErrStorageIO                  = errors.New("storage I/O failure")
	ErrStorageMigration           = errors.New("storage migration failed")
	ErrStorageSchemaMismatch      = errors.New("storage schema mismatch")
	ErrStorageCorrupt             = errors.New("storage is corrupt")
	ErrStorageIntegrity           = errors.New("storage integrity check failed")
	ErrStorageClosed              = ErrClosed
	ErrInvalidData                = errors.New("invalid persisted data")
	ErrNodeNotFound               = ErrNotFound
	ErrStaleRegistration          = fmt.Errorf("%w: stale node registration", ErrConflict)
	ErrNodeReregistrationRequired = errors.New("node reregistration required")
	ErrNodeOwnershipConflict      = fmt.Errorf("%w: node registration ownership conflict", ErrConflict)
	ErrNodeStateConflict          = fmt.Errorf("%w: node state conflict", ErrConflict)
	ErrNodeGenerationConflict     = fmt.Errorf("%w: node generation conflict", ErrConflict)
	ErrNodeTimeRegression         = fmt.Errorf("%w: node time regression", ErrConflict)
	ErrNodeConcurrentMutation     = fmt.Errorf("%w: concurrent node mutation", ErrConflict)
	ErrIdempotencyConflict        = errors.New("idempotency key is already bound to a different request")
	ErrSubmissionBindingCorrupt   = errors.New("submission binding is corrupt")
)

type Task struct {
	ID             model.TaskID
	Intent         string
	Requirements   []model.Capability
	Constraints    task.Constraints
	State          lifecycle.State
	FailureCode    string
	FailureMessage string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
	Version        int64
	Steps          []TaskStep
}

type TaskStep struct {
	ID              model.StepID
	TaskID          model.TaskID
	Sequence        int
	Capability      model.Capability
	IdempotencyMode model.IdempotencyMode
	Input           map[string]string
	State           lifecycle.StepState
	AssignedNodeID  *model.NodeID
	AttemptCount    int
	MaxAttempts     int
	FailureCode     string
	FailureMessage  string
	Result          *execution.StepResult
	CreatedAt       time.Time
	UpdatedAt       time.Time
	StartedAt       *time.Time
	CompletedAt     *time.Time
	Version         int64
}

type ExecutionState string

const (
	ExecutionStateCreated   ExecutionState = "created"
	ExecutionStateStarted   ExecutionState = "started"
	ExecutionStateSucceeded ExecutionState = "succeeded"
	ExecutionStateFailed    ExecutionState = "failed"
	ExecutionStateCanceled  ExecutionState = "canceled"
)

type Execution struct {
	ID             model.ExecutionID
	RequestID      string
	TaskID         model.TaskID
	StepID         model.StepID
	AttemptNo      int
	NodeID         model.NodeID
	State          ExecutionState
	Request        map[string]string
	Result         *execution.StepResult
	FailureCode    string
	FailureMessage string
	StartedAt      *time.Time
	CompletedAt    *time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	Version        int64
}

type TaskEvent struct {
	ID        int64
	TaskID    model.TaskID
	StepID    *model.StepID
	Type      string
	FromState string
	ToState   string
	Detail    map[string]any
	CreatedAt time.Time
}

type CreateTaskSubmissionRequest struct {
	Task               Task
	Event              TaskEvent
	IdempotencyKey     string
	RequestFingerprint string
}

type CreateTaskSubmissionResult struct {
	Task         Task
	Created      bool
	Deduplicated bool
}

type StartExecutionRequest struct {
	ExecutionID         model.ExecutionID
	TaskID              model.TaskID
	StepID              model.StepID
	AttemptNo           int
	NodeID              model.NodeID
	Request             map[string]string
	ExpectedStepState   lifecycle.StepState
	ExpectedStepVersion int64
	StartedAt           time.Time
	Event               TaskEvent
}

type CompleteExecutionRequest struct {
	ExecutionID              model.ExecutionID
	ExpectedExecutionState   ExecutionState
	ExpectedExecutionVersion int64
	ExpectedStepState        lifecycle.StepState
	ExpectedStepVersion      int64
	NewExecutionState        ExecutionState
	NewStepState             lifecycle.StepState
	Result                   *execution.StepResult
	FailureCode              string
	FailureMessage           string
	CompletedAt              time.Time
	Event                    TaskEvent
}

type ResolveInterruptedExecutionRequest struct {
	ExecutionID              model.ExecutionID
	ExpectedExecutionVersion int64
	ExpectedTaskVersion      int64
	ExpectedStepVersion      int64
	ResolvedAt               time.Time
}

type InterruptedExecutionResolution struct {
	RetryAllowed     bool
	IdempotencyMode  model.IdempotencyMode
	FailureCode      string
	ExecutionVersion int64
	TaskVersion      int64
	StepVersion      int64
}

type TaskStepProgressKind string

const (
	TaskStepProgressRetry         TaskStepProgressKind = "retrying"
	TaskStepProgressDispatch      TaskStepProgressKind = "dispatched"
	TaskStepProgressRemap         TaskStepProgressKind = "remapped"
	TaskStepProgressRecoveryRemap TaskStepProgressKind = "recovery_mapped_remap"
)

type RecoveryCursor struct {
	UpdatedAt time.Time
	TaskID    model.TaskID
}

type RecordTaskStepProgressRequest struct {
	Kind                TaskStepProgressKind
	TaskID              model.TaskID
	ExpectedTaskState   lifecycle.State
	ExpectedTaskVersion int64
	NewTaskState        lifecycle.State
	StepID              model.StepID
	ExpectedStepState   lifecycle.StepState
	ExpectedStepVersion int64
	NewStepState        lifecycle.StepState
	NewNodeID           *model.NodeID
	TaskEvent           TaskEvent
	StepEvent           TaskEvent
	UpdatedAt           time.Time
}

type NodeRecord struct {
	ID              model.NodeID
	Endpoint        string
	Capabilities    []model.Capability
	Status          node.Status
	Generation      int64
	RegistrationID  string
	Metadata        map[string]string
	RegisteredAt    time.Time
	LastHeartbeatAt time.Time
	LeaseExpiresAt  time.Time
	CreatedAt       time.Time
	UpdatedAt       time.Time
}

type RegisterNodeRequest struct {
	ID             model.NodeID
	Endpoint       string
	Capabilities   []model.Capability
	RegistrationID string
	Metadata       map[string]string
	RegisteredAt   time.Time
	LeaseExpiresAt time.Time
}

type RenewNodeLeaseRequest struct {
	ID              model.NodeID
	RegistrationID  string
	LastHeartbeatAt time.Time
	LeaseExpiresAt  time.Time
}

type SetNodeOfflineRequest struct {
	ID             model.NodeID
	RegistrationID string
	UpdatedAt      time.Time
}

type Repository interface {
	Database
	CreateTask(context.Context, Task) error
	CreateTaskSubmission(context.Context, CreateTaskSubmissionRequest) (CreateTaskSubmissionResult, error)
	RecordTaskPlan(context.Context, model.TaskID, lifecycle.State, int64, []TaskStep, TaskEvent) (int64, error)
	GetTask(context.Context, model.TaskID) (Task, error)
	ListTasks(context.Context, TaskFilter) (TaskPage, error)
	RecordStepAssignment(context.Context, model.TaskID, model.StepID, lifecycle.StepState, int64, model.NodeID, TaskEvent) (int64, error)
	RecordTaskStepProgress(context.Context, RecordTaskStepProgressRequest) (int64, int64, error)
	UpdateTaskState(context.Context, model.TaskID, lifecycle.State, int64, lifecycle.State, string, string, TaskEvent) (int64, error)
	UpdateStepState(context.Context, model.TaskID, model.StepID, lifecycle.StepState, int64, lifecycle.StepState, *execution.StepResult, TaskEvent) (int64, error)
	StartExecution(context.Context, StartExecutionRequest) (Execution, int64, error)
	CompleteExecution(context.Context, CompleteExecutionRequest) (int64, int64, error)
	GetExecution(context.Context, model.ExecutionID) (Execution, error)
	GetExecutions(context.Context, model.TaskID, model.StepID) ([]Execution, error)
	ListExecutionsByTask(context.Context, model.TaskID) ([]Execution, error)
	AppendTaskEvent(context.Context, TaskEvent) error
	ListTaskEvents(context.Context, model.TaskID) ([]TaskEvent, error)
	UpsertNode(context.Context, NodeRecord) error
	RegisterNode(context.Context, RegisterNodeRequest) (NodeRecord, error)
	RenewNodeLease(context.Context, RenewNodeLeaseRequest) error
	SetNodeOffline(context.Context, SetNodeOfflineRequest) error
	FailClosedNode(context.Context, model.NodeID, time.Time) error
	ExpireNodeLeases(context.Context, time.Time) (int64, error)
	MarkNodesStale(context.Context, time.Time) (int64, error)
	GetNode(context.Context, model.NodeID) (NodeRecord, error)
	ReplaceTaskForRecovery(context.Context, Task) error
	FindRecoverableTasks(context.Context, int) ([]Task, error)
}
