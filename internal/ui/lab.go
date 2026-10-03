package ui

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrLabOperationBusy      = errors.New("lab operation already in progress")
	ErrUnknownRuntime        = errors.New("unknown lab runtime")
	ErrRuntimeAlreadyRunning = errors.New("lab runtime is already running")
	ErrRuntimeAlreadyStopped = errors.New("lab runtime is already stopped")
)

// LabRuntimeView describes the harness-owned process state separately from
// the DTM membership state reported by Snapshot.Runtimes.
type LabRuntimeView struct {
	NodeID          string `json:"node_id"`
	ControlEndpoint string `json:"control_endpoint"`
	ProcessState    string `json:"process_state"`
	PID             int    `json:"pid,omitempty"`
	Error           string `json:"error,omitempty"`
}

type LabOperation struct {
	OperationID string    `json:"operation_id"`
	Type        string    `json:"type"`
	Target      string    `json:"target,omitempty"`
	Status      string    `json:"status"`
	Message     string    `json:"message,omitempty"`
	TaskID      string    `json:"task_id,omitempty"`
	PID         int       `json:"pid,omitempty"`
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
}

type LabView struct {
	Enabled         bool             `json:"enabled"`
	Mode            string           `json:"mode"`
	ControlsEnabled bool             `json:"controls_enabled"`
	SessionID       string           `json:"session_id,omitempty"`
	Runtimes        []LabRuntimeView `json:"runtimes"`
	LastOperation   *LabOperation    `json:"last_operation,omitempty"`
}

func ObserveLabView() LabView {
	return LabView{Enabled: false, Mode: "OBSERVE", ControlsEnabled: false, Runtimes: []LabRuntimeView{}}
}

type LabHarness interface {
	SessionID() string
	Runtimes() []LabRuntimeView
	StartRuntime(context.Context, string) error
	StopRuntime(context.Context, string) error
	RestartRuntime(context.Context, string) error
	Reset(context.Context) error
}

type GreenhouseSubmitter interface {
	SubmitGreenhouseTask(context.Context) (string, error)
}

type LabOperations interface {
	Enabled() bool
	Snapshot() LabView
	RunGreenhouseTask(context.Context) (LabOperation, error)
	StartRuntime(context.Context, string) (LabOperation, error)
	StopRuntime(context.Context, string) (LabOperation, error)
	RestartRuntime(context.Context, string) (LabOperation, error)
	StopCurrentCoordinator(context.Context) (LabOperation, error)
	Reset(context.Context) (LabOperation, error)
}

type LabControllerOptions struct {
	Harness          LabHarness
	Snapshot         func(context.Context) Snapshot
	Submitter        GreenhouseSubmitter
	OperationTimeout time.Duration
	Now              func() time.Time
}

type LabController struct {
	harness          LabHarness
	snapshot         func(context.Context) Snapshot
	submitter        GreenhouseSubmitter
	operationTimeout time.Duration
	now              func() time.Time
	gate             chan struct{}
	sequence         atomic.Uint64

	mu   sync.RWMutex
	last *LabOperation
}

func NewLabController(options LabControllerOptions) (*LabController, error) {
	if options.Harness == nil {
		return nil, errors.New("lab harness is required")
	}
	if options.Snapshot == nil {
		return nil, errors.New("lab snapshot function is required")
	}
	if options.Submitter == nil {
		return nil, errors.New("greenhouse submitter is required")
	}
	if options.OperationTimeout <= 0 {
		options.OperationTimeout = 10 * time.Second
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	gate := make(chan struct{}, 1)
	gate <- struct{}{}
	return &LabController{
		harness:          options.Harness,
		snapshot:         options.Snapshot,
		submitter:        options.Submitter,
		operationTimeout: options.OperationTimeout,
		now:              options.Now,
		gate:             gate,
	}, nil
}

func (controller *LabController) Enabled() bool { return controller != nil }

func (controller *LabController) Snapshot() LabView {
	if controller == nil {
		return ObserveLabView()
	}
	view := LabView{
		Enabled:         true,
		Mode:            "LAB",
		ControlsEnabled: true,
		SessionID:       controller.harness.SessionID(),
		Runtimes:        controller.harness.Runtimes(),
	}
	controller.mu.RLock()
	if controller.last != nil {
		copy := *controller.last
		view.LastOperation = &copy
	}
	controller.mu.RUnlock()
	sort.Slice(view.Runtimes, func(left, right int) bool { return view.Runtimes[left].NodeID < view.Runtimes[right].NodeID })
	return view
}

type labOperationResult struct {
	target  string
	message string
	taskID  string
	pid     int
}

func (controller *LabController) execute(ctx context.Context, operationType, target string, action func(context.Context) (labOperationResult, error)) (LabOperation, error) {
	started := controller.now().UTC()
	operation := LabOperation{
		OperationID: fmt.Sprintf("lab-%d-%d", started.UnixNano(), controller.sequence.Add(1)),
		Type:        operationType,
		Target:      target,
		Status:      "RUNNING",
		StartedAt:   started,
	}
	select {
	case <-controller.gate:
		defer func() { controller.gate <- struct{}{} }()
	default:
		operation.Status = "FAILED"
		operation.Message = ErrLabOperationBusy.Error()
		operation.CompletedAt = controller.now().UTC()
		controller.record(operation)
		return operation, ErrLabOperationBusy
	}

	if ctx == nil {
		ctx = context.Background()
	}
	bounded, cancel := context.WithTimeout(ctx, controller.operationTimeout)
	defer cancel()
	result, err := action(bounded)
	if err != nil {
		operation.Status = "FAILED"
		operation.Message = err.Error()
	} else {
		operation.Status = "SUCCESS"
		operation.Target = result.targetOr(target)
		operation.Message = result.message
		operation.TaskID = result.taskID
		operation.PID = result.pid
	}
	operation.CompletedAt = controller.now().UTC()
	controller.record(operation)
	return operation, err
}

func (result labOperationResult) targetOr(fallback string) string {
	if result.target != "" {
		return result.target
	}
	return fallback
}

func (controller *LabController) record(operation LabOperation) {
	controller.mu.Lock()
	copy := operation
	controller.last = &copy
	controller.mu.Unlock()
}

func (controller *LabController) RunGreenhouseTask(ctx context.Context) (LabOperation, error) {
	return controller.execute(ctx, "RUN_GREENHOUSE_TASK", "", func(actionCtx context.Context) (labOperationResult, error) {
		taskID, err := controller.submitter.SubmitGreenhouseTask(actionCtx)
		if err != nil {
			return labOperationResult{}, err
		}
		return labOperationResult{target: taskID, taskID: taskID, message: "task submitted"}, nil
	})
}

func (controller *LabController) StartRuntime(ctx context.Context, nodeID string) (LabOperation, error) {
	return controller.execute(ctx, "START_RUNTIME", strings.TrimSpace(nodeID), func(actionCtx context.Context) (labOperationResult, error) {
		if err := controller.harness.StartRuntime(actionCtx, nodeID); err != nil {
			return labOperationResult{}, err
		}
		return labOperationResult{message: "runtime start command completed", pid: controller.pidFor(nodeID)}, nil
	})
}

func (controller *LabController) StopRuntime(ctx context.Context, nodeID string) (LabOperation, error) {
	return controller.execute(ctx, "STOP_RUNTIME", strings.TrimSpace(nodeID), func(actionCtx context.Context) (labOperationResult, error) {
		if err := controller.harness.StopRuntime(actionCtx, nodeID); err != nil {
			return labOperationResult{}, err
		}
		return labOperationResult{message: "runtime stop command completed", pid: controller.pidFor(nodeID)}, nil
	})
}

func (controller *LabController) RestartRuntime(ctx context.Context, nodeID string) (LabOperation, error) {
	return controller.execute(ctx, "RESTART_RUNTIME", strings.TrimSpace(nodeID), func(actionCtx context.Context) (labOperationResult, error) {
		if err := controller.harness.RestartRuntime(actionCtx, nodeID); err != nil {
			return labOperationResult{}, err
		}
		return labOperationResult{message: "runtime restart command completed", pid: controller.pidFor(nodeID)}, nil
	})
}

func (controller *LabController) StopCurrentCoordinator(ctx context.Context) (LabOperation, error) {
	return controller.execute(ctx, "STOP_CURRENT_COORDINATOR", "", func(actionCtx context.Context) (labOperationResult, error) {
		snapshot := controller.snapshot(actionCtx)
		coordinator := strings.TrimSpace(snapshot.Mesh.Coordinator)
		if coordinator == "" || coordinator == "NONE" || coordinator == "CONFLICT" || coordinator == "UNKNOWN" || snapshot.Status == "UNAVAILABLE" {
			return labOperationResult{}, fmt.Errorf("current Coordinator is unavailable: %s", coordinator)
		}
		if err := controller.harness.StopRuntime(actionCtx, coordinator); err != nil {
			return labOperationResult{}, err
		}
		return labOperationResult{target: coordinator, message: "current Coordinator stop command completed", pid: controller.pidFor(coordinator)}, nil
	})
}

func (controller *LabController) Reset(ctx context.Context) (LabOperation, error) {
	return controller.execute(ctx, "RESET_DEMO", "demo", func(actionCtx context.Context) (labOperationResult, error) {
		if err := controller.harness.Reset(actionCtx); err != nil {
			return labOperationResult{}, err
		}
		return labOperationResult{message: "demo runtimes restarted"}, nil
	})
}

func (controller *LabController) pidFor(nodeID string) int {
	for _, runtime := range controller.harness.Runtimes() {
		if runtime.NodeID == nodeID {
			return runtime.PID
		}
	}
	return 0
}
