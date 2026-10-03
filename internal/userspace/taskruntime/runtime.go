package taskruntime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"dtm/internal/kernel"
	"dtm/internal/userspace"
)

// ExecutionWorkPort is the only dependency of the Child Task Runtime.  It is
// intentionally narrower than the Kernel API and is implemented by the
// existing USEO Orchestrator.
type ExecutionWorkPort interface {
	Execute(context.Context, userspace.ExecutionWorkItem) (userspace.ExecutionProjection, error)
}

// Runtime owns the in-memory Child Task lifecycle and one-shot submission
// boundary.  It has no worker pool, scheduler, retry loop, persistence, or
// Root Task state.
type Runtime struct {
	port ExecutionWorkPort

	mu    sync.Mutex
	tasks map[ChildTaskID]*taskRecord
}

type taskRecord struct {
	mu      sync.RWMutex
	binding childTaskBinding
	task    ChildTask
	done    chan struct{}
	err     error
}

// Manager is a descriptive alias for Runtime.  It does not introduce a
// second lifecycle owner.
type Manager = Runtime

// ChildTaskRuntime is a descriptive alias for Runtime.
type ChildTaskRuntime = Runtime

// NewRuntime creates an in-memory Child Task Runtime.  A nil port is accepted
// so Execute can return a typed, zero-side-effect local failure.
func NewRuntime(port ExecutionWorkPort) *Runtime {
	return &Runtime{
		port:  port,
		tasks: make(map[ChildTaskID]*taskRecord),
	}
}

// NewManager is a constructor alias that retains the same one-shot runtime.
func NewManager(port ExecutionWorkPort) *Runtime { return NewRuntime(port) }

// GetChildTaskSnapshot returns the current retained Child Task value without
// waiting for completion or invoking any execution/observation path.  Runtime
// mutex ownership ends after locating the record; the record lock then makes
// the task and diagnostic clone a coherent publication.  The two locks are
// never held together.
func (runtime *Runtime) GetChildTaskSnapshot(id ChildTaskID) (ChildTaskSnapshot, error) {
	if runtime == nil {
		return ChildTaskSnapshot{}, ErrChildTaskSnapshotUnavailable
	}
	if err := id.Validate(); err != nil {
		return ChildTaskSnapshot{}, fmt.Errorf("%w: %v", ErrInvalidChildTask, err)
	}

	runtime.mu.Lock()
	record, exists := runtime.tasks[id]
	runtime.mu.Unlock()
	if !exists {
		return ChildTaskSnapshot{}, ErrChildTaskNotFound
	}
	if record == nil {
		return ChildTaskSnapshot{}, ErrChildTaskSnapshotUnavailable
	}

	record.mu.RLock()
	task := record.task.Clone()
	record.mu.RUnlock()
	if task.ChildTaskID != id {
		return ChildTaskSnapshot{}, fmt.Errorf("%w: record identity %q does not match requested %q", ErrChildTaskSnapshotUnavailable, task.ChildTaskID, id)
	}
	snapshot, err := childTaskSnapshotFromTask(task)
	if err != nil {
		return ChildTaskSnapshot{}, err
	}
	return snapshot.Clone(), nil
}

// ReserveCreated atomically claims a ChildTaskID in the Child Runtime before
// any Root membership is changed.  The input is deliberately a
// lifecycle-free ChildTaskSpec; Runtime constructs the authoritative,
// pristine CREATED value and stores it before returning.  A same-binding
// CREATED replay returns the existing value, while a submitted record or a
// different binding fails without invoking USEO.
func (runtime *Runtime) ReserveCreated(spec ChildTaskSpec) (ChildTask, error) {
	task, err := New(spec)
	if err != nil {
		return ChildTask{}, err
	}
	if err := task.ValidatePristineCreated(); err != nil {
		return ChildTask{}, errors.Join(ErrInvalidChildTask, err)
	}
	if runtime == nil {
		return ChildTask{}, ErrChildTaskRuntimeUnavailable
	}

	binding := newChildTaskBinding(task)
	runtime.mu.Lock()
	if runtime.tasks == nil {
		runtime.tasks = make(map[ChildTaskID]*taskRecord)
	}
	if existing, exists := runtime.tasks[task.ChildTaskID]; exists {
		if existing == nil {
			runtime.mu.Unlock()
			return ChildTask{}, ErrChildTaskSnapshotUnavailable
		}
		if !binding.equal(existing.binding) {
			runtime.mu.Unlock()
			return ChildTask{}, fmt.Errorf("%w: child task %s", ErrChildTaskBindingConflict, task.ChildTaskID)
		}

		existing.mu.RLock()
		current := existing.task.Clone()
		existing.mu.RUnlock()
		runtime.mu.Unlock()

		switch current.State {
		case ChildTaskStateCreated:
			if err := current.ValidatePristineCreated(); err != nil {
				return ChildTask{}, errors.Join(ErrChildTaskSnapshotUnavailable, err)
			}
			return current.Clone(), nil
		case ChildTaskStateExecuting,
			ChildTaskStateSucceeded,
			ChildTaskStateFailed,
			ChildTaskStateRejected,
			ChildTaskStateUnknown,
			ChildTaskStateCancelledBeforeExecution:
			return current.Clone(), fmt.Errorf("%w: child task %s is %s", ErrChildTaskAlreadySubmitted, task.ChildTaskID, current.State)
		default:
			return ChildTask{}, fmt.Errorf("%w: unsupported stored child task state %q", ErrChildTaskSnapshotUnavailable, current.State)
		}
	}

	record := &taskRecord{
		binding: binding,
		task:    task.Clone(),
		done:    make(chan struct{}),
	}
	runtime.tasks[task.ChildTaskID] = record
	runtime.mu.Unlock()
	return task.Clone(), nil
}

// ReserveCreatedTask is a descriptive alias for ReserveCreated.
func (runtime *Runtime) ReserveCreatedTask(spec ChildTaskSpec) (ChildTask, error) {
	return runtime.ReserveCreated(spec)
}

type executionPreparation struct {
	workItem    userspace.ExecutionWorkItem
	committed   ChildTask
	terminal    ChildTask
	terminalErr error
}

// prepareExecution performs all local work before the exact CREATED to
// EXECUTING commit. It never calls USEO and returns a terminal local fact when
// cancellation, an unavailable port, identity generation, or work-item
// validation prevents submission.
func (runtime *Runtime) prepareExecution(ctx context.Context, task ChildTask) executionPreparation {
	if err := ctx.Err(); err != nil {
		cancelled, taskErr := localTaskFailure(task, ChildTaskStateCancelledBeforeExecution, "child task cancelled before execution", errors.Join(ErrChildTaskCancelledBeforeExecution, err))
		return executionPreparation{terminal: cancelled, terminalErr: taskErr}
	}
	if isNilExecutionWorkPort(runtime.port) {
		rejected, taskErr := localTaskFailure(task, ChildTaskStateRejected, "execution work port is unavailable", ErrExecutionWorkPortUnavailable)
		return executionPreparation{terminal: rejected, terminalErr: taskErr}
	}

	invocationID, err := kernel.NewInvocationID()
	if err != nil {
		rejected, taskErr := localTaskFailure(task, ChildTaskStateRejected, "invocation identity generation failed", errors.Join(ErrInvocationIdentityGeneration, err))
		return executionPreparation{terminal: rejected, terminalErr: taskErr}
	}
	workItemID := "child-work-item-" + task.ChildTaskID.String()
	requestID := "child-request-" + task.ChildTaskID.String()
	workItem := task.ToExecutionWorkItem(workItemID, requestID, invocationID)
	if err := workItem.Validate(); err != nil {
		rejected, taskErr := localTaskFailure(task, ChildTaskStateRejected, "invalid execution work item", errors.Join(ErrInvalidChildTask, err))
		return executionPreparation{terminal: rejected, terminalErr: taskErr}
	}
	// Identity generation and work-item validation happen before the commit
	// point. Recheck cancellation so a cancellation during that preparation
	// window still has the promised zero-call outcome.
	if err := ctx.Err(); err != nil {
		cancelled, taskErr := localTaskFailure(task, ChildTaskStateCancelledBeforeExecution, "child task cancelled before execution", errors.Join(ErrChildTaskCancelledBeforeExecution, err))
		return executionPreparation{terminal: cancelled, terminalErr: taskErr}
	}

	committed := task.Clone()
	committed.State = ChildTaskStateExecuting
	committed.ExecutionWorkItemID = workItem.WorkItemID
	committed.RequestID = workItem.RequestID
	committed.InvocationID = workItem.InvocationID
	committed.ExecutionProjection = userspace.ExecutionProjection{}
	committed.Failure = nil
	return executionPreparation{workItem: workItem, committed: committed}
}

func canonicalChildTask(task ChildTask) (ChildTask, error) {
	return New(ChildTaskSpec{
		ChildTaskID:        task.ChildTaskID,
		RootTaskRef:        task.RootTaskRef,
		ExecutionContextID: task.ExecutionContextID,
		CapabilityHandleID: task.CapabilityHandleID,
		CallerIdentity:     task.CallerIdentity,
		Scope:              task.Scope,
		Operation:          task.Operation,
		Payload:            task.Payload,
	})
}

// Execute submits one Child Task at most once.  The context is consulted only
// before the runtime commits the task to EXECUTING.  After that commit the
// USEO call receives a background context so caller cancellation cannot invent
// a task outcome or prevent the already-committed one-shot request.
func (runtime *Runtime) Execute(ctx context.Context, task ChildTask) (ChildTask, error) {
	task = task.Clone()
	if err := task.Validate(); err != nil {
		return localTaskFailure(task, ChildTaskStateRejected, "invalid child task", errors.Join(ErrInvalidChildTask, err))
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if runtime == nil {
		return localTaskFailure(task, ChildTaskStateRejected, "execution work port is unavailable", ErrExecutionWorkPortUnavailable)
	}

	binding := newChildTaskBinding(task)
	runtime.mu.Lock()
	if runtime.tasks == nil {
		runtime.tasks = make(map[ChildTaskID]*taskRecord)
	}
	if existing, exists := runtime.tasks[task.ChildTaskID]; exists {
		if existing == nil {
			runtime.mu.Unlock()
			return localTaskFailure(task, ChildTaskStateRejected, "child task snapshot is unavailable", ErrChildTaskSnapshotUnavailable)
		}
		if !binding.equal(existing.binding) {
			runtime.mu.Unlock()
			return localTaskFailure(task, ChildTaskStateRejected, "child task binding conflict", errors.Join(ErrChildTaskBindingConflict, ErrChildTaskAlreadySubmitted))
		}

		existing.mu.RLock()
		current := existing.task.Clone()
		existingState := current.State
		done := existing.done
		existing.mu.RUnlock()

		switch existingState {
		case ChildTaskStateCreated:
			// A reserved CREATED record is authoritative. Ignore all
			// lifecycle/result fields supplied by the Execute caller and use
			// only the immutable binding stored by Runtime.
			if err := current.ValidatePristineCreated(); err != nil {
				runtime.mu.Unlock()
				return localTaskFailure(task, ChildTaskStateRejected, "child task CREATED record is not pristine", errors.Join(ErrChildTaskSnapshotUnavailable, err))
			}
			preparation := runtime.prepareExecution(ctx, current)
			if preparation.terminalErr != nil {
				existing.mu.Lock()
				existing.task = preparation.terminal.Clone()
				existing.err = cloneError(preparation.terminalErr)
				existing.mu.Unlock()
				if done == nil {
					runtime.mu.Unlock()
					return localTaskFailure(task, ChildTaskStateRejected, "child task completion signal is unavailable", ErrChildTaskSnapshotUnavailable)
				}
				close(done)
				runtime.mu.Unlock()
				return preparation.terminal.Clone(), cloneError(preparation.terminalErr)
			}
			existing.mu.Lock()
			existing.task = preparation.committed.Clone()
			existing.err = nil
			existing.mu.Unlock()
			runtime.mu.Unlock()

			// The reservation is now atomically promoted. A late caller
			// cancellation cannot cancel this already-committed one-shot call.
			projection, executeErr := runtime.port.Execute(context.Background(), preparation.workItem)
			completed, taskErr := project(current, preparation.workItem, projection, executeErr)
			existing.finish(completed, taskErr)
			return completed.Clone(), cloneError(taskErr)

		case ChildTaskStateExecuting,
			ChildTaskStateSucceeded,
			ChildTaskStateFailed,
			ChildTaskStateRejected,
			ChildTaskStateUnknown,
			ChildTaskStateCancelledBeforeExecution:
			runtime.mu.Unlock()
			// The task record is installed before the first USEO call. A
			// duplicate therefore waits on the same immutable record and
			// never submits a new work item, even when the first call is in
			// flight. Terminal records have an already-closed signal.
			if done == nil {
				return localTaskFailure(task, ChildTaskStateRejected, "child task completion signal is unavailable", ErrChildTaskSnapshotUnavailable)
			}
			<-done
			return existing.result()
		default:
			runtime.mu.Unlock()
			return localTaskFailure(task, ChildTaskStateRejected, "invalid stored child task state", ErrChildTaskSnapshotUnavailable)
		}
	}

	// A caller may replay a returned terminal value against a fresh Runtime,
	// but without the original record there is no safe way to infer that the
	// underlying one-shot request is still unique.
	if task.State != "" && task.State != ChildTaskStateCreated {
		runtime.mu.Unlock()
		return localTaskFailure(task, ChildTaskStateRejected, "child task has already been submitted", ErrChildTaskAlreadySubmitted)
	}

	// Canonicalize the caller's immutable binding before installing a direct
	// execution record. This preserves legacy direct Execute behavior while
	// ensuring caller-provided lifecycle/result fields can never become the
	// authoritative record.
	canonical, canonicalErr := canonicalChildTask(task)
	if canonicalErr != nil {
		runtime.mu.Unlock()
		return localTaskFailure(task, ChildTaskStateRejected, "invalid child task binding", errors.Join(ErrInvalidChildTask, canonicalErr))
	}
	preparation := runtime.prepareExecution(ctx, canonical)
	if preparation.terminalErr != nil {
		runtime.tasks[task.ChildTaskID] = newPublishedTerminalRecord(newChildTaskBinding(canonical), preparation.terminal, preparation.terminalErr)
		runtime.mu.Unlock()
		return preparation.terminal.Clone(), cloneError(preparation.terminalErr)
	}

	// Installing this record with EXECUTING is the exact Child Task commit
	// point. The call below is then made once and only once.
	record := &taskRecord{
		binding: newChildTaskBinding(canonical),
		task:    preparation.committed.Clone(),
		done:    make(chan struct{}),
	}
	runtime.tasks[task.ChildTaskID] = record
	runtime.mu.Unlock()

	// The task is committed.  A late caller cancellation must not cancel an
	// already-issued one-shot request, so the port sees a non-cancellable call
	// context.  USEO retains its own bounded synchronous semantics.
	projection, executeErr := runtime.port.Execute(context.Background(), preparation.workItem)
	completed, taskErr := project(canonical, preparation.workItem, projection, executeErr)
	record.finish(completed, taskErr)
	return completed.Clone(), cloneError(taskErr)
}

// ExecuteChildTask is a descriptive method alias for Execute.
func (runtime *Runtime) ExecuteChildTask(ctx context.Context, task ChildTask) (ChildTask, error) {
	return runtime.Execute(ctx, task)
}

// Submit is an explicit one-shot spelling for callers that model the first
// call as submission.  It does not add another path or retry policy.
func (runtime *Runtime) Submit(ctx context.Context, task ChildTask) (ChildTask, error) {
	return runtime.Execute(ctx, task)
}

func (record *taskRecord) finish(task ChildTask, err error) {
	record.mu.Lock()
	record.task = task.Clone()
	record.err = cloneError(err)
	record.mu.Unlock()
	close(record.done)
}

func (record *taskRecord) result() (ChildTask, error) {
	record.mu.RLock()
	defer record.mu.RUnlock()
	return record.task.Clone(), cloneError(record.err)
}

func newPublishedTerminalRecord(binding childTaskBinding, task ChildTask, err error) *taskRecord {
	done := make(chan struct{})
	close(done)
	return &taskRecord{
		binding: binding,
		task:    task.Clone(),
		done:    done,
		err:     cloneError(err),
	}
}

func localTaskFailure(task ChildTask, state ChildTaskState, kind string, cause error) (ChildTask, error) {
	task.State = state
	task.ExecutionProjection = userspace.ExecutionProjection{}
	task.Failure = &TaskExecutionError{Kind: kind, ChildTaskID: task.ChildTaskID, Cause: cause}
	return task.Clone(), task.Failure
}

func project(task ChildTask, workItem userspace.ExecutionWorkItem, projection userspace.ExecutionProjection, executeErr error) (ChildTask, error) {
	projection = projection.Clone()
	projected := task.Clone()
	projected.ExecutionWorkItemID = workItem.WorkItemID
	projected.RequestID = workItem.RequestID
	projected.InvocationID = projection.InvocationID
	if projected.InvocationID.IsZero() {
		// The work item identity is the only trusted pre-call identity available
		// when a test double returns an incomplete projection.  This is not a
		// inferred Kernel state; it is the runtime's request correlation.
		projected.InvocationID = workItem.InvocationID
	}
	projected.ExecutionProjection = projection

	state, stateErr := childStateForProjection(projection)
	projected.State = state
	var cause error
	if stateErr != nil {
		cause = errors.Join(stateErr, executeErr)
	} else {
		cause = executeErr
	}

	switch state {
	case ChildTaskStateSucceeded:
		if cause == nil {
			projected.Failure = nil
			return projected.Clone(), nil
		}
		projected.Failure = cloneError(cause)
		return projected.Clone(), &TaskExecutionError{Kind: "child task execution completed with diagnostic", ChildTaskID: task.ChildTaskID, Cause: cause}
	case ChildTaskStateFailed:
		taskErr := &TaskExecutionError{Kind: "child task execution failed", ChildTaskID: task.ChildTaskID, Cause: errors.Join(ErrChildTaskExecutionFailed, cause)}
		projected.Failure = cloneError(taskErr)
		return projected.Clone(), taskErr
	case ChildTaskStateRejected:
		taskErr := &TaskExecutionError{Kind: "child task execution rejected", ChildTaskID: task.ChildTaskID, Cause: errors.Join(ErrChildTaskExecutionRejected, cause)}
		projected.Failure = cloneError(taskErr)
		return projected.Clone(), taskErr
	case ChildTaskStateUnknown:
		// UNKNOWN is terminal for this bounded projection.  Retry-safe facts
		// are retained in the original projection and never cause another call.
		taskErr := &TaskExecutionError{Kind: "child task execution is unknown", ChildTaskID: task.ChildTaskID, Cause: errors.Join(ErrChildTaskExecutionUnknown, cause)}
		projected.Failure = cloneError(taskErr)
		return projected.Clone(), taskErr
	case ChildTaskStateCancelledBeforeExecution:
		taskErr := &TaskExecutionError{Kind: "child task cancelled before execution", ChildTaskID: task.ChildTaskID, Cause: errors.Join(ErrChildTaskCancelledBeforeExecution, cause)}
		projected.Failure = cloneError(taskErr)
		return projected.Clone(), taskErr
	case ChildTaskStateExecuting:
		if cause == nil {
			projected.Failure = nil
			return projected.Clone(), nil
		}
		projected.Failure = cloneError(cause)
		return projected.Clone(), &TaskExecutionError{Kind: "child task execution remains in progress", ChildTaskID: task.ChildTaskID, Cause: cause}
	default:
		// childStateForProjection always normalizes an invalid state to UNKNOWN;
		// keep the default fail-closed for future state additions.
		taskErr := &TaskExecutionError{Kind: "invalid child task execution projection", ChildTaskID: task.ChildTaskID, Cause: errors.Join(ErrInvalidExecutionProjection, cause)}
		projected.State = ChildTaskStateUnknown
		projected.Failure = cloneError(taskErr)
		return projected.Clone(), taskErr
	}
}

func childStateForProjection(projection userspace.ExecutionProjection) (ChildTaskState, error) {
	switch projection.State {
	case userspace.WorkStateSucceeded:
		return ChildTaskStateSucceeded, nil
	case userspace.WorkStateFailed:
		return ChildTaskStateFailed, nil
	case userspace.WorkStateRejected:
		return ChildTaskStateRejected, nil
	case userspace.WorkStateUnknown:
		return ChildTaskStateUnknown, nil
	case userspace.WorkStateCancelledBeforeSubmit:
		return ChildTaskStateCancelledBeforeExecution, nil
	case userspace.WorkStateSubmitted:
		return ChildTaskStateExecuting, nil
	case userspace.WorkStateReady:
		return ChildTaskStateUnknown, ErrInvalidExecutionProjection
	case "":
		// Accept a projection test double that sets only the embedded Kernel
		// status, while still rejecting an entirely empty projection.
		switch projection.Status {
		case kernel.LogicalExecutionCompleted:
			return ChildTaskStateSucceeded, nil
		case kernel.LogicalExecutionFailed:
			return ChildTaskStateFailed, nil
		case kernel.LogicalExecutionRejected:
			return ChildTaskStateRejected, nil
		case kernel.LogicalExecutionUnknown:
			return ChildTaskStateUnknown, nil
		default:
			return ChildTaskStateUnknown, ErrInvalidExecutionProjection
		}
	default:
		return ChildTaskStateUnknown, fmt.Errorf("%w: unsupported USEO state %q", ErrInvalidExecutionProjection, projection.State)
	}
}

func isNilExecutionWorkPort(port ExecutionWorkPort) bool {
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

var _ ExecutionWorkPort = (*userspace.Orchestrator)(nil)
