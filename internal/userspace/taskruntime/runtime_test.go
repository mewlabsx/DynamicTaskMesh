package taskruntime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dtm/internal/kernel"
	"dtm/internal/kernel/capability"
	"dtm/internal/userspace"
)

type fakeWorkPort struct {
	mu sync.Mutex

	calls        int
	requests     []userspace.ExecutionWorkItem
	projection   userspace.ExecutionProjection
	executeErr   error
	projectionFn func(userspace.ExecutionWorkItem) (userspace.ExecutionProjection, error)
	entered      chan struct{}
	release      chan struct{}
}

func (port *fakeWorkPort) Execute(_ context.Context, item userspace.ExecutionWorkItem) (userspace.ExecutionProjection, error) {
	port.mu.Lock()
	port.calls++
	callNumber := port.calls
	port.requests = append(port.requests, item.Clone())
	projection := port.projection.Clone()
	requestErr := port.executeErr
	projectionFn := port.projectionFn
	entered := port.entered
	release := port.release
	port.mu.Unlock()

	if entered != nil && callNumber == 1 {
		close(entered)
	}
	if release != nil {
		<-release
	}
	if projectionFn != nil {
		return projectionFn(item)
	}
	if projection.State == "" {
		projection = successfulProjection(item)
	}
	return projection, requestErr
}

func (port *fakeWorkPort) callCount() int {
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.calls
}

func (port *fakeWorkPort) requestSnapshot() userspace.ExecutionWorkItem {
	port.mu.Lock()
	defer port.mu.Unlock()
	if len(port.requests) == 0 {
		return userspace.ExecutionWorkItem{}
	}
	return port.requests[0].Clone()
}

func validChildTask(t *testing.T, id ChildTaskID) ChildTask {
	t.Helper()
	task, err := New(ChildTaskSpec{
		ChildTaskID:        id,
		RootTaskRef:        "root-opaque",
		ExecutionContextID: kernel.ExecutionContextID("context-test"),
		CapabilityHandleID: kernel.CapabilityHandleID("handle-test"),
		CallerIdentity:     "subject-test",
		Scope:              "scope-test",
		Operation:          "execute",
		Payload:            []byte("payload"),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return task
}

func successfulProjection(item userspace.ExecutionWorkItem) userspace.ExecutionProjection {
	result := kernel.LogicalExecutionResult{
		InvocationID: item.InvocationID,
		RequestID:    item.RequestID,
		Status:       kernel.LogicalExecutionCompleted,
	}
	return userspace.ExecutionProjection{
		WorkItemID:             item.WorkItemID,
		RootTaskRef:            item.RootTaskRef,
		ChildTaskRef:           item.ChildTaskRef,
		InvocationID:           item.InvocationID,
		RequestID:              item.RequestID,
		State:                  userspace.WorkStateSucceeded,
		LogicalExecutionResult: result,
		Result:                 result.Clone(),
	}
}

func projectionForState(item userspace.ExecutionWorkItem, state userspace.WorkState) userspace.ExecutionProjection {
	status := kernel.LogicalExecutionUnknown
	switch state {
	case userspace.WorkStateSucceeded:
		status = kernel.LogicalExecutionCompleted
	case userspace.WorkStateFailed:
		status = kernel.LogicalExecutionFailed
	case userspace.WorkStateRejected:
		status = kernel.LogicalExecutionRejected
	}
	result := kernel.LogicalExecutionResult{
		InvocationID: item.InvocationID,
		RequestID:    item.RequestID,
		Status:       status,
	}
	return userspace.ExecutionProjection{
		WorkItemID:             item.WorkItemID,
		RootTaskRef:            item.RootTaskRef,
		ChildTaskRef:           item.ChildTaskRef,
		InvocationID:           item.InvocationID,
		RequestID:              item.RequestID,
		State:                  state,
		LogicalExecutionResult: result,
		Result:                 result.Clone(),
	}
}

func TestRuntimeSuccessfulChildTaskSubmitsOnce(t *testing.T) {
	port := &fakeWorkPort{}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-success")

	result, err := runtime.Execute(context.Background(), task)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if result.State != ChildTaskStateSucceeded || result.ChildTaskID != task.ChildTaskID {
		t.Fatalf("result = %#v, want succeeded child task", result)
	}
	if result.ExecutionWorkItemID == "" || result.InvocationID.IsZero() {
		t.Fatalf("result correlation = %#v, want assigned work item and invocation", result)
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("USEO calls = %d, want 1", calls)
	}
	work := port.requestSnapshot()
	if work.ChildTaskRef != string(task.ChildTaskID) || work.RootTaskRef != task.RootTaskRef {
		t.Fatalf("work item lineage = %#v, want stable child/root references", work)
	}
}

func TestRuntimeProjectsRejectedFailedAndUnknownWithoutRetry(t *testing.T) {
	tests := []struct {
		name      string
		workState userspace.WorkState
		wantState ChildTaskState
		wantErr   error
	}{
		{name: "rejected", workState: userspace.WorkStateRejected, wantState: ChildTaskStateRejected, wantErr: ErrChildTaskExecutionRejected},
		{name: "failed", workState: userspace.WorkStateFailed, wantState: ChildTaskStateFailed, wantErr: ErrChildTaskExecutionFailed},
		{name: "unknown", workState: userspace.WorkStateUnknown, wantState: ChildTaskStateUnknown, wantErr: ErrChildTaskExecutionUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			port := &fakeWorkPort{projectionFn: func(item userspace.ExecutionWorkItem) (userspace.ExecutionProjection, error) {
				projection := projectionForState(item, test.workState)
				if test.workState == userspace.WorkStateUnknown {
					projection.RetrySafetyFact = kernel.RetrySafetyExplicitSafe
					projection.LogicalExecutionResult.RetrySafetyFact = kernel.RetrySafetyExplicitSafe
					projection.Result.RetrySafetyFact = kernel.RetrySafetyExplicitSafe
				}
				return projection, nil
			}}
			runtime := NewRuntime(port)
			task := validChildTask(t, ChildTaskID("child-"+test.name))
			result, err := runtime.Execute(context.Background(), task)
			if result.State != test.wantState {
				t.Fatalf("state = %s, want %s", result.State, test.wantState)
			}
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want errors.Is(..., %v)", err, test.wantErr)
			}
			snapshot, snapshotErr := runtime.GetChildTaskSnapshot(task.ChildTaskID)
			if snapshotErr != nil || snapshot.ChildTaskID != task.ChildTaskID || snapshot.RootTaskRef != task.RootTaskRef || snapshot.State != test.wantState || snapshot.ExecutionProjection.State != test.workState {
				t.Fatalf("snapshot = %#v, error = %v, want retained %s projection", snapshot, snapshotErr, test.wantState)
			}
			if calls := port.callCount(); calls != 1 {
				t.Fatalf("USEO calls = %d, want 1", calls)
			}
			duplicate, duplicateErr := runtime.Execute(context.Background(), task)
			if duplicate.State != test.wantState || !errors.Is(duplicateErr, test.wantErr) {
				t.Fatalf("duplicate = %#v, error = %v, want cached %s", duplicate, duplicateErr, test.wantState)
			}
			if calls := port.callCount(); calls != 1 {
				t.Fatalf("duplicate crossed USEO: calls = %d", calls)
			}
		})
	}
}

func TestRuntimeConcurrentDuplicateExecutionUsesSingleUSEOCall(t *testing.T) {
	port := &fakeWorkPort{entered: make(chan struct{}), release: make(chan struct{})}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-concurrent")

	type outcome struct {
		task ChildTask
		err  error
	}
	outcomes := make(chan outcome, 2)
	go func() {
		result, err := runtime.Execute(context.Background(), task)
		outcomes <- outcome{task: result, err: err}
	}()
	<-port.entered
	go func() {
		result, err := runtime.Execute(context.Background(), task)
		outcomes <- outcome{task: result, err: err}
	}()
	close(port.release)

	first := <-outcomes
	second := <-outcomes
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent errors = %v / %v, want nil", first.err, second.err)
	}
	if first.task.ChildTaskID != second.task.ChildTaskID || first.task.State != ChildTaskStateSucceeded || second.task.State != ChildTaskStateSucceeded {
		t.Fatalf("concurrent tasks = %#v / %#v, want same succeeded task", first.task, second.task)
	}
	if first.task.InvocationID != second.task.InvocationID || first.task.ExecutionProjection != second.task.ExecutionProjection {
		t.Fatalf("concurrent projections differ: %#v / %#v", first.task.ExecutionProjection, second.task.ExecutionProjection)
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("USEO calls = %d, want 1", calls)
	}
}

func TestRuntimeBindingConflictRejectsWithoutUSEOCall(t *testing.T) {
	port := &fakeWorkPort{}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-conflict")
	if _, err := runtime.Execute(context.Background(), task); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	conflict := task
	conflict.Operation = "different-operation"
	result, err := runtime.Execute(context.Background(), conflict)
	if result.State != ChildTaskStateRejected || !errors.Is(err, ErrChildTaskBindingConflict) {
		t.Fatalf("conflict result = %#v, error = %v, want rejected binding conflict", result, err)
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("binding conflict crossed USEO: calls = %d", calls)
	}
}

func TestRuntimePreSubmitCancellationIsCachedAndMakesNoUSEOCall(t *testing.T) {
	port := &fakeWorkPort{}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-cancelled")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := runtime.Execute(ctx, task)
	if result.State != ChildTaskStateCancelledBeforeExecution || !errors.Is(err, ErrChildTaskCancelledBeforeExecution) {
		t.Fatalf("cancelled result = %#v, error = %v, want cached pre-submit cancellation", result, err)
	}
	replay, replayErr := runtime.Execute(context.Background(), task)
	if replay.State != ChildTaskStateCancelledBeforeExecution || !errors.Is(replayErr, ErrChildTaskCancelledBeforeExecution) {
		t.Fatalf("replay = %#v, error = %v, want same cancellation", replay, replayErr)
	}
	snapshot, snapshotErr := runtime.GetChildTaskSnapshot(task.ChildTaskID)
	if snapshotErr != nil || snapshot.ChildTaskID != task.ChildTaskID || snapshot.RootTaskRef != task.RootTaskRef || snapshot.State != ChildTaskStateCancelledBeforeExecution {
		t.Fatalf("cancelled snapshot = %#v, error = %v, want valid cached cancellation", snapshot, snapshotErr)
	}
	if calls := port.callCount(); calls != 0 {
		t.Fatalf("cancelled task crossed USEO: calls = %d", calls)
	}
}

func TestRuntimeLateCancellationWaitsForCommittedUSEOResult(t *testing.T) {
	port := &fakeWorkPort{entered: make(chan struct{}), release: make(chan struct{})}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-late-cancel")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		task ChildTask
		err  error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := runtime.Execute(ctx, task)
		done <- outcome{task: result, err: err}
	}()
	<-port.entered
	cancel()
	close(port.release)
	result := <-done
	if result.err != nil || result.task.State != ChildTaskStateSucceeded {
		t.Fatalf("late cancellation = %#v, error = %v, want terminal success", result.task, result.err)
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("late cancellation USEO calls = %d, want 1", calls)
	}
}

func TestRuntimeRootTaskRefIsOpaqueImmutableLineage(t *testing.T) {
	port := &fakeWorkPort{}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-root")
	if _, err := runtime.Execute(context.Background(), task); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	work := port.requestSnapshot()
	if work.RootTaskRef != task.RootTaskRef || work.ChildTaskRef != task.ChildTaskID.String() {
		t.Fatalf("work lineage = %#v, want opaque root and stable child ref", work)
	}
	conflict := task
	conflict.RootTaskRef = "another-opaque-root"
	result, err := runtime.Execute(context.Background(), conflict)
	if result.State != ChildTaskStateRejected || !errors.Is(err, ErrChildTaskBindingConflict) {
		t.Fatalf("root conflict = %#v, error = %v, want binding conflict", result, err)
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("root conflict crossed USEO: calls = %d", calls)
	}
}

func TestRuntimeOptionalRootTaskRefUsesOpaqueFallbackForUSEO(t *testing.T) {
	port := &fakeWorkPort{}
	runtime := NewRuntime(port)
	task, err := New(ChildTaskSpec{
		ChildTaskID:        "child-no-root",
		ExecutionContextID: kernel.ExecutionContextID("context-test"),
		CapabilityHandleID: kernel.CapabilityHandleID("handle-test"),
		Operation:          "execute",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if _, err := runtime.Execute(context.Background(), task); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	work := port.requestSnapshot()
	if work.RootTaskRef != task.ChildTaskID.String() {
		t.Fatalf("fallback RootTaskRef = %q, want opaque child identity", work.RootTaskRef)
	}
}

func TestRuntimeErrorCorrelationUsesChildTaskIDAndPreservesLowerDiagnostic(t *testing.T) {
	lower := &kernel.LogicalErrorInfo{
		Category: kernel.LogicalErrorExecutionUnresolved,
		Phase:    kernel.LogicalPhasePostProvider,
		InvocationID: func() kernel.InvocationID {
			id, err := kernel.NewInvocationID()
			if err != nil {
				t.Fatalf("NewInvocationID() error = %v", err)
			}
			return id
		}(),
		RequestID:              "lower-level-request",
		ReconciliationRequired: true,
	}
	port := &fakeWorkPort{
		projectionFn: func(item userspace.ExecutionWorkItem) (userspace.ExecutionProjection, error) {
			projection := projectionForState(item, userspace.WorkStateRejected)
			return projection, lower
		},
	}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-error-correlation")
	result, err := runtime.Execute(context.Background(), task)
	if result.State != ChildTaskStateRejected || !errors.Is(err, ErrChildTaskExecutionRejected) {
		t.Fatalf("result = %#v, error = %v, want rejected task error", result, err)
	}
	var taskErr *TaskExecutionError
	if !errors.As(err, &taskErr) || taskErr.ChildTaskID != task.ChildTaskID {
		t.Fatalf("task error = %#v, want ChildTaskID correlation", taskErr)
	}
	var lowerErr *kernel.LogicalErrorInfo
	if !errors.As(err, &lowerErr) || lowerErr.RequestID != lower.RequestID {
		t.Fatalf("lower error = %#v, want preserved RequestID %q", lowerErr, lower.RequestID)
	}
	duplicate, duplicateErr := runtime.Execute(context.Background(), task)
	if duplicate.State != ChildTaskStateRejected || !errors.Is(duplicateErr, ErrChildTaskExecutionRejected) {
		t.Fatalf("duplicate = %#v, error = %v, want replayed task rejection", duplicate, duplicateErr)
	}
	var duplicateLower *kernel.LogicalErrorInfo
	if !errors.As(duplicateErr, &duplicateLower) || duplicateLower.RequestID != lower.RequestID {
		t.Fatalf("duplicate lower error = %#v, want preserved RequestID %q", duplicateLower, lower.RequestID)
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("error replay crossed USEO: calls = %d", calls)
	}
}

func TestRuntimeRejectedExecutionErrorPreservesTaskAndUSEODiagnostic(t *testing.T) {
	port := &fakeWorkPort{projectionFn: func(item userspace.ExecutionWorkItem) (userspace.ExecutionProjection, error) {
		return projectionForState(item, userspace.WorkStateRejected), lowerExecutionError(item, "rejected")
	}}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-rejected-error")
	result, err := runtime.Execute(context.Background(), task)
	assertTaskAndUSEOError(t, result, err, task.ChildTaskID, ChildTaskStateRejected, ErrChildTaskExecutionRejected)
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("USEO calls = %d, want 1", calls)
	}
}

func TestRuntimeFailedExecutionErrorPreservesTaskAndUSEODiagnostic(t *testing.T) {
	port := &fakeWorkPort{projectionFn: func(item userspace.ExecutionWorkItem) (userspace.ExecutionProjection, error) {
		return projectionForState(item, userspace.WorkStateFailed), lowerExecutionError(item, "failed")
	}}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-failed-error")
	result, err := runtime.Execute(context.Background(), task)
	assertTaskAndUSEOError(t, result, err, task.ChildTaskID, ChildTaskStateFailed, ErrChildTaskExecutionFailed)
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("USEO calls = %d, want 1", calls)
	}
}

func TestRuntimeUnknownExecutionErrorPreservesTaskAndUSEODiagnostic(t *testing.T) {
	port := &fakeWorkPort{projectionFn: func(item userspace.ExecutionWorkItem) (userspace.ExecutionProjection, error) {
		projection := projectionForState(item, userspace.WorkStateUnknown)
		projection.RetrySafetyFact = kernel.RetrySafetyExplicitSafe
		projection.LogicalExecutionResult.RetrySafetyFact = kernel.RetrySafetyExplicitSafe
		projection.Result.RetrySafetyFact = kernel.RetrySafetyExplicitSafe
		return projection, lowerExecutionError(item, "unknown")
	}}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-unknown-error")
	result, err := runtime.Execute(context.Background(), task)
	assertTaskAndUSEOError(t, result, err, task.ChildTaskID, ChildTaskStateUnknown, ErrChildTaskExecutionUnknown)
	duplicate, duplicateErr := runtime.Execute(context.Background(), task)
	assertTaskAndUSEOError(t, duplicate, duplicateErr, task.ChildTaskID, ChildTaskStateUnknown, ErrChildTaskExecutionUnknown)
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("UNKNOWN replay crossed USEO: calls = %d", calls)
	}
}

func TestRuntimeConcurrentRejectedErrorReplayPreservesBothLayers(t *testing.T) {
	port := &fakeWorkPort{
		entered: make(chan struct{}),
		release: make(chan struct{}),
		projectionFn: func(item userspace.ExecutionWorkItem) (userspace.ExecutionProjection, error) {
			return projectionForState(item, userspace.WorkStateRejected), lowerExecutionError(item, "concurrent-rejected")
		},
	}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-concurrent-error")
	type outcome struct {
		task ChildTask
		err  error
	}
	outcomes := make(chan outcome, 2)
	go func() {
		result, err := runtime.Execute(context.Background(), task)
		outcomes <- outcome{task: result, err: err}
	}()
	<-port.entered
	go func() {
		result, err := runtime.Execute(context.Background(), task)
		outcomes <- outcome{task: result, err: err}
	}()
	close(port.release)
	first := <-outcomes
	second := <-outcomes
	assertTaskAndUSEOError(t, first.task, first.err, task.ChildTaskID, ChildTaskStateRejected, ErrChildTaskExecutionRejected)
	assertTaskAndUSEOError(t, second.task, second.err, task.ChildTaskID, ChildTaskStateRejected, ErrChildTaskExecutionRejected)
	if first.task.ExecutionProjection != second.task.ExecutionProjection {
		t.Fatalf("concurrent projections differ: %#v / %#v", first.task.ExecutionProjection, second.task.ExecutionProjection)
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("concurrent error replay crossed USEO: calls = %d", calls)
	}
}

func TestCloneErrorPreservesJoinStructureAndTypedChildren(t *testing.T) {
	lower := &userspace.ExecutionError{
		Kind:         "lower execution diagnostic",
		WorkItemID:   "work-original",
		ChildTaskRef: "child-join",
		Cause:        errors.New("provider detail"),
	}
	joined := errors.Join(ErrChildTaskExecutionRejected, lower)
	cloned := cloneError(joined)
	if !errors.Is(cloned, ErrChildTaskExecutionRejected) {
		t.Fatalf("cloned join = %v, want task sentinel preserved", cloned)
	}
	var clonedLower *userspace.ExecutionError
	if !errors.As(cloned, &clonedLower) || clonedLower.WorkItemID != lower.WorkItemID {
		t.Fatalf("cloned join lower = %#v, want USEO diagnostic preserved", clonedLower)
	}
	if clonedLower == lower {
		t.Fatalf("cloned lower diagnostic aliases original pointer")
	}
	clonedLower.Kind = "mutated clone"
	if lower.Kind != "lower execution diagnostic" {
		t.Fatalf("mutating cloned diagnostic changed original: %q", lower.Kind)
	}

	taskErr := &TaskExecutionError{
		Kind:        "child task execution rejected",
		ChildTaskID: "child-join",
		Cause:       joined,
	}
	taskClone, ok := cloneError(taskErr).(*TaskExecutionError)
	if !ok || !errors.Is(taskClone, ErrChildTaskExecutionRejected) {
		t.Fatalf("cloned TaskExecutionError = %#v, want outer task semantic", taskClone)
	}
	var nestedLower *userspace.ExecutionError
	if !errors.As(taskClone, &nestedLower) {
		t.Fatalf("cloned TaskExecutionError = %v, want nested USEO diagnostic", taskClone)
	}
}

func lowerExecutionError(item userspace.ExecutionWorkItem, label string) *userspace.ExecutionError {
	return &userspace.ExecutionError{
		Kind:         "lower " + label + " diagnostic",
		WorkItemID:   item.WorkItemID,
		ChildTaskRef: item.ChildTaskRef,
		Cause:        errors.New("lower provider detail: " + label),
	}
}

func assertTaskAndUSEOError(t *testing.T, task ChildTask, err error, childID ChildTaskID, wantState ChildTaskState, sentinel error) {
	t.Helper()
	if task.ChildTaskID != childID || task.State != wantState {
		t.Fatalf("task = %#v, want %s for %s", task, wantState, childID)
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want errors.Is(..., %v)", err, sentinel)
	}
	var taskErr *TaskExecutionError
	if !errors.As(err, &taskErr) || taskErr.ChildTaskID != childID {
		t.Fatalf("task error = %#v, want ChildTaskID %s", taskErr, childID)
	}
	var lower *userspace.ExecutionError
	if !errors.As(err, &lower) || lower.ChildTaskRef != childID.String() {
		t.Fatalf("lower error = %#v, want USEO ChildTaskRef %s", lower, childID)
	}
	if task.Failure == nil || !errors.Is(task.Failure, sentinel) || !errors.As(task.Failure, &lower) {
		t.Fatalf("task failure = %v, want cloned task and USEO error layers", task.Failure)
	}
}

func TestRuntimeInvalidChildTaskFailsBeforeUSEO(t *testing.T) {
	port := &fakeWorkPort{}
	runtime := NewRuntime(port)
	result, err := runtime.Execute(context.Background(), ChildTask{})
	if result.State != ChildTaskStateRejected || !errors.Is(err, ErrInvalidChildTask) {
		t.Fatalf("invalid task = %#v, error = %v, want local rejection", result, err)
	}
	if calls := port.callCount(); calls != 0 {
		t.Fatalf("invalid task crossed USEO: calls = %d", calls)
	}
}

func TestRuntimeGetChildTaskSnapshotUnknownAndInvalidID(t *testing.T) {
	port := &fakeWorkPort{}
	runtime := NewRuntime(port)

	if _, err := runtime.GetChildTaskSnapshot("missing-child"); !errors.Is(err, ErrChildTaskNotFound) {
		t.Fatalf("unknown snapshot error = %v, want ErrChildTaskNotFound", err)
	}
	if _, err := runtime.GetChildTaskSnapshot(""); !errors.Is(err, ErrInvalidChildTask) {
		t.Fatalf("invalid snapshot error = %v, want ErrInvalidChildTask", err)
	}
	if calls := port.callCount(); calls != 0 {
		t.Fatalf("snapshot query crossed USEO: calls = %d", calls)
	}
}

func TestRuntimeGetChildTaskSnapshotFailsClosedOnIdentityMismatch(t *testing.T) {
	runtime := NewRuntime(&fakeWorkPort{})
	requested := validChildTask(t, "child-snapshot-requested")
	stored := validChildTask(t, "child-snapshot-stored")
	done := make(chan struct{})
	close(done)
	runtime.mu.Lock()
	runtime.tasks[requested.ChildTaskID] = &taskRecord{task: stored, done: done}
	runtime.mu.Unlock()

	if _, err := runtime.GetChildTaskSnapshot(requested.ChildTaskID); !errors.Is(err, ErrChildTaskSnapshotUnavailable) {
		t.Fatalf("identity-mismatch snapshot error = %v, want ErrChildTaskSnapshotUnavailable", err)
	}
}

func TestRuntimeGetChildTaskSnapshotTerminalAndImmutable(t *testing.T) {
	port := &fakeWorkPort{
		projectionFn: func(item userspace.ExecutionWorkItem) (userspace.ExecutionProjection, error) {
			projection := projectionForState(item, userspace.WorkStateRejected)
			projection.LogicalExecutionResult.ErrorInfo = &kernel.LogicalErrorInfo{
				Category:     kernel.LogicalErrorAuthorizationDenied,
				Phase:        kernel.LogicalPhaseAuthorityValidation,
				Diagnostic:   "authoritative diagnostic",
				RequestID:    item.RequestID,
				InvocationID: item.InvocationID,
			}
			projection.Result.ErrorInfo = projection.LogicalExecutionResult.ErrorInfo.Clone()
			return projection, lowerExecutionError(item, "snapshot-rejected")
		},
	}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-snapshot-terminal")
	result, executeErr := runtime.Execute(context.Background(), task)
	if result.State != ChildTaskStateRejected || !errors.Is(executeErr, ErrChildTaskExecutionRejected) {
		t.Fatalf("Execute() = %#v, error = %v, want rejected terminal", result, executeErr)
	}

	snapshot, err := runtime.GetChildTaskSnapshot(task.ChildTaskID)
	if err != nil {
		t.Fatalf("GetChildTaskSnapshot() error = %v", err)
	}
	if snapshot.ChildTaskID != task.ChildTaskID || snapshot.RootTaskRef != task.RootTaskRef || snapshot.State != ChildTaskStateRejected {
		t.Fatalf("snapshot identity/lineage/state = %#v, want %s/%s/REJECTED", snapshot, task.ChildTaskID, task.RootTaskRef)
	}
	if snapshot.InvocationID != result.InvocationID || snapshot.ExecutionProjection.WorkItemID != result.ExecutionProjection.WorkItemID || snapshot.ExecutionProjection.RequestID != result.ExecutionProjection.RequestID || snapshot.ExecutionProjection.State != result.ExecutionProjection.State || snapshot.ExecutionProjection.Status != result.ExecutionProjection.Status {
		t.Fatalf("snapshot execution projection = %#v, want terminal correlation/state from %#v", snapshot.ExecutionProjection, result.ExecutionProjection)
	}
	if snapshot.Failure == nil {
		t.Fatalf("snapshot failure = nil, want task-level diagnostic")
	}

	// Mutate every caller-visible field that can be changed without reaching the
	// Runtime record, including cloned nested Kernel diagnostics.
	snapshot.RootTaskRef = "mutated-root"
	snapshot.State = ChildTaskStateSucceeded
	snapshot.ExecutionProjection.State = userspace.WorkStateSucceeded
	snapshot.ExecutionProjection.LogicalExecutionResult.Status = kernel.LogicalExecutionCompleted
	if snapshot.ExecutionProjection.LogicalExecutionResult.ErrorInfo != nil {
		snapshot.ExecutionProjection.LogicalExecutionResult.ErrorInfo.Diagnostic = "mutated-diagnostic"
	}
	var snapshotFailure *TaskExecutionError
	if errors.As(snapshot.Failure, &snapshotFailure) {
		snapshotFailure.Kind = "mutated-task-error"
	}

	again, err := runtime.GetChildTaskSnapshot(task.ChildTaskID)
	if err != nil {
		t.Fatalf("second GetChildTaskSnapshot() error = %v", err)
	}
	if again.RootTaskRef != task.RootTaskRef || again.State != ChildTaskStateRejected || again.ExecutionProjection.State != userspace.WorkStateRejected {
		t.Fatalf("authoritative snapshot changed after caller mutation = %#v", again)
	}
	if again.ExecutionProjection.LogicalExecutionResult.Status != kernel.LogicalExecutionRejected {
		t.Fatalf("authoritative status = %s, want REJECTED", again.ExecutionProjection.LogicalExecutionResult.Status)
	}
	if info := again.ExecutionProjection.LogicalExecutionResult.ErrorInfo; info == nil || info.Diagnostic != "authoritative diagnostic" {
		t.Fatalf("authoritative diagnostic = %#v, want original diagnostic", info)
	}
	var againFailure *TaskExecutionError
	if !errors.As(again.Failure, &againFailure) || againFailure.Kind == "mutated-task-error" {
		t.Fatalf("authoritative task failure = %#v, want independent clone", againFailure)
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("snapshot reads changed USEO cardinality: calls = %d", calls)
	}
}

func TestRuntimeGetChildTaskSnapshotWhileExecutingDoesNotWait(t *testing.T) {
	port := &fakeWorkPort{entered: make(chan struct{}), release: make(chan struct{})}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-snapshot-executing")
	type outcome struct {
		task ChildTask
		err  error
	}
	executionDone := make(chan outcome, 1)
	go func() {
		result, err := runtime.Execute(context.Background(), task)
		executionDone <- outcome{task: result, err: err}
	}()
	<-port.entered

	readDone := make(chan struct{})
	var snapshot ChildTaskSnapshot
	var snapshotErr error
	go func() {
		snapshot, snapshotErr = runtime.GetChildTaskSnapshot(task.ChildTaskID)
		close(readDone)
	}()
	select {
	case <-readDone:
	case <-time.After(2 * time.Second):
		close(port.release)
		<-executionDone
		t.Fatal("GetChildTaskSnapshot blocked on execution completion")
	}
	if snapshotErr != nil {
		t.Fatalf("executing GetChildTaskSnapshot() error = %v", snapshotErr)
	}
	if snapshot.State != ChildTaskStateExecuting || snapshot.ChildTaskID != task.ChildTaskID || snapshot.RootTaskRef != task.RootTaskRef {
		t.Fatalf("executing snapshot = %#v, want EXECUTING with stable identity/lineage", snapshot)
	}
	if snapshot.InvocationID.IsZero() {
		t.Fatalf("executing snapshot invocation ID is zero")
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("executing snapshot changed USEO cardinality: calls = %d", calls)
	}

	close(port.release)
	result := <-executionDone
	if result.err != nil || result.task.State != ChildTaskStateSucceeded {
		t.Fatalf("completed execution = %#v, error = %v, want success", result.task, result.err)
	}
	terminal, err := runtime.GetChildTaskSnapshot(task.ChildTaskID)
	if err != nil || terminal.State != ChildTaskStateSucceeded {
		t.Fatalf("terminal snapshot = %#v, error = %v, want SUCCEEDED", terminal, err)
	}
}

func TestRuntimeRepeatedChildTaskSnapshotsHaveNoSideEffects(t *testing.T) {
	port := &fakeWorkPort{}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-snapshot-repeat")
	if _, err := runtime.Execute(context.Background(), task); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	for i := 0; i < 100; i++ {
		snapshot, err := runtime.GetChildTaskSnapshot(task.ChildTaskID)
		if err != nil {
			t.Fatalf("snapshot %d error = %v", i, err)
		}
		if snapshot.ChildTaskID != task.ChildTaskID || snapshot.State != ChildTaskStateSucceeded {
			t.Fatalf("snapshot %d = %#v, want stable succeeded value", i, snapshot)
		}
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("repeated snapshots changed USEO cardinality: calls = %d", calls)
	}
}

func TestRuntimeConcurrentChildTaskSnapshotReaders(t *testing.T) {
	port := &fakeWorkPort{entered: make(chan struct{}), release: make(chan struct{})}
	runtime := NewRuntime(port)
	task := validChildTask(t, "child-snapshot-readers")
	executionDone := make(chan error, 1)
	go func() {
		_, err := runtime.Execute(context.Background(), task)
		executionDone <- err
	}()
	<-port.entered

	const readers = 32
	start := make(chan struct{})
	activeResults := make(chan ChildTaskSnapshot, readers)
	activeErrors := make(chan error, readers)
	var wg sync.WaitGroup
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			<-start
			snapshot, err := runtime.GetChildTaskSnapshot(task.ChildTaskID)
			if err != nil {
				activeErrors <- err
				return
			}
			activeResults <- snapshot
		}()
	}
	close(start)
	wg.Wait()
	close(activeResults)
	close(activeErrors)
	for err := range activeErrors {
		t.Fatalf("concurrent active snapshot error = %v", err)
	}
	for snapshot := range activeResults {
		if snapshot.ChildTaskID != task.ChildTaskID || snapshot.State != ChildTaskStateExecuting || snapshot.RootTaskRef != task.RootTaskRef {
			t.Fatalf("concurrent active snapshot = %#v, want valid EXECUTING value", snapshot)
		}
	}

	close(port.release)
	if err := <-executionDone; err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	terminalResults := make(chan ChildTaskSnapshot, readers)
	terminalErrors := make(chan error, readers)
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			snapshot, err := runtime.GetChildTaskSnapshot(task.ChildTaskID)
			if err != nil {
				terminalErrors <- err
				return
			}
			terminalResults <- snapshot
		}()
	}
	wg.Wait()
	close(terminalResults)
	close(terminalErrors)
	for err := range terminalErrors {
		t.Fatalf("concurrent terminal snapshot error = %v", err)
	}
	for snapshot := range terminalResults {
		if snapshot.ChildTaskID != task.ChildTaskID || snapshot.State != ChildTaskStateSucceeded || snapshot.RootTaskRef != task.RootTaskRef {
			t.Fatalf("concurrent terminal snapshot = %#v, want valid SUCCEEDED value", snapshot)
		}
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("concurrent snapshots changed USEO cardinality: calls = %d", calls)
	}
}

func TestRuntimeLocalFailurePublicationHasNoZeroValueSnapshot(t *testing.T) {
	runtime := NewRuntime(nil)
	task := validChildTask(t, "child-snapshot-publication")
	const readers = 32
	start := make(chan struct{})
	var wg sync.WaitGroup
	var found atomic.Int32
	unexpected := make(chan error, readers)
	wg.Add(readers)
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < 100; j++ {
				snapshot, err := runtime.GetChildTaskSnapshot(task.ChildTaskID)
				if err != nil {
					if errors.Is(err, ErrChildTaskNotFound) {
						continue
					}
					unexpected <- err
					return
				}
				found.Add(1)
				if snapshot.ChildTaskID != task.ChildTaskID || snapshot.RootTaskRef != task.RootTaskRef || snapshot.State != ChildTaskStateRejected {
					unexpected <- fmt.Errorf("invalid published snapshot: %#v", snapshot)
					return
				}
			}
		}()
	}
	close(start)
	result, err := runtime.Execute(context.Background(), task)
	wg.Wait()
	close(unexpected)
	for err := range unexpected {
		t.Fatal(err)
	}
	if result.State != ChildTaskStateRejected || !errors.Is(err, ErrExecutionWorkPortUnavailable) {
		t.Fatalf("nil-port execution = %#v, error = %v, want rejected local terminal", result, err)
	}
	final, finalErr := runtime.GetChildTaskSnapshot(task.ChildTaskID)
	if finalErr != nil || final.ChildTaskID != task.ChildTaskID || final.RootTaskRef != task.RootTaskRef || final.State != ChildTaskStateRejected {
		t.Fatalf("final publication snapshot = %#v, error = %v, want valid REJECTED value", final, finalErr)
	}
	if found.Load() == 0 {
		t.Fatalf("concurrent readers never observed the published record")
	}
}

func TestRuntimeIntegrationWithRealUSEOAndKernel(t *testing.T) {
	provider := &countingProvider{}
	k := kernel.New(provider)
	resourceObject, err := k.Resources.CreateResource("local")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	declaration, err := k.Capabilities.RegisterDeclaration(capability.DeclarationSpec{
		ResourceID: resourceObject.ID,
		Name:       "test.execute",
		Version:    "v1",
	})
	if err != nil {
		t.Fatalf("RegisterDeclaration() error = %v", err)
	}
	instance, err := k.Capabilities.CreateInstance(declaration.ID)
	if err != nil {
		t.Fatalf("CreateInstance() error = %v", err)
	}
	executionContext, err := k.Executions.CreateContext("subject", "local")
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}
	handle, err := k.Capabilities.CreateHandle(executionContext.ID, instance.ID, []string{"execute"}, "local")
	if err != nil {
		t.Fatalf("CreateHandle() error = %v", err)
	}
	facade, err := kernel.NewLogicalExecutionFacade(k)
	if err != nil {
		t.Fatalf("NewLogicalExecutionFacade() error = %v", err)
	}
	workPort := &countingWorkPort{delegate: userspace.NewOrchestrator(facade)}
	runtime := NewRuntime(workPort)
	task, err := New(ChildTaskSpec{
		ChildTaskID:        "child-real-kernel",
		RootTaskRef:        "root-real-kernel",
		ExecutionContextID: executionContext.ID,
		CapabilityHandleID: handle.ID,
		CallerIdentity:     executionContext.Subject,
		Scope:              executionContext.Scope,
		Operation:          "execute",
		Payload:            []byte("integration-payload"),
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	first, err := runtime.Execute(context.Background(), task)
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	second, err := runtime.Execute(context.Background(), task)
	if err != nil {
		t.Fatalf("duplicate Execute() error = %v", err)
	}
	if first.State != ChildTaskStateSucceeded || second.State != ChildTaskStateSucceeded {
		t.Fatalf("results = %#v / %#v, want succeeded", first, second)
	}
	if workPort.callCount() != 1 || provider.Count() != 1 {
		t.Fatalf("layered calls = USEO %d, provider %d, want 1/1", workPort.callCount(), provider.Count())
	}
	if first.ExecutionProjection.CurrentOccupancy != kernel.CurrentOccupancyEnded || first.ExecutionProjection.Lifecycle != kernel.AllocationReleased {
		t.Fatalf("Kernel projection = %#v, want definitive ended/released", first.ExecutionProjection)
	}
	snapshot, err := runtime.GetChildTaskSnapshot(task.ChildTaskID)
	if err != nil {
		t.Fatalf("GetChildTaskSnapshot() error = %v", err)
	}
	if snapshot.ChildTaskID != task.ChildTaskID || snapshot.RootTaskRef != task.RootTaskRef || snapshot.State != ChildTaskStateSucceeded {
		t.Fatalf("real-stack snapshot = %#v, want stable terminal child value", snapshot)
	}
	if snapshot.InvocationID != first.InvocationID || snapshot.ExecutionProjection.WorkItemID != first.ExecutionProjection.WorkItemID || snapshot.ExecutionProjection.RequestID != first.ExecutionProjection.RequestID || snapshot.ExecutionProjection.State != first.ExecutionProjection.State || snapshot.ExecutionProjection.Status != first.ExecutionProjection.Status || snapshot.ExecutionProjection.CurrentOccupancy != first.ExecutionProjection.CurrentOccupancy || snapshot.ExecutionProjection.Lifecycle != first.ExecutionProjection.Lifecycle {
		t.Fatalf("real-stack snapshot projection = %#v, want terminal correlation/state from %#v", snapshot.ExecutionProjection, first.ExecutionProjection)
	}
}

type countingWorkPort struct {
	delegate *userspace.Orchestrator
	calls    atomic.Int32
}

func (port *countingWorkPort) Execute(ctx context.Context, item userspace.ExecutionWorkItem) (userspace.ExecutionProjection, error) {
	port.calls.Add(1)
	return port.delegate.Execute(ctx, item)
}

func (port *countingWorkPort) callCount() int { return int(port.calls.Load()) }

type countingProvider struct {
	calls atomic.Int32
}

func (provider *countingProvider) Invoke(capability.ProviderOperation) error {
	provider.calls.Add(1)
	return nil
}

func (provider *countingProvider) Count() int { return int(provider.calls.Load()) }

var _ ExecutionWorkPort = (*fakeWorkPort)(nil)
var _ ExecutionWorkPort = (*userspace.Orchestrator)(nil)
var _ capability.CapabilityProvider = (*countingProvider)(nil)
