package userspace

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"

	"dtm/internal/kernel"
)

// Orchestrator is the bounded, synchronous US-0 execution owner.  Its map is
// only local correlation/idempotency state: Kernel remains the authority for
// Context, Capability, allocation, occupancy, Provider crossing, and events.
type Orchestrator struct {
	port ExecutionKernelPort

	mu      sync.Mutex
	byChild map[string]*workRecord
}

type workRecord struct {
	binding workBinding
	item    ExecutionWorkItem
	done    chan struct{}

	// result and err are written before done is closed and are immutable after
	// that point.  Closing done provides the happens-before edge for readers.
	result ExecutionProjection
	err    error
}

// workBinding mirrors the currently frozen Kernel InvocationBinding fields
// and includes the caller-supplied InvocationID.  RequestID and WorkItemID are
// correlation-only and therefore do not participate in idempotency equality.
type workBinding struct {
	InvocationID       kernel.InvocationID
	ExecutionContextID kernel.ExecutionContextID
	CapabilityHandleID kernel.CapabilityHandleID
	CallerIdentity     string
	Scope              string
	Operation          string
	Payload            []byte
	RootTaskRef        string
	ChildTaskRef       string
}

func newWorkBinding(item ExecutionWorkItem) workBinding {
	return workBinding{
		InvocationID:       item.InvocationID,
		ExecutionContextID: item.ExecutionContextID,
		CapabilityHandleID: item.CapabilityHandleID,
		CallerIdentity:     item.CallerIdentity,
		Scope:              item.Scope,
		Operation:          item.Operation,
		Payload:            append([]byte(nil), item.Payload...),
		RootTaskRef:        item.RootTaskRef,
		ChildTaskRef:       item.ChildTaskRef,
	}
}

func (binding workBinding) equal(other workBinding) bool {
	return binding.InvocationID == other.InvocationID &&
		binding.ExecutionContextID == other.ExecutionContextID &&
		binding.CapabilityHandleID == other.CapabilityHandleID &&
		binding.CallerIdentity == other.CallerIdentity &&
		binding.Scope == other.Scope &&
		binding.Operation == other.Operation &&
		bytes.Equal(binding.Payload, other.Payload) &&
		binding.RootTaskRef == other.RootTaskRef &&
		binding.ChildTaskRef == other.ChildTaskRef
}

// NewOrchestrator creates an in-memory bounded execution owner.  A nil port is
// accepted so the zero-side-effect failure can be reported by Execute; this
// keeps construction side-effect free and easy to compose in tests.
func NewOrchestrator(port ExecutionKernelPort) *Orchestrator {
	return &Orchestrator{
		port:    port,
		byChild: make(map[string]*workRecord),
	}
}

// New is a concise constructor alias.  It does not add another execution
// mechanism or policy surface.
func New(port ExecutionKernelPort) *Orchestrator {
	return NewOrchestrator(port)
}

// Execute performs at most one Kernel logical execution request for the
// root/child work item.  Cancellation is checked only before submission; once
// the port call begins this synchronous method waits for its returned value.
func (orchestrator *Orchestrator) Execute(ctx context.Context, item ExecutionWorkItem) (ExecutionProjection, error) {
	item = item.Clone()
	if err := item.Validate(); err != nil {
		return localFailure(item, WorkStateRejected, ErrInvalidWorkItem.Error(), err)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	if orchestrator == nil {
		return localFailure(item, WorkStateRejected, ErrKernelPortUnavailable.Error(), ErrKernelPortUnavailable)
	}

	key := childKey(item)
	binding := newWorkBinding(item)
	orchestrator.mu.Lock()
	if orchestrator.byChild == nil {
		orchestrator.byChild = make(map[string]*workRecord)
	}
	if existing, exists := orchestrator.byChild[key]; exists {
		orchestrator.mu.Unlock()
		if !binding.equal(existing.binding) {
			return localFailure(item, WorkStateRejected, ErrWorkItemAlreadySubmitted.Error(), errors.Join(ErrWorkItemAlreadySubmitted, ErrWorkItemConflict))
		}
		// A duplicate of an already-submitted work item is a read of the
		// existing projection.  Its caller context cannot cancel the
		// already-started execution, and it cannot create a second request.
		<-existing.done
		projection := existing.result.Clone()
		projection = projectionForCorrelation(projection, item.WorkItemID, item.RequestID)
		return projection, cloneExecutionError(existing.err)
	}

	// Mark the work item before releasing the mutex.  A second caller for the
	// same opaque ChildTaskRef therefore waits for this one-shot attempt rather
	// than entering the Kernel a second time.
	if err := ctx.Err(); err != nil {
		record := &workRecord{
			binding: binding,
			item:    item.Clone(),
			done:    make(chan struct{}),
		}
		orchestrator.byChild[key] = record
		orchestrator.mu.Unlock()
		projection, localErr := localFailure(item, WorkStateCancelledBeforeSubmit, ErrCancelledBeforeSubmit.Error(), errors.Join(ErrCancelledBeforeSubmit, err))
		record.finish(projection, localErr)
		return projection, localErr
	}
	if isNilExecutionKernelPort(orchestrator.port) {
		record := &workRecord{
			binding: binding,
			item:    item.Clone(),
			done:    make(chan struct{}),
		}
		orchestrator.byChild[key] = record
		orchestrator.mu.Unlock()
		projection, localErr := localFailure(item, WorkStateRejected, ErrKernelPortUnavailable.Error(), ErrKernelPortUnavailable)
		record.finish(projection, localErr)
		return projection, localErr
	}
	record := &workRecord{
		binding: binding,
		item:    item.Clone(),
		done:    make(chan struct{}),
	}
	orchestrator.byChild[key] = record
	orchestrator.mu.Unlock()

	// Cancellation can race with the transition from local submission to the
	// synchronous port call.  A final pre-call check preserves the explicit
	// zero-call cancellation guarantee.  Once this check passes, the call is
	// considered started and later cancellation is ignored by this slice.
	if err := ctx.Err(); err != nil {
		projection, localErr := localFailure(item, WorkStateCancelledBeforeSubmit, ErrCancelledBeforeSubmit.Error(), errors.Join(ErrCancelledBeforeSubmit, err))
		record.finish(projection, localErr)
		return projection, localErr
	}

	result, requestErr := orchestrator.port.RequestExecution(item.LogicalExecutionRequest())
	projection, err := orchestrator.project(item, result, requestErr)
	record.finish(projection, err)
	return projection.Clone(), cloneExecutionError(err)
}

// ExecuteWorkItem is a descriptive method alias for callers using the model
// name.  It has exactly the same one-shot semantics as Execute.
func (orchestrator *Orchestrator) ExecuteWorkItem(ctx context.Context, item ExecutionWorkItem) (ExecutionProjection, error) {
	return orchestrator.Execute(ctx, item)
}

func (record *workRecord) finish(projection ExecutionProjection, err error) {
	record.result = projection.Clone()
	record.err = cloneExecutionError(err)
	close(record.done)
}

func (orchestrator *Orchestrator) project(item ExecutionWorkItem, result kernel.LogicalExecutionResult, requestErr error) (ExecutionProjection, error) {
	result = result.Clone()
	projection := projectionFromResult(item, result)
	requestErr = normalizeKernelError(result, requestErr)

	// A valid request identity is supplied by the work item and is never
	// regenerated.  If the port violates the frozen echo contract, fail closed
	// in the User-Space projection without inventing a new identity.
	if result.InvocationID.IsZero() || result.InvocationID != item.InvocationID {
		identityErr := fmt.Errorf("%w: invocation identity must echo the submitted request", ErrInvalidKernelResult)
		projection.State = WorkStateUnknown
		projection.LogicalExecutionResult.ReconciliationRequired = true
		projection.Result = projection.LogicalExecutionResult.Clone()
		requestErr = joinErrors(requestErr, identityErr)
		// The result identity is untrusted.  Do not use it as an observation
		// key, even when the malformed snapshot also requests reconciliation.
		return projection, requestErr
	}

	if result.ReconciliationRequired || (result.ErrorInfo != nil && result.ErrorInfo.ReconciliationRequired) {
		return orchestrator.observeOnce(item, projection, requestErr, result.InvocationID)
	}
	return projection, requestErr
}

func (orchestrator *Orchestrator) observeOnce(item ExecutionWorkItem, initial ExecutionProjection, initialErr error, invocationID kernel.InvocationID) (ExecutionProjection, error) {
	initial.ObserveInvocationCalled = true
	observed, observeErr := orchestrator.port.ObserveInvocation(invocationID)
	observed = observed.Clone()
	initial.InvocationObservationAvailable = true
	initial.InvocationObservation = observed

	if observed.InvocationID.IsZero() {
		// The read itself happened exactly once, but it did not provide a valid
		// snapshot.  Keep the first result's facts and retain UNKNOWN.
		initial.State = WorkStateUnknown
		initial.LogicalExecutionResult.ReconciliationRequired = true
		initial.Result = initial.LogicalExecutionResult.Clone()
		return initial, joinErrors(initialErr, normalizeKernelError(observed.LogicalExecutionResult, observeErr))
	}
	if observed.InvocationID != item.InvocationID {
		initial.State = WorkStateUnknown
		initial.LogicalExecutionResult.ReconciliationRequired = true
		initial.Result = initial.LogicalExecutionResult.Clone()
		identityErr := fmt.Errorf("%w: observation identity does not match the submitted request", ErrInvalidKernelResult)
		return initial, joinErrors(initialErr, errors.Join(normalizeKernelError(observed.LogicalExecutionResult, observeErr), identityErr))
	}

	latest := observed.LogicalExecutionResult.Clone()
	if latest.RequestID == "" {
		latest.RequestID = item.RequestID
	}
	initial.LogicalExecutionResult = latest
	initial.Result = latest.Clone()
	initial.State = stateForResult(latest)
	if latest.ReconciliationRequired || (latest.ErrorInfo != nil && latest.ErrorInfo.ReconciliationRequired) || observed.State == kernel.InvocationObservationUnresolvedUnknown || observed.State == kernel.InvocationObservationInvariantUnknown {
		initial.State = WorkStateUnknown
		initial.LogicalExecutionResult.ReconciliationRequired = true
		initial.Result = initial.LogicalExecutionResult.Clone()
	}

	latestErr := normalizeKernelError(latest, observeErr)
	// A definitive latest observation supersedes the first unresolved
	// diagnostic.  If it is itself errored or still unresolved, preserve both
	// typed facts for the caller without taking another action.
	if initial.State == WorkStateSucceeded && !latest.ReconciliationRequired && latestErr == nil {
		return initial, nil
	}
	if initial.State == WorkStateFailed && !latest.ReconciliationRequired && latestErr == nil {
		return initial, nil
	}
	if initial.State == WorkStateRejected && !latest.ReconciliationRequired && latestErr == nil {
		return initial, nil
	}
	return initial, joinErrors(initialErr, latestErr)
}

func projectionFromResult(item ExecutionWorkItem, result kernel.LogicalExecutionResult) ExecutionProjection {
	result = result.Clone()
	return ExecutionProjection{
		WorkItemID:             item.WorkItemID,
		RootTaskRef:            item.RootTaskRef,
		ChildTaskRef:           item.ChildTaskRef,
		InvocationID:           item.InvocationID,
		RequestID:              item.RequestID,
		State:                  stateForResult(result),
		LogicalExecutionResult: result,
		Result:                 result.Clone(),
	}
}

func stateForResult(result kernel.LogicalExecutionResult) WorkState {
	switch result.Status {
	case kernel.LogicalExecutionCompleted:
		return WorkStateSucceeded
	case kernel.LogicalExecutionFailed:
		return WorkStateFailed
	case kernel.LogicalExecutionRejected:
		return WorkStateRejected
	case kernel.LogicalExecutionAccepted, kernel.LogicalExecutionAllocated, kernel.LogicalExecutionDispatched, kernel.LogicalExecutionRunning:
		return WorkStateSubmitted
	case kernel.LogicalExecutionUnknown:
		return WorkStateUnknown
	default:
		if result.ReconciliationRequired {
			return WorkStateUnknown
		}
		return WorkStateRejected
	}
}

func localFailure(item ExecutionWorkItem, state WorkState, kind string, cause error) (ExecutionProjection, error) {
	result := kernel.LogicalExecutionResult{
		InvocationID:              item.InvocationID,
		RequestID:                 item.RequestID,
		Status:                    kernel.LogicalExecutionRejected,
		RetrySafetyFact:           kernel.RetrySafetyNotDeclared,
		ProviderCrossing:          "",
		ProviderCrossingAvailable: false,
	}
	projection := projectionFromResult(item, result)
	projection.State = state
	return projection, localError(kind, item, cause)
}

func normalizeKernelError(result kernel.LogicalExecutionResult, err error) error {
	if result.ErrorInfo != nil {
		info := result.ErrorInfo.Clone()
		if err != nil {
			return errors.Join(info, err)
		}
		return info
	}
	return err
}

// projectionForCorrelation returns the stored execution facts with the
// current caller's correlation identifiers.  WorkItemID and RequestID never
// participate in the authoritative one-shot binding, so a valid replay may
// project new values without mutating the stored record or Kernel identity.
func projectionForCorrelation(projection ExecutionProjection, workItemID, requestID string) ExecutionProjection {
	projection.WorkItemID = workItemID
	projection.RequestID = requestID
	projection.LogicalExecutionResult.RequestID = requestID
	projection.Result.RequestID = requestID
	if projection.LogicalExecutionResult.ErrorInfo != nil {
		projection.LogicalExecutionResult.ErrorInfo = projection.LogicalExecutionResult.ErrorInfo.Clone()
		projection.LogicalExecutionResult.ErrorInfo.RequestID = requestID
	}
	if projection.Result.ErrorInfo != nil {
		projection.Result.ErrorInfo = projection.Result.ErrorInfo.Clone()
		projection.Result.ErrorInfo.RequestID = requestID
	}
	if projection.InvocationObservationAvailable {
		projection.InvocationObservation.LogicalExecutionResult.RequestID = requestID
		if projection.InvocationObservation.LogicalExecutionResult.ErrorInfo != nil {
			projection.InvocationObservation.LogicalExecutionResult.ErrorInfo = projection.InvocationObservation.LogicalExecutionResult.ErrorInfo.Clone()
			projection.InvocationObservation.LogicalExecutionResult.ErrorInfo.RequestID = requestID
		}
	}
	return projection
}

func childKey(item ExecutionWorkItem) string {
	return item.RootTaskRef + "\x00" + item.ChildTaskRef
}

func isNilExecutionKernelPort(port ExecutionKernelPort) bool {
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

func joinErrors(left, right error) error {
	switch {
	case left == nil:
		return right
	case right == nil:
		return left
	default:
		return errors.Join(left, right)
	}
}

func cloneExecutionError(err error) error {
	if err == nil {
		return nil
	}
	var local *ExecutionError
	if errors.As(err, &local) && local != nil {
		clone := *local
		clone.Cause = cloneExecutionError(local.Cause)
		return &clone
	}
	type joinedErrors interface {
		Unwrap() []error
	}
	var joined joinedErrors
	if errors.As(err, &joined) && joined != nil {
		parts := joined.Unwrap()
		cloned := make([]error, 0, len(parts))
		for _, part := range parts {
			cloned = append(cloned, cloneExecutionError(part))
		}
		return errors.Join(cloned...)
	}
	var info *kernel.LogicalErrorInfo
	if errors.As(err, &info) && info != nil {
		return info.Clone()
	}
	return err
}
