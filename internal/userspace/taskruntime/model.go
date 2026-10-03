// Package taskruntime owns the bounded User-Space Child Task lifecycle.
//
// It deliberately sits above userspace.Orchestrator.  The package owns one
// ChildTaskID and its local lifecycle record; USEO remains the owner of the
// ExecutionWorkItem and the Kernel remains the authority for execution facts.
package taskruntime

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"unicode/utf8"

	"dtm/internal/kernel"
	"dtm/internal/userspace"
)

// ChildTaskID is the stable User-Space identity for one bounded child work
// unit.  It is intentionally distinct from the legacy TaskID model and is
// never replaced by a WorkItemID or RequestID.
type ChildTaskID string

// String returns the opaque Child Task identity.
func (id ChildTaskID) String() string { return string(id) }

// Validate checks only the local identity shape.  It does not establish
// Kernel authority or inspect a Root Task.
func (id ChildTaskID) Validate() error {
	value := string(id)
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || strings.ContainsRune(value, '\x00') {
		return errors.New("child task id must be non-blank valid UTF-8 text")
	}
	return nil
}

// ChildTaskState is the intentionally small lifecycle vocabulary for US-1.
// CREATED and READY have no distinct behavior in this phase, so READY is an
// alias of CREATED rather than a second state.
type ChildTaskState string

const (
	ChildTaskStateCreated                  ChildTaskState = "CREATED"
	ChildTaskStateExecuting                ChildTaskState = "EXECUTING"
	ChildTaskStateSucceeded                ChildTaskState = "SUCCEEDED"
	ChildTaskStateFailed                   ChildTaskState = "FAILED"
	ChildTaskStateRejected                 ChildTaskState = "REJECTED"
	ChildTaskStateUnknown                  ChildTaskState = "UNKNOWN"
	ChildTaskStateCancelledBeforeExecution ChildTaskState = "CANCELLED_BEFORE_EXECUTION"

	// Ready is an intentional compatibility spelling for the single
	// pre-submission state.  It does not add a transition or policy.
	ChildTaskStateReady = ChildTaskStateCreated
)

// Short state aliases keep the model easy to use without introducing another
// state vocabulary.
const (
	StateCreated                  = ChildTaskStateCreated
	StateReady                    = ChildTaskStateReady
	StateExecuting                = ChildTaskStateExecuting
	StateSucceeded                = ChildTaskStateSucceeded
	StateFailed                   = ChildTaskStateFailed
	StateRejected                 = ChildTaskStateRejected
	StateUnknown                  = ChildTaskStateUnknown
	StateCancelledBeforeExecution = ChildTaskStateCancelledBeforeExecution
)

var (
	// ErrInvalidChildTask identifies a local model or binding-shape failure.
	ErrInvalidChildTask = errors.New("invalid child task")
	// ErrChildTaskNotFound identifies a Child Task that is not retained by this
	// process-local Runtime.
	ErrChildTaskNotFound = errors.New("child task not found")
	// ErrChildTaskSnapshotUnavailable identifies an internal record that cannot
	// produce a coherent value projection.  It is fail-closed and is distinct
	// from an execution outcome.
	ErrChildTaskSnapshotUnavailable = errors.New("child task snapshot unavailable")
	// ErrChildTaskBindingConflict means that an existing ChildTaskID was
	// presented with a different immutable execution binding.
	ErrChildTaskBindingConflict = errors.New("child task execution binding conflicts with existing task")
	// ErrChildTaskAlreadySubmitted means a ChildTaskID already has a submitted
	// or terminal record, so it cannot be reserved or submitted again.
	ErrChildTaskAlreadySubmitted = errors.New("child task has already been submitted")
	// ErrChildTaskNonPristineCreated means a caller attempted to present a
	// lifecycle-bearing CREATED value whose execution-owned fields are already
	// populated.  The Child Runtime, rather than a caller, constructs the
	// canonical CREATED record.
	ErrChildTaskNonPristineCreated = errors.New("child task CREATED value is not pristine")
	// ErrChildTaskRuntimeUnavailable identifies a nil Child Runtime receiver.
	ErrChildTaskRuntimeUnavailable = errors.New("child task runtime is unavailable")
	// ErrChildTaskCancelledBeforeExecution is the bounded local cancellation
	// outcome.  It never implies that a Kernel execution occurred.
	ErrChildTaskCancelledBeforeExecution = errors.New("child task cancelled before execution")
	// ErrChildTaskExecutionRejected identifies a USEO/Kernel rejection
	// projected at the Child Task boundary.
	ErrChildTaskExecutionRejected = errors.New("child task execution rejected")
	// ErrChildTaskExecutionFailed identifies a definitive failed execution.
	ErrChildTaskExecutionFailed = errors.New("child task execution failed")
	// ErrChildTaskExecutionUnknown identifies unresolved execution knowledge.
	ErrChildTaskExecutionUnknown = errors.New("child task execution is unknown")
	// ErrInvalidExecutionProjection means the narrow USEO port returned no
	// usable state.  The runtime fails closed as UNKNOWN.
	ErrInvalidExecutionProjection = errors.New("invalid child task execution projection")
	// ErrExecutionWorkPortUnavailable identifies a missing USEO port.
	ErrExecutionWorkPortUnavailable = errors.New("child task execution work port is unavailable")
	// ErrInvocationIdentityGeneration identifies an inability to create the
	// one Kernel invocation identity required by the frozen USEO contract.
	ErrInvocationIdentityGeneration = errors.New("child task invocation identity generation failed")
)

// TaskExecutionError is the small task-level error surface.  ChildTaskID is
// the authoritative correlation at this boundary; Cause retains the original
// USEO/Kernel diagnostic without rewriting its lower-level RequestID or
// WorkItemID fields.
type TaskExecutionError struct {
	Kind        string
	ChildTaskID ChildTaskID
	Cause       error
}

func (err *TaskExecutionError) Error() string {
	if err == nil {
		return ""
	}
	message := err.Kind
	if err.ChildTaskID != "" {
		message = fmt.Sprintf("%s (child task %s)", message, err.ChildTaskID)
	}
	if err.Cause != nil {
		message = fmt.Sprintf("%s: %v", message, err.Cause)
	}
	return message
}

// Unwrap preserves errors.Is/errors.As access to the local sentinel and the
// lower-level diagnostic cause.
func (err *TaskExecutionError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

// ChildTaskSpec is the immutable input used to create a Child Task.  RootTaskRef
// is optional opaque lineage; the other fields describe the one execution
// binding carried to USEO.
type ChildTaskSpec struct {
	ChildTaskID        ChildTaskID
	RootTaskRef        string
	ExecutionContextID kernel.ExecutionContextID
	CapabilityHandleID kernel.CapabilityHandleID
	CallerIdentity     string
	Scope              string
	Operation          string
	Payload            []byte
}

// Clone returns an independent lifecycle-free specification value.
func (spec ChildTaskSpec) Clone() ChildTaskSpec {
	spec.Payload = append([]byte(nil), spec.Payload...)
	return spec
}

// Validate checks the immutable binding shape without constructing or
// publishing a Child Runtime record.
func (spec ChildTaskSpec) Validate() error {
	_, err := New(spec)
	return err
}

// ChildTask is one bounded User-Space work unit.  State and execution
// projection are owned by Runtime; the projection itself remains the USEO
// snapshot and never becomes Kernel authority.
type ChildTask struct {
	ChildTaskID        ChildTaskID
	RootTaskRef        string
	ExecutionContextID kernel.ExecutionContextID
	CapabilityHandleID kernel.CapabilityHandleID
	CallerIdentity     string
	Scope              string
	Operation          string
	Payload            []byte

	State               ChildTaskState
	ExecutionWorkItemID string
	RequestID           string
	InvocationID        kernel.InvocationID
	ExecutionProjection userspace.ExecutionProjection
	Failure             error
}

// New creates a Child Task in the single pre-submission CREATED state.
func New(spec ChildTaskSpec) (ChildTask, error) {
	task := ChildTask{
		ChildTaskID:        spec.ChildTaskID,
		RootTaskRef:        spec.RootTaskRef,
		ExecutionContextID: spec.ExecutionContextID,
		CapabilityHandleID: spec.CapabilityHandleID,
		CallerIdentity:     spec.CallerIdentity,
		Scope:              spec.Scope,
		Operation:          spec.Operation,
		Payload:            append([]byte(nil), spec.Payload...),
		State:              ChildTaskStateCreated,
	}
	if err := task.Validate(); err != nil {
		return ChildTask{}, err
	}
	return task, nil
}

// NewChildTask is the descriptive constructor name for New.
func NewChildTask(spec ChildTaskSpec) (ChildTask, error) { return New(spec) }

// ValidatePristineCreated enforces the canonical pre-submission invariant.
// A CREATED Child has its immutable binding only; all execution-derived
// fields are zero and no failure is attached.  Runtime is the sole authority
// that can construct and publish such a value through ReserveCreated.
func (task ChildTask) ValidatePristineCreated() error {
	if task.State != ChildTaskStateCreated {
		return fmt.Errorf("%w: state is %q", ErrChildTaskNonPristineCreated, task.State)
	}
	if task.ExecutionWorkItemID != "" || task.RequestID != "" || !task.InvocationID.IsZero() {
		return fmt.Errorf("%w: execution identity is populated", ErrChildTaskNonPristineCreated)
	}
	if !reflect.DeepEqual(task.ExecutionProjection, userspace.ExecutionProjection{}) {
		return fmt.Errorf("%w: execution projection is populated", ErrChildTaskNonPristineCreated)
	}
	if task.Failure != nil {
		return fmt.Errorf("%w: failure is populated", ErrChildTaskNonPristineCreated)
	}
	return nil
}

// IsPristineCreated reports whether the Child value satisfies the canonical
// pre-submission invariant.
func (task ChildTask) IsPristineCreated() bool { return task.ValidatePristineCreated() == nil }

// Validate checks the Child Task's local shape and accepted state vocabulary.
// It does not perform authorization, resource selection, persistence, or
// Root Task lookup.
func (task ChildTask) Validate() error {
	if err := task.ChildTaskID.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidChildTask, err)
	}
	if err := validateOptionalOpaque(task.RootTaskRef, "root task reference"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidChildTask, err)
	}
	if err := task.ExecutionContextID.Validate(); err != nil {
		return fmt.Errorf("%w: execution context: %v", ErrInvalidChildTask, err)
	}
	if err := task.CapabilityHandleID.Validate(); err != nil {
		return fmt.Errorf("%w: capability handle: %v", ErrInvalidChildTask, err)
	}
	if err := validateText(task.CallerIdentity, "caller identity"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidChildTask, err)
	}
	if task.Scope != "" {
		if err := validateText(task.Scope, "scope"); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidChildTask, err)
		}
	}
	if err := validateTextRequired(task.Operation, "operation"); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidChildTask, err)
	}
	switch task.State {
	case "", ChildTaskStateCreated, ChildTaskStateExecuting, ChildTaskStateSucceeded, ChildTaskStateFailed, ChildTaskStateRejected, ChildTaskStateUnknown, ChildTaskStateCancelledBeforeExecution:
	default:
		return fmt.Errorf("%w: unsupported child task state %q", ErrInvalidChildTask, task.State)
	}
	return nil
}

// Clone returns an independent Child Task value snapshot.
func (task ChildTask) Clone() ChildTask {
	task.Payload = append([]byte(nil), task.Payload...)
	task.ExecutionProjection = task.ExecutionProjection.Clone()
	task.Failure = cloneError(task.Failure)
	return task
}

// ChildTaskSnapshot is the read-only value projection exposed by Runtime.
// It contains only the identity, stored lineage, lifecycle, invocation
// correlation, execution projection, and cloned task-level diagnostic needed
// by a future Root Task consumer.  It never exposes taskRecord, its lock,
// completion channel, binding, payload, or mutable record storage.
type ChildTaskSnapshot struct {
	ChildTaskID         ChildTaskID
	RootTaskRef         string
	State               ChildTaskState
	InvocationID        kernel.InvocationID
	ExecutionProjection userspace.ExecutionProjection
	Failure             error
}

// Clone returns an independent snapshot value.  Nested execution and error
// values use the existing accepted clone semantics.
func (snapshot ChildTaskSnapshot) Clone() ChildTaskSnapshot {
	snapshot.ExecutionProjection = snapshot.ExecutionProjection.Clone()
	snapshot.Failure = cloneError(snapshot.Failure)
	return snapshot
}

func childTaskSnapshotFromTask(task ChildTask) (ChildTaskSnapshot, error) {
	if err := task.ChildTaskID.Validate(); err != nil {
		return ChildTaskSnapshot{}, fmt.Errorf("%w: invalid child task id: %v", ErrChildTaskSnapshotUnavailable, err)
	}
	switch task.State {
	case ChildTaskStateCreated, ChildTaskStateExecuting, ChildTaskStateSucceeded, ChildTaskStateFailed, ChildTaskStateRejected, ChildTaskStateUnknown, ChildTaskStateCancelledBeforeExecution:
	default:
		return ChildTaskSnapshot{}, fmt.Errorf("%w: unsupported child task state %q", ErrChildTaskSnapshotUnavailable, task.State)
	}
	if task.State == "" {
		return ChildTaskSnapshot{}, fmt.Errorf("%w: child task state is unpublished", ErrChildTaskSnapshotUnavailable)
	}
	return ChildTaskSnapshot{
		ChildTaskID:         task.ChildTaskID,
		RootTaskRef:         task.RootTaskRef,
		State:               task.State,
		InvocationID:        task.InvocationID,
		ExecutionProjection: task.ExecutionProjection.Clone(),
		Failure:             cloneError(task.Failure),
	}, nil
}

// ToExecutionWorkItem maps the stable Child Task binding into one USEO work
// item.  The generated correlation IDs are supplied by Runtime.  An empty
// RootTaskRef gets a deterministic opaque fallback solely because the frozen
// USEO work-item contract requires a non-empty root reference; no Root Task
// lifecycle or authority is created by that fallback.
func (task ChildTask) ToExecutionWorkItem(workItemID, requestID string, invocationID kernel.InvocationID) userspace.ExecutionWorkItem {
	rootTaskRef := task.RootTaskRef
	if rootTaskRef == "" {
		rootTaskRef = task.ChildTaskID.String()
	}
	return userspace.ExecutionWorkItem{
		WorkItemID:         workItemID,
		RootTaskRef:        rootTaskRef,
		ChildTaskRef:       task.ChildTaskID.String(),
		RequestID:          requestID,
		InvocationID:       invocationID,
		ExecutionContextID: task.ExecutionContextID,
		CapabilityHandleID: task.CapabilityHandleID,
		CallerIdentity:     task.CallerIdentity,
		Scope:              task.Scope,
		Operation:          task.Operation,
		Payload:            append([]byte(nil), task.Payload...),
	}
}

type childTaskBinding struct {
	RootTaskRef        string
	ExecutionContextID kernel.ExecutionContextID
	CapabilityHandleID kernel.CapabilityHandleID
	CallerIdentity     string
	Scope              string
	Operation          string
	Payload            []byte
}

func newChildTaskBinding(task ChildTask) childTaskBinding {
	return childTaskBinding{
		RootTaskRef:        task.RootTaskRef,
		ExecutionContextID: task.ExecutionContextID,
		CapabilityHandleID: task.CapabilityHandleID,
		CallerIdentity:     task.CallerIdentity,
		Scope:              task.Scope,
		Operation:          task.Operation,
		Payload:            append([]byte(nil), task.Payload...),
	}
}

func (binding childTaskBinding) equal(other childTaskBinding) bool {
	return binding.RootTaskRef == other.RootTaskRef &&
		binding.ExecutionContextID == other.ExecutionContextID &&
		binding.CapabilityHandleID == other.CapabilityHandleID &&
		binding.CallerIdentity == other.CallerIdentity &&
		binding.Scope == other.Scope &&
		binding.Operation == other.Operation &&
		bytes.Equal(binding.Payload, other.Payload)
}

func validateOptionalOpaque(value, name string) error {
	if value == "" {
		return nil
	}
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s is invalid", name)
	}
	return nil
}

func validateText(value, name string) error {
	if !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') || (value != "" && strings.TrimSpace(value) == "") {
		return fmt.Errorf("%s is invalid", name)
	}
	return nil
}

func validateTextRequired(value, name string) error {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s is required", name)
	}
	return nil
}

func cloneError(err error) error {
	if err == nil {
		return nil
	}
	// Inspect only the current concrete node before traversing any child
	// errors.  A recursive errors.As at this point could find a nested
	// diagnostic inside an errors.Join and replace the whole semantic tree.
	if taskErr, ok := err.(*TaskExecutionError); ok && taskErr != nil {
		clone := *taskErr
		clone.Cause = cloneError(taskErr.Cause)
		return &clone
	}
	// errors.Join exposes its children through Unwrap() []error.  Rebuild that
	// structure before considering any individual typed child so both task
	// sentinels and lower-level diagnostics remain discoverable by errors.Is /
	// errors.As after replay.
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		members := joined.Unwrap()
		clones := make([]error, 0, len(members))
		for _, member := range members {
			clones = append(clones, cloneError(member))
		}
		return errors.Join(clones...)
	}
	if executionErr, ok := err.(*userspace.ExecutionError); ok && executionErr != nil {
		clone := *executionErr
		clone.Cause = cloneError(executionErr.Cause)
		return &clone
	}
	if logicalErr, ok := err.(*kernel.LogicalErrorInfo); ok && logicalErr != nil {
		return logicalErr.Clone()
	}
	// Unknown wrapper types are intentionally retained.  The current contract
	// does not attempt to serialize arbitrary Go error graphs.
	return err
}
