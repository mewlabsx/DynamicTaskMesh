package userspace

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"dtm/internal/kernel"
)

// WorkState is the local, in-memory projection state owned by USEO.  It is
// deliberately smaller than a Task or runtime lifecycle and has no retry,
// scheduling, or remapping state.
type WorkState string

const (
	WorkStateReady                 WorkState = "READY"
	WorkStateCancelledBeforeSubmit WorkState = "CANCELLED_BEFORE_SUBMIT"
	WorkStateSubmitted             WorkState = "SUBMITTED"
	WorkStateSucceeded             WorkState = "SUCCEEDED"
	WorkStateFailed                WorkState = "FAILED"
	WorkStateUnknown               WorkState = "UNKNOWN"
	WorkStateRejected              WorkState = "REJECTED"
)

// ExecutionWorkItemState is an explicit name for callers that prefer the
// full model name.  It is an alias, not a second state vocabulary.
type ExecutionWorkItemState = WorkState

const (
	ExecutionWorkItemReady                 = WorkStateReady
	ExecutionWorkItemCancelledBeforeSubmit = WorkStateCancelledBeforeSubmit
	ExecutionWorkItemSubmitted             = WorkStateSubmitted
	ExecutionWorkItemSucceeded             = WorkStateSucceeded
	ExecutionWorkItemFailed                = WorkStateFailed
	ExecutionWorkItemUnknown               = WorkStateUnknown
	ExecutionWorkItemRejected              = WorkStateRejected

	StateReady                 = WorkStateReady
	StateCancelledBeforeSubmit = WorkStateCancelledBeforeSubmit
	StateSubmitted             = WorkStateSubmitted
	StateSucceeded             = WorkStateSucceeded
	StateFailed                = WorkStateFailed
	StateUnknown               = WorkStateUnknown
	StateRejected              = WorkStateRejected
)

var (
	ErrInvalidWorkItem          = errors.New("invalid user-space execution work item")
	ErrWorkItemAlreadySubmitted = errors.New("execution work item has already been submitted")
	ErrWorkItemConflict         = errors.New("execution work item conflicts with its existing submission")
	ErrCancelledBeforeSubmit    = errors.New("execution work item cancelled before submit")
	ErrKernelPortUnavailable    = errors.New("user-space execution Kernel port is unavailable")
	ErrInvalidKernelResult      = errors.New("Kernel returned an invalid logical execution result")
)

// ExecutionError is a typed local USEO error.  Kernel errors are returned as
// their original typed values and are not converted into this wrapper.
type ExecutionError struct {
	Kind         string
	WorkItemID   string
	ChildTaskRef string
	Cause        error
}

func (err *ExecutionError) Error() string {
	if err == nil {
		return ""
	}
	if err.Cause == nil {
		return err.Kind
	}
	return fmt.Sprintf("%s: %v", err.Kind, err.Cause)
}

func (err *ExecutionError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

func localError(kind string, item ExecutionWorkItem, cause error) *ExecutionError {
	return &ExecutionError{
		Kind:         kind,
		WorkItemID:   item.WorkItemID,
		ChildTaskRef: item.ChildTaskRef,
		Cause:        cause,
	}
}

// ExecutionWorkItem is one already-decided User-Space execution request.  It
// carries opaque task references for correlation and one concrete Kernel
// request binding; it is not a Root Task or Child Task lifecycle object.
//
// The payload is cloned whenever it crosses this boundary.  All fields are
// value data; USEO never stores a Kernel manager, Resource, Provider, or
// mutable allocation object.
type ExecutionWorkItem struct {
	// WorkItemID is an optional local correlation identity.  ChildTaskRef is
	// the one-child cardinality key and remains opaque to Kernel.
	WorkItemID   string
	RootTaskRef  string
	ChildTaskRef string

	RequestID string

	InvocationID       kernel.InvocationID
	ExecutionContextID kernel.ExecutionContextID
	CapabilityHandleID kernel.CapabilityHandleID
	CallerIdentity     string
	Scope              string
	Operation          string
	Payload            []byte
}

// WorkRequest is the name used by the implementation planning document.
// It intentionally aliases ExecutionWorkItem so there is one model and one
// cardinality rule.
type WorkRequest = ExecutionWorkItem

// Clone returns a defensive value snapshot of the work item.
func (item ExecutionWorkItem) Clone() ExecutionWorkItem {
	item.Payload = append([]byte(nil), item.Payload...)
	return item
}

// Validate checks only local shape and the existing Kernel request structure.
// It does not inspect or recreate Kernel authority rules.
func (item ExecutionWorkItem) Validate() error {
	if err := validateOpaque(item.WorkItemID, "work item identity", false); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidWorkItem, err)
	}
	if err := validateOpaque(item.RootTaskRef, "root task reference", true); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidWorkItem, err)
	}
	if err := validateOpaque(item.ChildTaskRef, "child task reference", true); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidWorkItem, err)
	}
	request := item.LogicalExecutionRequest()
	if err := request.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidWorkItem, err)
	}
	return nil
}

// LogicalExecutionRequest maps the already-selected work item to the frozen
// Kernel request value.  It performs no authority, allocation, or provider
// operation.
func (item ExecutionWorkItem) LogicalExecutionRequest() kernel.LogicalExecutionRequest {
	return kernel.LogicalExecutionRequest{
		InvocationID:       item.InvocationID,
		RequestID:          item.RequestID,
		RootTaskRef:        item.RootTaskRef,
		ChildTaskRef:       item.ChildTaskRef,
		ExecutionContextID: item.ExecutionContextID,
		CapabilityHandleID: item.CapabilityHandleID,
		CallerIdentity:     item.CallerIdentity,
		Scope:              item.Scope,
		Operation:          item.Operation,
		Payload:            append([]byte(nil), item.Payload...),
	}
}

// ToLogicalExecutionRequest is a descriptive alias for the mechanical
// mapping above.
func (item ExecutionWorkItem) ToLogicalExecutionRequest() kernel.LogicalExecutionRequest {
	return item.LogicalExecutionRequest()
}

func validateOpaque(value, name string, required bool) error {
	if !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s is not valid UTF-8 text", name)
	}
	if required && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if !required && value != "" && strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is blank", name)
	}
	return nil
}

// ExecutionProjection is a stable value snapshot returned to User Space.
// The anonymous Kernel result embedding keeps the existing semantic fields
// available without exposing a Kernel implementation object.  Result is a
// named copy for callers that prefer an explicit field.
type ExecutionProjection struct {
	WorkItemID   string
	RootTaskRef  string
	ChildTaskRef string
	InvocationID kernel.InvocationID
	RequestID    string

	State WorkState

	kernel.LogicalExecutionResult
	Result kernel.LogicalExecutionResult

	// InvocationObservation is the optional one-shot read performed after a
	// reconciliation-required result.  It is a value snapshot, not authority.
	InvocationObservation          kernel.LogicalInvocationObservation
	InvocationObservationAvailable bool
	ObserveInvocationCalled        bool
}

// WorkResult is the implementation-plan name for the same projection.
type WorkResult = ExecutionProjection

// Clone returns an independent projection snapshot.  In particular, the
// nested Kernel ErrorInfo and observation payloads are cloned.
func (projection ExecutionProjection) Clone() ExecutionProjection {
	projection.LogicalExecutionResult = projection.LogicalExecutionResult.Clone()
	projection.Result = projection.Result.Clone()
	projection.InvocationObservation = projection.InvocationObservation.Clone()
	return projection
}
