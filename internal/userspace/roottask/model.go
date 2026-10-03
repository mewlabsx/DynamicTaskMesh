// Package roottask owns the bounded User-Space Root Task aggregate.
//
// A Root Task is a goal-level closure mechanism. It owns Root identity,
// finite Child membership, and the Root lifecycle, while Child Task state is
// read only through taskruntime.ChildTaskSnapshot values.
package roottask

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"dtm/internal/userspace/taskruntime"
)

// RootTaskID is the live-process identity of one Root Task. It is deliberately
// distinct from taskruntime.ChildTaskID and has no restart-global meaning.
type RootTaskID string

// String returns the opaque Root Task identity.
func (id RootTaskID) String() string { return string(id) }

// Validate checks the local identity shape only. It does not establish
// authority, persistence, or Child membership.
func (id RootTaskID) Validate() error {
	value := string(id)
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || strings.ContainsRune(value, '\x00') {
		return errors.New("root task id must be non-blank valid UTF-8 text")
	}
	return nil
}

// GoalDescriptor is the smallest opaque goal-level value retained by a Root
// Task and passed to its closure predicate. Root Runtime never interprets the
// descriptor as a prompt, plan, workflow, or business policy.
type GoalDescriptor struct {
	Ref string
}

// GoalRef is a compact spelling for callers that model the descriptor as one
// opaque reference. It aliases string so either `string` or `GoalRef` values
// can populate RootTaskSpec.GoalRef while GoalDescriptor remains canonical.
type GoalRef = string

// Validate checks the descriptor's local value shape.
func (descriptor GoalDescriptor) Validate() error {
	if !utf8.ValidString(descriptor.Ref) || strings.TrimSpace(descriptor.Ref) == "" || strings.ContainsRune(descriptor.Ref, '\x00') {
		return errors.New("goal descriptor reference must be non-blank valid UTF-8 text")
	}
	return nil
}

// MembershipState is the explicit Root child-set lifecycle.
type MembershipState string

const (
	MembershipOpen   MembershipState = "OPEN"
	MembershipClosed MembershipState = "CLOSED"
)

const (
	MembershipStateOpen   = MembershipOpen
	MembershipStateClosed = MembershipClosed
)

// RootTaskState is the goal-level Root lifecycle. There are no retry,
// recovery, replanning, or remapping states in this bounded phase.
type RootTaskState string

const (
	RootTaskStateOpen      RootTaskState = "OPEN"
	RootTaskStateActive    RootTaskState = "ACTIVE"
	RootTaskStateSucceeded RootTaskState = "SUCCEEDED"
	RootTaskStateFailed    RootTaskState = "FAILED"
	RootTaskStateUnknown   RootTaskState = "UNKNOWN"
	RootTaskStateCancelled RootTaskState = "CANCELLED"
)

const (
	RootStateOpen      = RootTaskStateOpen
	RootStateActive    = RootTaskStateActive
	RootStateSucceeded = RootTaskStateSucceeded
	RootStateFailed    = RootTaskStateFailed
	RootStateUnknown   = RootTaskStateUnknown
	RootStateCancelled = RootTaskStateCancelled
)

// Short aliases preserve the compact state vocabulary used by the accepted
// User-Space contract.
const (
	StateOpen      = RootTaskStateOpen
	StateActive    = RootTaskStateActive
	StateSucceeded = RootTaskStateSucceeded
	StateFailed    = RootTaskStateFailed
	StateUnknown   = RootTaskStateUnknown
	StateCancelled = RootTaskStateCancelled
)

// ClosureDecision is the tri-state result accepted from a deterministic
// closure predicate.
type ClosureDecision string

const (
	ClosureDecisionSucceeded ClosureDecision = "SUCCEEDED"
	ClosureDecisionFailed    ClosureDecision = "FAILED"
	ClosureDecisionUnknown   ClosureDecision = "UNKNOWN"
)

// ChildEvidence is an immutable value copied from the authoritative Child
// Task Runtime snapshot surface. The alias intentionally keeps the Child
// snapshot's lower-layer fields without importing Kernel or USEO here.
type ChildEvidence = taskruntime.ChildTaskSnapshot

// RootTask is the read-only value projection supplied to callers and closure
// predicates. Runtime returns independent clones, so mutating its slices or
// nested Child evidence cannot mutate the authoritative Root record.
//
// ChildProjections is ordered according to ChildTaskIDs. Only terminal Child
// snapshots are retained as closure evidence.
type RootTask struct {
	RootTaskID       RootTaskID
	GoalDescriptor   GoalDescriptor
	GoalRef          string
	State            RootTaskState
	MembershipState  MembershipState
	ChildTaskIDs     []taskruntime.ChildTaskID
	ChildProjections []ChildEvidence
	ClosureResult    ClosureDecision
	ClosureEvaluated bool
	Failure          error
}

// RootTaskSnapshot is the descriptive name for the immutable Root value
// projection. RootTask remains the predicate-facing name from the contract.
type RootTaskSnapshot = RootTask

// RootTaskSpec is the explicit creation input. RootTaskID is caller supplied
// and must be valid; Root Runtime does not mint or persist identities.
type RootTaskSpec struct {
	RootTaskID     RootTaskID
	GoalDescriptor GoalDescriptor
	// GoalRef is a compact compatibility spelling for callers that use the
	// contract's GoalRef form. GoalDescriptor.Ref takes precedence when both
	// fields are supplied.
	GoalRef          string
	ClosurePredicate ClosurePredicate
	// Predicate is a descriptive compatibility alias. ClosurePredicate takes
	// precedence when both are supplied.
	Predicate ClosurePredicate
}

// ClosurePredicate is the policy boundary for Root goal closure. It receives
// a cloned Root value and an ordered cloned list of terminal Child evidence.
// Implementations must be deterministic, synchronous, side-effect free, and
// must not call Kernel managers, Providers, or observation paths.
type ClosurePredicate interface {
	Evaluate(RootTaskSnapshot, []ChildEvidence) (ClosureDecision, error)
}

// ClosurePredicateFunc adapts a function to ClosurePredicate.
type ClosurePredicateFunc func(RootTaskSnapshot, []ChildEvidence) (ClosureDecision, error)

// Evaluate invokes the adapted function.
func (predicate ClosurePredicateFunc) Evaluate(root RootTaskSnapshot, evidence []ChildEvidence) (ClosureDecision, error) {
	if predicate == nil {
		return ClosureDecisionUnknown, ErrInvalidClosurePredicate
	}
	return predicate(root, evidence)
}

var (
	// ErrInvalidRootTask identifies malformed Root creation input or an
	// unsupported Root value.
	ErrInvalidRootTask = errors.New("invalid root task")
	// ErrRootTaskNotFound identifies a Root Task not retained by this
	// process-local Runtime.
	ErrRootTaskNotFound = errors.New("root task not found")
	// ErrRootTaskAlreadyExists identifies a duplicate live-process Root ID.
	ErrRootTaskAlreadyExists = errors.New("root task already exists")
	// ErrRootMembershipClosed identifies an attempted mutation after the
	// explicit membership freeze.
	ErrRootMembershipClosed = errors.New("root task membership is closed")
	// ErrRootChildAlreadyRegistered identifies duplicate Child membership.
	ErrRootChildAlreadyRegistered = errors.New("root child task already registered")
	// ErrRootChildNotRegistered identifies a Child that is not in the frozen
	// or currently open membership set.
	ErrRootChildNotRegistered = errors.New("root child task is not registered")
	// ErrRootChildLineageMismatch identifies a snapshot whose RootTaskRef does
	// not exactly match the Root being evaluated.
	ErrRootChildLineageMismatch = errors.New("root child task lineage mismatch")
	// ErrRootChildSnapshotIdentityMismatch identifies a snapshot whose Child
	// identity does not match the requested member.
	ErrRootChildSnapshotIdentityMismatch = errors.New("root child snapshot identity mismatch")
	// ErrRootChildSnapshotUnavailable identifies a failed or unusable snapshot
	// read. No missing Child is silently removed from membership.
	ErrRootChildSnapshotUnavailable = errors.New("root child task snapshot unavailable")
	// ErrRootClosureNotReady identifies a closed Root with at least one
	// registered Child that is not terminal yet.
	ErrRootClosureNotReady = errors.New("root closure is not ready")
	// ErrRootMembershipNotClosed identifies an evaluation requested too early.
	ErrRootMembershipNotClosed = errors.New("root task membership is not closed")
	// ErrRootAlreadyTerminal identifies a forbidden mutation of a terminal
	// Root projection.
	ErrRootAlreadyTerminal = errors.New("root task is already terminal")
	// ErrInvalidClosurePredicate identifies nil or malformed closure policy.
	ErrInvalidClosurePredicate = errors.New("invalid root closure predicate")
	// ErrInvalidClosureDecision identifies a predicate result outside the
	// accepted SUCCEEDED/FAILED/UNKNOWN vocabulary.
	ErrInvalidClosureDecision = errors.New("invalid root closure decision")
	// ErrRootClosurePredicateFailed identifies a closure predicate that could
	// not produce a trustworthy decision. The bounded Root projection becomes
	// UNKNOWN and retains the original diagnostic as its cause.
	ErrRootClosurePredicateFailed = errors.New("root closure predicate failed")
)

// RootTaskError preserves RootTaskID (and, where relevant, ChildTaskID) as
// the authoritative correlation while retaining a typed sentinel and the
// lower-layer cause for errors.Is/errors.As.
type RootTaskError struct {
	Kind        error
	RootTaskID  RootTaskID
	ChildTaskID taskruntime.ChildTaskID
	Cause       error
}

func (err *RootTaskError) Error() string {
	if err == nil {
		return ""
	}
	message := "root task error"
	if err.Kind != nil {
		message = err.Kind.Error()
	}
	if err.RootTaskID != "" {
		message = fmt.Sprintf("%s (root task %s)", message, err.RootTaskID)
	}
	if err.ChildTaskID != "" {
		message = fmt.Sprintf("%s (child task %s)", message, err.ChildTaskID)
	}
	if err.Cause != nil {
		message = fmt.Sprintf("%s: %v", message, err.Cause)
	}
	return message
}

// Unwrap preserves both the Root sentinel and a lower-layer cause.
func (err *RootTaskError) Unwrap() error {
	if err == nil {
		return nil
	}
	if err.Kind == nil {
		return err.Cause
	}
	if err.Cause == nil {
		return err.Kind
	}
	return errors.Join(err.Kind, err.Cause)
}

func newRootError(kind error, rootID RootTaskID, childID taskruntime.ChildTaskID, cause error) error {
	return &RootTaskError{Kind: kind, RootTaskID: rootID, ChildTaskID: childID, Cause: cause}
}

func terminalRootState(state RootTaskState) bool {
	switch state {
	case RootTaskStateSucceeded, RootTaskStateFailed, RootTaskStateUnknown, RootTaskStateCancelled:
		return true
	default:
		return false
	}
}

func terminalChildState(state taskruntime.ChildTaskState) bool {
	switch state {
	case taskruntime.ChildTaskStateSucceeded,
		taskruntime.ChildTaskStateFailed,
		taskruntime.ChildTaskStateRejected,
		taskruntime.ChildTaskStateUnknown,
		taskruntime.ChildTaskStateCancelledBeforeExecution:
		return true
	default:
		return false
	}
}

func cloneEvidence(evidence []ChildEvidence) []ChildEvidence {
	if len(evidence) == 0 {
		return nil
	}
	clones := make([]ChildEvidence, len(evidence))
	for index, snapshot := range evidence {
		clones[index] = snapshot.Clone()
	}
	return clones
}

func cloneRootTask(root RootTask) RootTask {
	root.ChildTaskIDs = append([]taskruntime.ChildTaskID(nil), root.ChildTaskIDs...)
	root.ChildProjections = cloneEvidence(root.ChildProjections)
	root.Failure = cloneRootFailure(root.Failure)
	return root
}

// Clone returns an independent Root Task value projection.
func (root RootTask) Clone() RootTask { return cloneRootTask(root) }

// RootTask currently stores only RootTaskError and predicate diagnostics as
// its own failure surface. Rebuild RootTaskError graphs so snapshots do not
// expose mutable Root-owned error nodes; arbitrary lower-layer errors remain
// opaque values just as they are in ChildTaskSnapshot.
func cloneRootFailure(err error) error {
	if err == nil {
		return nil
	}
	if rootErr, ok := err.(*RootTaskError); ok && rootErr != nil {
		clone := *rootErr
		clone.Cause = cloneRootFailure(rootErr.Cause)
		return &clone
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		members := joined.Unwrap()
		clones := make([]error, 0, len(members))
		for _, member := range members {
			clones = append(clones, cloneRootFailure(member))
		}
		return errors.Join(clones...)
	}
	return err
}
