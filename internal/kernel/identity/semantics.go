package identity

import (
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidObjectReference = errors.New("invalid kernel object reference")

// ObjectKind identifies the kind of object addressed by an ObjectReference.
// Intent is a split application/future desired-state vocabulary. The current
// K2 Kernel ObjectReference surface admits neither UserIntent nor KernelIntent;
// ObjectKindIntent remains reserved until a later ABI/lifecycle design
// explicitly admits it. EventRecord is a Kernel object and therefore has a
// first-class reference kind.
type ObjectKind uint8

const (
	ObjectKindUnknown ObjectKind = iota
	ObjectKindResource
	ObjectKindCapabilityDeclaration
	ObjectKindCapabilityInstance
	ObjectKindCapabilityHandle
	ObjectKindExecutionContext
	ObjectKindEventRecord
	ObjectKindIntent
	ObjectKindExecutionAllocation
)

func (kind ObjectKind) String() string {
	switch kind {
	case ObjectKindResource:
		return "Resource"
	case ObjectKindCapabilityDeclaration:
		return "CapabilityDeclaration"
	case ObjectKindCapabilityInstance:
		return "CapabilityInstance"
	case ObjectKindCapabilityHandle:
		return "CapabilityHandle"
	case ObjectKindExecutionContext:
		return "ExecutionContext"
	case ObjectKindEventRecord:
		return "EventRecord"
	case ObjectKindIntent:
		return "Intent"
	case ObjectKindExecutionAllocation:
		return "ExecutionAllocation"
	default:
		return "Unknown"
	}
}

func (kind ObjectKind) Validate() error {
	switch kind {
	case ObjectKindResource,
		ObjectKindCapabilityDeclaration,
		ObjectKindCapabilityInstance,
		ObjectKindCapabilityHandle,
		ObjectKindExecutionContext,
		ObjectKindEventRecord,
		ObjectKindExecutionAllocation:
		return nil
	default:
		return fmt.Errorf("%w: kind %d", ErrInvalidObjectReference, kind)
	}
}

type ObjectReference struct {
	Kind ObjectKind
	ID   string
}

func NewObjectReference(kind ObjectKind, id string) (ObjectReference, error) {
	reference := ObjectReference{Kind: kind, ID: id}
	if err := reference.Validate(); err != nil {
		return ObjectReference{}, err
	}
	return reference, nil
}

func (reference ObjectReference) Validate() error {
	if err := reference.Kind.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidObjectReference, err)
	}
	if strings.TrimSpace(reference.ID) == "" || strings.ContainsRune(reference.ID, '\x00') {
		return fmt.Errorf("%w: object reference %s id is required", ErrInvalidObjectReference, reference.Kind)
	}
	return nil
}

func (reference ObjectReference) String() string {
	return reference.Kind.String() + ":" + reference.ID
}

// LifecycleState answers whether an object exists and what terminal boundary
// it has reached. AvailabilityState is deliberately separate: an existing
// object can be degraded or unavailable without being removed.
type LifecycleState string

const (
	LifecycleCreated    LifecycleState = "CREATED"
	LifecycleActive     LifecycleState = "ACTIVE"
	LifecycleSuspended  LifecycleState = "SUSPENDED"
	LifecycleTerminated LifecycleState = "TERMINATED"
	LifecycleRemoved    LifecycleState = "REMOVED"
)

func (state LifecycleState) Validate() error {
	switch state {
	case LifecycleCreated, LifecycleActive, LifecycleSuspended, LifecycleTerminated, LifecycleRemoved:
		return nil
	default:
		return fmt.Errorf("invalid lifecycle state %q", state)
	}
}

type AvailabilityState string

const (
	AvailabilityAvailable   AvailabilityState = "AVAILABLE"
	AvailabilityDegraded    AvailabilityState = "DEGRADED"
	AvailabilityUnavailable AvailabilityState = "UNAVAILABLE"
)

func (state AvailabilityState) Validate() error {
	switch state {
	case AvailabilityAvailable, AvailabilityDegraded, AvailabilityUnavailable:
		return nil
	default:
		return fmt.Errorf("invalid availability state %q", state)
	}
}

// EvaluationState is a compatibility vocabulary for User Space or a future
// explicitly admitted evaluation boundary. It is not a required KernelIntent
// lifecycle dimension and is not owned by the identity package or any alias.
// K0.5 defines the vocabulary only and performs no evaluation or policy
// decision.
type EvaluationState string

const (
	EvaluationSatisfied   EvaluationState = "SATISFIED"
	EvaluationUnsatisfied EvaluationState = "UNSATISFIED"
	EvaluationUnknown     EvaluationState = "UNKNOWN"
)

func (state EvaluationState) Validate() error {
	switch state {
	case EvaluationSatisfied, EvaluationUnsatisfied, EvaluationUnknown:
		return nil
	default:
		return fmt.Errorf("invalid evaluation state %q", state)
	}
}
