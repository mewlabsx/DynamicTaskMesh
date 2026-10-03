// Package taskcreation owns the narrow User-Space materialization boundary
// above the Root and Child Task runtimes.
//
// It accepts already-decided value requests and coordinates the existing
// lifecycle owners. It does not derive work, execute a Child, close/evaluate
// a Root, or keep a second authoritative registry.
package taskcreation

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"

	"dtm/internal/userspace"
	"dtm/internal/userspace/roottask"
	"dtm/internal/userspace/taskruntime"
)

// RootTaskPort is the minimal Root Runtime authority needed for
// materialization. The boundary never writes Root records directly.
type RootTaskPort interface {
	Create(roottask.RootTaskSpec) (roottask.RootTaskID, error)
	AddChild(roottask.RootTaskID, taskruntime.ChildTaskID) error
}

// RootRuntimePort is a descriptive alias for RootTaskPort.
type RootRuntimePort = RootTaskPort

// ChildTaskCreationPort is the minimal Child Runtime authority needed before
// Root membership registration. ReserveCreated accepts only a lifecycle-free
// specification; the Child Runtime constructs the authoritative CREATED
// value. The snapshot method is the existing read-only publication surface.
type ChildTaskCreationPort interface {
	ReserveCreated(taskruntime.ChildTaskSpec) (taskruntime.ChildTask, error)
	GetChildTaskSnapshot(taskruntime.ChildTaskID) (taskruntime.ChildTaskSnapshot, error)
}

// ChildRuntimePort is a descriptive alias for ChildTaskCreationPort.
type ChildRuntimePort = ChildTaskCreationPort

var (
	// ErrInvalidCreationRequest identifies malformed local request input.
	ErrInvalidCreationRequest = errors.New("invalid task creation request")
	// ErrRootRuntimeUnavailable identifies a missing Root Runtime port.
	ErrRootRuntimeUnavailable = errors.New("root task runtime is unavailable")
	// ErrChildRuntimeUnavailable identifies a missing Child Runtime port.
	ErrChildRuntimeUnavailable = errors.New("child task runtime is unavailable")
	// ErrRootCreationFailed preserves a Root Runtime creation failure.
	ErrRootCreationFailed = errors.New("root task creation failed")
	// ErrChildReservationFailed preserves a Child Runtime reservation failure.
	ErrChildReservationFailed = errors.New("child task reservation failed")
	// ErrChildSnapshotUnavailable identifies a failed or incoherent Child
	// snapshot read at the registration boundary.
	ErrChildSnapshotUnavailable = errors.New("child task snapshot unavailable for registration")
	// ErrChildSnapshotIdentityMismatch identifies a snapshot for a different
	// Child identity than the registration request.
	ErrChildSnapshotIdentityMismatch = errors.New("child task snapshot identity mismatch")
	// ErrChildLineageMismatch identifies a snapshot bound to another Root.
	ErrChildLineageMismatch = errors.New("child task lineage does not match root registration")
	// ErrChildNotCreated identifies a registration attempt after Child
	// submission or another terminal local outcome.
	ErrChildNotCreated = errors.New("child task is not in CREATED state")
	// ErrInvalidReservedChild identifies a Child Runtime port that returned a
	// value outside the canonical reservation contract.
	ErrInvalidReservedChild = errors.New("invalid reserved child task")
	// ErrRootRegistrationFailed preserves a Root Runtime membership failure.
	ErrRootRegistrationFailed = errors.New("root child registration failed")
)

// CreationError adds Root/Child correlation while retaining the boundary
// sentinel and the authoritative lower-layer error through errors.Is and
// errors.As.
type CreationError struct {
	Kind        error
	RootTaskID  roottask.RootTaskID
	ChildTaskID taskruntime.ChildTaskID
	Cause       error
}

func (err *CreationError) Error() string {
	if err == nil {
		return ""
	}
	message := "task creation error"
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

// Unwrap preserves both the boundary classification and the lower-layer
// diagnostic tree.
func (err *CreationError) Unwrap() error {
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

func newCreationError(kind error, rootID roottask.RootTaskID, childID taskruntime.ChildTaskID, cause error) error {
	return &CreationError{Kind: kind, RootTaskID: rootID, ChildTaskID: childID, Cause: cause}
}

// RootCreationRequest is an already-decided Root materialization request.
// Root Runtime remains responsible for validating and owning the resulting
// Root lifecycle.
type RootCreationRequest struct {
	RootTaskID     roottask.RootTaskID
	GoalDescriptor roottask.GoalDescriptor
	// GoalRef is a compact compatibility spelling. GoalDescriptor.Ref takes
	// precedence when both values are supplied, matching RootTaskSpec.
	GoalRef          string
	ClosurePredicate roottask.ClosurePredicate
	// Predicate is a compatibility spelling for ClosurePredicate. The
	// explicit ClosurePredicate takes precedence when both are supplied.
	Predicate roottask.ClosurePredicate
}

// CreateRootRequest is a compatibility spelling for RootCreationRequest.
type CreateRootRequest = RootCreationRequest

// ChildCreationRequest is an already-decided Child materialization request.
// RootTaskID supplies the authoritative lineage; Spec contains only immutable
// Child identity and execution-binding fields.
type ChildCreationRequest struct {
	RootTaskID roottask.RootTaskID
	Spec       taskruntime.ChildTaskSpec
}

// CreateChildRequest is a compatibility spelling for ChildCreationRequest.
type CreateChildRequest = ChildCreationRequest

// Boundary coordinates Root and Child Runtime authorities without owning
// lifecycle, membership, execution, observation, retry, or persistence state.
type Boundary struct {
	roots RootTaskPort
	child ChildTaskCreationPort
}

// TaskCreationBoundary is a descriptive alias for Boundary.
type TaskCreationBoundary = Boundary

// New creates a stateless task materialization boundary.
func New(root RootTaskPort, child ChildTaskCreationPort) *Boundary {
	return &Boundary{roots: root, child: child}
}

// NewBoundary is a descriptive constructor alias.
func NewBoundary(root RootTaskPort, child ChildTaskCreationPort) *Boundary {
	return New(root, child)
}

// NewTaskCreationBoundary is a descriptive constructor alias.
func NewTaskCreationBoundary(root RootTaskPort, child ChildTaskCreationPort) *Boundary {
	return New(root, child)
}

// ValidateRootCreationRequest performs the boundary's local request checks
// without calling a Runtime.
func ValidateRootCreationRequest(request RootCreationRequest) error {
	if err := request.RootTaskID.Validate(); err != nil {
		return fmt.Errorf("%w: root task id: %v", ErrInvalidCreationRequest, err)
	}
	if request.GoalDescriptor.Ref == "" {
		request.GoalDescriptor.Ref = request.GoalRef
	}
	if err := request.GoalDescriptor.Validate(); err != nil {
		return fmt.Errorf("%w: goal descriptor: %v", ErrInvalidCreationRequest, err)
	}
	if isNil(request.ClosurePredicate) && !isNil(request.Predicate) {
		request.ClosurePredicate = request.Predicate
	}
	if isNil(request.ClosurePredicate) {
		return fmt.Errorf("%w: closure predicate is required", ErrInvalidCreationRequest)
	}
	return nil
}

// ValidateChildCreationRequest performs local ID, lineage, and immutable
// binding validation without calling a Runtime.
func ValidateChildCreationRequest(request ChildCreationRequest) error {
	if err := request.RootTaskID.Validate(); err != nil {
		return fmt.Errorf("%w: root task id: %v", ErrInvalidCreationRequest, err)
	}
	spec := request.Spec
	if spec.RootTaskRef == "" {
		spec.RootTaskRef = request.RootTaskID.String()
	}
	if spec.RootTaskRef != request.RootTaskID.String() {
		return newCreationError(ErrChildLineageMismatch, request.RootTaskID, spec.ChildTaskID, nil)
	}
	if _, err := taskruntime.New(spec); err != nil {
		return fmt.Errorf("%w: child task: %v", ErrInvalidCreationRequest, err)
	}
	return nil
}

func isNil(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

func childBindingMatchesSpec(task taskruntime.ChildTask, spec taskruntime.ChildTaskSpec) bool {
	return task.ChildTaskID == spec.ChildTaskID &&
		task.RootTaskRef == spec.RootTaskRef &&
		task.ExecutionContextID == spec.ExecutionContextID &&
		task.CapabilityHandleID == spec.CapabilityHandleID &&
		task.CallerIdentity == spec.CallerIdentity &&
		task.Scope == spec.Scope &&
		task.Operation == spec.Operation &&
		bytes.Equal(task.Payload, spec.Payload)
}

func pristineSnapshot(snapshot taskruntime.ChildTaskSnapshot) bool {
	return snapshot.InvocationID.IsZero() &&
		reflect.DeepEqual(snapshot.ExecutionProjection, userspace.ExecutionProjection{}) &&
		snapshot.Failure == nil
}
