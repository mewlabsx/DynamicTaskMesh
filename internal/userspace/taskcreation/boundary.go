package taskcreation

import (
	"errors"
	"fmt"

	"dtm/internal/userspace/roottask"
	"dtm/internal/userspace/taskruntime"
)

var (
	_ RootTaskPort          = (*roottask.Runtime)(nil)
	_ ChildTaskCreationPort = (*taskruntime.Runtime)(nil)
)

// CreateRoot validates and delegates one Root creation request. It does not
// create a Child, register membership, submit execution, or evaluate closure.
func (boundary *Boundary) CreateRoot(request RootCreationRequest) (roottask.RootTaskID, error) {
	if err := ValidateRootCreationRequest(request); err != nil {
		return "", err
	}
	if boundary == nil || isNil(boundary.roots) {
		return "", newCreationError(ErrRootRuntimeUnavailable, request.RootTaskID, "", nil)
	}

	goal := request.GoalDescriptor
	if goal.Ref == "" {
		goal.Ref = request.GoalRef
	}
	predicate := request.ClosurePredicate
	if isNil(predicate) {
		predicate = request.Predicate
	}
	id, err := boundary.roots.Create(roottask.RootTaskSpec{
		RootTaskID:       request.RootTaskID,
		GoalDescriptor:   goal,
		ClosurePredicate: predicate,
	})
	if err != nil {
		return "", newCreationError(ErrRootCreationFailed, request.RootTaskID, "", err)
	}
	if id != request.RootTaskID {
		return "", newCreationError(ErrRootCreationFailed, request.RootTaskID, "", fmt.Errorf("%w: Root Runtime returned %q", ErrInvalidCreationRequest, id))
	}
	return id, nil
}

// CreateRootTask is a descriptive alias for CreateRoot.
func (boundary *Boundary) CreateRootTask(request RootCreationRequest) (roottask.RootTaskID, error) {
	return boundary.CreateRoot(request)
}

// CreateChild validates an already-decided Child binding, derives the exact
// RootTaskRef, and asks Child Runtime to install the authoritative pristine
// CREATED record. It does not register membership or execute the Child.
func (boundary *Boundary) CreateChild(request ChildCreationRequest) (taskruntime.ChildTask, error) {
	if err := ValidateChildCreationRequest(request); err != nil {
		return taskruntime.ChildTask{}, err
	}
	if boundary == nil || isNil(boundary.child) {
		return taskruntime.ChildTask{}, newCreationError(ErrChildRuntimeUnavailable, request.RootTaskID, request.Spec.ChildTaskID, nil)
	}

	spec := request.Spec
	if spec.RootTaskRef == "" {
		spec.RootTaskRef = request.RootTaskID.String()
	}
	reserved, err := boundary.child.ReserveCreated(spec)
	if err != nil {
		return reserved.Clone(), newCreationError(ErrChildReservationFailed, request.RootTaskID, spec.ChildTaskID, err)
	}
	if !childBindingMatchesSpec(reserved, spec) || reserved.RootTaskRef != request.RootTaskID.String() {
		return taskruntime.ChildTask{}, newCreationError(ErrInvalidReservedChild, request.RootTaskID, spec.ChildTaskID, errors.New("Child Runtime returned a different immutable binding"))
	}
	if err := reserved.ValidatePristineCreated(); err != nil {
		return taskruntime.ChildTask{}, newCreationError(ErrInvalidReservedChild, request.RootTaskID, spec.ChildTaskID, err)
	}
	// The returned value is the Child Runtime's authoritative value, never a
	// local copy constructed by this boundary.
	return reserved.Clone(), nil
}

// CreateChildTask is a descriptive alias for CreateChild.
func (boundary *Boundary) CreateChildTask(request ChildCreationRequest) (taskruntime.ChildTask, error) {
	return boundary.CreateChild(request)
}

// RegisterChild reads the authoritative Child Runtime snapshot, verifies
// exact identity, lineage, and pristine CREATED state, then delegates only
// the Child identity to Root Runtime. It never accepts caller-authored Child
// lifecycle or projection data and never changes Child state.
func (boundary *Boundary) RegisterChild(rootID roottask.RootTaskID, childID taskruntime.ChildTaskID) error {
	if err := rootID.Validate(); err != nil {
		return newCreationError(ErrInvalidCreationRequest, rootID, childID, err)
	}
	if err := childID.Validate(); err != nil {
		return newCreationError(ErrInvalidCreationRequest, rootID, childID, err)
	}
	if boundary == nil || isNil(boundary.child) {
		return newCreationError(ErrChildRuntimeUnavailable, rootID, childID, nil)
	}
	if isNil(boundary.roots) {
		return newCreationError(ErrRootRuntimeUnavailable, rootID, childID, nil)
	}

	snapshot, err := boundary.child.GetChildTaskSnapshot(childID)
	if err != nil {
		return newCreationError(ErrChildSnapshotUnavailable, rootID, childID, err)
	}
	if snapshot.ChildTaskID != childID {
		return newCreationError(ErrChildSnapshotIdentityMismatch, rootID, childID, fmt.Errorf("snapshot identity is %q", snapshot.ChildTaskID))
	}
	if snapshot.RootTaskRef != rootID.String() {
		return newCreationError(ErrChildLineageMismatch, rootID, childID, nil)
	}
	if snapshot.State != taskruntime.ChildTaskStateCreated {
		return newCreationError(ErrChildNotCreated, rootID, childID, fmt.Errorf("authoritative state is %s", snapshot.State))
	}
	if !pristineSnapshot(snapshot) {
		return newCreationError(ErrInvalidReservedChild, rootID, childID, errors.New("CREATED snapshot contains execution-owned fields"))
	}
	if err := boundary.roots.AddChild(rootID, childID); err != nil {
		return newCreationError(ErrRootRegistrationFailed, rootID, childID, err)
	}
	return nil
}

// RegisterChildTask is a descriptive alias for RegisterChild.
func (boundary *Boundary) RegisterChildTask(rootID roottask.RootTaskID, childID taskruntime.ChildTaskID) error {
	return boundary.RegisterChild(rootID, childID)
}
