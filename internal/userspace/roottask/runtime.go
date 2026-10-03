package roottask

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"dtm/internal/userspace/taskruntime"
)

// ChildTaskSnapshotPort is the only dependency of Root Task Runtime. The
// Child Task Runtime satisfies it structurally through its accepted,
// read-only value snapshot method.
type ChildTaskSnapshotPort interface {
	GetChildTaskSnapshot(taskruntime.ChildTaskID) (taskruntime.ChildTaskSnapshot, error)
}

var _ ChildTaskSnapshotPort = (*taskruntime.Runtime)(nil)

// Runtime owns Root identity, explicit finite membership, closure evidence,
// and Root lifecycle. It has no execution path, worker pool, poller,
// persistence, recovery, retry, replan, or Kernel/USEO dependency.
type Runtime struct {
	port ChildTaskSnapshotPort

	mu    sync.Mutex
	roots map[RootTaskID]*rootRecord
}

type rootRecord struct {
	mu sync.Mutex

	root      RootTask
	predicate ClosurePredicate
	evidence  map[taskruntime.ChildTaskID]ChildEvidence

	// evaluating serializes the external snapshot/predicate boundary without
	// holding record.mu across either call. Waiters use the close-only channel
	// and retry the local state read after the owner publishes a result.
	evaluating     bool
	evaluationDone chan struct{}
}

// Manager is a descriptive alias for Runtime; it does not add another
// lifecycle owner.
type Manager = Runtime

// RootTaskRuntime is a descriptive alias for Runtime.
type RootTaskRuntime = Runtime

// NewRuntime creates a process-local in-memory Root Task Runtime. A nil port
// is accepted so a later evaluation fails closed without creating a lower
// execution path.
func NewRuntime(port ChildTaskSnapshotPort) *Runtime {
	return &Runtime{
		port:  port,
		roots: make(map[RootTaskID]*rootRecord),
	}
}

// NewManager is a constructor alias retaining the same one-shot runtime.
func NewManager(port ChildTaskSnapshotPort) *Runtime { return NewRuntime(port) }

// NewRootTaskRuntime is a descriptive constructor alias.
func NewRootTaskRuntime(port ChildTaskSnapshotPort) *Runtime { return NewRuntime(port) }

// New is the compact constructor alias.
func New(port ChildTaskSnapshotPort) *Runtime { return NewRuntime(port) }

// Create registers one Root Task with OPEN membership and OPEN lifecycle. It
// performs no Child creation and no lower-layer call.
func (runtime *Runtime) Create(spec RootTaskSpec) (RootTaskID, error) {
	if runtime == nil {
		return "", newRootError(ErrInvalidRootTask, spec.RootTaskID, "", errors.New("root runtime is nil"))
	}
	if err := spec.RootTaskID.Validate(); err != nil {
		return "", newRootError(ErrInvalidRootTask, spec.RootTaskID, "", err)
	}
	if spec.GoalDescriptor.Ref == "" {
		spec.GoalDescriptor.Ref = spec.GoalRef
	}
	if err := spec.GoalDescriptor.Validate(); err != nil {
		return "", newRootError(ErrInvalidRootTask, spec.RootTaskID, "", err)
	}
	if closurePredicateNil(spec.ClosurePredicate) && !closurePredicateNil(spec.Predicate) {
		spec.ClosurePredicate = spec.Predicate
	}
	if closurePredicateNil(spec.ClosurePredicate) {
		return "", newRootError(ErrInvalidRootTask, spec.RootTaskID, "", ErrInvalidClosurePredicate)
	}

	record := &rootRecord{
		root: RootTask{
			RootTaskID:      spec.RootTaskID,
			GoalDescriptor:  spec.GoalDescriptor,
			GoalRef:         spec.GoalDescriptor.Ref,
			State:           RootTaskStateOpen,
			MembershipState: MembershipOpen,
		},
		predicate: spec.ClosurePredicate,
		evidence:  make(map[taskruntime.ChildTaskID]ChildEvidence),
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.roots == nil {
		runtime.roots = make(map[RootTaskID]*rootRecord)
	}
	if _, exists := runtime.roots[spec.RootTaskID]; exists {
		return "", newRootError(ErrRootTaskAlreadyExists, spec.RootTaskID, "", nil)
	}
	runtime.roots[spec.RootTaskID] = record
	return spec.RootTaskID, nil
}

func closurePredicateNil(predicate ClosurePredicate) bool {
	if predicate == nil {
		return true
	}
	value := reflect.ValueOf(predicate)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// CreateRootTask is the descriptive creation name for Create.
func (runtime *Runtime) CreateRootTask(spec RootTaskSpec) (RootTaskID, error) {
	return runtime.Create(spec)
}

// AddChild registers a validated ChildTaskID while membership is OPEN. The
// ID is only a Root membership reference; authoritative Child lineage is
// checked when a snapshot is fetched.
func (runtime *Runtime) AddChild(rootID RootTaskID, childID taskruntime.ChildTaskID) error {
	if runtime == nil {
		return newRootError(ErrRootTaskNotFound, rootID, childID, errors.New("root runtime is nil"))
	}
	if err := rootID.Validate(); err != nil {
		return newRootError(ErrInvalidRootTask, rootID, childID, err)
	}
	if err := childID.Validate(); err != nil {
		return newRootError(ErrInvalidRootTask, rootID, childID, err)
	}
	record, err := runtime.record(rootID)
	if err != nil {
		return err
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if terminalRootState(record.root.State) {
		return newRootError(ErrRootAlreadyTerminal, rootID, childID, nil)
	}
	if record.root.MembershipState != MembershipOpen {
		return newRootError(ErrRootMembershipClosed, rootID, childID, nil)
	}
	for _, registered := range record.root.ChildTaskIDs {
		if registered == childID {
			return newRootError(ErrRootChildAlreadyRegistered, rootID, childID, nil)
		}
	}
	record.root.ChildTaskIDs = append(record.root.ChildTaskIDs, childID)
	return nil
}

// CloseMembership irreversibly freezes the finite ordered Child set and
// moves an OPEN Root to ACTIVE. Empty membership is permitted; the supplied
// predicate owns the meaning of an empty goal evidence set.
func (runtime *Runtime) CloseMembership(rootID RootTaskID) error {
	if runtime == nil {
		return newRootError(ErrRootTaskNotFound, rootID, "", errors.New("root runtime is nil"))
	}
	if err := rootID.Validate(); err != nil {
		return newRootError(ErrInvalidRootTask, rootID, "", err)
	}
	record, err := runtime.record(rootID)
	if err != nil {
		return err
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if terminalRootState(record.root.State) {
		return newRootError(ErrRootAlreadyTerminal, rootID, "", nil)
	}
	if record.root.MembershipState != MembershipOpen {
		return newRootError(ErrRootMembershipClosed, rootID, "", nil)
	}
	record.root.MembershipState = MembershipClosed
	record.root.State = RootTaskStateActive
	return nil
}

// RecordChildTerminalFact reads one authoritative Child snapshot through the
// narrow port, validates identity/lineage/terminality, and retains a clone.
// It accepts an OPEN or CLOSED membership because a Child may finish before
// the policy has finished adding all explicit members.
func (runtime *Runtime) RecordChildTerminalFact(rootID RootTaskID, childID taskruntime.ChildTaskID) error {
	if runtime == nil {
		return newRootError(ErrRootTaskNotFound, rootID, childID, errors.New("root runtime is nil"))
	}
	if err := rootID.Validate(); err != nil {
		return newRootError(ErrInvalidRootTask, rootID, childID, err)
	}
	if err := childID.Validate(); err != nil {
		return newRootError(ErrInvalidRootTask, rootID, childID, err)
	}
	record, err := runtime.record(rootID)
	if err != nil {
		return err
	}
	record.mu.Lock()
	if terminalRootState(record.root.State) {
		record.mu.Unlock()
		return newRootError(ErrRootAlreadyTerminal, rootID, childID, nil)
	}
	if !containsChild(record.root.ChildTaskIDs, childID) {
		record.mu.Unlock()
		return newRootError(ErrRootChildNotRegistered, rootID, childID, nil)
	}
	if _, alreadyRecorded := record.evidence[childID]; alreadyRecorded {
		record.mu.Unlock()
		return nil
	}
	port := runtime.port
	record.mu.Unlock()

	snapshot, ready, err := fetchChildEvidence(port, rootID, childID)
	if err != nil {
		return err
	}
	if !ready {
		return newRootError(ErrRootClosureNotReady, rootID, childID, nil)
	}

	record.mu.Lock()
	defer record.mu.Unlock()
	if terminalRootState(record.root.State) {
		return newRootError(ErrRootAlreadyTerminal, rootID, childID, nil)
	}
	if !containsChild(record.root.ChildTaskIDs, childID) {
		return newRootError(ErrRootChildNotRegistered, rootID, childID, nil)
	}
	record.evidence[childID] = snapshot.Clone()
	return nil
}

// EvaluateClosure performs one synchronous bounded closure attempt. It never
// waits for a Child, polls, observes Kernel state, or executes work. Concurrent
// callers serialize the external attempt and replay the same terminal value.
func (runtime *Runtime) EvaluateClosure(rootID RootTaskID) (RootTaskSnapshot, error) {
	if runtime == nil {
		return RootTaskSnapshot{}, newRootError(ErrRootTaskNotFound, rootID, "", errors.New("root runtime is nil"))
	}
	if err := rootID.Validate(); err != nil {
		return RootTaskSnapshot{}, newRootError(ErrInvalidRootTask, rootID, "", err)
	}
	record, err := runtime.record(rootID)
	if err != nil {
		return RootTaskSnapshot{}, err
	}

	for {
		record.mu.Lock()
		if terminalRootState(record.root.State) {
			snapshot := record.snapshotLocked()
			evaluationErr := cloneRootFailure(record.root.Failure)
			record.mu.Unlock()
			return snapshot, evaluationErr
		}
		if record.root.MembershipState != MembershipClosed {
			snapshot := record.snapshotLocked()
			record.mu.Unlock()
			return snapshot, newRootError(ErrRootMembershipNotClosed, rootID, "", nil)
		}
		if record.evaluating {
			done := record.evaluationDone
			record.mu.Unlock()
			<-done
			continue
		}

		record.evaluating = true
		record.evaluationDone = make(chan struct{})
		memberIDs := append([]taskruntime.ChildTaskID(nil), record.root.ChildTaskIDs...)
		predicate := record.predicate
		existing := make(map[taskruntime.ChildTaskID]ChildEvidence, len(record.evidence))
		for childID, evidence := range record.evidence {
			existing[childID] = evidence.Clone()
		}
		record.mu.Unlock()

		orderedEvidence, fetchedEvidence, ready, evaluationErr := runtime.collectEvidence(rootID, memberIDs, existing)

		record.mu.Lock()
		if terminalRootState(record.root.State) {
			finishEvaluationLocked(record)
			snapshot := record.snapshotLocked()
			terminalErr := cloneRootFailure(record.root.Failure)
			record.mu.Unlock()
			return snapshot, terminalErr
		}
		for childID, evidence := range fetchedEvidence {
			record.evidence[childID] = evidence.Clone()
		}
		if evaluationErr != nil {
			finishEvaluationLocked(record)
			snapshot := record.snapshotLocked()
			record.mu.Unlock()
			return snapshot, evaluationErr
		}
		if !ready {
			finishEvaluationLocked(record)
			snapshot := record.snapshotLocked()
			record.mu.Unlock()
			return snapshot, newRootError(ErrRootClosureNotReady, rootID, firstNonTerminal(memberIDs, existing, fetchedEvidence), nil)
		}

		rootInput := record.snapshotLocked()
		predicateInput := cloneEvidence(orderedEvidence)
		record.mu.Unlock()

		decision, predicateErr := predicate.Evaluate(rootInput, predicateInput)
		if predicateErr != nil {
			decision = ClosureDecisionUnknown
		}
		if decision != ClosureDecisionSucceeded && decision != ClosureDecisionFailed && decision != ClosureDecisionUnknown {
			predicateErr = errors.Join(ErrInvalidClosureDecision, fmt.Errorf("unsupported decision %q", decision))
			decision = ClosureDecisionUnknown
		}

		record.mu.Lock()
		if terminalRootState(record.root.State) {
			finishEvaluationLocked(record)
			snapshot := record.snapshotLocked()
			terminalErr := cloneRootFailure(record.root.Failure)
			record.mu.Unlock()
			return snapshot, terminalErr
		}
		record.root.ClosureEvaluated = true
		switch decision {
		case ClosureDecisionSucceeded:
			record.root.State = RootTaskStateSucceeded
		case ClosureDecisionFailed:
			record.root.State = RootTaskStateFailed
		case ClosureDecisionUnknown:
			record.root.State = RootTaskStateUnknown
		}
		record.root.ClosureResult = decision
		if predicateErr != nil {
			errKind := ErrRootClosurePredicateFailed
			if errors.Is(predicateErr, ErrInvalidClosureDecision) {
				errKind = ErrInvalidClosureDecision
			}
			record.root.Failure = newRootError(errKind, rootID, "", predicateErr)
		}
		finishEvaluationLocked(record)
		snapshot := record.snapshotLocked()
		resultErr := cloneRootFailure(record.root.Failure)
		record.mu.Unlock()
		return snapshot, resultErr
	}
}

// Evaluate is the compact alias for EvaluateClosure.
func (runtime *Runtime) Evaluate(rootID RootTaskID) (RootTaskSnapshot, error) {
	return runtime.EvaluateClosure(rootID)
}

// CancelRoot projects Root-level cancellation while the Root is OPEN or
// ACTIVE. It never propagates cancellation to Child Runtime, USEO, or Kernel.
func (runtime *Runtime) CancelRoot(rootID RootTaskID) error {
	if runtime == nil {
		return newRootError(ErrRootTaskNotFound, rootID, "", errors.New("root runtime is nil"))
	}
	if err := rootID.Validate(); err != nil {
		return newRootError(ErrInvalidRootTask, rootID, "", err)
	}
	record, err := runtime.record(rootID)
	if err != nil {
		return err
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if terminalRootState(record.root.State) {
		return newRootError(ErrRootAlreadyTerminal, rootID, "", nil)
	}
	record.root.State = RootTaskStateCancelled
	record.root.MembershipState = MembershipClosed
	return nil
}

// GetRootSnapshot returns an independent value projection without waiting for
// evaluation or invoking any lower-layer path.
func (runtime *Runtime) GetRootSnapshot(rootID RootTaskID) (RootTaskSnapshot, error) {
	if runtime == nil {
		return RootTaskSnapshot{}, newRootError(ErrRootTaskNotFound, rootID, "", errors.New("root runtime is nil"))
	}
	if err := rootID.Validate(); err != nil {
		return RootTaskSnapshot{}, newRootError(ErrInvalidRootTask, rootID, "", err)
	}
	record, err := runtime.record(rootID)
	if err != nil {
		return RootTaskSnapshot{}, err
	}
	record.mu.Lock()
	snapshot := record.snapshotLocked()
	record.mu.Unlock()
	return snapshot, nil
}

// GetRootTask is a descriptive alias for GetRootSnapshot.
func (runtime *Runtime) GetRootTask(rootID RootTaskID) (RootTaskSnapshot, error) {
	return runtime.GetRootSnapshot(rootID)
}

func (runtime *Runtime) record(rootID RootTaskID) (*rootRecord, error) {
	runtime.mu.Lock()
	record, exists := runtime.roots[rootID]
	runtime.mu.Unlock()
	if !exists || record == nil {
		return nil, newRootError(ErrRootTaskNotFound, rootID, "", nil)
	}
	return record, nil
}

func (record *rootRecord) snapshotLocked() RootTaskSnapshot {
	snapshot := cloneRootTask(record.root)
	if len(record.root.ChildTaskIDs) == 0 {
		snapshot.ChildProjections = nil
		return snapshot
	}
	snapshot.ChildProjections = make([]ChildEvidence, 0, len(record.root.ChildTaskIDs))
	for _, childID := range record.root.ChildTaskIDs {
		if evidence, ok := record.evidence[childID]; ok {
			snapshot.ChildProjections = append(snapshot.ChildProjections, evidence.Clone())
		}
	}
	return snapshot
}

func (runtime *Runtime) collectEvidence(rootID RootTaskID, memberIDs []taskruntime.ChildTaskID, existing map[taskruntime.ChildTaskID]ChildEvidence) ([]ChildEvidence, map[taskruntime.ChildTaskID]ChildEvidence, bool, error) {
	ordered := make([]ChildEvidence, 0, len(memberIDs))
	fetched := make(map[taskruntime.ChildTaskID]ChildEvidence)
	ready := true
	for _, childID := range memberIDs {
		if evidence, ok := existing[childID]; ok {
			ordered = append(ordered, evidence.Clone())
			continue
		}
		evidence, terminal, err := fetchChildEvidence(runtime.port, rootID, childID)
		if err != nil {
			return ordered, fetched, false, err
		}
		if !terminal {
			ready = false
			continue
		}
		fetched[childID] = evidence.Clone()
		ordered = append(ordered, evidence.Clone())
	}
	if len(ordered) != len(memberIDs) {
		ready = false
	}
	return ordered, fetched, ready, nil
}

func fetchChildEvidence(port ChildTaskSnapshotPort, rootID RootTaskID, childID taskruntime.ChildTaskID) (ChildEvidence, bool, error) {
	if snapshotPortNil(port) {
		return ChildEvidence{}, false, newRootError(ErrRootChildSnapshotUnavailable, rootID, childID, errors.New("child snapshot port is unavailable"))
	}
	snapshot, err := port.GetChildTaskSnapshot(childID)
	if err != nil {
		return ChildEvidence{}, false, newRootError(ErrRootChildSnapshotUnavailable, rootID, childID, err)
	}
	if snapshot.ChildTaskID != childID {
		return ChildEvidence{}, false, newRootError(ErrRootChildSnapshotIdentityMismatch, rootID, childID, fmt.Errorf("returned child task %q", snapshot.ChildTaskID))
	}
	if snapshot.RootTaskRef != string(rootID) {
		return ChildEvidence{}, false, newRootError(ErrRootChildLineageMismatch, rootID, childID, fmt.Errorf("returned root task reference %q", snapshot.RootTaskRef))
	}
	if !terminalChildState(snapshot.State) {
		return snapshot.Clone(), false, nil
	}
	return snapshot.Clone(), true, nil
}

func snapshotPortNil(port ChildTaskSnapshotPort) bool {
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

func containsChild(children []taskruntime.ChildTaskID, requested taskruntime.ChildTaskID) bool {
	for _, childID := range children {
		if childID == requested {
			return true
		}
	}
	return false
}

func firstNonTerminal(memberIDs []taskruntime.ChildTaskID, existing, fetched map[taskruntime.ChildTaskID]ChildEvidence) taskruntime.ChildTaskID {
	for _, childID := range memberIDs {
		if _, ok := existing[childID]; ok {
			continue
		}
		if _, ok := fetched[childID]; ok {
			continue
		}
		return childID
	}
	return ""
}

func finishEvaluationLocked(record *rootRecord) {
	if !record.evaluating {
		return
	}
	record.evaluating = false
	close(record.evaluationDone)
	record.evaluationDone = nil
}
