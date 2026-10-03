package roottask

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
	"dtm/internal/userspace/taskruntime"
)

type fakeSnapshotPort struct {
	mu        sync.Mutex
	snapshots map[taskruntime.ChildTaskID]taskruntime.ChildTaskSnapshot
	errors    map[taskruntime.ChildTaskID]error
	calls     atomic.Int32
	entered   chan struct{}
	release   chan struct{}
	once      sync.Once
}

func newFakeSnapshotPort() *fakeSnapshotPort {
	return &fakeSnapshotPort{
		snapshots: make(map[taskruntime.ChildTaskID]taskruntime.ChildTaskSnapshot),
		errors:    make(map[taskruntime.ChildTaskID]error),
	}
}

func (port *fakeSnapshotPort) GetChildTaskSnapshot(id taskruntime.ChildTaskID) (taskruntime.ChildTaskSnapshot, error) {
	port.calls.Add(1)
	if port.entered != nil {
		port.once.Do(func() { close(port.entered) })
	}
	if port.release != nil {
		<-port.release
	}
	port.mu.Lock()
	snapshot, ok := port.snapshots[id]
	err := port.errors[id]
	port.mu.Unlock()
	if err != nil {
		return taskruntime.ChildTaskSnapshot{}, err
	}
	if !ok {
		return taskruntime.ChildTaskSnapshot{}, taskruntime.ErrChildTaskNotFound
	}
	return snapshot.Clone(), nil
}

func (port *fakeSnapshotPort) set(snapshot taskruntime.ChildTaskSnapshot) {
	port.setFor(snapshot.ChildTaskID, snapshot)
}

func (port *fakeSnapshotPort) setFor(requested taskruntime.ChildTaskID, snapshot taskruntime.ChildTaskSnapshot) {
	port.mu.Lock()
	port.snapshots[requested] = snapshot.Clone()
	delete(port.errors, requested)
	port.mu.Unlock()
}

func (port *fakeSnapshotPort) setError(id taskruntime.ChildTaskID, err error) {
	port.mu.Lock()
	port.errors[id] = err
	port.mu.Unlock()
}

func (port *fakeSnapshotPort) callCount() int { return int(port.calls.Load()) }

func rootWithPredicate(t *testing.T, port ChildTaskSnapshotPort, predicate ClosurePredicate) (*Runtime, RootTaskID) {
	t.Helper()
	runtime := NewRuntime(port)
	id, err := runtime.Create(RootTaskSpec{
		RootTaskID:       RootTaskID("root-test"),
		GoalDescriptor:   GoalDescriptor{Ref: "goal-test"},
		ClosurePredicate: predicate,
	})
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	return runtime, id
}

func childSnapshot(id taskruntime.ChildTaskID, root RootTaskID, state taskruntime.ChildTaskState) taskruntime.ChildTaskSnapshot {
	return taskruntime.ChildTaskSnapshot{
		ChildTaskID: id,
		RootTaskRef: string(root),
		State:       state,
	}
}

func TestCreateRootTaskInitializesOnlyRootState(t *testing.T) {
	port := newFakeSnapshotPort()
	predicate := ClosurePredicateFunc(func(root RootTaskSnapshot, evidence []ChildEvidence) (ClosureDecision, error) {
		return ClosureDecisionSucceeded, nil
	})
	runtime, id := rootWithPredicate(t, port, predicate)
	snapshot, err := runtime.GetRootSnapshot(id)
	if err != nil {
		t.Fatalf("GetRootSnapshot() error = %v", err)
	}
	if snapshot.RootTaskID != id || snapshot.GoalDescriptor.Ref != "goal-test" || snapshot.GoalRef != "goal-test" || snapshot.State != RootTaskStateOpen || snapshot.MembershipState != MembershipOpen {
		t.Fatalf("initial snapshot = %#v", snapshot)
	}
	if len(snapshot.ChildTaskIDs) != 0 || len(snapshot.ChildProjections) != 0 || snapshot.ClosureEvaluated {
		t.Fatalf("initial membership/evidence = %#v", snapshot)
	}
	if port.callCount() != 0 {
		t.Fatalf("creation crossed snapshot port: calls = %d", port.callCount())
	}
}

func TestCreateRootTaskValidationAndDuplicate(t *testing.T) {
	port := newFakeSnapshotPort()
	valid := RootTaskSpec{RootTaskID: "root", GoalDescriptor: GoalDescriptor{Ref: "goal"}, ClosurePredicate: AllRequiredChildrenSucceededPredicate()}
	for name, spec := range map[string]RootTaskSpec{
		"empty id":      {GoalDescriptor: valid.GoalDescriptor, ClosurePredicate: valid.ClosurePredicate},
		"nul id":        {RootTaskID: "root\x00", GoalDescriptor: valid.GoalDescriptor, ClosurePredicate: valid.ClosurePredicate},
		"empty goal":    {RootTaskID: "root", ClosurePredicate: valid.ClosurePredicate},
		"nil predicate": {RootTaskID: "root", GoalDescriptor: valid.GoalDescriptor},
	} {
		t.Run(name, func(t *testing.T) {
			runtime := NewRuntime(port)
			if _, err := runtime.Create(spec); !errors.Is(err, ErrInvalidRootTask) {
				t.Fatalf("Create() error = %v, want ErrInvalidRootTask", err)
			}
		})
	}
	runtime := NewRuntime(port)
	if _, err := runtime.Create(valid); err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	if _, err := runtime.Create(valid); !errors.Is(err, ErrRootTaskAlreadyExists) {
		t.Fatalf("duplicate Create() error = %v, want ErrRootTaskAlreadyExists", err)
	}
}

func TestRootMembershipIsFiniteOrderedAndIrreversible(t *testing.T) {
	runtime, id := rootWithPredicate(t, newFakeSnapshotPort(), AllRequiredChildrenSucceededPredicate())
	children := []taskruntime.ChildTaskID{"child-1", "child-2"}
	for _, childID := range children {
		if err := runtime.AddChild(id, childID); err != nil {
			t.Fatalf("AddChild(%s) error = %v", childID, err)
		}
	}
	if err := runtime.AddChild(id, children[0]); !errors.Is(err, ErrRootChildAlreadyRegistered) {
		t.Fatalf("duplicate AddChild() error = %v", err)
	}
	if err := runtime.CloseMembership(id); err != nil {
		t.Fatalf("CloseMembership() error = %v", err)
	}
	if err := runtime.AddChild(id, "child-3"); !errors.Is(err, ErrRootMembershipClosed) {
		t.Fatalf("post-close AddChild() error = %v", err)
	}
	if err := runtime.CloseMembership(id); !errors.Is(err, ErrRootMembershipClosed) {
		t.Fatalf("repeat CloseMembership() error = %v", err)
	}
	snapshot, err := runtime.GetRootSnapshot(id)
	if err != nil {
		t.Fatalf("GetRootSnapshot() error = %v", err)
	}
	if snapshot.State != RootTaskStateActive || snapshot.MembershipState != MembershipClosed {
		t.Fatalf("closed snapshot = %#v", snapshot)
	}
	if len(snapshot.ChildTaskIDs) != 2 || snapshot.ChildTaskIDs[0] != children[0] || snapshot.ChildTaskIDs[1] != children[1] {
		t.Fatalf("ordered membership = %#v", snapshot.ChildTaskIDs)
	}
	if err := runtime.AddChild("missing-root", "child"); !errors.Is(err, ErrRootTaskNotFound) {
		t.Fatalf("unknown root AddChild() error = %v", err)
	}
	if err := runtime.AddChild(id, ""); !errors.Is(err, ErrInvalidRootTask) {
		t.Fatalf("invalid child AddChild() error = %v", err)
	}
}

func TestRootSnapshotLineageAndIdentityValidation(t *testing.T) {
	tests := []struct {
		name      string
		snapshot  taskruntime.ChildTaskSnapshot
		wantError error
	}{
		{name: "lineage", snapshot: childSnapshot("child-1", "root-other", taskruntime.ChildTaskStateSucceeded), wantError: ErrRootChildLineageMismatch},
		{name: "identity", snapshot: childSnapshot("child-2", "root-test", taskruntime.ChildTaskStateSucceeded), wantError: ErrRootChildSnapshotIdentityMismatch},
		{name: "nonterminal", snapshot: childSnapshot("child-1", "root-test", taskruntime.ChildTaskStateExecuting), wantError: ErrRootClosureNotReady},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			port := newFakeSnapshotPort()
			if test.name == "identity" {
				port.setFor("child-1", test.snapshot)
			} else {
				port.set(test.snapshot)
			}
			runtime, id := rootWithPredicate(t, port, AllRequiredChildrenSucceededPredicate())
			if err := runtime.AddChild(id, "child-1"); err != nil {
				t.Fatalf("AddChild() error = %v", err)
			}
			if err := runtime.CloseMembership(id); err != nil {
				t.Fatalf("CloseMembership() error = %v", err)
			}
			if err := runtime.RecordChildTerminalFact(id, "child-1"); !errors.Is(err, test.wantError) {
				t.Fatalf("RecordChildTerminalFact() error = %v, want %v", err, test.wantError)
			}
			snapshot, err := runtime.GetRootSnapshot(id)
			if err != nil {
				t.Fatalf("GetRootSnapshot() error = %v", err)
			}
			if len(snapshot.ChildProjections) != 0 {
				t.Fatalf("invalid evidence retained: %#v", snapshot.ChildProjections)
			}
			_, err = runtime.Evaluate(id)
			if !errors.Is(err, test.wantError) {
				t.Fatalf("Evaluate() error = %v, want %v", err, test.wantError)
			}
		})
	}
}

func TestRootEvaluationRequiresClosedMembershipAndDoesNotWait(t *testing.T) {
	port := newFakeSnapshotPort()
	var predicateCalls atomic.Int32
	predicate := ClosurePredicateFunc(func(root RootTaskSnapshot, evidence []ChildEvidence) (ClosureDecision, error) {
		predicateCalls.Add(1)
		return ClosureDecisionSucceeded, nil
	})
	runtime, id := rootWithPredicate(t, port, predicate)
	if err := runtime.AddChild(id, "child-1"); err != nil {
		t.Fatalf("AddChild() error = %v", err)
	}
	if _, err := runtime.Evaluate(id); !errors.Is(err, ErrRootMembershipNotClosed) {
		t.Fatalf("Evaluate() error = %v, want membership-not-closed", err)
	}
	port.set(childSnapshot("child-1", id, taskruntime.ChildTaskStateExecuting))
	if err := runtime.CloseMembership(id); err != nil {
		t.Fatalf("CloseMembership() error = %v", err)
	}
	start := time.Now()
	snapshot, err := runtime.Evaluate(id)
	if time.Since(start) > 250*time.Millisecond {
		t.Fatal("Evaluate() waited for non-terminal Child evidence")
	}
	if !errors.Is(err, ErrRootClosureNotReady) || snapshot.State != RootTaskStateActive || predicateCalls.Load() != 0 {
		t.Fatalf("not-ready result = %#v, error = %v, predicate calls = %d", snapshot, err, predicateCalls.Load())
	}
}

func TestRootFirstSliceClosurePolicy(t *testing.T) {
	tests := []struct {
		name      string
		states    []taskruntime.ChildTaskState
		wantState RootTaskState
	}{
		{name: "success", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateSucceeded, taskruntime.ChildTaskStateSucceeded}, wantState: RootTaskStateSucceeded},
		{name: "failed", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateSucceeded, taskruntime.ChildTaskStateFailed}, wantState: RootTaskStateFailed},
		{name: "rejected", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateRejected}, wantState: RootTaskStateFailed},
		{name: "unknown", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateSucceeded, taskruntime.ChildTaskStateUnknown}, wantState: RootTaskStateUnknown},
		{name: "cancelled-before-execution", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateCancelledBeforeExecution}, wantState: RootTaskStateFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			port := newFakeSnapshotPort()
			var predicateCalls atomic.Int32
			predicate := ClosurePredicateFunc(func(root RootTaskSnapshot, evidence []ChildEvidence) (ClosureDecision, error) {
				predicateCalls.Add(1)
				return AllRequiredChildrenSucceeded(root, evidence)
			})
			runtime, id := rootWithPredicate(t, port, predicate)
			for index, state := range test.states {
				childID := taskruntime.ChildTaskID(fmt.Sprintf("child-%d", index))
				if err := runtime.AddChild(id, childID); err != nil {
					t.Fatalf("AddChild() error = %v", err)
				}
				port.set(childSnapshot(childID, id, state))
			}
			if err := runtime.CloseMembership(id); err != nil {
				t.Fatalf("CloseMembership() error = %v", err)
			}
			snapshot, err := runtime.Evaluate(id)
			if err != nil {
				t.Fatalf("Evaluate() error = %v", err)
			}
			if snapshot.State != test.wantState || predicateCalls.Load() != 1 || len(snapshot.ChildProjections) != len(test.states) {
				t.Fatalf("result = %#v, predicate calls = %d", snapshot, predicateCalls.Load())
			}
		})
	}
}

func TestRootPredicateErrorProjectsUnknownAndReplays(t *testing.T) {
	port := newFakeSnapshotPort()
	predicateErr := errors.New("predicate diagnostic")
	var predicateCalls atomic.Int32
	predicate := ClosurePredicateFunc(func(root RootTaskSnapshot, evidence []ChildEvidence) (ClosureDecision, error) {
		predicateCalls.Add(1)
		return ClosureDecisionSucceeded, predicateErr
	})
	runtime, id := rootWithPredicate(t, port, predicate)
	if err := runtime.AddChild(id, "child-1"); err != nil {
		t.Fatalf("AddChild() error = %v", err)
	}
	port.set(childSnapshot("child-1", id, taskruntime.ChildTaskStateSucceeded))
	if err := runtime.CloseMembership(id); err != nil {
		t.Fatalf("CloseMembership() error = %v", err)
	}
	first, err := runtime.Evaluate(id)
	if !errors.Is(err, ErrRootClosurePredicateFailed) || !errors.Is(err, predicateErr) || first.State != RootTaskStateUnknown {
		t.Fatalf("first result = %#v, error = %v", first, err)
	}
	second, err := runtime.Evaluate(id)
	if !errors.Is(err, ErrRootClosurePredicateFailed) || second.State != RootTaskStateUnknown || predicateCalls.Load() != 1 {
		t.Fatalf("replay result = %#v, error = %v, calls = %d", second, err, predicateCalls.Load())
	}
}

func TestRootInvalidPredicateDecisionProjectsUnknown(t *testing.T) {
	port := newFakeSnapshotPort()
	runtime, id := rootWithPredicate(t, port, ClosurePredicateFunc(func(root RootTaskSnapshot, evidence []ChildEvidence) (ClosureDecision, error) {
		return ClosureDecision("NOT_A_DECISION"), nil
	}))
	if err := runtime.AddChild(id, "child-1"); err != nil {
		t.Fatalf("AddChild() error = %v", err)
	}
	port.set(childSnapshot("child-1", id, taskruntime.ChildTaskStateSucceeded))
	if err := runtime.CloseMembership(id); err != nil {
		t.Fatalf("CloseMembership() error = %v", err)
	}
	snapshot, err := runtime.Evaluate(id)
	if !errors.Is(err, ErrInvalidClosureDecision) || snapshot.State != RootTaskStateUnknown {
		t.Fatalf("invalid decision result = %#v, error = %v", snapshot, err)
	}
}

func TestRootConcurrentEvaluateInvokesPredicateOnce(t *testing.T) {
	port := newFakeSnapshotPort()
	var predicateCalls atomic.Int32
	entered := make(chan struct{})
	release := make(chan struct{})
	predicate := ClosurePredicateFunc(func(root RootTaskSnapshot, evidence []ChildEvidence) (ClosureDecision, error) {
		if predicateCalls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return ClosureDecisionSucceeded, nil
	})
	runtime, id := rootWithPredicate(t, port, predicate)
	if err := runtime.AddChild(id, "child-1"); err != nil {
		t.Fatalf("AddChild() error = %v", err)
	}
	port.set(childSnapshot("child-1", id, taskruntime.ChildTaskStateSucceeded))
	if err := runtime.CloseMembership(id); err != nil {
		t.Fatalf("CloseMembership() error = %v", err)
	}
	type outcome struct {
		snapshot RootTaskSnapshot
		err      error
	}
	outcomes := make(chan outcome, 12)
	for index := 0; index < cap(outcomes); index++ {
		go func() {
			snapshot, err := runtime.Evaluate(id)
			outcomes <- outcome{snapshot: snapshot, err: err}
		}()
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("predicate was not entered")
	}
	close(release)
	for index := 0; index < cap(outcomes); index++ {
		result := <-outcomes
		if result.err != nil || result.snapshot.State != RootTaskStateSucceeded || result.snapshot.RootTaskID != id {
			t.Fatalf("concurrent result = %#v, error = %v", result.snapshot, result.err)
		}
	}
	if predicateCalls.Load() != 1 {
		t.Fatalf("predicate calls = %d, want 1", predicateCalls.Load())
	}
}

func TestRootConcurrentMembershipFreezeHasOneDefinitiveOutcome(t *testing.T) {
	for iteration := 0; iteration < 50; iteration++ {
		runtime, id := rootWithPredicate(t, newFakeSnapshotPort(), AllRequiredChildrenSucceededPredicate())
		childID := taskruntime.ChildTaskID("race-child")
		start := make(chan struct{})
		var addErr, closeErr error
		var wait sync.WaitGroup
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			addErr = runtime.AddChild(id, childID)
		}()
		go func() {
			defer wait.Done()
			<-start
			closeErr = runtime.CloseMembership(id)
		}()
		close(start)
		wait.Wait()
		if closeErr == nil {
			snapshot, err := runtime.GetRootSnapshot(id)
			if err != nil {
				t.Fatalf("GetRootSnapshot() error = %v", err)
			}
			if snapshot.MembershipState != MembershipClosed {
				t.Fatalf("membership = %s, want CLOSED", snapshot.MembershipState)
			}
			if addErr == nil && (len(snapshot.ChildTaskIDs) != 1 || snapshot.ChildTaskIDs[0] != childID) {
				t.Fatalf("included child missing from frozen membership: %#v", snapshot.ChildTaskIDs)
			}
		} else if addErr != nil {
			t.Fatalf("both concurrent membership operations failed: add=%v close=%v", addErr, closeErr)
		}
	}
}

func TestRootSnapshotFailureFailsClosed(t *testing.T) {
	port := newFakeSnapshotPort()
	port.setError("child-1", taskruntime.ErrChildTaskNotFound)
	runtime, id := rootWithPredicate(t, port, AllRequiredChildrenSucceededPredicate())
	if err := runtime.AddChild(id, "child-1"); err != nil {
		t.Fatalf("AddChild() error = %v", err)
	}
	if err := runtime.CloseMembership(id); err != nil {
		t.Fatalf("CloseMembership() error = %v", err)
	}
	snapshot, err := runtime.Evaluate(id)
	if !errors.Is(err, ErrRootChildSnapshotUnavailable) || !errors.Is(err, taskruntime.ErrChildTaskNotFound) || snapshot.State != RootTaskStateActive {
		t.Fatalf("snapshot failure result = %#v, error = %v", snapshot, err)
	}
	if len(snapshot.ChildTaskIDs) != 1 || len(snapshot.ChildProjections) != 0 {
		t.Fatalf("failed child was dropped: %#v", snapshot)
	}
}

func TestRootSnapshotAndPredicateInputsAreIndependentClones(t *testing.T) {
	port := newFakeSnapshotPort()
	var predicateCalls atomic.Int32
	predicate := ClosurePredicateFunc(func(root RootTaskSnapshot, evidence []ChildEvidence) (ClosureDecision, error) {
		predicateCalls.Add(1)
		root.ChildTaskIDs[0] = "mutated-by-predicate"
		evidence[0].RootTaskRef = "mutated-by-predicate"
		return ClosureDecisionSucceeded, nil
	})
	runtime, id := rootWithPredicate(t, port, predicate)
	if err := runtime.AddChild(id, "child-1"); err != nil {
		t.Fatalf("AddChild() error = %v", err)
	}
	port.set(childSnapshot("child-1", id, taskruntime.ChildTaskStateSucceeded))
	if err := runtime.CloseMembership(id); err != nil {
		t.Fatalf("CloseMembership() error = %v", err)
	}
	if _, err := runtime.Evaluate(id); err != nil {
		t.Fatalf("Evaluate() error = %v", err)
	}
	snapshot, err := runtime.GetRootSnapshot(id)
	if err != nil {
		t.Fatalf("GetRootSnapshot() error = %v", err)
	}
	if snapshot.ChildTaskIDs[0] != "child-1" || snapshot.ChildProjections[0].RootTaskRef != string(id) {
		t.Fatalf("predicate mutated Root state: %#v", snapshot)
	}
	snapshot.ChildTaskIDs[0] = "mutated-by-caller"
	snapshot.ChildProjections[0].RootTaskRef = "mutated-by-caller"
	again, err := runtime.GetRootSnapshot(id)
	if err != nil || again.ChildTaskIDs[0] != "child-1" || again.ChildProjections[0].RootTaskRef != string(id) {
		t.Fatalf("caller mutated Root state: %#v, error=%v", again, err)
	}
}

func TestCancelRootIsRootOnlyAndTerminal(t *testing.T) {
	port := newFakeSnapshotPort()
	var predicateCalls atomic.Int32
	runtime, id := rootWithPredicate(t, port, ClosurePredicateFunc(func(root RootTaskSnapshot, evidence []ChildEvidence) (ClosureDecision, error) {
		predicateCalls.Add(1)
		return ClosureDecisionSucceeded, nil
	}))
	if err := runtime.AddChild(id, "child-1"); err != nil {
		t.Fatalf("AddChild() error = %v", err)
	}
	if err := runtime.CancelRoot(id); err != nil {
		t.Fatalf("CancelRoot() error = %v", err)
	}
	snapshot, err := runtime.GetRootSnapshot(id)
	if err != nil || snapshot.State != RootTaskStateCancelled || snapshot.MembershipState != MembershipClosed {
		t.Fatalf("cancelled snapshot = %#v, error = %v", snapshot, err)
	}
	if err := runtime.AddChild(id, "child-2"); !errors.Is(err, ErrRootAlreadyTerminal) {
		t.Fatalf("post-cancel AddChild() error = %v", err)
	}
	if _, err := runtime.Evaluate(id); err != nil {
		t.Fatalf("terminal Evaluate() error = %v", err)
	}
	if predicateCalls.Load() != 0 || port.callCount() != 0 {
		t.Fatalf("cancel crossed lower policy boundary: predicate=%d port=%d", predicateCalls.Load(), port.callCount())
	}
	if err := runtime.CancelRoot(id); !errors.Is(err, ErrRootAlreadyTerminal) {
		t.Fatalf("repeat CancelRoot() error = %v", err)
	}
}

func TestRootRuntimeIntegrationWithRealChildUSEOAndKernel(t *testing.T) {
	provider := &rootCountingProvider{}
	k := kernel.New(provider)
	resourceObject, err := k.Resources.CreateResource("local")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	declaration, err := k.Capabilities.RegisterDeclaration(capability.DeclarationSpec{ResourceID: resourceObject.ID, Name: "root.test.execute", Version: "v1"})
	if err != nil {
		t.Fatalf("RegisterDeclaration() error = %v", err)
	}
	instance, err := k.Capabilities.CreateInstance(declaration.ID)
	if err != nil {
		t.Fatalf("CreateInstance() error = %v", err)
	}
	executionContext, err := k.Executions.CreateContext("root-subject", "local")
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
	workPort := &rootCountingWorkPort{delegate: userspace.NewOrchestrator(facade)}
	childRuntime := taskruntime.NewRuntime(workPort)
	rootRuntime := NewRuntime(childRuntime)
	rootID := RootTaskID("root-real")
	created, err := rootRuntime.Create(RootTaskSpec{RootTaskID: rootID, GoalDescriptor: GoalDescriptor{Ref: "real-goal"}, ClosurePredicate: AllRequiredChildrenSucceededPredicate()})
	if err != nil || created != rootID {
		t.Fatalf("Root Create() = %s, error = %v", created, err)
	}
	child, err := taskruntime.New(taskruntime.ChildTaskSpec{
		ChildTaskID:        "child-real",
		RootTaskRef:        string(rootID),
		ExecutionContextID: executionContext.ID,
		CapabilityHandleID: handle.ID,
		CallerIdentity:     executionContext.Subject,
		Scope:              executionContext.Scope,
		Operation:          "execute",
		Payload:            []byte("root-integration"),
	})
	if err != nil {
		t.Fatalf("Child New() error = %v", err)
	}
	if _, err := childRuntime.Execute(context.Background(), child); err != nil {
		t.Fatalf("Child Execute() error = %v", err)
	}
	if err := rootRuntime.AddChild(rootID, child.ChildTaskID); err != nil {
		t.Fatalf("Root AddChild() error = %v", err)
	}
	if err := rootRuntime.CloseMembership(rootID); err != nil {
		t.Fatalf("Root CloseMembership() error = %v", err)
	}
	snapshot, err := rootRuntime.Evaluate(rootID)
	if err != nil || snapshot.State != RootTaskStateSucceeded || len(snapshot.ChildProjections) != 1 {
		t.Fatalf("Root Evaluate() = %#v, error = %v", snapshot, err)
	}
	if workPort.callCount() != 1 || provider.Count() != 1 {
		t.Fatalf("real stack calls = USEO %d provider %d, want 1/1", workPort.callCount(), provider.Count())
	}
	if snapshot.ChildProjections[0].State != taskruntime.ChildTaskStateSucceeded {
		t.Fatalf("Root evidence = %#v, want succeeded Child snapshot", snapshot.ChildProjections[0])
	}
}

func TestRootRuntimeRealChildLineageRejection(t *testing.T) {
	childRuntime := taskruntime.NewRuntime(nil)
	rootRuntime := NewRuntime(childRuntime)
	rootID := RootTaskID("root-one")
	if _, err := rootRuntime.Create(RootTaskSpec{RootTaskID: rootID, GoalDescriptor: GoalDescriptor{Ref: "goal"}, ClosurePredicate: AllRequiredChildrenSucceededPredicate()}); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	child, err := taskruntime.New(taskruntime.ChildTaskSpec{
		ChildTaskID:        "child-wrong-lineage",
		RootTaskRef:        "root-two",
		ExecutionContextID: "context",
		CapabilityHandleID: "handle",
		CallerIdentity:     "subject",
		Scope:              "scope",
		Operation:          "execute",
	})
	if err != nil {
		t.Fatalf("Child New() error = %v", err)
	}
	// The nil work port publishes a local terminal rejection, which is enough
	// to expose the real Child snapshot lineage without crossing USEO.
	_, _ = childRuntime.Execute(context.Background(), child)
	if err := rootRuntime.AddChild(rootID, child.ChildTaskID); err != nil {
		t.Fatalf("AddChild() error = %v", err)
	}
	if err := rootRuntime.CloseMembership(rootID); err != nil {
		t.Fatalf("CloseMembership() error = %v", err)
	}
	_, err = rootRuntime.Evaluate(rootID)
	if !errors.Is(err, ErrRootChildLineageMismatch) {
		t.Fatalf("Evaluate() error = %v, want lineage mismatch", err)
	}
}

type rootCountingWorkPort struct {
	delegate *userspace.Orchestrator
	calls    atomic.Int32
}

func (port *rootCountingWorkPort) Execute(ctx context.Context, item userspace.ExecutionWorkItem) (userspace.ExecutionProjection, error) {
	port.calls.Add(1)
	return port.delegate.Execute(ctx, item)
}

func (port *rootCountingWorkPort) callCount() int { return int(port.calls.Load()) }

type rootCountingProvider struct{ calls atomic.Int32 }

func (provider *rootCountingProvider) Invoke(capability.ProviderOperation) error {
	provider.calls.Add(1)
	return nil
}

func (provider *rootCountingProvider) Count() int { return int(provider.calls.Load()) }
