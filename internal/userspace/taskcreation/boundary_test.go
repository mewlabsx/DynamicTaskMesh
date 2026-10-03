package taskcreation

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	"dtm/internal/kernel"
	"dtm/internal/kernel/capability"
	"dtm/internal/userspace"
	"dtm/internal/userspace/roottask"
	"dtm/internal/userspace/taskruntime"
)

type fakeRootPort struct {
	mu sync.Mutex

	created []roottask.RootTaskSpec
	added   [][2]string
	create  func(roottask.RootTaskSpec) (roottask.RootTaskID, error)
	add     func(roottask.RootTaskID, taskruntime.ChildTaskID) error
}

func (port *fakeRootPort) Create(spec roottask.RootTaskSpec) (roottask.RootTaskID, error) {
	port.mu.Lock()
	port.created = append(port.created, spec)
	create := port.create
	port.mu.Unlock()
	if create != nil {
		return create(spec)
	}
	return spec.RootTaskID, nil
}

func (port *fakeRootPort) AddChild(rootID roottask.RootTaskID, childID taskruntime.ChildTaskID) error {
	port.mu.Lock()
	port.added = append(port.added, [2]string{rootID.String(), childID.String()})
	add := port.add
	port.mu.Unlock()
	if add != nil {
		return add(rootID, childID)
	}
	return nil
}

type fakeChildPort struct {
	mu sync.Mutex

	reserveCalls  int
	snapshotCalls int
	reserved      taskruntime.ChildTask
	snapshot      taskruntime.ChildTaskSnapshot
	reserveErr    error
	snapshotErr   error
}

func (port *fakeChildPort) ReserveCreated(spec taskruntime.ChildTaskSpec) (taskruntime.ChildTask, error) {
	port.mu.Lock()
	defer port.mu.Unlock()
	port.reserveCalls++
	if port.reserveErr != nil {
		return taskruntime.ChildTask{}, port.reserveErr
	}
	if port.reserved.ChildTaskID == "" {
		reserved, err := taskruntime.New(spec)
		if err != nil {
			return taskruntime.ChildTask{}, err
		}
		port.reserved = reserved
		port.snapshot = taskruntime.ChildTaskSnapshot{ChildTaskID: reserved.ChildTaskID, RootTaskRef: reserved.RootTaskRef, State: reserved.State}
	}
	return port.reserved.Clone(), nil
}

func (port *fakeChildPort) GetChildTaskSnapshot(id taskruntime.ChildTaskID) (taskruntime.ChildTaskSnapshot, error) {
	port.mu.Lock()
	defer port.mu.Unlock()
	port.snapshotCalls++
	if port.snapshotErr != nil {
		return taskruntime.ChildTaskSnapshot{}, port.snapshotErr
	}
	if port.snapshot.ChildTaskID == "" {
		return taskruntime.ChildTaskSnapshot{}, taskruntime.ErrChildTaskNotFound
	}
	return port.snapshot.Clone(), nil
}

func rootRequest(id roottask.RootTaskID) RootCreationRequest {
	return RootCreationRequest{
		RootTaskID:     id,
		GoalDescriptor: roottask.GoalDescriptor{Ref: "goal-" + id.String()},
		ClosurePredicate: roottask.ClosurePredicateFunc(func(roottask.RootTaskSnapshot, []roottask.ChildEvidence) (roottask.ClosureDecision, error) {
			return roottask.ClosureDecisionSucceeded, nil
		}),
	}
}

func childRequest(rootID roottask.RootTaskID, childID taskruntime.ChildTaskID) ChildCreationRequest {
	return ChildCreationRequest{
		RootTaskID: rootID,
		Spec: taskruntime.ChildTaskSpec{
			ChildTaskID:        childID,
			ExecutionContextID: kernel.ExecutionContextID("context-creation"),
			CapabilityHandleID: kernel.CapabilityHandleID("handle-creation"),
			CallerIdentity:     "subject-creation",
			Scope:              "scope-creation",
			Operation:          "execute",
			Payload:            []byte("creation-payload"),
		},
	}
}

func TestCreateRootDelegatesOnlyToRootRuntime(t *testing.T) {
	root := &fakeRootPort{}
	child := &fakeChildPort{}
	boundary := New(root, child)
	request := rootRequest("root-create")

	id, err := boundary.CreateRoot(request)
	if err != nil || id != request.RootTaskID {
		t.Fatalf("CreateRoot() = %s, error = %v, want caller RootTaskID", id, err)
	}
	if len(root.created) != 1 || root.created[0].GoalDescriptor != request.GoalDescriptor || root.created[0].ClosurePredicate == nil {
		t.Fatalf("Root Runtime requests = %#v, want one exact request", root.created)
	}
	if child.reserveCalls != 0 {
		t.Fatalf("CreateRoot() called Child Runtime %d times", child.reserveCalls)
	}
}

func TestCreateRootInvalidInputMakesNoRuntimeCall(t *testing.T) {
	root := &fakeRootPort{}
	child := &fakeChildPort{}
	boundary := New(root, child)
	_, err := boundary.CreateRoot(RootCreationRequest{RootTaskID: "", GoalDescriptor: roottask.GoalDescriptor{Ref: "goal"}})
	if !errors.Is(err, ErrInvalidCreationRequest) {
		t.Fatalf("invalid CreateRoot() error = %v, want invalid request", err)
	}
	if len(root.created) != 0 || child.reserveCalls != 0 {
		t.Fatalf("invalid CreateRoot() crossed runtimes: root=%d child=%d", len(root.created), child.reserveCalls)
	}
}

func TestCreateChildDerivesLineageAndUsesChildAuthority(t *testing.T) {
	root := &fakeRootPort{}
	child := &fakeChildPort{}
	boundary := New(root, child)
	request := childRequest("root-child", "child-materialized")

	created, err := boundary.CreateChild(request)
	if err != nil {
		t.Fatalf("CreateChild() error = %v", err)
	}
	if created.State != taskruntime.ChildTaskStateCreated || created.RootTaskRef != request.RootTaskID.String() || !created.IsPristineCreated() {
		t.Fatalf("created = %#v, want pristine authoritative CREATED", created)
	}
	if created.ChildTaskID != request.Spec.ChildTaskID || !reflect.DeepEqual(created.Payload, request.Spec.Payload) {
		t.Fatalf("created binding = %#v, want request binding", created)
	}
	if child.reserveCalls != 1 || len(root.created) != 0 || len(root.added) != 0 {
		t.Fatalf("CreateChild() calls = reserve %d root creates %d adds %d", child.reserveCalls, len(root.created), len(root.added))
	}

	created.Payload[0] = 'X'
	replayed, err := boundary.CreateChild(request)
	if err != nil || !reflect.DeepEqual(replayed.Payload, request.Spec.Payload) {
		t.Fatalf("replayed CreateChild() = %#v, error = %v, want independent original payload", replayed, err)
	}
}

func TestCreateChildRejectsMismatchedExplicitLineageBeforeReservation(t *testing.T) {
	child := &fakeChildPort{}
	boundary := New(&fakeRootPort{}, child)
	request := childRequest("root-one", "child-lineage")
	request.Spec.RootTaskRef = "root-two"

	_, err := boundary.CreateChild(request)
	if !errors.Is(err, ErrChildLineageMismatch) || child.reserveCalls != 0 {
		t.Fatalf("mismatched CreateChild() error = %v, reserve calls = %d", err, child.reserveCalls)
	}
}

func TestCreateChildRejectsMalformedChildRuntimeResult(t *testing.T) {
	child := &fakeChildPort{}
	child.reserved = taskruntime.ChildTask{
		ChildTaskID: "child-malformed",
		RootTaskRef: "root-malformed",
		State:       taskruntime.ChildTaskStateCreated,
		Failure:     errors.New("forged"),
	}
	boundary := New(&fakeRootPort{}, child)
	request := childRequest("root-malformed", "child-malformed")

	_, err := boundary.CreateChild(request)
	if !errors.Is(err, ErrInvalidReservedChild) {
		t.Fatalf("malformed reservation error = %v, want invalid reserved child", err)
	}
}

func TestRegisterChildReadsAuthoritativeSnapshotAndDelegatesIdentityOnly(t *testing.T) {
	root := &fakeRootPort{}
	child := &fakeChildPort{}
	boundary := New(root, child)
	request := childRequest("root-register", "child-register")
	if _, err := boundary.CreateChild(request); err != nil {
		t.Fatalf("CreateChild() error = %v", err)
	}
	if err := boundary.RegisterChild(request.RootTaskID, request.Spec.ChildTaskID); err != nil {
		t.Fatalf("RegisterChild() error = %v", err)
	}
	if child.snapshotCalls != 1 || len(root.added) != 1 || root.added[0] != [2]string{"root-register", "child-register"} {
		t.Fatalf("registration calls = snapshots %d adds %#v", child.snapshotCalls, root.added)
	}
}

func TestRegisterChildRejectsLineageAndNonCreatedWithoutRootMutation(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(*fakeChildPort)
		wantError error
	}{
		{
			name: "lineage",
			setup: func(child *fakeChildPort) {
				child.snapshot = taskruntime.ChildTaskSnapshot{ChildTaskID: "child-register", RootTaskRef: "root-other", State: taskruntime.ChildTaskStateCreated}
			},
			wantError: ErrChildLineageMismatch,
		},
		{
			name: "executing",
			setup: func(child *fakeChildPort) {
				child.snapshot = taskruntime.ChildTaskSnapshot{ChildTaskID: "child-register", RootTaskRef: "root-register", State: taskruntime.ChildTaskStateExecuting}
			},
			wantError: ErrChildNotCreated,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := &fakeRootPort{}
			child := &fakeChildPort{}
			test.setup(child)
			boundary := New(root, child)
			err := boundary.RegisterChild("root-register", "child-register")
			if !errors.Is(err, test.wantError) {
				t.Fatalf("RegisterChild() error = %v, want %v", err, test.wantError)
			}
			if len(root.added) != 0 {
				t.Fatalf("invalid registration mutated Root membership: %#v", root.added)
			}
		})
	}
}

func TestRegisterChildPreservesRootRuntimeError(t *testing.T) {
	rootErr := errors.New("root duplicate diagnostic")
	root := &fakeRootPort{add: func(roottask.RootTaskID, taskruntime.ChildTaskID) error {
		return errors.Join(roottask.ErrRootChildAlreadyRegistered, rootErr)
	}}
	child := &fakeChildPort{}
	child.snapshot = taskruntime.ChildTaskSnapshot{ChildTaskID: "child-register", RootTaskRef: "root-register", State: taskruntime.ChildTaskStateCreated}
	boundary := New(root, child)
	err := boundary.RegisterChild("root-register", "child-register")
	if !errors.Is(err, ErrRootRegistrationFailed) || !errors.Is(err, roottask.ErrRootChildAlreadyRegistered) || !errors.Is(err, rootErr) {
		t.Fatalf("RegisterChild() error = %v, want boundary and Root diagnostics", err)
	}
}

func TestTaskCreationBoundaryRealStackIntegration(t *testing.T) {
	provider := &countingProvider{}
	k := kernel.New(provider)
	resourceObject, err := k.Resources.CreateResource("local")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	declaration, err := k.Capabilities.RegisterDeclaration(capability.DeclarationSpec{ResourceID: resourceObject.ID, Name: "taskcreation.execute", Version: "v1"})
	if err != nil {
		t.Fatalf("RegisterDeclaration() error = %v", err)
	}
	instance, err := k.Capabilities.CreateInstance(declaration.ID)
	if err != nil {
		t.Fatalf("CreateInstance() error = %v", err)
	}
	executionContext, err := k.Executions.CreateContext("creation-subject", "local")
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
	childRuntime := taskruntime.NewRuntime(userspace.NewOrchestrator(facade))
	rootRuntime := roottask.NewRuntime(childRuntime)
	boundary := New(rootRuntime, childRuntime)

	rootID, err := boundary.CreateRoot(RootCreationRequest{
		RootTaskID:     "root-real-creation",
		GoalDescriptor: roottask.GoalDescriptor{Ref: "goal-real-creation"},
		ClosurePredicate: roottask.ClosurePredicateFunc(func(root roottask.RootTaskSnapshot, evidence []roottask.ChildEvidence) (roottask.ClosureDecision, error) {
			return roottask.AllRequiredChildrenSucceeded(root, evidence)
		}),
	})
	if err != nil {
		t.Fatalf("CreateRoot() error = %v", err)
	}
	child, err := boundary.CreateChild(ChildCreationRequest{
		RootTaskID: rootID,
		Spec: taskruntime.ChildTaskSpec{
			ChildTaskID:        "child-real-creation",
			ExecutionContextID: executionContext.ID,
			CapabilityHandleID: handle.ID,
			CallerIdentity:     executionContext.Subject,
			Scope:              executionContext.Scope,
			Operation:          "execute",
			Payload:            []byte("real-creation-payload"),
		},
	})
	if err != nil {
		t.Fatalf("CreateChild() error = %v", err)
	}
	if err := boundary.RegisterChild(rootID, child.ChildTaskID); err != nil {
		t.Fatalf("RegisterChild() error = %v", err)
	}
	if _, err := childRuntime.Execute(context.Background(), child); err != nil {
		t.Fatalf("explicit Child Execute() error = %v", err)
	}
	if err := rootRuntime.CloseMembership(rootID); err != nil {
		t.Fatalf("CloseMembership() error = %v", err)
	}
	rootSnapshot, err := rootRuntime.Evaluate(rootID)
	if err != nil || rootSnapshot.State != roottask.RootTaskStateSucceeded || len(rootSnapshot.ChildProjections) != 1 {
		t.Fatalf("Evaluate() = %#v, error = %v, want succeeded Root with one Child", rootSnapshot, err)
	}
	if rootSnapshot.ChildProjections[0].State != taskruntime.ChildTaskStateSucceeded || provider.Count() != 1 {
		t.Fatalf("Root evidence/provider count = %#v/%d, want succeeded/1", rootSnapshot.ChildProjections[0], provider.Count())
	}
}

func TestTaskCreationBoundaryRealStackIdentityConflictAcrossRoots(t *testing.T) {
	childRuntime := taskruntime.NewRuntime(nil)
	rootRuntime := roottask.NewRuntime(childRuntime)
	boundary := New(rootRuntime, childRuntime)
	for _, rootID := range []roottask.RootTaskID{"root-one", "root-two"} {
		if _, err := boundary.CreateRoot(rootRequest(rootID)); err != nil {
			t.Fatalf("CreateRoot(%s) error = %v", rootID, err)
		}
	}
	first, err := boundary.CreateChild(childRequest("root-one", "child-shared"))
	if err != nil {
		t.Fatalf("first CreateChild() error = %v", err)
	}
	if err := boundary.RegisterChild("root-one", first.ChildTaskID); err != nil {
		t.Fatalf("first RegisterChild() error = %v", err)
	}
	conflict := childRequest("root-two", "child-shared")
	_, err = boundary.CreateChild(conflict)
	if !errors.Is(err, ErrChildReservationFailed) || !errors.Is(err, taskruntime.ErrChildTaskBindingConflict) {
		t.Fatalf("conflicting CreateChild() error = %v, want reservation/binding conflict", err)
	}
	rootOne, err := rootRuntime.GetRootSnapshot("root-one")
	if err != nil || len(rootOne.ChildTaskIDs) != 1 || rootOne.ChildTaskIDs[0] != first.ChildTaskID {
		t.Fatalf("root-one membership = %#v, error = %v, want one child", rootOne.ChildTaskIDs, err)
	}
	rootTwo, err := rootRuntime.GetRootSnapshot("root-two")
	if err != nil || len(rootTwo.ChildTaskIDs) != 0 {
		t.Fatalf("root-two membership = %#v, error = %v, want no child", rootTwo.ChildTaskIDs, err)
	}
}

func TestTaskCreationBoundaryConcurrentCreateChildInheritsRuntimeReservation(t *testing.T) {
	childRuntime := taskruntime.NewRuntime(nil)
	boundary := New(&fakeRootPort{}, childRuntime)
	request := childRequest("root-concurrent", "child-concurrent")
	const callers = 32
	results := make(chan taskruntime.ChildTask, callers)
	errorsCh := make(chan error, callers)
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wait.Done()
			<-start
			result, err := boundary.CreateChild(request)
			results <- result
			errorsCh <- err
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatalf("concurrent CreateChild() error = %v", err)
		}
	}
	for result := range results {
		if result.ChildTaskID != request.Spec.ChildTaskID || result.State != taskruntime.ChildTaskStateCreated || !result.IsPristineCreated() {
			t.Fatalf("concurrent CreateChild() result = %#v, want pristine CREATED", result)
		}
	}
}

type countingProvider struct{ calls atomic.Int32 }

func (provider *countingProvider) Invoke(capability.ProviderOperation) error {
	provider.calls.Add(1)
	return nil
}

func (provider *countingProvider) Count() int { return int(provider.calls.Load()) }

var _ RootTaskPort = (*roottask.Runtime)(nil)
var _ ChildTaskCreationPort = (*taskruntime.Runtime)(nil)
var _ capability.CapabilityProvider = (*countingProvider)(nil)
