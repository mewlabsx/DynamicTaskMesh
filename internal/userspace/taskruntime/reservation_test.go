package taskruntime

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"dtm/internal/kernel"
	"dtm/internal/userspace"
)

func reservationSpec(id ChildTaskID) ChildTaskSpec {
	return ChildTaskSpec{
		ChildTaskID:        id,
		RootTaskRef:        "root-reservation",
		ExecutionContextID: kernel.ExecutionContextID("context-reservation"),
		CapabilityHandleID: kernel.CapabilityHandleID("handle-reservation"),
		CallerIdentity:     "subject-reservation",
		Scope:              "scope-reservation",
		Operation:          "execute",
		Payload:            []byte("reservation-payload"),
	}
}

func TestRuntimeReserveCreatedPublishesPristineCanonicalChild(t *testing.T) {
	port := &fakeWorkPort{}
	runtime := NewRuntime(port)
	spec := reservationSpec("child-reserved")

	reserved, err := runtime.ReserveCreated(spec)
	if err != nil {
		t.Fatalf("ReserveCreated() error = %v", err)
	}
	if reserved.State != ChildTaskStateCreated || reserved.ChildTaskID != spec.ChildTaskID || reserved.RootTaskRef != spec.RootTaskRef {
		t.Fatalf("reserved = %#v, want authoritative CREATED binding", reserved)
	}
	if err := reserved.ValidatePristineCreated(); err != nil {
		t.Fatalf("reserved pristine validation error = %v", err)
	}
	if !reflect.DeepEqual(reserved.ExecutionProjection, userspace.ExecutionProjection{}) || reserved.Failure != nil || reserved.ExecutionWorkItemID != "" || reserved.RequestID != "" || !reserved.InvocationID.IsZero() {
		t.Fatalf("reserved derived fields = %#v, want zero", reserved)
	}
	if calls := port.callCount(); calls != 0 {
		t.Fatalf("reservation crossed USEO: calls = %d", calls)
	}

	snapshot, err := runtime.GetChildTaskSnapshot(spec.ChildTaskID)
	if err != nil {
		t.Fatalf("GetChildTaskSnapshot() error = %v", err)
	}
	if snapshot.ChildTaskID != spec.ChildTaskID || snapshot.RootTaskRef != spec.RootTaskRef || snapshot.State != ChildTaskStateCreated || !snapshot.InvocationID.IsZero() || !reflect.DeepEqual(snapshot.ExecutionProjection, userspace.ExecutionProjection{}) || snapshot.Failure != nil {
		t.Fatalf("reserved snapshot = %#v, want pristine CREATED", snapshot)
	}
}

func TestRuntimeReserveCreatedSameBindingReplayIsIdempotentAndIndependent(t *testing.T) {
	runtime := NewRuntime(&fakeWorkPort{})
	spec := reservationSpec("child-reserved-replay")

	first, err := runtime.ReserveCreated(spec)
	if err != nil {
		t.Fatalf("first ReserveCreated() error = %v", err)
	}
	first.Payload[0] = 'X'
	second, err := runtime.ReserveCreated(spec)
	if err != nil {
		t.Fatalf("same-binding ReserveCreated() error = %v", err)
	}
	if !reflect.DeepEqual(second.Payload, spec.Payload) || second.Payload[0] == 'X' {
		t.Fatalf("replayed payload = %q, want independent original payload %q", second.Payload, spec.Payload)
	}
	if second.State != ChildTaskStateCreated || !second.IsPristineCreated() {
		t.Fatalf("replayed reservation = %#v, want pristine CREATED", second)
	}
}

func TestRuntimeReserveCreatedConflictingBindingsFailClosed(t *testing.T) {
	base := reservationSpec("child-reserved-conflict")
	runtime := NewRuntime(&fakeWorkPort{})
	if _, err := runtime.ReserveCreated(base); err != nil {
		t.Fatalf("initial ReserveCreated() error = %v", err)
	}

	variants := []struct {
		name   string
		mutate func(*ChildTaskSpec)
	}{
		{name: "root", mutate: func(spec *ChildTaskSpec) { spec.RootTaskRef = "root-other" }},
		{name: "context", mutate: func(spec *ChildTaskSpec) { spec.ExecutionContextID = "context-other" }},
		{name: "handle", mutate: func(spec *ChildTaskSpec) { spec.CapabilityHandleID = "handle-other" }},
		{name: "caller", mutate: func(spec *ChildTaskSpec) { spec.CallerIdentity = "subject-other" }},
		{name: "scope", mutate: func(spec *ChildTaskSpec) { spec.Scope = "scope-other" }},
		{name: "operation", mutate: func(spec *ChildTaskSpec) { spec.Operation = "different-operation" }},
		{name: "payload", mutate: func(spec *ChildTaskSpec) { spec.Payload = []byte("different-payload") }},
	}
	for _, test := range variants {
		t.Run(test.name, func(t *testing.T) {
			conflict := base
			test.mutate(&conflict)
			reserved, err := runtime.ReserveCreated(conflict)
			if !errors.Is(err, ErrChildTaskBindingConflict) {
				t.Fatalf("ReserveCreated() error = %v, want binding conflict", err)
			}
			if !reflect.DeepEqual(reserved, ChildTask{}) {
				t.Fatalf("conflict result = %#v, want zero value", reserved)
			}
		})
	}
	snapshot, err := runtime.GetChildTaskSnapshot(base.ChildTaskID)
	if err != nil || snapshot.State != ChildTaskStateCreated || snapshot.RootTaskRef != base.RootTaskRef {
		t.Fatalf("authoritative reservation = %#v, error = %v, want unchanged CREATED", snapshot, err)
	}
}

func TestRuntimeReserveCreatedAfterSubmissionReturnsAlreadySubmitted(t *testing.T) {
	port := &fakeWorkPort{}
	runtime := NewRuntime(port)
	spec := reservationSpec("child-reserved-submitted")
	reserved, err := runtime.ReserveCreated(spec)
	if err != nil {
		t.Fatalf("ReserveCreated() error = %v", err)
	}
	if _, err := runtime.Execute(context.Background(), reserved); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	replayed, err := runtime.ReserveCreated(spec)
	if !errors.Is(err, ErrChildTaskAlreadySubmitted) {
		t.Fatalf("post-submit ReserveCreated() error = %v, want already submitted", err)
	}
	if replayed.State != ChildTaskStateSucceeded {
		t.Fatalf("post-submit reservation result = %#v, want retained terminal value", replayed)
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("post-submit reservation changed USEO calls = %d, want 1", calls)
	}
}

func TestRuntimeReservedExecuteUsesAuthoritativeBindingAndIgnoresDerivedFields(t *testing.T) {
	port := &fakeWorkPort{}
	runtime := NewRuntime(port)
	spec := reservationSpec("child-reserved-derived-fields")
	reserved, err := runtime.ReserveCreated(spec)
	if err != nil {
		t.Fatalf("ReserveCreated() error = %v", err)
	}
	forged := reserved.Clone()
	forged.State = ChildTaskStateSucceeded
	forged.ExecutionWorkItemID = "forged-work-item"
	forged.RequestID = "forged-request"
	forged.InvocationID, err = kernel.NewInvocationID()
	if err != nil {
		t.Fatalf("NewInvocationID() error = %v", err)
	}
	forged.ExecutionProjection = userspace.ExecutionProjection{State: userspace.WorkStateSucceeded}
	forged.Failure = errors.New("forged failure")

	result, err := runtime.Execute(context.Background(), forged)
	if err != nil || result.State != ChildTaskStateSucceeded {
		t.Fatalf("Execute(forged) = %#v, error = %v, want authoritative success", result, err)
	}
	if result.ExecutionWorkItemID == forged.ExecutionWorkItemID || result.RequestID == forged.RequestID || result.InvocationID == forged.InvocationID {
		t.Fatalf("forged derived fields survived promotion: %#v", result)
	}
	if result.Failure != nil {
		t.Fatalf("result failure = %v, want nil", result.Failure)
	}
	work := port.requestSnapshot()
	if work.RootTaskRef != spec.RootTaskRef || work.ChildTaskRef != spec.ChildTaskID.String() || work.Operation != spec.Operation || !reflect.DeepEqual(work.Payload, spec.Payload) {
		t.Fatalf("submitted binding = %#v, want reserved binding", work)
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("USEO calls = %d, want 1", calls)
	}
}

func TestRuntimeReservedExecuteConcurrentPromotesOnce(t *testing.T) {
	port := &fakeWorkPort{entered: make(chan struct{}), release: make(chan struct{})}
	runtime := NewRuntime(port)
	reserved, err := runtime.ReserveCreated(reservationSpec("child-reserved-concurrent"))
	if err != nil {
		t.Fatalf("ReserveCreated() error = %v", err)
	}

	const callers = 32
	type outcome struct {
		task ChildTask
		err  error
	}
	outcomes := make(chan outcome, callers)
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wait.Done()
			<-start
			result, executeErr := runtime.Execute(context.Background(), reserved)
			outcomes <- outcome{task: result, err: executeErr}
		}()
	}
	close(start)
	select {
	case <-port.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("reserved execution did not reach USEO")
	}
	close(port.release)
	wait.Wait()
	close(outcomes)
	for result := range outcomes {
		if result.err != nil || result.task.State != ChildTaskStateSucceeded {
			t.Fatalf("concurrent reserved result = %#v, error = %v, want success", result.task, result.err)
		}
	}
	if calls := port.callCount(); calls != 1 {
		t.Fatalf("concurrent reserved USEO calls = %d, want 1", calls)
	}
}

func TestRuntimeReservedExecuteBindingConflictDoesNotCallUSEO(t *testing.T) {
	port := &fakeWorkPort{}
	runtime := NewRuntime(port)
	base := reservationSpec("child-reserved-execute-conflict")
	reserved, err := runtime.ReserveCreated(base)
	if err != nil {
		t.Fatalf("ReserveCreated() error = %v", err)
	}
	conflict := reserved.Clone()
	conflict.RootTaskRef = "root-other"
	result, err := runtime.Execute(context.Background(), conflict)
	if result.State != ChildTaskStateRejected || !errors.Is(err, ErrChildTaskBindingConflict) {
		t.Fatalf("conflicting Execute() = %#v, error = %v, want binding conflict", result, err)
	}
	if calls := port.callCount(); calls != 0 {
		t.Fatalf("conflicting reserved Execute crossed USEO: calls = %d", calls)
	}
	snapshot, err := runtime.GetChildTaskSnapshot(base.ChildTaskID)
	if err != nil || snapshot.State != ChildTaskStateCreated || snapshot.RootTaskRef != base.RootTaskRef {
		t.Fatalf("reservation after conflict = %#v, error = %v, want unchanged CREATED", snapshot, err)
	}
}

func TestRuntimeConcurrentReserveCreatedSameBindingHasOneCanonicalRecord(t *testing.T) {
	runtime := NewRuntime(&fakeWorkPort{})
	spec := reservationSpec("child-reserved-many")
	const callers = 64
	results := make(chan ChildTask, callers)
	errorsCh := make(chan error, callers)
	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(callers)
	for i := 0; i < callers; i++ {
		go func() {
			defer wait.Done()
			<-start
			result, err := runtime.ReserveCreated(spec)
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
			t.Fatalf("concurrent ReserveCreated() error = %v", err)
		}
	}
	for result := range results {
		if result.ChildTaskID != spec.ChildTaskID || result.State != ChildTaskStateCreated || !result.IsPristineCreated() || !reflect.DeepEqual(result.Payload, spec.Payload) {
			t.Fatalf("concurrent reservation result = %#v, want same pristine value", result)
		}
	}
	snapshot, err := runtime.GetChildTaskSnapshot(spec.ChildTaskID)
	if err != nil || snapshot.State != ChildTaskStateCreated || snapshot.ChildTaskID != spec.ChildTaskID {
		t.Fatalf("canonical concurrent snapshot = %#v, error = %v", snapshot, err)
	}
}

func TestRuntimeReserveCreatedWhileExecutingReturnsWithoutWaiting(t *testing.T) {
	port := &fakeWorkPort{entered: make(chan struct{}), release: make(chan struct{})}
	runtime := NewRuntime(port)
	spec := reservationSpec("child-reserved-in-flight")
	reserved, err := runtime.ReserveCreated(spec)
	if err != nil {
		t.Fatalf("ReserveCreated() error = %v", err)
	}
	executionDone := make(chan error, 1)
	go func() {
		_, executeErr := runtime.Execute(context.Background(), reserved)
		executionDone <- executeErr
	}()
	select {
	case <-port.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("reserved Execute() did not reach USEO")
	}
	start := time.Now()
	_, replayErr := runtime.ReserveCreated(spec)
	if time.Since(start) > 250*time.Millisecond || !errors.Is(replayErr, ErrChildTaskAlreadySubmitted) {
		t.Fatalf("in-flight ReserveCreated() error = %v, elapsed = %s, want immediate already-submitted", replayErr, time.Since(start))
	}
	close(port.release)
	if err := <-executionDone; err != nil {
		t.Fatalf("reserved Execute() error = %v", err)
	}
}
