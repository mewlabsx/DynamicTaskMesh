package userspace

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"dtm/internal/kernel"
	"dtm/internal/kernel/capability"
	"dtm/internal/kernel/execution"
)

type fakeExecutionPort struct {
	mu sync.Mutex

	requestCalls  int
	observeCalls  int
	requests      []kernel.LogicalExecutionRequest
	requestResult kernel.LogicalExecutionResult
	requestErr    error
	observeResult kernel.LogicalInvocationObservation
	observeErr    error
	requestFn     func(kernel.LogicalExecutionRequest) (kernel.LogicalExecutionResult, error)
	observeFn     func(kernel.InvocationID) (kernel.LogicalInvocationObservation, error)
	entered       chan struct{}
	release       chan struct{}
}

func (port *fakeExecutionPort) RequestExecution(request kernel.LogicalExecutionRequest) (kernel.LogicalExecutionResult, error) {
	port.mu.Lock()
	port.requestCalls++
	port.requests = append(port.requests, request.Clone())
	fn := port.requestFn
	result := port.requestResult.Clone()
	err := port.requestErr
	entered := port.entered
	release := port.release
	callNumber := port.requestCalls
	port.mu.Unlock()

	if entered != nil && callNumber == 1 {
		close(entered)
	}
	if release != nil {
		<-release
	}
	if fn != nil {
		return fn(request)
	}
	if result.InvocationID.IsZero() {
		result = successfulResult(request)
	}
	return result, err
}

func (port *fakeExecutionPort) ObserveInvocation(invocationID kernel.InvocationID) (kernel.LogicalInvocationObservation, error) {
	port.mu.Lock()
	port.observeCalls++
	fn := port.observeFn
	result := port.observeResult.Clone()
	err := port.observeErr
	port.mu.Unlock()
	if fn != nil {
		return fn(invocationID)
	}
	return result, err
}

func (port *fakeExecutionPort) calls() (requests, observations int) {
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.requestCalls, port.observeCalls
}

func (port *fakeExecutionPort) requestSnapshot() kernel.LogicalExecutionRequest {
	port.mu.Lock()
	defer port.mu.Unlock()
	return port.requests[0].Clone()
}

func successfulResult(request kernel.LogicalExecutionRequest) kernel.LogicalExecutionResult {
	return kernel.LogicalExecutionResult{
		InvocationID:              request.InvocationID,
		RequestID:                 request.RequestID,
		ExecutionAllocationID:     kernel.ExecutionAllocationID("allocation-test"),
		Status:                    kernel.LogicalExecutionCompleted,
		ProviderCrossingAvailable: true,
		ProviderCrossing:          kernel.ProviderCrossingCrossed,
		ObservationAvailable:      true,
		Observation: kernel.ExecutionObservation{
			AllocationID: kernel.ExecutionAllocationID("allocation-test"),
			InvocationID: request.InvocationID,
			Outcome:      kernel.ExecutionOutcomeSuccess,
			Occupancy:    kernel.OccupancyConclusionEnded,
			Reason:       "provider completed",
		},
		CurrentOccupancy:       kernel.CurrentOccupancyEnded,
		Lifecycle:              kernel.AllocationReleased,
		RetrySafetyFact:        kernel.RetrySafetyNotDeclared,
		ReconciliationRequired: false,
	}
}

func validWorkItem(t *testing.T, child string) ExecutionWorkItem {
	t.Helper()
	invocationID, err := kernel.NewInvocationID()
	if err != nil {
		t.Fatalf("NewInvocationID() error = %v", err)
	}
	contextID := kernel.ExecutionContextID("execution-context-test")
	handleID := kernel.CapabilityHandleID("capability-handle-test")
	return ExecutionWorkItem{
		WorkItemID:         "work-" + child,
		RootTaskRef:        "root-test",
		ChildTaskRef:       child,
		RequestID:          "request-" + child,
		InvocationID:       invocationID,
		ExecutionContextID: contextID,
		CapabilityHandleID: handleID,
		CallerIdentity:     "subject",
		Scope:              "local",
		Operation:          "execute",
		Payload:            []byte("payload-" + child),
	}
}

func TestOrchestratorSuccessfulExecutionProjectsOneRequest(t *testing.T) {
	port := &fakeExecutionPort{}
	orchestrator := NewOrchestrator(port)
	item := validWorkItem(t, "child-success")

	projection, err := orchestrator.Execute(context.Background(), item)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if projection.State != WorkStateSucceeded || projection.Status != kernel.LogicalExecutionCompleted {
		t.Fatalf("projection = %#v, want SUCCEEDED/COMPLETED", projection)
	}
	if projection.ExecutionAllocationID == "" || projection.CurrentOccupancy != kernel.CurrentOccupancyEnded || projection.Lifecycle != kernel.AllocationReleased {
		t.Fatalf("projection Kernel facts = %#v, want allocation and definitive ENDED", projection)
	}
	if projection.RootTaskRef != item.RootTaskRef || projection.ChildTaskRef != item.ChildTaskRef {
		t.Fatalf("opaque references = %q/%q, want %q/%q", projection.RootTaskRef, projection.ChildTaskRef, item.RootTaskRef, item.ChildTaskRef)
	}
	requests, observations := port.calls()
	if requests != 1 || observations != 0 {
		t.Fatalf("port calls = request %d observe %d, want 1/0", requests, observations)
	}
	request := port.requestSnapshot()
	if request.RootTaskRef != item.RootTaskRef || request.ChildTaskRef != item.ChildTaskRef || string(request.Payload) != string(item.Payload) {
		t.Fatalf("Kernel request = %#v, lost opaque request data", request)
	}

	item.Payload[0] = 'X'
	if got := string(port.requestSnapshot().Payload); got != "payload-child-success" {
		t.Fatalf("request payload changed through caller mutation: %q", got)
	}
}

func TestOrchestratorRejectsInvalidWorkWithoutKernelCall(t *testing.T) {
	port := &fakeExecutionPort{}
	orchestrator := NewOrchestrator(port)
	item := validWorkItem(t, "child-invalid")
	item.ChildTaskRef = "   "

	projection, err := orchestrator.Execute(context.Background(), item)
	if err == nil || !errors.Is(err, ErrInvalidWorkItem) {
		t.Fatalf("Execute() error = %v, want ErrInvalidWorkItem", err)
	}
	if projection.State != WorkStateRejected {
		t.Fatalf("state = %s, want REJECTED", projection.State)
	}
	requests, observations := port.calls()
	if requests != 0 || observations != 0 {
		t.Fatalf("port calls = request %d observe %d, want 0/0", requests, observations)
	}
}

func TestOrchestratorPreservesAuthorityAndCapacityRejections(t *testing.T) {
	cases := []struct {
		name     string
		category kernel.LogicalErrorCategory
	}{
		{name: "authority", category: kernel.LogicalErrorAuthorizationDenied},
		{name: "capacity", category: kernel.LogicalErrorCapacityUnavailable},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			item := validWorkItem(t, "child-"+testCase.name)
			port := &fakeExecutionPort{requestFn: func(request kernel.LogicalExecutionRequest) (kernel.LogicalExecutionResult, error) {
				result := kernel.LogicalExecutionResult{
					InvocationID:              request.InvocationID,
					RequestID:                 request.RequestID,
					Status:                    kernel.LogicalExecutionRejected,
					ProviderCrossingAvailable: true,
					ProviderCrossing:          kernel.ProviderCrossingNotCrossedDefinite,
					RetrySafetyFact:           kernel.RetrySafetyNotDeclared,
				}
				result.ErrorInfo = &kernel.LogicalErrorInfo{
					Category:                  testCase.category,
					Phase:                     kernel.LogicalPhaseAuthorityValidation,
					ProviderCrossingAvailable: true,
					ProviderCrossing:          kernel.ProviderCrossingNotCrossedDefinite,
					InvocationID:              request.InvocationID,
					RequestID:                 request.RequestID,
				}
				return result, result.ErrorInfo
			}}
			projection, err := NewOrchestrator(port).Execute(context.Background(), item)
			if err == nil {
				t.Fatal("Execute() error = nil, want typed Kernel error")
			}
			var info *kernel.LogicalErrorInfo
			if !errors.As(err, &info) || info.Category != testCase.category {
				t.Fatalf("error = %T %v, want %s", err, err, testCase.category)
			}
			if projection.State != WorkStateRejected || projection.ProviderCrossing != kernel.ProviderCrossingNotCrossedDefinite {
				t.Fatalf("projection = %#v, want rejected and not crossed", projection)
			}
			requests, observations := port.calls()
			if requests != 1 || observations != 0 {
				t.Fatalf("port calls = request %d observe %d, want 1/0", requests, observations)
			}
		})
	}
}

func TestOrchestratorPreservesPreProviderFailureAndDefinitiveFailure(t *testing.T) {
	tests := []struct {
		name     string
		result   func(kernel.LogicalExecutionRequest) kernel.LogicalExecutionResult
		category kernel.LogicalErrorCategory
		state    WorkState
	}{
		{
			name: "provider unavailable",
			result: func(request kernel.LogicalExecutionRequest) kernel.LogicalExecutionResult {
				result := successfulResult(request)
				result.Status = kernel.LogicalExecutionFailed
				result.ProviderCrossing = kernel.ProviderCrossingNotCrossedDefinite
				result.ErrorInfo = &kernel.LogicalErrorInfo{
					Category:                  kernel.LogicalErrorProviderNotAvailable,
					Phase:                     kernel.LogicalPhasePreProvider,
					ProviderCrossingAvailable: true,
					ProviderCrossing:          kernel.ProviderCrossingNotCrossedDefinite,
					InvocationID:              request.InvocationID,
					RequestID:                 request.RequestID,
				}
				return result
			},
			category: kernel.LogicalErrorProviderNotAvailable,
			state:    WorkStateFailed,
		},
		{
			name: "definitive provider failure",
			result: func(request kernel.LogicalExecutionRequest) kernel.LogicalExecutionResult {
				result := successfulResult(request)
				result.Status = kernel.LogicalExecutionFailed
				result.Observation.Outcome = kernel.ExecutionOutcomeFailed
				result.Observation.Reason = "provider rejected operation"
				return result
			},
			state: WorkStateFailed,
		},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			item := validWorkItem(t, "child-"+testCase.name)
			port := &fakeExecutionPort{requestFn: func(request kernel.LogicalExecutionRequest) (kernel.LogicalExecutionResult, error) {
				result := testCase.result(request)
				if result.ErrorInfo != nil {
					return result, result.ErrorInfo
				}
				return result, nil
			}}
			projection, err := NewOrchestrator(port).Execute(context.Background(), item)
			if projection.State != testCase.state {
				t.Fatalf("state = %s, want %s", projection.State, testCase.state)
			}
			if testCase.category != "" {
				var info *kernel.LogicalErrorInfo
				if !errors.As(err, &info) || info.Category != testCase.category {
					t.Fatalf("error = %T %v, want %s", err, err, testCase.category)
				}
			} else if err != nil {
				t.Fatalf("definitive failure error = %v, want nil", err)
			}
			requests, observations := port.calls()
			if requests != 1 || observations != 0 {
				t.Fatalf("port calls = request %d observe %d, want 1/0", requests, observations)
			}
		})
	}
}

func TestOrchestratorReconcilesExactlyOnceAndStops(t *testing.T) {
	item := validWorkItem(t, "child-reconcile")
	port := &fakeExecutionPort{}
	orchestrator := NewOrchestrator(port)
	port.requestFn = func(request kernel.LogicalExecutionRequest) (kernel.LogicalExecutionResult, error) {
		result := successfulResult(request)
		result.Status = kernel.LogicalExecutionUnknown
		result.CurrentOccupancy = kernel.CurrentOccupancyUnknown
		result.Lifecycle = kernel.AllocationRunning
		result.Observation.Outcome = kernel.ExecutionOutcomeUnknown
		result.Observation.Occupancy = kernel.OccupancyConclusionUnknown
		result.ReconciliationRequired = true
		result.RetrySafetyFact = kernel.RetrySafetyUnsafeOrUnknown
		result.ErrorInfo = &kernel.LogicalErrorInfo{
			Category:                  kernel.LogicalErrorExecutionUnresolved,
			Phase:                     kernel.LogicalPhasePostProvider,
			ProviderCrossingAvailable: true,
			ProviderCrossing:          kernel.ProviderCrossingCrossed,
			RetrySafetyFact:           kernel.RetrySafetyUnsafeOrUnknown,
			InvocationID:              request.InvocationID,
			RequestID:                 request.RequestID,
			ReconciliationRequired:    true,
		}
		return result, result.ErrorInfo
	}
	port.observeFn = func(invocationID kernel.InvocationID) (kernel.LogicalInvocationObservation, error) {
		result := successfulResult(kernel.LogicalExecutionRequest{InvocationID: invocationID, RequestID: item.RequestID})
		return kernel.LogicalInvocationObservation{
			LogicalExecutionResult: result,
			State:                  kernel.InvocationObservationEndedReleased,
		}, nil
	}

	projection, err := orchestrator.Execute(context.Background(), item)
	if err != nil {
		t.Fatalf("Execute() error = %v, want definitive observation to supersede UNKNOWN", err)
	}
	if projection.State != WorkStateSucceeded || projection.Status != kernel.LogicalExecutionCompleted {
		t.Fatalf("projection = %#v, want SUCCEEDED/COMPLETED", projection)
	}
	if !projection.ObserveInvocationCalled || !projection.InvocationObservationAvailable {
		t.Fatalf("projection = %#v, want one observation snapshot", projection)
	}
	requests, observations := port.calls()
	if requests != 1 || observations != 1 {
		t.Fatalf("port calls = request %d observe %d, want 1/1", requests, observations)
	}

	// The cached projection is terminal and cannot trigger another request or
	// observation when the same work item is read again.
	second, err := orchestrator.Execute(context.Background(), item)
	if err != nil || second.State != WorkStateSucceeded {
		t.Fatalf("duplicate projection = %#v, error = %v, want cached success", second, err)
	}
	requests, observations = port.calls()
	if requests != 1 || observations != 1 {
		t.Fatalf("cached port calls = request %d observe %d, want 1/1", requests, observations)
	}
}

func TestOrchestratorMalformedResultIdentityDoesNotObserve(t *testing.T) {
	item := validWorkItem(t, "child-malformed-result")
	otherInvocationID, err := kernel.NewInvocationID()
	if err != nil {
		t.Fatalf("NewInvocationID() error = %v", err)
	}
	port := &fakeExecutionPort{}
	port.requestFn = func(request kernel.LogicalExecutionRequest) (kernel.LogicalExecutionResult, error) {
		result := successfulResult(request)
		result.InvocationID = otherInvocationID
		result.Status = kernel.LogicalExecutionUnknown
		result.CurrentOccupancy = kernel.CurrentOccupancyUnknown
		result.Lifecycle = kernel.AllocationRunning
		result.ReconciliationRequired = true
		result.ErrorInfo = &kernel.LogicalErrorInfo{
			Category:               kernel.LogicalErrorExecutionUnresolved,
			Phase:                  kernel.LogicalPhasePostProvider,
			InvocationID:           otherInvocationID,
			RequestID:              request.RequestID,
			ReconciliationRequired: true,
		}
		return result, result.ErrorInfo
	}

	projection, err := NewOrchestrator(port).Execute(context.Background(), item)
	if err == nil || !errors.Is(err, ErrInvalidKernelResult) {
		t.Fatalf("Execute() error = %v, want ErrInvalidKernelResult", err)
	}
	if projection.State != WorkStateUnknown || !projection.ReconciliationRequired {
		t.Fatalf("projection = %#v, want UNKNOWN/reconciliation", projection)
	}
	if projection.ObserveInvocationCalled || projection.InvocationObservationAvailable {
		t.Fatalf("projection = %#v, malformed result must not trigger observation", projection)
	}
	requests, observations := port.calls()
	if requests != 1 || observations != 0 {
		t.Fatalf("port calls = request %d observe %d, want 1/0", requests, observations)
	}
}

func TestOrchestratorUnknownObservationRemainsUnknownWithoutRetry(t *testing.T) {
	item := validWorkItem(t, "child-unknown")
	port := &fakeExecutionPort{}
	port.requestFn = func(request kernel.LogicalExecutionRequest) (kernel.LogicalExecutionResult, error) {
		result := successfulResult(request)
		result.Status = kernel.LogicalExecutionUnknown
		result.CurrentOccupancy = kernel.CurrentOccupancyUnknown
		result.Lifecycle = kernel.AllocationRunning
		result.Observation.Outcome = kernel.ExecutionOutcomeUnknown
		result.Observation.Occupancy = kernel.OccupancyConclusionUnknown
		result.ReconciliationRequired = true
		result.RetrySafetyFact = kernel.RetrySafetyExplicitSafe
		return result, nil
	}
	port.observeFn = func(invocationID kernel.InvocationID) (kernel.LogicalInvocationObservation, error) {
		result := successfulResult(kernel.LogicalExecutionRequest{InvocationID: invocationID, RequestID: item.RequestID})
		result.Status = kernel.LogicalExecutionUnknown
		result.CurrentOccupancy = kernel.CurrentOccupancyUnknown
		result.Lifecycle = kernel.AllocationRunning
		result.Observation.Outcome = kernel.ExecutionOutcomeUnknown
		result.Observation.Occupancy = kernel.OccupancyConclusionUnknown
		result.ReconciliationRequired = true
		result.RetrySafetyFact = kernel.RetrySafetyUnsafeOrUnknown
		return kernel.LogicalInvocationObservation{
			LogicalExecutionResult: result,
			State:                  kernel.InvocationObservationUnresolvedUnknown,
		}, nil
	}

	projection, err := NewOrchestrator(port).Execute(context.Background(), item)
	if err != nil {
		t.Fatalf("Execute() error = %v, want unresolved value without fabricated error", err)
	}
	if projection.State != WorkStateUnknown || !projection.ReconciliationRequired || projection.RetrySafetyFact != kernel.RetrySafetyUnsafeOrUnknown {
		t.Fatalf("projection = %#v, want UNKNOWN/reconciliation", projection)
	}
	requests, observations := port.calls()
	if requests != 1 || observations != 1 {
		t.Fatalf("port calls = request %d observe %d, want 1/1", requests, observations)
	}
}

func TestOrchestratorDoesNotRetryExplicitlySafeFact(t *testing.T) {
	item := validWorkItem(t, "child-safe-fact")
	port := &fakeExecutionPort{requestFn: func(request kernel.LogicalExecutionRequest) (kernel.LogicalExecutionResult, error) {
		result := successfulResult(request)
		result.Status = kernel.LogicalExecutionUnknown
		result.CurrentOccupancy = kernel.CurrentOccupancyUnknown
		result.Lifecycle = kernel.AllocationRunning
		result.ReconciliationRequired = false
		result.RetrySafetyFact = kernel.RetrySafetyExplicitSafe
		return result, nil
	}}
	projection, err := NewOrchestrator(port).Execute(context.Background(), item)
	if err != nil || projection.State != WorkStateUnknown || projection.RetrySafetyFact != kernel.RetrySafetyExplicitSafe {
		t.Fatalf("projection = %#v, error = %v, want UNKNOWN with fact only", projection, err)
	}
	requests, _ := port.calls()
	if requests != 1 {
		t.Fatalf("RequestExecution calls = %d, want 1", requests)
	}
}

func TestOrchestratorEnforcesOneShotChildWorkItem(t *testing.T) {
	port := &fakeExecutionPort{}
	orchestrator := NewOrchestrator(port)
	item := validWorkItem(t, "child-one-shot")
	if _, err := orchestrator.Execute(context.Background(), item); err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	second := item
	second.WorkItemID = "work-child-one-shot-2"
	second.RequestID = "request-child-one-shot-2"
	second.Operation = "different-operation"
	projection, err := orchestrator.Execute(context.Background(), second)
	if err == nil || !errors.Is(err, ErrWorkItemAlreadySubmitted) {
		t.Fatalf("second Execute() error = %v, want ErrWorkItemAlreadySubmitted", err)
	}
	if projection.State != WorkStateRejected {
		t.Fatalf("second state = %s, want REJECTED", projection.State)
	}
	requests, _ := port.calls()
	if requests != 1 {
		t.Fatalf("RequestExecution calls = %d, want 1", requests)
	}
}

func TestOrchestratorSameBindingIsIdempotentAndRequestIDIsCorrelationOnly(t *testing.T) {
	port := &fakeExecutionPort{}
	orchestrator := NewOrchestrator(port)
	item := validWorkItem(t, "child-idempotent")
	first, err := orchestrator.Execute(context.Background(), item)
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	secondItem := item
	secondItem.RequestID = "request-replay"
	second, err := orchestrator.Execute(context.Background(), secondItem)
	if err != nil {
		t.Fatalf("same-binding Execute() error = %v", err)
	}
	if second.State != first.State || second.ExecutionAllocationID != first.ExecutionAllocationID || second.RequestID != "request-replay" {
		t.Fatalf("duplicate projection = %#v, want same result with new correlation", second)
	}
	requests, observations := port.calls()
	if requests != 1 || observations != 0 {
		t.Fatalf("port calls = request %d observe %d, want 1/0", requests, observations)
	}
}

func TestOrchestratorSameWorkItemIDReplayPreservesCurrentCorrelation(t *testing.T) {
	port := &fakeExecutionPort{}
	orchestrator := NewOrchestrator(port)
	item := validWorkItem(t, "child-same-work-item-replay")
	first, err := orchestrator.Execute(context.Background(), item)
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	second, err := orchestrator.Execute(context.Background(), item)
	if err != nil {
		t.Fatalf("same WorkItemID replay error = %v", err)
	}
	if second.WorkItemID != item.WorkItemID || second.RequestID != item.RequestID || second.InvocationID != first.InvocationID || second.ExecutionAllocationID != first.ExecutionAllocationID {
		t.Fatalf("replay projection = %#v, want current correlation and stored execution facts", second)
	}
	requests, observations := port.calls()
	if requests != 1 || observations != 0 {
		t.Fatalf("port calls = request %d observe %d, want 1/0", requests, observations)
	}
}

func TestOrchestratorDifferentWorkItemIDSameBindingUsesCurrentCorrelation(t *testing.T) {
	port := &fakeExecutionPort{}
	orchestrator := NewOrchestrator(port)
	firstItem := validWorkItem(t, "child-different-work-item-replay")
	first, err := orchestrator.Execute(context.Background(), firstItem)
	if err != nil {
		t.Fatalf("first Execute() error = %v", err)
	}
	secondItem := firstItem
	secondItem.WorkItemID = "work-child-different-work-item-replay-2"
	secondItem.RequestID = "request-child-different-work-item-replay-2"
	second, err := orchestrator.Execute(context.Background(), secondItem)
	if err != nil {
		t.Fatalf("different WorkItemID replay error = %v", err)
	}
	if second.WorkItemID != secondItem.WorkItemID || second.RequestID != secondItem.RequestID {
		t.Fatalf("replay correlation = %q/%q, want %q/%q", second.WorkItemID, second.RequestID, secondItem.WorkItemID, secondItem.RequestID)
	}
	if second.InvocationID != first.InvocationID || second.ExecutionAllocationID != first.ExecutionAllocationID || second.Status != first.Status {
		t.Fatalf("replay execution facts = %#v, want stored facts from first call", second)
	}
	requests, observations := port.calls()
	if requests != 1 || observations != 0 {
		t.Fatalf("port calls = request %d observe %d, want 1/0", requests, observations)
	}
}

func TestOrchestratorConcurrentDuplicateExecutionUsesSingleRequest(t *testing.T) {
	port := &fakeExecutionPort{entered: make(chan struct{}), release: make(chan struct{})}
	orchestrator := NewOrchestrator(port)
	firstItem := validWorkItem(t, "child-concurrent-duplicate")
	secondItem := firstItem
	secondItem.WorkItemID = "work-child-concurrent-duplicate-2"
	secondItem.RequestID = "request-child-concurrent-duplicate-2"

	type outcome struct {
		projection ExecutionProjection
		err        error
	}
	firstDone := make(chan outcome, 1)
	go func() {
		projection, err := orchestrator.Execute(context.Background(), firstItem)
		firstDone <- outcome{projection: projection, err: err}
	}()

	select {
	case <-port.entered:
	case <-time.After(time.Second):
		t.Fatal("first RequestExecution did not start")
	}

	secondDone := make(chan outcome, 1)
	go func() {
		projection, err := orchestrator.Execute(context.Background(), secondItem)
		secondDone <- outcome{projection: projection, err: err}
	}()

	// Release the only Kernel call.  The duplicate must receive the same
	// completed record, rather than entering the port a second time.
	close(port.release)

	var first, second outcome
	select {
	case first = <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first Execute did not return")
	}
	select {
	case second = <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent duplicate Execute did not return")
	}
	if first.err != nil || second.err != nil {
		t.Fatalf("concurrent errors = %v / %v, want nil", first.err, second.err)
	}
	if first.projection.InvocationID != second.projection.InvocationID {
		t.Fatalf("invocation IDs = %s / %s, want one shared Kernel invocation", first.projection.InvocationID, second.projection.InvocationID)
	}
	if second.projection.WorkItemID != secondItem.WorkItemID || second.projection.RequestID != secondItem.RequestID {
		t.Fatalf("duplicate correlation = %q/%q, want %q/%q", second.projection.WorkItemID, second.projection.RequestID, secondItem.WorkItemID, secondItem.RequestID)
	}
	requests, observations := port.calls()
	if requests != 1 || observations != 0 {
		t.Fatalf("port calls = request %d observe %d, want 1/0", requests, observations)
	}
}

func TestOrchestratorPreservesInvocationConflictAcrossWorkReferences(t *testing.T) {
	firstItem := validWorkItem(t, "child-conflict-first")
	secondItem := firstItem
	secondItem.ChildTaskRef = "child-conflict-second"
	secondItem.WorkItemID = "work-child-conflict-second"
	port := &fakeExecutionPort{requestFn: func(request kernel.LogicalExecutionRequest) (kernel.LogicalExecutionResult, error) {
		if request.ChildTaskRef == firstItem.ChildTaskRef {
			return successfulResult(request), nil
		}
		result := kernel.LogicalExecutionResult{
			InvocationID:              request.InvocationID,
			RequestID:                 request.RequestID,
			Status:                    kernel.LogicalExecutionRejected,
			ProviderCrossingAvailable: false,
			RetrySafetyFact:           kernel.RetrySafetyUnsafeOrUnknown,
		}
		result.ErrorInfo = &kernel.LogicalErrorInfo{
			Category:        kernel.LogicalErrorInvocationConflict,
			Phase:           kernel.LogicalPhaseRequestValidation,
			InvocationID:    request.InvocationID,
			RequestID:       request.RequestID,
			RetrySafetyFact: kernel.RetrySafetyUnsafeOrUnknown,
		}
		return result, result.ErrorInfo
	}}
	orchestrator := NewOrchestrator(port)
	first, err := orchestrator.Execute(context.Background(), firstItem)
	if err != nil || first.State != WorkStateSucceeded {
		t.Fatalf("first projection = %#v, error = %v, want success", first, err)
	}
	second, err := orchestrator.Execute(context.Background(), secondItem)
	var info *kernel.LogicalErrorInfo
	if err == nil || !errors.As(err, &info) || info.Category != kernel.LogicalErrorInvocationConflict {
		t.Fatalf("conflicting projection = %#v, error = %v, want INVOCATION_CONFLICT", second, err)
	}
	if second.State != WorkStateRejected || second.ExecutionAllocationID != "" {
		t.Fatalf("conflicting projection = %#v, want rejected without fabricated allocation", second)
	}
	if first.State != WorkStateSucceeded || first.ExecutionAllocationID == "" {
		t.Fatalf("original projection changed after conflict: %#v", first)
	}
	requests, observations := port.calls()
	if requests != 2 || observations != 0 {
		t.Fatalf("port calls = request %d observe %d, want 2/0", requests, observations)
	}
}

func TestOrchestratorPreSubmitCancellationMakesZeroKernelCalls(t *testing.T) {
	port := &fakeExecutionPort{}
	orchestrator := NewOrchestrator(port)
	item := validWorkItem(t, "child-cancelled")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	projection, err := orchestrator.Execute(ctx, item)
	if err == nil || !errors.Is(err, ErrCancelledBeforeSubmit) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want cancellation errors", err)
	}
	if projection.State != WorkStateCancelledBeforeSubmit {
		t.Fatalf("state = %s, want CANCELLED_BEFORE_SUBMIT", projection.State)
	}
	requests, observations := port.calls()
	if requests != 0 || observations != 0 {
		t.Fatalf("port calls = request %d observe %d, want 0/0", requests, observations)
	}
	active, err := orchestrator.Execute(context.Background(), item)
	if err == nil || !errors.Is(err, ErrCancelledBeforeSubmit) || active.State != WorkStateCancelledBeforeSubmit {
		t.Fatalf("replay after pre-submit cancellation = %#v, error = %v, want cached cancellation", active, err)
	}
	requests, observations = port.calls()
	if requests != 0 || observations != 0 {
		t.Fatalf("resubmission crossed the port: request %d observe %d", requests, observations)
	}
}

func TestOrchestratorCancellationAfterCallStartWaitsForKernelResult(t *testing.T) {
	port := &fakeExecutionPort{entered: make(chan struct{}), release: make(chan struct{})}
	orchestrator := NewOrchestrator(port)
	item := validWorkItem(t, "child-late-cancel")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type outcome struct {
		projection ExecutionProjection
		err        error
	}
	done := make(chan outcome, 1)
	go func() {
		projection, err := orchestrator.Execute(ctx, item)
		done <- outcome{projection: projection, err: err}
	}()
	<-port.entered
	cancel()
	close(port.release)
	result := <-done
	if result.err != nil || result.projection.State != WorkStateSucceeded {
		t.Fatalf("late-cancel result = %#v, error = %v, want terminal success", result.projection, result.err)
	}
	if result.projection.CurrentOccupancy != kernel.CurrentOccupancyEnded {
		t.Fatalf("late-cancel occupancy = %s, want ENDED from Kernel only", result.projection.CurrentOccupancy)
	}
	requests, observations := port.calls()
	if requests != 1 || observations != 0 {
		t.Fatalf("port calls = request %d observe %d, want 1/0", requests, observations)
	}
}

func TestOrchestratorIntegrationWithLogicalExecutionFacade(t *testing.T) {
	provider := &integrationProvider{}
	k := kernel.New(provider)
	resourceObject, err := k.Resources.CreateResource("local")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	declaration, err := k.Capabilities.RegisterDeclaration(capability.DeclarationSpec{
		ResourceID: resourceObject.ID,
		Name:       "test.execute",
		Version:    "v1",
	})
	if err != nil {
		t.Fatalf("RegisterDeclaration() error = %v", err)
	}
	instance, err := k.Capabilities.CreateInstance(declaration.ID)
	if err != nil {
		t.Fatalf("CreateInstance() error = %v", err)
	}
	executionContext, err := k.Executions.CreateContext("subject", "local")
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
	invocationID, err := kernel.NewInvocationID()
	if err != nil {
		t.Fatalf("NewInvocationID() error = %v", err)
	}
	item := ExecutionWorkItem{
		WorkItemID:         "work-integration",
		RootTaskRef:        "root-integration",
		ChildTaskRef:       "child-integration",
		RequestID:          "request-integration",
		InvocationID:       invocationID,
		ExecutionContextID: executionContext.ID,
		CapabilityHandleID: handle.ID,
		CallerIdentity:     executionContext.Subject,
		Scope:              executionContext.Scope,
		Operation:          "execute",
		Payload:            []byte("integration-payload"),
	}

	projection, err := NewOrchestrator(facade).Execute(context.Background(), item)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if projection.State != WorkStateSucceeded || projection.Status != kernel.LogicalExecutionCompleted || projection.ExecutionAllocationID == "" {
		t.Fatalf("projection = %#v, want one terminal Kernel execution", projection)
	}
	if projection.ProviderCrossing != kernel.ProviderCrossingCrossed || projection.CurrentOccupancy != kernel.CurrentOccupancyEnded || projection.Lifecycle != kernel.AllocationReleased {
		t.Fatalf("projection facts = %#v, want crossed and definitive ENDED", projection)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}

	duplicate := item
	duplicate.RequestID = "request-integration-replay"
	if _, err := NewOrchestrator(facade).Execute(context.Background(), duplicate); err != nil {
		t.Fatalf("new orchestrator duplicate through same facade error = %v", err)
	}
	if provider.calls != 1 {
		t.Fatalf("duplicate crossed provider again: calls = %d", provider.calls)
	}
}

type integrationProvider struct {
	calls int
}

func (provider *integrationProvider) Invoke(operation capability.ProviderOperation) error {
	provider.calls++
	return nil
}

var _ ExecutionKernelPort = (*fakeExecutionPort)(nil)
var _ ExecutionKernelPort = (*kernel.LogicalExecutionFacade)(nil)

// Keep the execution package imported in this test file as a compile-time
// reminder that the integration path is using Kernel's existing observations,
// not a userspace-defined outcome vocabulary.
var _ = execution.ExecutionOutcomeSuccess
