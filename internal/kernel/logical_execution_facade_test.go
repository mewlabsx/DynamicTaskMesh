package kernel_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"dtm/internal/kernel"
	"dtm/internal/kernel/event"
	"dtm/internal/kernel/execution"
)

func newLogicalFacade(t *testing.T, fixture ownershipFixture, source ...kernel.TrustedResolutionSource) *kernel.LogicalExecutionFacade {
	t.Helper()
	facade, err := kernel.NewLogicalExecutionFacade(fixture.kernel, source...)
	if err != nil {
		t.Fatalf("NewLogicalExecutionFacade() error = %v", err)
	}
	return facade
}

func logicalRequest(t *testing.T, fixture ownershipFixture, invocationID kernel.InvocationID, operation, payload string) kernel.LogicalExecutionRequest {
	t.Helper()
	return kernel.LogicalExecutionRequest{
		InvocationID:       invocationID,
		ExecutionContextID: fixture.context.ID,
		CapabilityHandleID: fixture.handle.ID,
		CallerIdentity:     fixture.context.Subject,
		Scope:              fixture.context.Scope,
		Operation:          operation,
		Payload:            []byte(payload),
	}
}

func newLogicalInvocationID(t *testing.T) kernel.InvocationID {
	t.Helper()
	id, err := kernel.NewInvocationID()
	if err != nil {
		t.Fatalf("NewInvocationID() error = %v", err)
	}
	return id
}

func requireLogicalCategory(t *testing.T, result kernel.LogicalExecutionResult, err error, want kernel.LogicalErrorCategory) {
	t.Helper()
	if err == nil {
		t.Fatalf("error = nil, want %s; result = %#v", want, result)
	}
	var info *kernel.LogicalErrorInfo
	if !errors.As(err, &info) {
		t.Fatalf("error = %T %v, want typed LogicalErrorInfo", err, err)
	}
	if info.Category != want || result.ErrorInfo == nil || result.ErrorInfo.Category != want || info.ProviderCrossingAvailable != result.ProviderCrossingAvailable || info.ProviderCrossing != result.ProviderCrossing {
		t.Fatalf("typed error = %#v, result.ErrorInfo = %#v, want category %s", info, result.ErrorInfo, want)
	}
}

func requireLogicalResolutionCategory(t *testing.T, result kernel.LogicalResolutionResult, err error, want kernel.LogicalErrorCategory) {
	t.Helper()
	if err == nil {
		t.Fatalf("resolution error = nil, want %s; result = %#v", want, result)
	}
	var info *kernel.LogicalErrorInfo
	if !errors.As(err, &info) {
		t.Fatalf("resolution error = %T %v, want typed LogicalErrorInfo", err, err)
	}
	if info.Category != want || result.ErrorInfo == nil || result.ErrorInfo.Category != want || info.ProviderCrossingAvailable != result.ProviderCrossingAvailable || info.ProviderCrossing != result.ProviderCrossing {
		t.Fatalf("resolution typed error = %#v, result.ErrorInfo = %#v, want category %s", info, result.ErrorInfo, want)
	}
}

func TestLogicalExecutionFacadeMissingInvocationIDFailsClosed(t *testing.T) {
	provider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	request := logicalRequest(t, fixture, kernel.InvocationID{}, "execute", "payload")

	result, err := facade.RequestExecution(request)
	requireLogicalCategory(t, result, err, kernel.LogicalErrorInvalidRequest)
	if result.ProviderCrossingAvailable || result.ProviderCrossing != "" {
		t.Fatalf("ProviderCrossingAvailable=%v ProviderCrossing=%q, want unavailable crossing fact", result.ProviderCrossingAvailable, result.ProviderCrossing)
	}
	if provider.Calls() != 0 || len(fixture.kernel.ListExecutionAllocations()) != 0 {
		t.Fatalf("missing InvocationID crossed a side-effect boundary: provider calls=%d allocations=%d", provider.Calls(), len(fixture.kernel.ListExecutionAllocations()))
	}
}

func TestLogicalExecutionFacadeSequentialDuplicateAndRequestIDExclusion(t *testing.T) {
	provider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	id := newLogicalInvocationID(t)
	firstRequest := logicalRequest(t, fixture, id, "execute", "payload")
	firstRequest.RequestID = "request-1"
	first, err := facade.RequestExecution(firstRequest)
	if err != nil || first.Status != kernel.LogicalExecutionCompleted {
		t.Fatalf("first RequestExecution() = %#v, error = %v, want COMPLETED", first, err)
	}

	secondRequest := firstRequest
	secondRequest.RequestID = "request-2"
	second, err := facade.RequestExecution(secondRequest)
	if err != nil || second.Status != kernel.LogicalExecutionCompleted {
		t.Fatalf("same-binding RequestExecution() = %#v, error = %v, want recovered COMPLETED", second, err)
	}
	if second.RequestID != "request-2" || second.ExecutionAllocationID != first.ExecutionAllocationID {
		t.Fatalf("duplicate correlation = %#v, want current RequestID and original allocation", second)
	}
	if provider.Calls() != 1 || len(fixture.kernel.ListExecutionAllocations()) != 1 {
		t.Fatalf("duplicate created a second side effect: provider calls=%d allocations=%d", provider.Calls(), len(fixture.kernel.ListExecutionAllocations()))
	}
}

func TestLogicalExecutionFacadeBindingConflictsDoNotMutateExistingInvocation(t *testing.T) {
	provider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	id := newLogicalInvocationID(t)
	base := logicalRequest(t, fixture, id, "execute", "payload")
	if _, err := facade.RequestExecution(base); err != nil {
		t.Fatalf("initial RequestExecution() error = %v", err)
	}
	variants := []struct {
		name   string
		mutate func(*kernel.LogicalExecutionRequest)
	}{
		{name: "operation", mutate: func(request *kernel.LogicalExecutionRequest) { request.Operation = "other-operation" }},
		{name: "payload", mutate: func(request *kernel.LogicalExecutionRequest) { request.Payload = []byte("different-payload") }},
		{name: "root task", mutate: func(request *kernel.LogicalExecutionRequest) { request.RootTaskRef = "root-2" }},
		{name: "child task", mutate: func(request *kernel.LogicalExecutionRequest) { request.ChildTaskRef = "child-2" }},
		{name: "execution context", mutate: func(request *kernel.LogicalExecutionRequest) {
			request.ExecutionContextID = kernel.ExecutionContextID("execution-context-other")
		}},
		{name: "capability handle", mutate: func(request *kernel.LogicalExecutionRequest) {
			request.CapabilityHandleID = kernel.CapabilityHandleID("capability-handle-other")
		}},
	}
	for _, variant := range variants {
		t.Run(variant.name, func(t *testing.T) {
			request := base
			variant.mutate(&request)
			result, err := facade.RequestExecution(request)
			requireLogicalCategory(t, result, err, kernel.LogicalErrorInvocationConflict)
			if result.ExecutionAllocationID != "" || result.ProviderCrossingAvailable || result.ProviderCrossing != "" {
				t.Fatalf("conflict result = %#v, must not fabricate allocation or crossing fact", result)
			}
		})
	}
	if provider.Calls() != 1 || len(fixture.kernel.ListExecutionAllocations()) != 1 {
		t.Fatalf("conflicts mutated existing invocation: provider calls=%d allocations=%d", provider.Calls(), len(fixture.kernel.ListExecutionAllocations()))
	}
}

func TestLogicalExecutionFacadePayloadDefensiveCopy(t *testing.T) {
	provider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	id := newLogicalInvocationID(t)
	payload := []byte("original")
	request := logicalRequest(t, fixture, id, "execute", string(payload))
	request.Payload = payload
	if _, err := facade.RequestExecution(request); err != nil {
		t.Fatalf("initial RequestExecution() error = %v", err)
	}
	payload[0] = 'X'

	unchanged := request
	unchanged.Payload = []byte("original")
	if result, err := facade.RequestExecution(unchanged); err != nil || result.ExecutionAllocationID == "" {
		t.Fatalf("duplicate after caller payload mutation = %#v, error = %v, want original binding recovery", result, err)
	}
	mutated := request
	mutated.Payload = payload
	result, err := facade.RequestExecution(mutated)
	requireLogicalCategory(t, result, err, kernel.LogicalErrorInvocationConflict)
	if provider.Calls() != 1 {
		t.Fatalf("payload mutation caused a second Provider call: %d", provider.Calls())
	}
}

func TestLogicalExecutionFacadeConcurrentSameIDCrossesProviderOnce(t *testing.T) {
	provider := &ownershipProvider{entered: make(chan struct{}), release: make(chan struct{})}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	id := newLogicalInvocationID(t)
	request := logicalRequest(t, fixture, id, "execute", "payload")
	results := make(chan struct {
		result kernel.LogicalExecutionResult
		err    error
	}, 2)
	go func() {
		result, err := facade.RequestExecution(request)
		results <- struct {
			result kernel.LogicalExecutionResult
			err    error
		}{result, err}
	}()
	select {
	case <-provider.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Provider was not entered")
	}
	go func() {
		result, err := facade.RequestExecution(request)
		results <- struct {
			result kernel.LogicalExecutionResult
			err    error
		}{result, err}
	}()
	select {
	case <-results:
		t.Fatal("same-binding caller returned before the first Provider call completed")
	case <-time.After(50 * time.Millisecond):
	}
	close(provider.release)
	first := <-results
	second := <-results
	if first.err != nil || second.err != nil || first.result.ExecutionAllocationID != second.result.ExecutionAllocationID {
		t.Fatalf("concurrent same-binding results = %#v / %#v, want one recovered allocation", first, second)
	}
	if provider.Calls() != 1 || len(fixture.kernel.ListExecutionAllocations()) != 1 {
		t.Fatalf("concurrent same ID crossed more than once: provider calls=%d allocations=%d", provider.Calls(), len(fixture.kernel.ListExecutionAllocations()))
	}
}

func TestLogicalExecutionFacadeConcurrentConflictingIDReturnsConflict(t *testing.T) {
	provider := &ownershipProvider{entered: make(chan struct{}), release: make(chan struct{})}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	id := newLogicalInvocationID(t)
	firstRequest := logicalRequest(t, fixture, id, "execute", "payload")
	firstResult := make(chan error, 1)
	go func() {
		_, err := facade.RequestExecution(firstRequest)
		firstResult <- err
	}()
	select {
	case <-provider.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Provider was not entered")
	}
	conflicting := firstRequest
	conflicting.Payload = []byte("different")
	result, err := facade.RequestExecution(conflicting)
	requireLogicalCategory(t, result, err, kernel.LogicalErrorInvocationConflict)
	if provider.Calls() != 1 {
		t.Fatalf("conflicting concurrent caller changed Provider calls: %d", provider.Calls())
	}
	close(provider.release)
	if err := <-firstResult; err != nil {
		t.Fatalf("first RequestExecution() error = %v", err)
	}
}

func TestLogicalExecutionFacadeResponseLossIsObservableWithoutReplay(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("provider response lost")}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	id := newLogicalInvocationID(t)
	request := logicalRequest(t, fixture, id, "execute", "payload")
	first, firstErr := facade.RequestExecution(request)
	requireLogicalCategory(t, first, firstErr, kernel.LogicalErrorExecutionUnresolved)
	if first.ProviderCrossing != kernel.ProviderCrossingCrossed || first.CurrentOccupancy != kernel.CurrentOccupancyUnknown || !first.ReconciliationRequired || first.RetrySafetyFact != kernel.RetrySafetyUnsafeOrUnknown {
		t.Fatalf("response-loss result = %#v, want crossed UNKNOWN with reconciliation required", first)
	}
	observation, observationErr := facade.ObserveInvocation(id)
	if observationErr == nil || observation.State != kernel.InvocationObservationUnresolvedUnknown {
		t.Fatalf("ObserveInvocation() = %#v, error = %v, want UNRESOLVED_UNKNOWN", observation, observationErr)
	}
	if observation.CurrentOccupancy != kernel.CurrentOccupancyUnknown || observation.Lifecycle != kernel.AllocationRunning || !observation.ReconciliationRequired {
		t.Fatalf("authoritative unknown observation = %#v, want RUNNING/UNKNOWN with claim held", observation)
	}
	duplicate, duplicateErr := facade.RequestExecution(request)
	if duplicateErr == nil || duplicate.Status != kernel.LogicalExecutionUnknown {
		t.Fatalf("duplicate after response loss = %#v, error = %v, want recovered UNKNOWN", duplicate, duplicateErr)
	}
	if provider.Calls() != 1 {
		t.Fatalf("response-loss recovery replayed Provider: %d calls", provider.Calls())
	}
	occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
	if err != nil || occupancy != 1 {
		t.Fatalf("Resource occupancy = %d, error = %v, want held exact claim", occupancy, err)
	}
	if _, err := fixture.kernel.ReleaseExecution(first.ExecutionAllocationID, "must not release UNKNOWN"); !errors.Is(err, kernel.ErrReleaseNotAllowed) {
		t.Fatalf("ReleaseExecution(UNKNOWN) error = %v, want release not allowed", err)
	}
}

func TestLogicalExecutionFacadeDefinitiveProviderFailureIsNotInternalFailure(t *testing.T) {
	provider := &observingOwnershipProvider{
		observation: execution.ExecutionObservation{
			Outcome:   execution.ExecutionOutcomeFailed,
			Occupancy: execution.OccupancyConclusionEnded,
			Reason:    "provider observed a definitive failure",
		},
		err: errors.New("provider diagnostic: command returned failure"),
	}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	result, err := facade.RequestExecution(logicalRequest(t, fixture, newLogicalInvocationID(t), "execute", "payload"))
	if err != nil {
		t.Fatalf("definitive provider failure = %#v, error = %v, want logical failure result without facade error", result, err)
	}
	if result.Status != kernel.LogicalExecutionFailed || !result.ProviderCrossingAvailable || result.ProviderCrossing != kernel.ProviderCrossingCrossed || result.Observation.Outcome != kernel.ExecutionOutcomeFailed || result.Observation.Occupancy != kernel.OccupancyConclusionEnded {
		t.Fatalf("definitive provider failure = %#v, want FAILED/CROSSED with definitive observation", result)
	}
	if result.CurrentOccupancy != kernel.CurrentOccupancyEnded || result.Lifecycle != kernel.AllocationReleased || result.ReconciliationRequired {
		t.Fatalf("definitive provider failure = %#v, want ENDED/RELEASED without reconciliation", result)
	}
	if result.ErrorInfo != nil {
		t.Fatalf("definitive provider failure ErrorInfo = %#v, want nil rather than INTERNAL_FAILURE", result.ErrorInfo)
	}
	if result.Diagnostic == "" {
		t.Fatal("definitive provider failure lost both observation and provider diagnostic")
	}
	claim, claimErr := fixture.kernel.Resources.ExecutionOccupancyForAllocation(fixture.resource.ID, result.ExecutionAllocationID)
	if claimErr != nil || claim {
		t.Fatalf("definitive provider failure exact claim = %v, error = %v, want absent", claim, claimErr)
	}
}

func TestLogicalExecutionFacadeResolutionRequiresTrustedAuthority(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("ambiguous provider result")}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	id := newLogicalInvocationID(t)
	request := logicalRequest(t, fixture, id, "execute", "payload")
	first, err := facade.RequestExecution(request)
	if first.CurrentOccupancy != kernel.CurrentOccupancyUnknown {
		t.Fatalf("initial result = %#v, want UNKNOWN", first)
	}
	if err == nil {
		t.Fatal("initial UNKNOWN result must carry a structured unresolved error")
	}
	resolution, resolutionErr := facade.SubmitOccupancyResolution(kernel.LogicalOccupancyResolutionRequest{InvocationID: id, RequestID: "resolution-1"})
	requireLogicalResolutionCategory(t, resolution, resolutionErr, kernel.LogicalErrorResolutionNotAllowed)
	if !resolution.ProviderCrossingAvailable || resolution.ProviderCrossing != kernel.ProviderCrossingCrossed || !resolution.ReconciliationRequired {
		t.Fatalf("unavailable-authority resolution = %#v, want bounded unresolved resolution fact", resolution)
	}
	occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
	if err != nil || occupancy != 1 {
		t.Fatalf("Resource occupancy after unauthorized resolution = %d, error = %v, want 1", occupancy, err)
	}
	if records := fixture.kernel.Events.Query(event.QueryFilter{EventType: event.ExecutionOccupancyResolved}); len(records) != 0 {
		t.Fatalf("unauthorized resolution emitted %d accepted events", len(records))
	}
}

func trustedSameFenceSource(authority kernel.ExecutionResolutionAuthority) kernel.TrustedResolutionSource {
	return kernel.TrustedResolutionSourceFunc(func(lookup kernel.TrustedResolutionLookup) (kernel.ExecutionOccupancyResolutionRequest, error) {
		return kernel.ExecutionOccupancyResolutionRequest{
			AllocationID: lookup.ExecutionAllocationID,
			ResourceID:   lookup.ResourceID,
			Authority:    authority,
			Evidence: kernel.ResolutionEvidence{
				Occupancy: kernel.OccupancyConclusionEnded,
				Reason:    "independently admitted Resource-side terminal evidence",
			},
		}, nil
	})
}

func TestLogicalExecutionFacadeTrustedSameFenceResolutionReusesPhase2Transition(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("ambiguous provider result")}
	fixture := newOwnershipFixture(t, provider, nil)
	authority, err := fixture.kernel.GetExecutionResolutionAuthority(fixture.resource.ID)
	if err != nil {
		t.Fatalf("independent authority admission error = %v", err)
	}
	facade, err := kernel.NewLogicalExecutionFacade(fixture.kernel, trustedSameFenceSource(authority))
	if err != nil {
		t.Fatalf("trusted facade construction error = %v", err)
	}
	id := newLogicalInvocationID(t)
	request := logicalRequest(t, fixture, id, "execute", "payload")
	first, err := facade.RequestExecution(request)
	if err == nil || first.CurrentOccupancy != kernel.CurrentOccupancyUnknown {
		t.Fatalf("trusted initial UNKNOWN execution = %#v, error = %v", first, err)
	}
	originalObservation := first.Observation

	resolved, resolveErr := facade.SubmitOccupancyResolution(kernel.LogicalOccupancyResolutionRequest{InvocationID: id, RequestID: "resolution-1"})
	if resolveErr != nil || resolved.Status != kernel.LogicalResolutionResolved || resolved.EventID == "" {
		t.Fatalf("SubmitOccupancyResolution() = %#v, error = %v, want RESOLVED with event", resolved, resolveErr)
	}
	if resolved.CurrentOccupancy != kernel.CurrentOccupancyEnded || resolved.Lifecycle != kernel.AllocationReleased {
		t.Fatalf("resolved result = %#v, want ENDED/RELEASED", resolved)
	}
	stored, err := fixture.kernel.GetExecutionAllocation(first.ExecutionAllocationID)
	if err != nil || stored.CurrentOccupancy != kernel.CurrentOccupancyEnded || stored.Lifecycle != kernel.AllocationReleased {
		t.Fatalf("stored allocation after resolution = %#v, error = %v, want ENDED/RELEASED", stored, err)
	}
	claim, err := fixture.kernel.Resources.ExecutionOccupancyForAllocation(fixture.resource.ID, first.ExecutionAllocationID)
	if err != nil || claim {
		t.Fatalf("exact claim after resolution = %v, error = %v, want absent", claim, err)
	}
	afterObservation, err := fixture.kernel.GetExecutionObservation(first.ExecutionAllocationID)
	if err != nil || afterObservation != originalObservation {
		t.Fatalf("observation after resolution = %#v, error = %v, want immutable original UNKNOWN observation", afterObservation, err)
	}
	observed, observeErr := facade.ObserveInvocation(id)
	if observeErr != nil || observed.State != kernel.InvocationObservationEndedReleased || observed.CurrentOccupancy != kernel.CurrentOccupancyEnded || observed.Lifecycle != kernel.AllocationReleased || observed.Observation != originalObservation {
		t.Fatalf("ObserveInvocation(after resolution) = %#v, error = %v, want ENDED_RELEASED with immutable observation", observed, observeErr)
	}
	if observed.Status != kernel.LogicalExecutionUnknown || observed.ErrorInfo != nil || !observed.ReconciliationRequired {
		t.Fatalf("resolved UNKNOWN observation = %#v, must not synthesize business success", observed)
	}
	repeated, repeatedErr := facade.SubmitOccupancyResolution(kernel.LogicalOccupancyResolutionRequest{InvocationID: id, RequestID: "resolution-2"})
	if repeatedErr != nil || repeated.Status != kernel.LogicalResolutionAlreadyResolved {
		t.Fatalf("repeated SubmitOccupancyResolution() = %#v, error = %v, want ALREADY_RESOLVED", repeated, repeatedErr)
	}
	if records := fixture.kernel.Events.Query(event.QueryFilter{EventType: event.ExecutionOccupancyResolved}); len(records) != 1 || records[0].ID != resolved.EventID {
		t.Fatalf("resolution events = %#v, want exactly one original event", records)
	}
	duplicate, duplicateErr := facade.RequestExecution(request)
	if duplicateErr != nil || duplicate.Status != kernel.LogicalExecutionUnknown || duplicate.ProviderCrossing != kernel.ProviderCrossingCrossed {
		t.Fatalf("duplicate after resolution = %#v, error = %v, want recovered UNKNOWN without replay", duplicate, duplicateErr)
	}
	if provider.Calls() != 1 {
		t.Fatalf("resolution recovery crossed Provider %d times", provider.Calls())
	}
}

func TestLogicalExecutionFacadeWrongFenceFailsClosed(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("ambiguous provider result")}
	fixture := newOwnershipFixture(t, provider, nil)
	id := newLogicalInvocationID(t)
	authority, err := fixture.kernel.GetExecutionResolutionAuthority(fixture.resource.ID)
	if err != nil {
		t.Fatalf("authority error = %v", err)
	}
	wrongFence := authority
	wrongFence.Fence = kernel.OwnershipFence("wrong-fence")
	facade := newLogicalFacade(t, fixture, trustedSameFenceSource(wrongFence))
	request := logicalRequest(t, fixture, id, "execute", "payload")
	first, err := facade.RequestExecution(request)
	if err == nil || first.CurrentOccupancy != kernel.CurrentOccupancyUnknown {
		t.Fatalf("initial UNKNOWN execution = %#v, error = %v", first, err)
	}
	resolution, resolutionErr := facade.SubmitOccupancyResolution(kernel.LogicalOccupancyResolutionRequest{InvocationID: id})
	requireLogicalResolutionCategory(t, resolution, resolutionErr, kernel.LogicalErrorResolutionNotAllowed)
	stored, err := fixture.kernel.GetExecutionAllocation(first.ExecutionAllocationID)
	if err != nil || stored.Lifecycle != kernel.AllocationRunning || stored.CurrentOccupancy != kernel.CurrentOccupancyUnknown {
		t.Fatalf("allocation after wrong-fence resolution = %#v, error = %v, want RUNNING/UNKNOWN", stored, err)
	}
	occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
	if err != nil || occupancy != 1 {
		t.Fatalf("Resource occupancy after wrong-fence resolution = %d, error = %v, want 1", occupancy, err)
	}
	if records := fixture.kernel.Events.Query(event.QueryFilter{EventType: event.ExecutionOccupancyResolved}); len(records) != 0 {
		t.Fatalf("wrong-fence resolution emitted %d events", len(records))
	}
}

func TestLogicalExecutionFacadeObserveUsesAuthoritativeStateNotRegistrySnapshot(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("ambiguous provider result")}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	id := newLogicalInvocationID(t)
	request := logicalRequest(t, fixture, id, "execute", "payload")
	first, err := facade.RequestExecution(request)
	if err == nil || first.CurrentOccupancy != kernel.CurrentOccupancyUnknown {
		t.Fatalf("initial UNKNOWN execution = %#v, error = %v", first, err)
	}
	authority, err := fixture.kernel.GetExecutionResolutionAuthority(fixture.resource.ID)
	if err != nil {
		t.Fatalf("authority error = %v", err)
	}
	if _, err := fixture.kernel.ResolveExecutionOccupancy(kernel.ExecutionOccupancyResolutionRequest{
		AllocationID: first.ExecutionAllocationID,
		ResourceID:   fixture.resource.ID,
		Authority:    authority,
		Evidence: kernel.ResolutionEvidence{
			Occupancy: kernel.OccupancyConclusionEnded,
			Reason:    "independent authoritative source",
		},
	}); err != nil {
		t.Fatalf("independent Phase 2 resolution error = %v", err)
	}
	observed, observeErr := facade.ObserveInvocation(id)
	if observeErr != nil || observed.State != kernel.InvocationObservationEndedReleased || observed.CurrentOccupancy != kernel.CurrentOccupancyEnded || observed.Lifecycle != kernel.AllocationReleased {
		t.Fatalf("ObserveInvocation() after external authoritative transition = %#v, error = %v, want ENDED_RELEASED", observed, observeErr)
	}
	if observed.Status != kernel.LogicalExecutionUnknown || observed.ErrorInfo != nil {
		t.Fatalf("external resolution observation = %#v, must not synthesize business success", observed)
	}
}

func TestLogicalExecutionFacadeRecognizesExternalPhase2ResolutionAuthoritatively(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("ambiguous provider result")}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	id := newLogicalInvocationID(t)
	first, firstErr := facade.RequestExecution(logicalRequest(t, fixture, id, "execute", "payload"))
	if firstErr == nil || first.CurrentOccupancy != kernel.CurrentOccupancyUnknown || !first.ProviderCrossingAvailable || first.ProviderCrossing != kernel.ProviderCrossingCrossed {
		t.Fatalf("initial UNKNOWN execution = %#v, error = %v, want crossed UNKNOWN", first, firstErr)
	}
	authority, err := fixture.kernel.GetExecutionResolutionAuthority(fixture.resource.ID)
	if err != nil {
		t.Fatalf("authority error = %v", err)
	}
	if _, err := fixture.kernel.ResolveExecutionOccupancy(kernel.ExecutionOccupancyResolutionRequest{
		AllocationID: first.ExecutionAllocationID,
		ResourceID:   fixture.resource.ID,
		Authority:    authority,
		Evidence: kernel.ResolutionEvidence{
			Occupancy: kernel.OccupancyConclusionEnded,
			Reason:    "external authoritative Phase 2 resolution",
		},
	}); err != nil {
		t.Fatalf("external Phase 2 resolution error = %v", err)
	}
	resolution, resolutionErr := facade.SubmitOccupancyResolution(kernel.LogicalOccupancyResolutionRequest{InvocationID: id, RequestID: "after-external-resolution"})
	if resolutionErr != nil || resolution.Status != kernel.LogicalResolutionAlreadyResolved {
		t.Fatalf("SubmitOccupancyResolution(after external resolution) = %#v, error = %v, want ALREADY_RESOLVED", resolution, resolutionErr)
	}
	if !resolution.ProviderCrossingAvailable || resolution.ProviderCrossing != kernel.ProviderCrossingCrossed || resolution.CurrentOccupancy != kernel.CurrentOccupancyEnded || resolution.Lifecycle != kernel.AllocationReleased {
		t.Fatalf("authoritative already-resolved result = %#v, want available CROSSED/ENDED/RELEASED", resolution)
	}
	if records := fixture.kernel.Events.Query(event.QueryFilter{EventType: event.ExecutionOccupancyResolved}); len(records) != 1 {
		t.Fatalf("external resolution event count = %d, want no second resolution event", len(records))
	}
}

func TestLogicalExecutionFacadePreProviderFailureIsDefiniteNonCrossing(t *testing.T) {
	fixture := newOwnershipFixture(t, nil, nil)
	facade := newLogicalFacade(t, fixture)
	id := newLogicalInvocationID(t)
	result, err := facade.RequestExecution(logicalRequest(t, fixture, id, "execute", "payload"))
	requireLogicalCategory(t, result, err, kernel.LogicalErrorProviderNotAvailable)
	if !result.ProviderCrossingAvailable || result.ProviderCrossing != kernel.ProviderCrossingNotCrossedDefinite || result.CurrentOccupancy != kernel.CurrentOccupancyEnded || result.Lifecycle != kernel.AllocationReleased {
		t.Fatalf("pre-provider failure = %#v, want available NOT_CROSSED_DEFINITE and ENDED/RELEASED", result)
	}
	occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
	if err != nil || occupancy != 0 {
		t.Fatalf("Resource occupancy after pre-provider failure = %d, error = %v, want 0", occupancy, err)
	}
	observed, observeErr := facade.ObserveInvocation(id)
	var observedError *kernel.LogicalErrorInfo
	if observed.State != kernel.InvocationObservationEndedReleased || !errors.As(observeErr, &observedError) {
		t.Fatalf("ObserveInvocation(pre-provider failure) = %#v, error = %v, want terminal typed failure", observed, observeErr)
	}
	resolution, resolutionErr := facade.SubmitOccupancyResolution(kernel.LogicalOccupancyResolutionRequest{InvocationID: id, RequestID: "unnecessary-pre-provider-resolution"})
	requireLogicalResolutionCategory(t, resolution, resolutionErr, kernel.LogicalErrorAlreadyReleased)
	if !resolution.ProviderCrossingAvailable || resolution.ProviderCrossing != kernel.ProviderCrossingNotCrossedDefinite {
		t.Fatalf("pre-provider resolution rejection = %#v, want available NOT_CROSSED_DEFINITE", resolution)
	}
	claim, claimErr := fixture.kernel.Resources.ExecutionOccupancyForAllocation(fixture.resource.ID, result.ExecutionAllocationID)
	if claimErr != nil || claim {
		t.Fatalf("pre-provider resolution changed exact claim = %v, error = %v, want absent", claim, claimErr)
	}
}

func TestLogicalExecutionFacadeCapacityAndAuthorityErrorsAreStructured(t *testing.T) {
	t.Run("capacity unavailable", func(t *testing.T) {
		provider := &ownershipProvider{}
		fixture := newOwnershipFixture(t, provider, nil)
		holder, holderErr := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "holder"))
		if holderErr != nil || !holder.Granted {
			t.Fatalf("TryAllocateExecution(holder) = %#v, error = %v, want one held allocation", holder, holderErr)
		}
		facade := newLogicalFacade(t, fixture)
		result, err := facade.RequestExecution(logicalRequest(t, fixture, newLogicalInvocationID(t), "execute", "second"))
		requireLogicalCategory(t, result, err, kernel.LogicalErrorCapacityUnavailable)
		if !result.ProviderCrossingAvailable || result.ProviderCrossing != kernel.ProviderCrossingNotCrossedDefinite || provider.Calls() != 0 {
			t.Fatalf("capacity rejection = %#v, provider calls = %d, want available NOT_CROSSED_DEFINITE and no Provider call", result, provider.Calls())
		}
		occupancy, occupancyErr := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
		if occupancyErr != nil || occupancy != 1 {
			t.Fatalf("capacity rejection changed Resource occupancy = %d, error = %v, want held capacity 1", occupancy, occupancyErr)
		}
	})

	t.Run("authority rejection", func(t *testing.T) {
		provider := &ownershipProvider{}
		fixture := newOwnershipFixture(t, provider, nil)
		facade := newLogicalFacade(t, fixture)
		badRequest := logicalRequest(t, fixture, newLogicalInvocationID(t), "execute", "bad-authority")
		badRequest.CapabilityHandleID = kernel.CapabilityHandleID("missing-handle")
		result, err := facade.RequestExecution(badRequest)
		requireLogicalCategory(t, result, err, kernel.LogicalErrorInvalidOrStaleAuthority)
		if !result.ProviderCrossingAvailable || result.ProviderCrossing != kernel.ProviderCrossingNotCrossedDefinite || provider.Calls() != 0 {
			t.Fatalf("authority rejection = %#v, provider calls = %d, want available NOT_CROSSED_DEFINITE and no Provider call", result, provider.Calls())
		}
		if len(fixture.kernel.ListExecutionAllocations()) != 0 {
			t.Fatal("authority rejection created an allocation")
		}
	})
}

func TestLogicalExecutionFacadeUnknownInvocationIsNotSafeReplay(t *testing.T) {
	provider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	id := newLogicalInvocationID(t)
	observed, err := facade.ObserveInvocation(id)
	if err != nil || observed.State != kernel.InvocationObservationNotKnown {
		t.Fatalf("ObserveInvocation(unknown) = %#v, error = %v, want NOT_KNOWN", observed, err)
	}
	if observed.ProviderCrossingAvailable || observed.ProviderCrossing != "" || observed.RetrySafetyFact != kernel.RetrySafetyUnsafeOrUnknown || !observed.ReconciliationRequired || provider.Calls() != 0 {
		t.Fatalf("unknown invocation observation = %#v, must not imply safe replay", observed)
	}
}

func TestLogicalExecutionFacadeInvalidResolutionRequestIsStructured(t *testing.T) {
	fixture := newOwnershipFixture(t, &ownershipProvider{}, nil)
	facade := newLogicalFacade(t, fixture)
	result, err := facade.SubmitOccupancyResolution(kernel.LogicalOccupancyResolutionRequest{})
	if err == nil || result.ErrorInfo == nil || result.ErrorInfo.Category != kernel.LogicalErrorInvalidRequest || result.ProviderCrossingAvailable || result.ProviderCrossing != "" {
		t.Fatalf("invalid resolution request = %#v, error = %v, want INVALID_REQUEST with unavailable crossing fact", result, err)
	}
}

func TestLogicalExecutionFacadeSameFenceSourceGetsExactLookup(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("ambiguous provider result")}
	fixture := newOwnershipFixture(t, provider, nil)
	var mu sync.Mutex
	var seen kernel.TrustedResolutionLookup
	authority, err := fixture.kernel.GetExecutionResolutionAuthority(fixture.resource.ID)
	if err != nil {
		t.Fatalf("authority error = %v", err)
	}
	source := kernel.TrustedResolutionSourceFunc(func(lookup kernel.TrustedResolutionLookup) (kernel.ExecutionOccupancyResolutionRequest, error) {
		mu.Lock()
		seen = lookup
		mu.Unlock()
		return trustedRequestForLookup(lookup, authority), nil
	})
	facade := newLogicalFacade(t, fixture, source)
	id := newLogicalInvocationID(t)
	request := logicalRequest(t, fixture, id, "execute", "payload")
	first, firstErr := facade.RequestExecution(request)
	if firstErr == nil || first.CurrentOccupancy != kernel.CurrentOccupancyUnknown {
		t.Fatalf("initial UNKNOWN execution = %#v, error = %v", first, firstErr)
	}
	resolved, resolveErr := facade.SubmitOccupancyResolution(kernel.LogicalOccupancyResolutionRequest{InvocationID: id})
	if resolveErr != nil || resolved.Status != kernel.LogicalResolutionResolved {
		t.Fatalf("trusted resolution = %#v, error = %v", resolved, resolveErr)
	}
	mu.Lock()
	lookup := seen
	mu.Unlock()
	if lookup.InvocationID != id || lookup.ExecutionAllocationID != first.ExecutionAllocationID || lookup.ResourceID != fixture.resource.ID || lookup.OwnershipFence != firstCurrentFence(t, fixture, first.ExecutionAllocationID) || lookup.CurrentOccupancy != kernel.CurrentOccupancyUnknown {
		t.Fatalf("trusted source lookup = %#v, want exact invocation/allocation/resource/fence/UNKNOWN target", lookup)
	}
}

func trustedRequestForLookup(lookup kernel.TrustedResolutionLookup, authority kernel.ExecutionResolutionAuthority) kernel.ExecutionOccupancyResolutionRequest {
	return kernel.ExecutionOccupancyResolutionRequest{
		AllocationID: lookup.ExecutionAllocationID,
		ResourceID:   lookup.ResourceID,
		Authority:    authority,
		Evidence: kernel.ResolutionEvidence{
			Occupancy: kernel.OccupancyConclusionEnded,
			Reason:    "trusted lookup exact target",
		},
	}
}

func firstCurrentFence(t *testing.T, fixture ownershipFixture, allocationID kernel.ExecutionAllocationID) kernel.OwnershipFence {
	t.Helper()
	allocation, err := fixture.kernel.GetExecutionAllocation(allocationID)
	if err != nil {
		t.Fatalf("GetExecutionAllocation() error = %v", err)
	}
	return allocation.OwnershipFence
}

func TestLogicalExecutionFacadeResolutionSourceErrorDoesNotMutateClaim(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("ambiguous provider result")}
	fixture := newOwnershipFixture(t, provider, nil)
	source := kernel.TrustedResolutionSourceFunc(func(kernel.TrustedResolutionLookup) (kernel.ExecutionOccupancyResolutionRequest, error) {
		return kernel.ExecutionOccupancyResolutionRequest{}, fmt.Errorf("source admission unavailable: %w", kernel.ErrTrustedResolutionUnavailable)
	})
	facade := newLogicalFacade(t, fixture, source)
	id := newLogicalInvocationID(t)
	first, firstErr := facade.RequestExecution(logicalRequest(t, fixture, id, "execute", "payload"))
	if firstErr == nil || first.CurrentOccupancy != kernel.CurrentOccupancyUnknown {
		t.Fatalf("initial UNKNOWN execution = %#v, error = %v", first, firstErr)
	}
	resolution, resolutionErr := facade.SubmitOccupancyResolution(kernel.LogicalOccupancyResolutionRequest{InvocationID: id})
	requireLogicalResolutionCategory(t, resolution, resolutionErr, kernel.LogicalErrorResolutionNotAllowed)
	claim, err := fixture.kernel.Resources.ExecutionOccupancyForAllocation(fixture.resource.ID, first.ExecutionAllocationID)
	if err != nil || !claim {
		t.Fatalf("claim after trusted source error = %v, error = %v, want held", claim, err)
	}
}

func TestLogicalExecutionFacadeConstructorRejectsMultipleSources(t *testing.T) {
	fixture := newOwnershipFixture(t, &ownershipProvider{}, nil)
	source := kernel.TrustedResolutionSourceFunc(func(kernel.TrustedResolutionLookup) (kernel.ExecutionOccupancyResolutionRequest, error) {
		return kernel.ExecutionOccupancyResolutionRequest{}, kernel.ErrTrustedResolutionUnavailable
	})
	if _, err := kernel.NewLogicalExecutionFacade(fixture.kernel, source, source); !errors.Is(err, kernel.ErrInvalidLogicalExecutionFacade) {
		t.Fatalf("NewLogicalExecutionFacade(multiple sources) error = %v, want invalid facade", err)
	}
}

func TestLogicalExecutionFacadeNoGenericEventAtomicityClaim(t *testing.T) {
	// The conformance surface only requires the existing Phase 2 transition to
	// publish its one immutable resolution event. Ordinary request/dispatch
	// success is not made dependent on a synthetic generic transaction event.
	provider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)
	facade := newLogicalFacade(t, fixture)
	result, err := facade.RequestExecution(logicalRequest(t, fixture, newLogicalInvocationID(t), "execute", "payload"))
	if err != nil || result.Status != kernel.LogicalExecutionCompleted {
		t.Fatalf("ordinary successful execution = %#v, error = %v", result, err)
	}
	if records := fixture.kernel.Events.Query(event.QueryFilter{EventType: event.ExecutionOccupancyResolved}); len(records) != 0 {
		t.Fatalf("ordinary success emitted %d resolution events, want no synthetic resolution event", len(records))
	}
}
