package kernel_test

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"dtm/internal/kernel"
	"dtm/internal/kernel/capability"
	"dtm/internal/kernel/event"
	"dtm/internal/kernel/execution"
	"dtm/internal/kernel/identity"
	"dtm/internal/kernel/resource"
)

type ownershipFixture struct {
	kernel   *kernel.Kernel
	provider *ownershipProvider
	resource resource.Resource
	context  execution.ExecutionContext
	handle   capability.CapabilityHandle
}

type ownershipProvider struct {
	mu         sync.Mutex
	calls      int
	operations []capability.ProviderOperation
	entered    chan struct{}
	release    chan struct{}
	err        error
}

func (provider *ownershipProvider) Invoke(operation capability.ProviderOperation) error {
	provider.mu.Lock()
	provider.calls++
	provider.operations = append(provider.operations, operation.Clone())
	if provider.entered != nil && provider.calls == 1 {
		close(provider.entered)
	}
	release := provider.release
	err := provider.err
	provider.mu.Unlock()
	if release != nil {
		<-release
	}
	return err
}

func (provider *ownershipProvider) Calls() int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.calls
}

func (provider *ownershipProvider) Operations() []capability.ProviderOperation {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	operations := make([]capability.ProviderOperation, len(provider.operations))
	for index := range provider.operations {
		operations[index] = provider.operations[index].Clone()
	}
	return operations
}

type observingOwnershipProvider struct {
	mu          sync.Mutex
	calls       int
	operations  []capability.ProviderOperation
	observation execution.ExecutionObservation
	err         error
}

func (provider *observingOwnershipProvider) Invoke(operation capability.ProviderOperation) error {
	return nil
}

func (provider *observingOwnershipProvider) InvokeWithObservation(operation capability.ProviderOperation) (execution.ExecutionObservation, error) {
	provider.mu.Lock()
	provider.calls++
	provider.operations = append(provider.operations, operation.Clone())
	observation := provider.observation
	err := provider.err
	provider.mu.Unlock()
	return observation, err
}

func (provider *observingOwnershipProvider) Calls() int {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.calls
}

func newOwnershipFixture(t *testing.T, provider capability.CapabilityProvider, constraints map[string]string) ownershipFixture {
	t.Helper()
	k := kernel.New()
	if provider != nil {
		if err := k.SetProvider(provider); err != nil {
			t.Fatalf("SetProvider() error = %v", err)
		}
	}
	resourceObject, err := k.Resources.CreateResource("local")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	declaration, err := k.Capabilities.RegisterDeclaration(capability.DeclarationSpec{
		ResourceID:  resourceObject.ID,
		Name:        "test.execute",
		Version:     "v1",
		Constraints: constraints,
	})
	if err != nil {
		t.Fatalf("RegisterDeclaration() error = %v", err)
	}
	instance, err := k.Capabilities.CreateInstance(declaration.ID)
	if err != nil {
		t.Fatalf("CreateInstance() error = %v", err)
	}
	context, err := k.Executions.CreateContext("subject", "local")
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}
	handle, err := k.Capabilities.CreateHandle(context.ID, instance.ID, []string{"execute"}, "local")
	if err != nil {
		t.Fatalf("CreateHandle() error = %v", err)
	}
	return ownershipFixture{kernel: k, provider: providerAsOwnershipProvider(provider), resource: resourceObject, context: context, handle: handle}
}

func providerAsOwnershipProvider(provider capability.CapabilityProvider) *ownershipProvider {
	if value, ok := provider.(*ownershipProvider); ok {
		return value
	}
	return nil
}

func (fixture ownershipFixture) request(t *testing.T, operation string, payload string) execution.ExecutionRequest {
	t.Helper()
	invocationID, err := execution.NewInvocationID()
	if err != nil {
		t.Fatalf("NewInvocationID() error = %v", err)
	}
	return execution.ExecutionRequest{
		InvocationID:       invocationID,
		ExecutionContextID: fixture.context.ID,
		CapabilityHandleID: fixture.handle.ID,
		CallerIdentity:     fixture.context.Subject,
		Scope:              fixture.context.Scope,
		Operation:          operation,
		Payload:            []byte(payload),
	}
}

func TestExecutionOwnershipAuthorityBarrier(t *testing.T) {
	provider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)
	request := fixture.request(t, "execute", "payload")
	request.CapabilityHandleID = capability.CapabilityHandleID("missing-handle")

	result, err := fixture.kernel.TryAllocateExecution(request)
	if !errors.Is(err, kernel.ErrAuthorityDenied) {
		t.Fatalf("TryAllocateExecution() error = %v, want authority denied", err)
	}
	if result.Granted || result.Reason != kernel.AllocationReasonAuthorityDenied {
		t.Fatalf("TryAllocateExecution() result = %#v, want authority denial", result)
	}
	if len(fixture.kernel.ListExecutionAllocations()) != 0 {
		t.Fatal("authority failure created an allocation")
	}
	if provider.Calls() != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.Calls())
	}
}

func TestExecutionOwnershipAllocationBarrier(t *testing.T) {
	provider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)

	_, err := fixture.kernel.DispatchExecution(kernel.ExecutionAllocationID("missing-allocation"))
	if !errors.Is(err, kernel.ErrAllocationNotFound) {
		t.Fatalf("DispatchExecution() error = %v, want allocation not found", err)
	}
	if provider.Calls() != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.Calls())
	}
}

func TestExecutionOwnershipDefaultExclusivityAndRelease(t *testing.T) {
	provider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)

	first, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "one"))
	if err != nil || !first.Granted {
		t.Fatalf("first allocation = %#v, error = %v", first, err)
	}
	second, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "two"))
	if !errors.Is(err, kernel.ErrExecutionResourceBusy) {
		t.Fatalf("second allocation error = %v, want resource busy", err)
	}
	if second.Granted || second.Reason != kernel.AllocationReasonResourceBusy {
		t.Fatalf("second allocation = %#v, want RESOURCE_BUSY", second)
	}
	occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
	if err != nil || occupancy != 1 {
		t.Fatalf("occupancy = %d, error = %v, want 1", occupancy, err)
	}

	released, err := fixture.kernel.ReleaseExecution(first.Allocation.ID, "test complete")
	if err != nil || released.Status != kernel.ReleaseStatusReleased {
		t.Fatalf("ReleaseExecution() = %#v, error = %v", released, err)
	}
	allocation, err := fixture.kernel.GetExecutionAllocation(first.Allocation.ID)
	if err != nil || allocation.Lifecycle != kernel.AllocationReleased {
		t.Fatalf("stored allocation = %#v, error = %v, want RELEASED", allocation, err)
	}
	repeatedRelease, err := fixture.kernel.ReleaseExecution(first.Allocation.ID, "repeated release")
	if err != nil || repeatedRelease.Status != kernel.ReleaseStatusAlreadyReleased {
		t.Fatalf("repeated ReleaseExecution() = %#v, error = %v, want ALREADY_RELEASED", repeatedRelease, err)
	}
	occupancy, err = fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
	if err != nil || occupancy != 0 {
		t.Fatalf("occupancy after release = %d, error = %v, want 0", occupancy, err)
	}
}

func TestExecutionOwnershipConcurrentGrantExactlyOne(t *testing.T) {
	fixture := newOwnershipFixture(t, &ownershipProvider{}, nil)
	results := make(chan struct {
		result kernel.TryAllocateResult
		err    error
	}, 2)
	var wait sync.WaitGroup
	for index := 0; index < 2; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			results <- func() (value struct {
				result kernel.TryAllocateResult
				err    error
			}) {
				value.result, value.err = fixture.kernel.TryAllocateExecution(fixture.request(t, fmt.Sprintf("execute-%d", index), "payload"))
				return value
			}()
		}(index)
	}
	wait.Wait()
	close(results)

	granted := 0
	busy := 0
	var grantedAllocation kernel.ExecutionAllocation
	for value := range results {
		if value.err != nil && !errors.Is(value.err, kernel.ErrExecutionResourceBusy) {
			t.Fatalf("concurrent allocation error = %v", value.err)
		}
		if value.result.Granted {
			granted++
			grantedAllocation = value.result.Allocation
		}
		if value.result.Reason == kernel.AllocationReasonResourceBusy {
			busy++
		}
	}
	if granted != 1 || busy != 1 {
		t.Fatalf("concurrent results = granted %d/busy %d, want 1/1", granted, busy)
	}
	if _, err := fixture.kernel.ReleaseExecution(grantedAllocation.ID, "test complete"); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
}

func TestExecutionOwnershipBindingIsImmutable(t *testing.T) {
	provider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)
	request := fixture.request(t, "execute", "original")
	result, err := fixture.kernel.TryAllocateExecution(request)
	if err != nil || !result.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", result, err)
	}
	request.Payload[0] = 'X'
	result.Allocation.Descriptor.Operation = "different"
	result.Allocation.Descriptor.Payload[0] = 'X'
	result.Allocation.Descriptor.ResourceID = resource.ResourceID("different-resource")

	observation, err := fixture.kernel.DispatchExecution(result.Allocation.ID)
	if err != nil || observation.Outcome != kernel.ExecutionOutcomeSuccess {
		t.Fatalf("DispatchExecution() = %#v, error = %v", observation, err)
	}
	operations := provider.Operations()
	if len(operations) != 1 || operations[0].Operation != "execute" || string(operations[0].Payload) != "original" {
		t.Fatalf("provider operations = %#v, want frozen execute/original binding", operations)
	}
	if _, err := fixture.kernel.ReleaseExecution(result.Allocation.ID, "test complete"); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
}

func TestExecutionOwnershipProviderIsResolvedAtDispatch(t *testing.T) {
	firstProvider := &ownershipProvider{}
	secondProvider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, firstProvider, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}
	if err := fixture.kernel.SetProvider(secondProvider); err != nil {
		t.Fatalf("SetProvider() error = %v", err)
	}
	if _, err := fixture.kernel.DispatchExecution(allocation.Allocation.ID); err != nil {
		t.Fatalf("DispatchExecution() error = %v", err)
	}
	if firstProvider.Calls() != 0 || secondProvider.Calls() != 1 {
		t.Fatalf("provider calls = first %d/second %d, want 0/1", firstProvider.Calls(), secondProvider.Calls())
	}
	if _, err := fixture.kernel.ReleaseExecution(allocation.Allocation.ID, "test complete"); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
}

func TestExecutionOwnershipConcurrentDispatchClaimsOnce(t *testing.T) {
	provider := &ownershipProvider{entered: make(chan struct{}), release: make(chan struct{})}
	lateProvider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}

	firstResult := make(chan struct {
		observation kernel.ExecutionObservation
		err         error
	}, 1)
	go func() {
		observation, dispatchErr := fixture.kernel.DispatchExecution(allocation.Allocation.ID)
		firstResult <- struct {
			observation kernel.ExecutionObservation
			err         error
		}{observation, dispatchErr}
	}()
	select {
	case <-provider.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("provider was not entered")
	}
	if err := fixture.kernel.SetProvider(lateProvider); err != nil {
		t.Fatalf("SetProvider() after dispatch claim = %v", err)
	}

	_, secondErr := fixture.kernel.DispatchExecution(allocation.Allocation.ID)
	if !errors.Is(secondErr, kernel.ErrAlreadyDispatched) {
		t.Fatalf("concurrent second dispatch error = %v, want already dispatched", secondErr)
	}
	close(provider.release)
	first := <-firstResult
	if first.err != nil || first.observation.Outcome != kernel.ExecutionOutcomeSuccess {
		t.Fatalf("first dispatch = %#v, error = %v", first.observation, first.err)
	}
	if provider.Calls() != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.Calls())
	}
	if lateProvider.Calls() != 0 {
		t.Fatalf("late provider calls after dispatch snapshot = %d, want 0", lateProvider.Calls())
	}
	if _, err := fixture.kernel.ReleaseExecution(allocation.Allocation.ID, "test complete"); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
}

func TestExecutionOwnershipPreCallFailureHasFailedEndedObservation(t *testing.T) {
	fixture := newOwnershipFixture(t, nil, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}

	observation, err := fixture.kernel.DispatchExecution(allocation.Allocation.ID)
	if !errors.Is(err, kernel.ErrProviderUnavailable) {
		t.Fatalf("DispatchExecution() error = %v, want provider unavailable", err)
	}
	if observation.Outcome != kernel.ExecutionOutcomeFailed || observation.Occupancy != kernel.OccupancyConclusionEnded {
		t.Fatalf("observation = %#v, want FAILED + ENDED", observation)
	}
	stored, err := fixture.kernel.GetExecutionAllocation(allocation.Allocation.ID)
	if err != nil || stored.Lifecycle != kernel.AllocationReleased || stored.CurrentOccupancy != kernel.CurrentOccupancyEnded {
		t.Fatalf("stored allocation = %#v, error = %v, want RELEASED/ENDED", stored, err)
	}
	if occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID); err != nil || occupancy != 0 {
		t.Fatalf("Resource occupancy = %d, error = %v, want 0", occupancy, err)
	}
	release, err := fixture.kernel.ReleaseExecution(allocation.Allocation.ID, "pre-call failure")
	if err != nil || release.Status != kernel.ReleaseStatusAlreadyReleased {
		t.Fatalf("ReleaseExecution() = %#v, error = %v, want ALREADY_RELEASED", release, err)
	}
}

func TestExecutionOwnershipProviderResolvesWhenSetAfterAllocation(t *testing.T) {
	fixture := newOwnershipFixture(t, nil, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}
	lateProvider := &ownershipProvider{}
	if err := fixture.kernel.SetProvider(lateProvider); err != nil {
		t.Fatalf("SetProvider() error = %v", err)
	}

	observation, dispatchErr := fixture.kernel.DispatchExecution(allocation.Allocation.ID)
	if dispatchErr != nil || observation.Outcome != kernel.ExecutionOutcomeSuccess {
		t.Fatalf("DispatchExecution() = %#v, error = %v, want successful late-bound provider dispatch", observation, dispatchErr)
	}
	if lateProvider.Calls() != 1 {
		t.Fatalf("late provider calls = %d, want 1", lateProvider.Calls())
	}
	if _, err := fixture.kernel.ReleaseExecution(allocation.Allocation.ID, "late provider"); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
}

func TestExecutionOwnershipLegacyProviderErrorRemainsUnknown(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("provider result is ambiguous")}
	fixture := newOwnershipFixture(t, provider, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}

	observation, dispatchErr := fixture.kernel.DispatchExecution(allocation.Allocation.ID)
	if !errors.Is(dispatchErr, provider.err) {
		t.Fatalf("DispatchExecution() error = %v, want provider error", dispatchErr)
	}
	if observation.Outcome != kernel.ExecutionOutcomeUnknown || observation.Occupancy != kernel.OccupancyConclusionUnknown {
		t.Fatalf("observation = %#v, want UNKNOWN + UNKNOWN", observation)
	}
	release, releaseErr := fixture.kernel.ReleaseExecution(allocation.Allocation.ID, "ambiguous provider result")
	if !errors.Is(releaseErr, kernel.ErrReleaseNotAllowed) || release.Status != kernel.ReleaseStatusNotAllowed {
		t.Fatalf("ReleaseExecution() = %#v, error = %v, want release not allowed", release, releaseErr)
	}
}

func TestExecutionOwnershipUnknownOutcomeAndOccupancyDoNotRelease(t *testing.T) {
	provider := &observingOwnershipProvider{observation: execution.ExecutionObservation{
		Outcome:   execution.ExecutionOutcomeUnknown,
		Occupancy: execution.OccupancyConclusionUnknown,
		Reason:    "connection lost",
	}}
	fixture := newOwnershipFixture(t, provider, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}

	observation, err := fixture.kernel.DispatchExecution(allocation.Allocation.ID)
	if err != nil {
		t.Fatalf("DispatchExecution() error = %v", err)
	}
	if observation.Outcome != kernel.ExecutionOutcomeUnknown || observation.Occupancy != kernel.OccupancyConclusionUnknown {
		t.Fatalf("observation = %#v, want UNKNOWN + UNKNOWN", observation)
	}
	if provider.Calls() != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.Calls())
	}
	if _, err := fixture.kernel.DispatchExecution(allocation.Allocation.ID); err != nil {
		t.Fatalf("repeated DispatchExecution() error = %v, want stored observation", err)
	}
	release, err := fixture.kernel.ReleaseExecution(allocation.Allocation.ID, "automatic release must not occur")
	if !errors.Is(err, kernel.ErrReleaseNotAllowed) || release.Status != kernel.ReleaseStatusNotAllowed {
		t.Fatalf("ReleaseExecution() = %#v, error = %v, want release not allowed", release, err)
	}
	stored, err := fixture.kernel.GetExecutionAllocation(allocation.Allocation.ID)
	if err != nil || stored.Lifecycle != kernel.AllocationRunning || stored.CurrentOccupancy != kernel.CurrentOccupancyUnknown {
		t.Fatalf("stored allocation = %#v, error = %v, want RUNNING/UNKNOWN", stored, err)
	}
	occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
	if err != nil || occupancy != 1 {
		t.Fatalf("occupancy = %d, error = %v, want 1", occupancy, err)
	}
}

func TestExecutionOwnershipAvailabilityIsSeparateFromRelease(t *testing.T) {
	fixture := newOwnershipFixture(t, &ownershipProvider{}, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}
	if err := fixture.kernel.Resources.UpdateAvailability(fixture.resource.ID, resource.AvailabilityUnavailable); err != nil {
		t.Fatalf("UpdateAvailability() error = %v", err)
	}
	if _, err := fixture.kernel.ReleaseExecution(allocation.Allocation.ID, "release while unavailable"); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
	current, err := fixture.kernel.Resources.GetResource(fixture.resource.ID)
	if err != nil || current.IsAvailable() || current.Availability != resource.AvailabilityUnavailable {
		t.Fatalf("resource after release = %#v, error = %v, want unavailable", current, err)
	}
	result, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if !errors.Is(err, kernel.ErrExecutionResourceUnavailable) || result.Reason != kernel.AllocationReasonResourceUnavailable {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v, want RESOURCE_UNAVAILABLE", result, err)
	}
	if err := fixture.kernel.Resources.UpdateAvailability(fixture.resource.ID, resource.AvailabilityAvailable); err != nil {
		t.Fatalf("restore availability error = %v", err)
	}
}

func TestExecutionOwnershipSupportedConstraint(t *testing.T) {
	fixture := newOwnershipFixture(t, &ownershipProvider{}, map[string]string{kernel.ExecutionOperationConstraintKey: "execute"})
	result, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "other", "payload"))
	if !errors.Is(err, kernel.ErrExecutionConstraintViolation) || result.Reason != kernel.AllocationReasonExecutionConstraintViolation {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v, want constraint violation", result, err)
	}
	if len(fixture.kernel.ListExecutionAllocations()) != 0 {
		t.Fatal("constraint violation created an allocation")
	}
}

func TestExecutionOwnershipAllocationInitialStateAndSnapshotImmutability(t *testing.T) {
	fixture := newOwnershipFixture(t, &ownershipProvider{}, nil)
	result, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !result.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", result, err)
	}
	if result.Allocation.Lifecycle != kernel.AllocationRunning || result.Allocation.CurrentOccupancy != kernel.CurrentOccupancyNotEstablished {
		t.Fatalf("initial allocation = %#v, want RUNNING/NOT_ESTABLISHED", result.Allocation)
	}
	if result.Allocation.OwnershipFence.IsZero() || result.Allocation.OwnershipFence != fixture.resource.CurrentOwnershipFence {
		t.Fatalf("allocation fence = %q, resource fence = %q, want equal non-zero snapshots", result.Allocation.OwnershipFence, fixture.resource.CurrentOwnershipFence)
	}

	mutated := result.Allocation.Clone()
	mutated.CurrentOccupancy = kernel.CurrentOccupancyUnknown
	mutated.OwnershipFence = kernel.OwnershipFence("caller-forged-fence")
	mutated.Descriptor.Operation = "mutated"
	mutated.Descriptor.Payload[0] = 'X'
	stored, err := fixture.kernel.GetExecutionAllocation(result.Allocation.ID)
	if err != nil {
		t.Fatalf("GetExecutionAllocation() error = %v", err)
	}
	if stored.CurrentOccupancy != kernel.CurrentOccupancyNotEstablished || stored.OwnershipFence != result.Allocation.OwnershipFence || stored.Descriptor.Operation != "execute" || string(stored.Descriptor.Payload) != "payload" {
		t.Fatalf("stored allocation changed through returned snapshot: %#v", stored)
	}

	resourceSnapshot, err := fixture.kernel.Resources.GetResource(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetResource() error = %v", err)
	}
	resourceSnapshot.CurrentOwnershipFence = kernel.OwnershipFence("caller-forged-resource-fence")
	currentResource, err := fixture.kernel.Resources.GetResource(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetResource(after mutation) error = %v", err)
	}
	if currentResource.CurrentOwnershipFence != fixture.resource.CurrentOwnershipFence {
		t.Fatalf("stored Resource fence changed through snapshot: %q", currentResource.CurrentOwnershipFence)
	}

	if _, err := fixture.kernel.ReleaseExecution(result.Allocation.ID, "pre-dispatch abandonment"); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
}

func TestExecutionOwnershipPreDispatchReleaseSetsEndedWithoutProviderCall(t *testing.T) {
	provider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}
	release, err := fixture.kernel.ReleaseExecution(allocation.Allocation.ID, "abandoned before provider")
	if err != nil || release.Status != kernel.ReleaseStatusReleased {
		t.Fatalf("ReleaseExecution() = %#v, error = %v", release, err)
	}
	stored, err := fixture.kernel.GetExecutionAllocation(allocation.Allocation.ID)
	if err != nil {
		t.Fatalf("GetExecutionAllocation() error = %v", err)
	}
	if stored.Lifecycle != kernel.AllocationReleased || stored.CurrentOccupancy != kernel.CurrentOccupancyEnded {
		t.Fatalf("stored allocation = %#v, want RELEASED/ENDED", stored)
	}
	occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
	if err != nil || occupancy != 0 {
		t.Fatalf("Resource occupancy = %d, error = %v, want 0", occupancy, err)
	}
	if provider.Calls() != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.Calls())
	}
	authority, err := fixture.kernel.GetExecutionResolutionAuthority(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetExecutionResolutionAuthority() error = %v", err)
	}
	resolution, resolveErr := fixture.kernel.ResolveExecutionOccupancy(kernel.ExecutionOccupancyResolutionRequest{
		AllocationID: allocation.Allocation.ID,
		ResourceID:   fixture.resource.ID,
		Authority:    authority,
		Evidence: kernel.ResolutionEvidence{
			Occupancy: kernel.OccupancyConclusionEnded,
			Reason:    "late resolution after pre-dispatch abandonment",
		},
	})
	if !errors.Is(resolveErr, kernel.ErrResolutionNotAllowed) || resolution.Status == kernel.ResolutionStatusAlreadyResolved {
		t.Fatalf("ResolveExecutionOccupancy(after pre-dispatch release) = %#v, error = %v, want rejected non-idempotent terminal state", resolution, resolveErr)
	}
	if records := fixture.kernel.Events.Query(event.QueryFilter{EventType: event.ExecutionOccupancyResolved}); len(records) != 0 {
		t.Fatalf("resolution events after pre-dispatch release = %d, want 0", len(records))
	}
}

func TestExecutionOwnershipImmediateEndedObservationAutoReleasesAndReplays(t *testing.T) {
	provider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, provider, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}

	observation, err := fixture.kernel.DispatchExecution(allocation.Allocation.ID)
	if err != nil || observation.Outcome != kernel.ExecutionOutcomeSuccess || observation.Occupancy != kernel.OccupancyConclusionEnded {
		t.Fatalf("DispatchExecution() = %#v, error = %v, want SUCCESS/ENDED", observation, err)
	}
	stored, err := fixture.kernel.GetExecutionAllocation(allocation.Allocation.ID)
	if err != nil {
		t.Fatalf("GetExecutionAllocation() error = %v", err)
	}
	if stored.Lifecycle != kernel.AllocationReleased || stored.CurrentOccupancy != kernel.CurrentOccupancyEnded {
		t.Fatalf("stored allocation = %#v, want RELEASED/ENDED", stored)
	}
	occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
	if err != nil || occupancy != 0 {
		t.Fatalf("Resource occupancy = %d, error = %v, want 0", occupancy, err)
	}
	replay, err := fixture.kernel.DispatchExecution(allocation.Allocation.ID)
	if err != nil || replay != observation {
		t.Fatalf("replayed DispatchExecution() = %#v, error = %v, want original observation", replay, err)
	}
	if provider.Calls() != 1 {
		t.Fatalf("provider calls after replay = %d, want 1", provider.Calls())
	}
	authority, err := fixture.kernel.GetExecutionResolutionAuthority(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetExecutionResolutionAuthority() error = %v", err)
	}
	resolution, resolveErr := fixture.kernel.ResolveExecutionOccupancy(kernel.ExecutionOccupancyResolutionRequest{
		AllocationID: allocation.Allocation.ID,
		ResourceID:   fixture.resource.ID,
		Authority:    authority,
		Evidence: kernel.ResolutionEvidence{
			Occupancy: kernel.OccupancyConclusionEnded,
			Reason:    "late resolution after definitive completion",
		},
	})
	if !errors.Is(resolveErr, kernel.ErrResolutionNotAllowed) || resolution.Status == kernel.ResolutionStatusAlreadyResolved {
		t.Fatalf("ResolveExecutionOccupancy(after definitive ENDED) = %#v, error = %v, want rejected non-idempotent terminal state", resolution, resolveErr)
	}
	if records := fixture.kernel.Events.Query(event.QueryFilter{EventType: event.ExecutionOccupancyResolved}); len(records) != 0 {
		t.Fatalf("resolution events after definitive ENDED = %d, want 0", len(records))
	}
	release, err := fixture.kernel.ReleaseExecution(allocation.Allocation.ID, "idempotent after auto release")
	if err != nil || release.Status != kernel.ReleaseStatusAlreadyReleased {
		t.Fatalf("ReleaseExecution(after auto release) = %#v, error = %v, want ALREADY_RELEASED", release, err)
	}
}

func TestExecutionOwnershipDefiniteFailedEndedObservationAutoReleases(t *testing.T) {
	providerErr := errors.New("definite provider failure")
	provider := &observingOwnershipProvider{
		observation: execution.ExecutionObservation{
			Outcome:   execution.ExecutionOutcomeFailed,
			Occupancy: execution.OccupancyConclusionEnded,
			Reason:    providerErr.Error(),
		},
		err: providerErr,
	}
	fixture := newOwnershipFixture(t, provider, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}
	observation, dispatchErr := fixture.kernel.DispatchExecution(allocation.Allocation.ID)
	if !errors.Is(dispatchErr, providerErr) || observation.Outcome != kernel.ExecutionOutcomeFailed || observation.Occupancy != kernel.OccupancyConclusionEnded {
		t.Fatalf("DispatchExecution() = %#v, error = %v, want FAILED/ENDED with provider error", observation, dispatchErr)
	}
	stored, err := fixture.kernel.GetExecutionAllocation(allocation.Allocation.ID)
	if err != nil {
		t.Fatalf("GetExecutionAllocation() error = %v", err)
	}
	if stored.Lifecycle != kernel.AllocationReleased || stored.CurrentOccupancy != kernel.CurrentOccupancyEnded {
		t.Fatalf("stored allocation = %#v, want RELEASED/ENDED", stored)
	}
	occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
	if err != nil || occupancy != 0 {
		t.Fatalf("Resource occupancy = %d, error = %v, want 0", occupancy, err)
	}
}

func TestExecutionOwnershipSameFenceResolutionReleasesClaimAndEmitsImmutableEvent(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("ambiguous provider result")}
	fixture := newOwnershipFixture(t, provider, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}
	originalObservation, dispatchErr := fixture.kernel.DispatchExecution(allocation.Allocation.ID)
	if !errors.Is(dispatchErr, provider.err) || originalObservation.Outcome != kernel.ExecutionOutcomeUnknown || originalObservation.Occupancy != kernel.OccupancyConclusionUnknown {
		t.Fatalf("ambiguous DispatchExecution() = %#v, error = %v, want UNKNOWN/UNKNOWN", originalObservation, dispatchErr)
	}
	authority, err := fixture.kernel.GetExecutionResolutionAuthority(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetExecutionResolutionAuthority() error = %v", err)
	}
	request := kernel.ExecutionOccupancyResolutionRequest{
		AllocationID: allocation.Allocation.ID,
		ResourceID:   fixture.resource.ID,
		Authority:    authority,
		Evidence: kernel.ResolutionEvidence{
			Occupancy: kernel.OccupancyConclusionEnded,
			Reason:    "Resource-side terminal evidence",
		},
	}
	resolved, err := fixture.kernel.ResolveExecutionOccupancy(request)
	if err != nil || resolved.Status != kernel.ResolutionStatusResolved || resolved.EventID == "" {
		t.Fatalf("ResolveExecutionOccupancy() = %#v, error = %v, want RESOLVED with EventID", resolved, err)
	}
	stored, err := fixture.kernel.GetExecutionAllocation(allocation.Allocation.ID)
	if err != nil {
		t.Fatalf("GetExecutionAllocation() error = %v", err)
	}
	if stored.Lifecycle != kernel.AllocationReleased || stored.CurrentOccupancy != kernel.CurrentOccupancyEnded {
		t.Fatalf("stored allocation = %#v, want RELEASED/ENDED", stored)
	}
	occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
	if err != nil || occupancy != 0 {
		t.Fatalf("Resource occupancy = %d, error = %v, want 0", occupancy, err)
	}
	afterObservation, err := fixture.kernel.GetExecutionObservation(allocation.Allocation.ID)
	if err != nil {
		t.Fatalf("GetExecutionObservation() error = %v", err)
	}
	if afterObservation != originalObservation {
		t.Fatalf("original observation changed after resolution: before=%#v after=%#v", originalObservation, afterObservation)
	}
	records := fixture.kernel.Events.Query(event.QueryFilter{EventType: event.ExecutionOccupancyResolved})
	if len(records) != 1 || records[0].ID != resolved.EventID {
		t.Fatalf("resolution events = %#v, want one accepted event", records)
	}
	if records[0].AffectedObject != (identity.ObjectReference{Kind: identity.ObjectKindResource, ID: fixture.resource.ID.String()}) ||
		!strings.Contains(records[0].Cause, allocation.Allocation.ID.String()) ||
		!strings.Contains(records[0].Cause, allocation.Allocation.OwnershipFence.String()) ||
		!strings.Contains(records[0].Result, allocation.Allocation.ID.String()) {
		t.Fatalf("resolution event = %#v, want Resource binding and exact allocation/fence details", records[0])
	}
	repeated, err := fixture.kernel.ResolveExecutionOccupancy(request)
	if err != nil || repeated.Status != kernel.ResolutionStatusAlreadyResolved {
		t.Fatalf("repeated ResolveExecutionOccupancy() = %#v, error = %v, want ALREADY_RESOLVED", repeated, err)
	}
	if records = fixture.kernel.Events.Query(event.QueryFilter{EventType: event.ExecutionOccupancyResolved}); len(records) != 1 {
		t.Fatalf("resolution event count after repeat = %d, want 1", len(records))
	}
	release, err := fixture.kernel.ReleaseExecution(allocation.Allocation.ID, "after resolution")
	if err != nil || release.Status != kernel.ReleaseStatusAlreadyReleased {
		t.Fatalf("ReleaseExecution(after resolution) = %#v, error = %v, want ALREADY_RELEASED", release, err)
	}
}

func TestExecutionOwnershipTerminalResolutionWithUnexpectedClaimFailsClosed(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("ambiguous provider result")}
	fixture := newOwnershipFixture(t, provider, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}
	observation, dispatchErr := fixture.kernel.DispatchExecution(allocation.Allocation.ID)
	if !errors.Is(dispatchErr, provider.err) || observation.Occupancy != kernel.OccupancyConclusionUnknown {
		t.Fatalf("DispatchExecution() = %#v, error = %v, want UNKNOWN occupancy", observation, dispatchErr)
	}
	authority, err := fixture.kernel.GetExecutionResolutionAuthority(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetExecutionResolutionAuthority() error = %v", err)
	}
	request := kernel.ExecutionOccupancyResolutionRequest{
		AllocationID: allocation.Allocation.ID,
		ResourceID:   fixture.resource.ID,
		Authority:    authority,
		Evidence: kernel.ResolutionEvidence{
			Occupancy: kernel.OccupancyConclusionEnded,
			Reason:    "accepted delayed terminal evidence",
		},
	}
	resolved, err := fixture.kernel.ResolveExecutionOccupancy(request)
	if err != nil || resolved.Status != kernel.ResolutionStatusResolved {
		t.Fatalf("initial ResolveExecutionOccupancy() = %#v, error = %v, want RESOLVED", resolved, err)
	}
	acquired, err := fixture.kernel.Resources.TryAcquireExecution(fixture.resource.ID, allocation.Allocation.ID)
	if err != nil || !acquired {
		t.Fatalf("reintroduced exact claim = acquired %v, error = %v, want test inconsistency", acquired, err)
	}
	repeated, err := fixture.kernel.ResolveExecutionOccupancy(request)
	if !errors.Is(err, kernel.ErrExecutionOwnershipInvariant) || repeated.Status != kernel.ResolutionStatusInvariantViolation || repeated.Status == kernel.ResolutionStatusAlreadyResolved {
		t.Fatalf("ResolveExecutionOccupancy(with unexpected terminal claim) = %#v, error = %v, want invariant failure, not ALREADY_RESOLVED", repeated, err)
	}
	occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
	if err != nil || occupancy != 1 {
		t.Fatalf("Resource occupancy after invariant failure = %d, error = %v, want claim retained at 1", occupancy, err)
	}
	if records := fixture.kernel.Events.Query(event.QueryFilter{EventType: event.ExecutionOccupancyResolved}); len(records) != 1 {
		t.Fatalf("resolution event count after invariant failure = %d, want 1", len(records))
	}
}

func TestExecutionOwnershipResolutionRejectsWrongBindingsAndFenceKnowledge(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("ambiguous provider result")}
	fixture := newOwnershipFixture(t, provider, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}
	if _, err := fixture.kernel.DispatchExecution(allocation.Allocation.ID); !errors.Is(err, provider.err) {
		t.Fatalf("DispatchExecution() error = %v, want provider error", err)
	}
	authority, err := fixture.kernel.GetExecutionResolutionAuthority(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetExecutionResolutionAuthority() error = %v", err)
	}
	otherResource, err := fixture.kernel.Resources.CreateResource("other")
	if err != nil {
		t.Fatalf("CreateResource(other) error = %v", err)
	}
	otherAuthority, err := fixture.kernel.GetExecutionResolutionAuthority(otherResource.ID)
	if err != nil {
		t.Fatalf("GetExecutionResolutionAuthority(other) error = %v", err)
	}
	baseEvidence := kernel.ResolutionEvidence{Occupancy: kernel.OccupancyConclusionEnded, Reason: "terminal"}
	cases := []struct {
		name    string
		request kernel.ExecutionOccupancyResolutionRequest
		want    error
	}{
		{
			name: "wrong allocation",
			request: kernel.ExecutionOccupancyResolutionRequest{
				AllocationID: execution.ExecutionAllocationID("execution-allocation-not-present"),
				ResourceID:   fixture.resource.ID,
				Authority:    authority,
				Evidence:     baseEvidence,
			},
			want: kernel.ErrAllocationNotFound,
		},
		{
			name: "wrong Resource",
			request: kernel.ExecutionOccupancyResolutionRequest{
				AllocationID: allocation.Allocation.ID,
				ResourceID:   otherResource.ID,
				Authority:    otherAuthority,
				Evidence:     baseEvidence,
			},
			want: kernel.ErrResolutionBindingMismatch,
		},
		{
			name: "wrong fence",
			request: kernel.ExecutionOccupancyResolutionRequest{
				AllocationID: allocation.Allocation.ID,
				ResourceID:   fixture.resource.ID,
				Authority: func() kernel.ExecutionResolutionAuthority {
					stale := authority
					stale.Fence = otherAuthority.Fence
					return stale
				}(),
				Evidence: baseEvidence,
			},
			want: kernel.ErrOwnershipFenceMismatch,
		},
		{
			name: "fence knowledge without manager authority",
			request: kernel.ExecutionOccupancyResolutionRequest{
				AllocationID: allocation.Allocation.ID,
				ResourceID:   fixture.resource.ID,
				Authority: kernel.ExecutionResolutionAuthority{
					ResourceID: fixture.resource.ID,
					Fence:      authority.Fence,
				},
				Evidence: baseEvidence,
			},
			want: kernel.ErrResolutionAuthorityDenied,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			result, resolveErr := fixture.kernel.ResolveExecutionOccupancy(test.request)
			if !errors.Is(resolveErr, test.want) {
				t.Fatalf("ResolveExecutionOccupancy() error = %v, want %v", resolveErr, test.want)
			}
			if result.Status == kernel.ResolutionStatusResolved {
				t.Fatalf("ResolveExecutionOccupancy() result = %#v, must not resolve", result)
			}
			stored, err := fixture.kernel.GetExecutionAllocation(allocation.Allocation.ID)
			if err != nil {
				t.Fatalf("GetExecutionAllocation() error = %v", err)
			}
			if stored.Lifecycle != kernel.AllocationRunning || stored.CurrentOccupancy != kernel.CurrentOccupancyUnknown {
				t.Fatalf("stored allocation after rejection = %#v, want RUNNING/UNKNOWN", stored)
			}
			occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID)
			if err != nil || occupancy != 1 {
				t.Fatalf("Resource occupancy after rejection = %d, error = %v, want 1", occupancy, err)
			}
		})
	}
}

func TestExecutionOwnershipResolutionMissingClaimFailsClosed(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("ambiguous provider result")}
	fixture := newOwnershipFixture(t, provider, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}
	if _, err := fixture.kernel.DispatchExecution(allocation.Allocation.ID); !errors.Is(err, provider.err) {
		t.Fatalf("DispatchExecution() error = %v, want provider error", err)
	}
	authority, err := fixture.kernel.GetExecutionResolutionAuthority(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetExecutionResolutionAuthority() error = %v", err)
	}
	if err := fixture.kernel.Resources.ReleaseExecution(fixture.resource.ID, allocation.Allocation.ID); err != nil {
		t.Fatalf("artificial claim removal error = %v", err)
	}
	result, err := fixture.kernel.ResolveExecutionOccupancy(kernel.ExecutionOccupancyResolutionRequest{
		AllocationID: allocation.Allocation.ID,
		ResourceID:   fixture.resource.ID,
		Authority:    authority,
		Evidence:     kernel.ResolutionEvidence{Occupancy: kernel.OccupancyConclusionEnded, Reason: "terminal"},
	})
	if !errors.Is(err, kernel.ErrExecutionOwnershipInvariant) || result.Status != kernel.ResolutionStatusInvariantViolation {
		t.Fatalf("ResolveExecutionOccupancy() = %#v, error = %v, want fail-closed invariant violation status", result, err)
	}
	stored, err := fixture.kernel.GetExecutionAllocation(allocation.Allocation.ID)
	if err != nil {
		t.Fatalf("GetExecutionAllocation() error = %v", err)
	}
	if stored.Lifecycle != kernel.AllocationRunning || stored.CurrentOccupancy != kernel.CurrentOccupancyUnknown {
		t.Fatalf("stored allocation after invariant failure = %#v, want RUNNING/UNKNOWN", stored)
	}
	if records := fixture.kernel.Events.Query(event.QueryFilter{EventType: event.ExecutionOccupancyResolved}); len(records) != 0 {
		t.Fatalf("resolution events after invariant failure = %d, want 0", len(records))
	}
}

func TestExecutionOwnershipConcurrentEquivalentResolutionHasOneTransition(t *testing.T) {
	provider := &ownershipProvider{err: errors.New("ambiguous provider result")}
	fixture := newOwnershipFixture(t, provider, nil)
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v", allocation, err)
	}
	if _, err := fixture.kernel.DispatchExecution(allocation.Allocation.ID); !errors.Is(err, provider.err) {
		t.Fatalf("DispatchExecution() error = %v, want provider error", err)
	}
	authority, err := fixture.kernel.GetExecutionResolutionAuthority(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetExecutionResolutionAuthority() error = %v", err)
	}
	request := kernel.ExecutionOccupancyResolutionRequest{
		AllocationID: allocation.Allocation.ID,
		ResourceID:   fixture.resource.ID,
		Authority:    authority,
		Evidence:     kernel.ResolutionEvidence{Occupancy: kernel.OccupancyConclusionEnded, Reason: "terminal"},
	}
	type resolutionCall struct {
		result kernel.ExecutionResolutionResult
		err    error
	}
	const callers = 12
	results := make(chan resolutionCall, callers)
	var wait sync.WaitGroup
	for index := 0; index < callers; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, resolveErr := fixture.kernel.ResolveExecutionOccupancy(request.Clone())
			results <- resolutionCall{result: result, err: resolveErr}
		}()
	}
	wait.Wait()
	close(results)
	resolved := 0
	alreadyResolved := 0
	for call := range results {
		if call.err != nil {
			t.Fatalf("concurrent resolution error = %v", call.err)
		}
		switch call.result.Status {
		case kernel.ResolutionStatusResolved:
			resolved++
		case kernel.ResolutionStatusAlreadyResolved:
			alreadyResolved++
		default:
			t.Fatalf("concurrent resolution result = %#v, want RESOLVED or ALREADY_RESOLVED", call.result)
		}
	}
	if resolved != 1 || alreadyResolved != callers-1 {
		t.Fatalf("concurrent resolution results = resolved %d/already %d, want 1/%d", resolved, alreadyResolved, callers-1)
	}
	if occupancy, err := fixture.kernel.Resources.ExecutionOccupancy(fixture.resource.ID); err != nil || occupancy != 0 {
		t.Fatalf("Resource occupancy after concurrent resolution = %d, error = %v, want 0", occupancy, err)
	}
	if records := fixture.kernel.Events.Query(event.QueryFilter{EventType: event.ExecutionOccupancyResolved}); len(records) != 1 {
		t.Fatalf("resolution event count after concurrent resolution = %d, want 1", len(records))
	}
}

func TestExecutionOwnershipAvailabilityAndFenceRemainSeparate(t *testing.T) {
	firstProvider := &ownershipProvider{}
	secondProvider := &ownershipProvider{}
	fixture := newOwnershipFixture(t, firstProvider, nil)
	initialFence := fixture.resource.CurrentOwnershipFence
	allocation, err := fixture.kernel.TryAllocateExecution(fixture.request(t, "execute", "payload"))
	if err != nil || !allocation.Granted || allocation.Allocation.OwnershipFence != initialFence {
		t.Fatalf("TryAllocateExecution() = %#v, error = %v, want initial fence snapshot", allocation, err)
	}
	if err := fixture.kernel.SetProvider(secondProvider); err != nil {
		t.Fatalf("SetProvider() error = %v", err)
	}
	if err := fixture.kernel.Resources.UpdateAvailability(fixture.resource.ID, resource.AvailabilityUnavailable); err != nil {
		t.Fatalf("UpdateAvailability(UNAVAILABLE) error = %v", err)
	}
	current, err := fixture.kernel.Resources.GetResource(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetResource() error = %v", err)
	}
	if current.CurrentOwnershipFence != initialFence {
		t.Fatalf("fence after provider/availability changes = %q, want %q", current.CurrentOwnershipFence, initialFence)
	}
	if _, err := fixture.kernel.ReleaseExecution(allocation.Allocation.ID, "release while unavailable"); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
	current, err = fixture.kernel.Resources.GetResource(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetResource(after release) error = %v", err)
	}
	if current.CurrentOwnershipFence != initialFence || current.Availability != resource.AvailabilityUnavailable || current.IsAvailable() {
		t.Fatalf("Resource after release = %#v, want unchanged fence and unavailable Resource", current)
	}
}
