package resource

import (
	"errors"
	"testing"
)

func TestResourceExecutionCapacityUsesCompatibilityDefaultAndOwnsOccupancy(t *testing.T) {
	manager := NewManager()
	created, err := manager.CreateResource("local")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	if created.ExecutionCapacity != 0 || created.EffectiveExecutionCapacity() != 1 {
		t.Fatalf("default capacity = stored %d/effective %d, want 0/1", created.ExecutionCapacity, created.EffectiveExecutionCapacity())
	}
	claimID := ExecutionAllocationID("allocation-1")
	acquired, err := manager.TryAcquireExecution(created.ID, claimID)
	if err != nil || !acquired {
		t.Fatalf("first TryAcquireExecution() = %t, error = %v", acquired, err)
	}
	if err := manager.ReleaseExecution(created.ID, ExecutionAllocationID("other-allocation")); !errors.Is(err, ErrExecutionOccupancyMismatch) {
		t.Fatalf("wrong-token ReleaseExecution() error = %v, want occupancy mismatch", err)
	}
	if occupancy, err := manager.ExecutionOccupancy(created.ID); err != nil || occupancy != 1 {
		t.Fatalf("occupancy after wrong-token release = %d, error = %v, want 1", occupancy, err)
	}
	acquired, err = manager.TryAcquireExecution(created.ID, claimID)
	if !errors.Is(err, ErrResourceBusy) || acquired {
		t.Fatalf("second TryAcquireExecution() = %t, error = %v, want busy", acquired, err)
	}
	if err := manager.ReleaseExecution(created.ID, claimID); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
	if occupancy, err := manager.ExecutionOccupancy(created.ID); err != nil || occupancy != 0 {
		t.Fatalf("occupancy after release = %d, error = %v, want 0", occupancy, err)
	}
}

func TestResourceExecutionCapacitySupportsOnlyFixedUnweightedSlots(t *testing.T) {
	manager := NewManager()
	created, err := manager.CreateResourceWithCapacity(2, "local")
	if err != nil {
		t.Fatalf("CreateResourceWithCapacity() error = %v", err)
	}
	if created.EffectiveExecutionCapacity() != 2 {
		t.Fatalf("effective capacity = %d, want 2", created.EffectiveExecutionCapacity())
	}
	claimIDs := []ExecutionAllocationID{"allocation-1", "allocation-2"}
	for attempt := 0; attempt < 2; attempt++ {
		claimID := claimIDs[attempt]
		acquired, acquireErr := manager.TryAcquireExecution(created.ID, claimID)
		if acquireErr != nil || !acquired {
			t.Fatalf("TryAcquireExecution(%d) = %t, error = %v", attempt, acquired, acquireErr)
		}
	}
	if available, err := manager.AvailableExecutionCapacity(created.ID); err != nil || available != 0 {
		t.Fatalf("available capacity = %d, error = %v, want 0", available, err)
	}
	if acquired, err := manager.TryAcquireExecution(created.ID, ExecutionAllocationID("allocation-3")); !errors.Is(err, ErrResourceBusy) || acquired {
		t.Fatalf("third TryAcquireExecution() = %t, error = %v, want busy", acquired, err)
	}
	if err := manager.ReleaseExecution(created.ID, ExecutionAllocationID("allocation-1")); err != nil {
		t.Fatalf("first ReleaseExecution() error = %v", err)
	}
	if err := manager.ReleaseExecution(created.ID, ExecutionAllocationID("allocation-2")); err != nil {
		t.Fatalf("second ReleaseExecution() error = %v", err)
	}
	if available, err := manager.AvailableExecutionCapacity(created.ID); err != nil || available != 2 {
		t.Fatalf("available capacity after releasing all claims = %d, error = %v, want 2", available, err)
	}
	if err := manager.ReleaseExecution(created.ID, ExecutionAllocationID("allocation-2")); !errors.Is(err, ErrExecutionOccupancyMismatch) {
		t.Fatalf("third ReleaseExecution() error = %v, want occupancy mismatch", err)
	}
}

func TestResourceReleaseDoesNotChangeAvailability(t *testing.T) {
	manager := NewManager()
	created, err := manager.CreateResource()
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	claimID := ExecutionAllocationID("allocation-1")
	acquired, err := manager.TryAcquireExecution(created.ID, claimID)
	if err != nil || !acquired {
		t.Fatalf("TryAcquireExecution() = %t, error = %v", acquired, err)
	}
	if err := manager.UpdateAvailability(created.ID, AvailabilityUnavailable); err != nil {
		t.Fatalf("UpdateAvailability() error = %v", err)
	}
	if err := manager.ReleaseExecution(created.ID, claimID); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
	current, err := manager.GetResource(created.ID)
	if err != nil {
		t.Fatalf("GetResource() error = %v", err)
	}
	if current.Availability != AvailabilityUnavailable || current.IsAvailable() {
		t.Fatalf("resource after release = %#v, want unavailable", current)
	}
}

func TestResourceResolutionAuthorityIsManagerIssuedAndFenceBound(t *testing.T) {
	manager := NewManager()
	created, err := manager.CreateResource("local")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	authority, err := manager.GetExecutionResolutionAuthority(created.ID)
	if err != nil {
		t.Fatalf("GetExecutionResolutionAuthority() error = %v", err)
	}
	if authority.ResourceID != created.ID || authority.Fence != created.CurrentOwnershipFence || authority.Fence.IsZero() {
		t.Fatalf("authority = %#v, resource = %#v, want exact non-zero Resource fence", authority, created)
	}
	if err := manager.ValidateExecutionResolutionAuthority(created.ID, authority); err != nil {
		t.Fatalf("ValidateExecutionResolutionAuthority() error = %v", err)
	}

	knownFenceOnly := ExecutionResolutionAuthority{ResourceID: created.ID, Fence: authority.Fence}
	if err := manager.ValidateExecutionResolutionAuthority(created.ID, knownFenceOnly); !errors.Is(err, ErrResolutionAuthorityDenied) {
		t.Fatalf("known-fence-only authority error = %v, want authority denied", err)
	}
	stale := authority
	stale.Fence = OwnershipFence("stale-fence")
	if err := manager.ValidateExecutionResolutionAuthority(created.ID, stale); !errors.Is(err, ErrOwnershipFenceMismatch) {
		t.Fatalf("stale authority error = %v, want fence mismatch", err)
	}

	acquired, fence, err := manager.TryAcquireExecutionWithFence(created.ID, ExecutionAllocationID("allocation-fence"))
	if err != nil || !acquired || fence != authority.Fence {
		t.Fatalf("TryAcquireExecutionWithFence() = acquired %t/fence %q/error %v, want claim with current fence", acquired, fence, err)
	}
	if err := manager.ReleaseExecution(created.ID, ExecutionAllocationID("allocation-fence")); err != nil {
		t.Fatalf("ReleaseExecution() error = %v", err)
	}
}

func TestResourceReleaseExecutionWithCommitRollsBackClaimOnCommitFailure(t *testing.T) {
	manager := NewManager()
	created, err := manager.CreateResource("local")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	allocationID := ExecutionAllocationID("allocation-commit")
	acquired, err := manager.TryAcquireExecution(created.ID, allocationID)
	if err != nil || !acquired {
		t.Fatalf("TryAcquireExecution() = %t, error = %v", acquired, err)
	}
	commitErr := errors.New("commit rejected")
	called := false
	if err := manager.ReleaseExecutionWithCommit(created.ID, allocationID, func() error {
		called = true
		return commitErr
	}); !errors.Is(err, commitErr) {
		t.Fatalf("ReleaseExecutionWithCommit() error = %v, want commit error", err)
	}
	if !called {
		t.Fatal("ReleaseExecutionWithCommit() did not invoke commit callback")
	}
	if occupancy, err := manager.ExecutionOccupancy(created.ID); err != nil || occupancy != 1 {
		t.Fatalf("occupancy after rejected commit = %d, error = %v, want claim retained", occupancy, err)
	}
	if err := manager.ReleaseExecution(created.ID, allocationID); err != nil {
		t.Fatalf("cleanup ReleaseExecution() error = %v", err)
	}
}
