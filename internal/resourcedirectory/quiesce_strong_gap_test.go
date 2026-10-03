package resourcedirectory

import (
	"testing"

	"dtm/internal/node"
)

// TestGapZeroValueQuiesceDegradesToRecoverable reproduces the M3-C gap: a
// zero-value Quiesce reason must be normalized to a strong state and must not
// be replaceable by a later recoverable reason.
//
// Sequence: QuiesceNode(none) then QuiesceNode(repository_read_failure) then
// a healthy authoritative Reconcile. BEFORE FIX: the recoverable reason
// replaces the zero value and the Reconcile clears the isolation.
func TestGapZeroValueQuiesceDegradesToRecoverable(t *testing.T) {
	directory := New()
	descriptor := mustDescriptor(t, "resource-zero-gap", "node-a", "temperature_sensor", []string{"read"}, nil, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", descriptor))
	active := lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, true)
	mustUpdateLifecycle(t, directory, active)
	assertEligibility(t, directory, descriptor.ID, true, IneligibleReasonNone)

	directory.QuiesceNode("node-a", IneligibleReasonNone)
	directory.QuiesceNode("node-a", IneligibleReasonRepositoryReadFailure)
	if err := directory.ReconcileNodeLifecycle(active); err == nil {
		t.Fatal("ReconcileNodeLifecycle() error = nil; zero-value quiesce must stay strong")
	}
	got, err := directory.GetByID(descriptor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Eligible {
		t.Fatalf("zero-value quiesce was degraded and cleared: %#v", got)
	}
}

// TestGapActivateClearsStrongQuiesceWithUnhealthyView reproduces the M3-C
// gap: ActivateNodeLifecycle must reject a View whose Fence is exact but
// whose runtime state is unhealthy. BEFORE FIX: the activation succeeds and
// clears the strong isolation.
func TestGapActivateClearsStrongQuiesceWithUnhealthyView(t *testing.T) {
	directory := New()
	descriptor := mustDescriptor(t, "resource-activate-gap", "node-a", "temperature_sensor", []string{"read"}, nil, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", descriptor))
	active := lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, true)
	mustUpdateLifecycle(t, directory, active)
	directory.QuiesceNode("node-a", IneligibleReasonPublicationInProgress)
	assertEligibility(t, directory, descriptor.ID, false, IneligibleReasonPublicationInProgress)

	unhealthy := lifecycleView("node-a", 1, "registration-a", node.StatusOffline, false, false)
	if err := directory.ActivateNodeLifecycle(unhealthy); err == nil {
		t.Fatal("ActivateNodeLifecycle() error = nil; unhealthy view must not clear strong isolation")
	}
	got, err := directory.GetByID(descriptor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Eligible {
		t.Fatalf("unhealthy activation cleared strong isolation: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-a"); reason != IneligibleReasonPublicationInProgress {
		t.Fatalf("quiesce reason after unhealthy activation = %q", reason)
	}
}
