package resourcedirectory

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"dtm/internal/node"
)

func TestNormalizeQuiesceReason(t *testing.T) {
	tests := []struct {
		name  string
		input IneligibleReason
		want  IneligibleReason
	}{
		{name: "zero value becomes unknown strong", input: IneligibleReasonNone, want: IneligibleReasonUnknownQuiesce},
		{name: "unknown string becomes unknown strong", input: IneligibleReason("unexpected_reason"), want: IneligibleReasonUnknownQuiesce},
		{name: "derivation reason becomes unknown strong", input: IneligibleReasonNodeNotActive, want: IneligibleReasonUnknownQuiesce},
		{name: "recoverable passes through", input: IneligibleReasonRepositoryReadFailure, want: IneligibleReasonRepositoryReadFailure},
		{name: "recoverable lifecycle passes through", input: IneligibleReasonNodeLifecycleUnavailable, want: IneligibleReasonNodeLifecycleUnavailable},
		{name: "recoverable persistence passes through", input: IneligibleReasonPersistenceFailure, want: IneligibleReasonPersistenceFailure},
		{name: "recoverable sweep passes through", input: IneligibleReasonSweepPersistenceFailure, want: IneligibleReasonSweepPersistenceFailure},
		{name: "recoverable offline passes through", input: IneligibleReasonOfflinePersistenceFailure, want: IneligibleReasonOfflinePersistenceFailure},
		{name: "strong publication passes through", input: IneligibleReasonPublicationInProgress, want: IneligibleReasonPublicationInProgress},
		{name: "strong post-commit passes through", input: IneligibleReasonPostCommitFailure, want: IneligibleReasonPostCommitFailure},
		{name: "strong commit outcome passes through", input: IneligibleReasonCommitOutcomeUnknown, want: IneligibleReasonCommitOutcomeUnknown},
		{name: "unknown quiesce passes through", input: IneligibleReasonUnknownQuiesce, want: IneligibleReasonUnknownQuiesce},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := normalizeQuiesceReason(test.input); got != test.want {
				t.Fatalf("normalizeQuiesceReason(%q) = %q, want %q", test.input, got, test.want)
			}
		})
	}
}

func TestZeroValueQuiesceIsNormalizedToStrong(t *testing.T) {
	directory, id := quiesceFixture(t)
	directory.QuiesceNode("node-a", IneligibleReasonNone)
	if got := directory.NodeQuiesceReason("node-a"); got != IneligibleReasonUnknownQuiesce {
		t.Fatalf("NodeQuiesceReason() = %q, want unknown_quiesce_reason", got)
	}
	if IsRecoverableQuiesceReason(directory.NodeQuiesceReason("node-a")) {
		t.Fatal("zero-value quiesce is recoverable")
	}
	assertEligibility(t, directory, id, false, IneligibleReasonUnknownQuiesce)
}

func TestUnknownStringQuiesceIsNormalizedToStrong(t *testing.T) {
	directory, id := quiesceFixture(t)
	directory.QuiesceNode("node-a", IneligibleReason("unexpected_reason"))
	if got := directory.NodeQuiesceReason("node-a"); got != IneligibleReasonUnknownQuiesce {
		t.Fatalf("NodeQuiesceReason() = %q, want unknown_quiesce_reason", got)
	}
	assertEligibility(t, directory, id, false, IneligibleReasonUnknownQuiesce)
}

func TestZeroValueStrongIsNotDegradedByRecoverable(t *testing.T) {
	directory, id := quiesceFixture(t)
	directory.QuiesceNode("node-a", IneligibleReasonNone)
	directory.QuiesceNode("node-a", IneligibleReasonRepositoryReadFailure)
	if got := directory.NodeQuiesceReason("node-a"); got != IneligibleReasonUnknownQuiesce {
		t.Fatalf("recoverable reason replaced zero-value strong: %q", got)
	}
	if err := directory.ReconcileNodeLifecycle(healthyReconcileView()); !errors.Is(err, ErrQuiesceRequiresRegistration) {
		t.Fatalf("reconcile after zero-value degradation error = %v", err)
	}
	assertEligibility(t, directory, id, false, IneligibleReasonUnknownQuiesce)
}

func TestUnknownStrongIsNotDegradedByRecoverable(t *testing.T) {
	directory, id := quiesceFixture(t)
	directory.QuiesceNode("node-a", IneligibleReason("unexpected_reason"))
	directory.QuiesceNode("node-a", IneligibleReasonPersistenceFailure)
	if got := directory.NodeQuiesceReason("node-a"); got != IneligibleReasonUnknownQuiesce {
		t.Fatalf("recoverable reason replaced unknown strong: %q", got)
	}
	assertEligibility(t, directory, id, false, IneligibleReasonUnknownQuiesce)
}

func TestRecoverableIsUpgradedByPostCommitStrong(t *testing.T) {
	directory, id := quiesceFixture(t)
	directory.QuiesceNode("node-a", IneligibleReasonRepositoryReadFailure)
	directory.QuiesceNode("node-a", IneligibleReasonPostCommitFailure)
	if got := directory.NodeQuiesceReason("node-a"); got != IneligibleReasonPostCommitFailure {
		t.Fatalf("recoverable was not upgraded to %q: %q", IneligibleReasonPostCommitFailure, got)
	}
	if err := directory.ReconcileNodeLifecycle(healthyReconcileView()); !errors.Is(err, ErrQuiesceRequiresRegistration) {
		t.Fatalf("reconcile after upgrade error = %v", err)
	}
	assertEligibility(t, directory, id, false, IneligibleReasonPostCommitFailure)
}

func TestPostCommitStrongIsNotDowngradedByRecoverable(t *testing.T) {
	directory, id := quiesceFixture(t)
	directory.QuiesceNode("node-a", IneligibleReasonPostCommitFailure)
	directory.QuiesceNode("node-a", IneligibleReasonPersistenceFailure)
	directory.QuiesceNode("node-a", IneligibleReasonRepositoryReadFailure)
	if got := directory.NodeQuiesceReason("node-a"); got != IneligibleReasonPostCommitFailure {
		t.Fatalf("post-commit strong was downgraded to %q", got)
	}
	assertEligibility(t, directory, id, false, IneligibleReasonPostCommitFailure)
}

func TestReconcileRejectsZeroValueAndUnknownStrong(t *testing.T) {
	for _, reason := range []IneligibleReason{IneligibleReasonNone, IneligibleReason("unexpected_reason")} {
		t.Run(string(reason), func(t *testing.T) {
			directory, id := quiesceFixture(t)
			directory.QuiesceNode("node-a", reason)
			err := directory.ReconcileNodeLifecycle(healthyReconcileView())
			if !errors.Is(err, ErrQuiesceRequiresRegistration) {
				t.Fatalf("reconcile error = %v, want %v", err, ErrQuiesceRequiresRegistration)
			}
			assertEligibility(t, directory, id, false, IneligibleReasonUnknownQuiesce)
		})
	}
}

func TestUpdateDoesNotClearZeroValueAndUnknownStrong(t *testing.T) {
	for _, reason := range []IneligibleReason{IneligibleReasonNone, IneligibleReason("unexpected_reason")} {
		t.Run(string(reason), func(t *testing.T) {
			directory, id := quiesceFixture(t)
			directory.QuiesceNode("node-a", reason)
			for iteration := 0; iteration < 3; iteration++ {
				if err := directory.UpdateNodeLifecycle(healthyReconcileView()); err != nil {
					t.Fatal(err)
				}
			}
			if got := directory.NodeQuiesceReason("node-a"); got != IneligibleReasonUnknownQuiesce {
				t.Fatalf("update changed normalized reason: %q", got)
			}
			assertEligibility(t, directory, id, false, IneligibleReasonUnknownQuiesce)
		})
	}
}

func TestActivateClearsNormalizedStrongWithExactHealthyView(t *testing.T) {
	for _, reason := range []IneligibleReason{IneligibleReasonNone, IneligibleReason("unexpected_reason"), IneligibleReasonPostCommitFailure} {
		t.Run(string(reason), func(t *testing.T) {
			directory, id := quiesceFixture(t)
			directory.QuiesceNode("node-a", reason)
			if err := directory.ActivateNodeLifecycle(healthyReconcileView()); err != nil {
				t.Fatalf("healthy exact activation error = %v", err)
			}
			assertEligibility(t, directory, id, true, IneligibleReasonNone)
			if got := directory.NodeQuiesceReason("node-a"); got != IneligibleReasonNone {
				t.Fatalf("reason after activation = %q", got)
			}
		})
	}
}

func TestActivateRejectsUnhealthyViews(t *testing.T) {
	tests := []struct {
		name    string
		view    NodeLifecycleView
		wantErr error
	}{
		{name: "not active", view: lifecycleView("node-a", 1, "registration-a", node.StatusOffline, true, true), wantErr: ErrNodeNotActive},
		{name: "invalid lease", view: lifecycleView("node-a", 1, "registration-a", node.StatusActive, false, true), wantErr: ErrLeaseInvalid},
		{name: "inactive endpoint", view: lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, false), wantErr: ErrEndpointInactive},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			directory, id := quiesceFixture(t)
			directory.QuiesceNode("node-a", IneligibleReasonPostCommitFailure)
			err := directory.ActivateNodeLifecycle(test.view)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ActivateNodeLifecycle() error = %v, want %v", err, test.wantErr)
			}
			assertEligibility(t, directory, id, false, IneligibleReasonPostCommitFailure)
			if reason := directory.NodeQuiesceReason("node-a"); reason != IneligibleReasonPostCommitFailure {
				t.Fatalf("reason after rejected activation = %q", reason)
			}
		})
	}
}

func TestActivateRejectsStaleFences(t *testing.T) {
	directory := New()
	descriptor := mustDescriptor(t, "resource-activate-stale", "node-a", "temperature_sensor", []string{"read"}, nil, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 5, "reg-new", descriptor))
	mustUpdateLifecycle(t, directory, lifecycleView("node-a", 5, "reg-new", node.StatusActive, true, true))
	directory.QuiesceNode("node-a", IneligibleReasonPublicationInProgress)

	tests := []struct {
		name    string
		view    NodeLifecycleView
		wantErr error
	}{
		{name: "old generation and old registration", view: lifecycleView("node-a", 4, "reg-old", node.StatusActive, true, true), wantErr: ErrStaleNodeGeneration},
		{name: "current generation old registration", view: lifecycleView("node-a", 5, "reg-old", node.StatusActive, true, true), wantErr: ErrStaleRegistration},
		{name: "future generation other registration", view: lifecycleView("node-a", 6, "reg-other", node.StatusActive, true, true), wantErr: ErrInvalidNodeLifecycleView},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := directory.ActivateNodeLifecycle(test.view)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ActivateNodeLifecycle() error = %v, want %v", err, test.wantErr)
			}
			assertEligibility(t, directory, descriptor.ID, false, IneligibleReasonPublicationInProgress)
			assertGeneration(t, directory, descriptor.ID, 1)
		})
	}
}

func TestConcurrentMixedQuiesceMergeNeverLeaksEligible(t *testing.T) {
	directory, id := quiesceFixture(t)
	var strongApplied atomic.Bool
	var workers sync.WaitGroup
	start := make(chan struct{})
	stop := make(chan struct{})
	for index := 0; index < 4; index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for {
				select {
				case <-stop:
					return
				default:
				}
				directory.QuiesceNode("node-a", IneligibleReasonRepositoryReadFailure)
				directory.QuiesceNode("node-a", IneligibleReasonNone)
				_ = directory.ReconcileNodeLifecycle(healthyReconcileView())
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		directory.QuiesceNode("node-a", IneligibleReasonPostCommitFailure)
		strongApplied.Store(true)
		for {
			select {
			case <-stop:
				return
			default:
			}
			directory.QuiesceNode("node-a", IneligibleReasonNone)
		}
	}()
	close(start)
	for !strongApplied.Load() {
	}
	for iteration := 0; iteration < 2000; iteration++ {
		if got := directory.ListEligible(); len(got) != 0 {
			t.Fatalf("eligible leak after strong quiesce: %#v", got)
		}
	}
	close(stop)
	workers.Wait()
	// The exact strong label depends on merge ordering (a concurrent
	// zero-value quiesce may already hold unknown_quiesce_reason when the
	// strong goroutine merges post_commit_publication_failure, and the merge
	// correctly keeps the first-applied strong reason). The frozen invariant
	// is that the state is strong, never recoverable, and eligible stays false.
	if reason := directory.NodeQuiesceReason("node-a"); IsRecoverableQuiesceReason(reason) {
		t.Fatalf("strong quiesce was degraded to recoverable %q", reason)
	}
	assertEligibility(t, directory, id, false, directory.NodeQuiesceReason("node-a"))
	if err := directory.ActivateNodeLifecycle(healthyReconcileView()); err != nil {
		t.Fatal(err)
	}
	assertEligibility(t, directory, id, true, IneligibleReasonNone)
}
