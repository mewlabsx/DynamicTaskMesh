package resourcedirectory

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"dtm/internal/model"
	"dtm/internal/node"
)

func quiesceFixture(t *testing.T) (*Directory, model.ResourceID) {
	t.Helper()
	directory := New()
	descriptor := mustDescriptor(t, "resource-recover", "node-a", "temperature_sensor", []string{"read"}, map[string]string{"empty": ""}, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", descriptor))
	active := lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, true)
	mustUpdateLifecycle(t, directory, active)
	assertEligibility(t, directory, descriptor.ID, true, IneligibleReasonNone)
	return directory, descriptor.ID
}

func healthyReconcileView() NodeLifecycleView {
	return lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, true)
}

func TestReconcileLiftsRecoverableQuiesceAndPreservesState(t *testing.T) {
	for _, reason := range []IneligibleReason{
		IneligibleReasonNodeLifecycleUnavailable,
		IneligibleReasonRepositoryReadFailure,
		IneligibleReasonPersistenceFailure,
		IneligibleReasonSweepPersistenceFailure,
		IneligibleReasonOfflinePersistenceFailure,
	} {
		t.Run(string(reason), func(t *testing.T) {
			directory, id := quiesceFixture(t)
			directory.QuiesceNode("node-a", reason)
			assertEligibility(t, directory, id, false, reason)
			if got := directory.NodeQuiesceReason("node-a"); got != reason {
				t.Fatalf("NodeQuiesceReason() = %q, want %q", got, reason)
			}
			if err := directory.ReconcileNodeLifecycle(healthyReconcileView()); err != nil {
				t.Fatalf("ReconcileNodeLifecycle() error = %v", err)
			}
			assertEligibility(t, directory, id, true, IneligibleReasonNone)
			if got := directory.NodeQuiesceReason("node-a"); got != IneligibleReasonNone {
				t.Fatalf("NodeQuiesceReason() after reconcile = %q, want none", got)
			}
			assertGeneration(t, directory, id, 1)
			got, err := directory.GetByID(id)
			if err != nil {
				t.Fatal(err)
			}
			if got.Descriptor.Attributes["empty"] != "" || got.PublicationState != PublicationStatePublished {
				t.Fatalf("reconcile changed descriptor or publication state: %#v", got)
			}
			if got.IneligibleReason != IneligibleReasonNone || !got.Eligible {
				t.Fatalf("reconciled view = %#v", got)
			}
		})
	}
}

func TestReconcileRejectsStrongQuiesce(t *testing.T) {
	for _, reason := range []IneligibleReason{
		IneligibleReasonPublicationInProgress,
		IneligibleReasonPostCommitFailure,
		IneligibleReasonCommitOutcomeUnknown,
	} {
		t.Run(string(reason), func(t *testing.T) {
			directory, id := quiesceFixture(t)
			directory.QuiesceNode("node-a", reason)
			err := directory.ReconcileNodeLifecycle(healthyReconcileView())
			if !errors.Is(err, ErrQuiesceRequiresRegistration) {
				t.Fatalf("ReconcileNodeLifecycle() error = %v, want %v", err, ErrQuiesceRequiresRegistration)
			}
			assertEligibility(t, directory, id, false, reason)
			if got := directory.NodeQuiesceReason("node-a"); got != reason {
				t.Fatalf("strong reason was replaced: %q", got)
			}
			assertGeneration(t, directory, id, 1)
		})
	}
}

func TestReconcileRejectsUnknownQuiesceReasons(t *testing.T) {
	for _, reason := range []IneligibleReason{
		IneligibleReasonNodeNotActive,
		IneligibleReasonLeaseInvalid,
		IneligibleReasonEndpointInactive,
	} {
		t.Run(string(reason), func(t *testing.T) {
			directory, id := quiesceFixture(t)
			directory.QuiesceNode("node-a", reason)
			err := directory.ReconcileNodeLifecycle(healthyReconcileView())
			if !errors.Is(err, ErrQuiesceRequiresRegistration) {
				t.Fatalf("unknown reason reconcile error = %v, want %v", err, ErrQuiesceRequiresRegistration)
			}
			assertEligibility(t, directory, id, false, IneligibleReasonUnknownQuiesce)
		})
	}

	t.Run("zero value reason", func(t *testing.T) {
		directory, id := quiesceFixture(t)
		directory.QuiesceNode("node-a", IneligibleReasonNone)
		if err := directory.ReconcileNodeLifecycle(healthyReconcileView()); !errors.Is(err, ErrQuiesceRequiresRegistration) {
			t.Fatalf("zero-value reason reconcile error = %v", err)
		}
		assertEligibility(t, directory, id, false, IneligibleReasonUnknownQuiesce)
	})
}

func TestMergeQuiesceReasonPriority(t *testing.T) {
	tests := []struct {
		name     string
		current  IneligibleReason
		incoming IneligibleReason
		want     IneligibleReason
	}{
		{name: "none to recoverable", current: IneligibleReasonNone, incoming: IneligibleReasonNodeLifecycleUnavailable, want: IneligibleReasonNodeLifecycleUnavailable},
		{name: "none to strong", current: IneligibleReasonNone, incoming: IneligibleReasonPublicationInProgress, want: IneligibleReasonPublicationInProgress},
		{name: "recoverable stays recoverable", current: IneligibleReasonRepositoryReadFailure, incoming: IneligibleReasonPersistenceFailure, want: IneligibleReasonRepositoryReadFailure},
		{name: "recoverable upgraded to strong", current: IneligibleReasonNodeLifecycleUnavailable, incoming: IneligibleReasonCommitOutcomeUnknown, want: IneligibleReasonCommitOutcomeUnknown},
		{name: "strong never downgraded", current: IneligibleReasonPublicationInProgress, incoming: IneligibleReasonPersistenceFailure, want: IneligibleReasonPublicationInProgress},
		{name: "strong keeps strong", current: IneligibleReasonPostCommitFailure, incoming: IneligibleReasonPublicationInProgress, want: IneligibleReasonPostCommitFailure},
		{name: "none to unknown fails closed", current: IneligibleReasonNone, incoming: IneligibleReasonNodeNotActive, want: IneligibleReasonNodeNotActive},
		{name: "unknown never replaced by recoverable", current: IneligibleReasonEndpointInactive, incoming: IneligibleReasonPersistenceFailure, want: IneligibleReasonEndpointInactive},
		{name: "repeated recoverable idempotent", current: IneligibleReasonSweepPersistenceFailure, incoming: IneligibleReasonSweepPersistenceFailure, want: IneligibleReasonSweepPersistenceFailure},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := mergeQuiesceReason(test.current, test.incoming); got != test.want {
				t.Fatalf("mergeQuiesceReason(%q, %q) = %q, want %q", test.current, test.incoming, got, test.want)
			}
		})
	}
}

func TestQuiesceMergeKeepsStrongIsolation(t *testing.T) {
	directory, id := quiesceFixture(t)
	directory.QuiesceNode("node-a", IneligibleReasonPublicationInProgress)
	directory.QuiesceNode("node-a", IneligibleReasonNodeLifecycleUnavailable)
	if got := directory.NodeQuiesceReason("node-a"); got != IneligibleReasonPublicationInProgress {
		t.Fatalf("strong isolation was downgraded to %q", got)
	}
	if err := directory.ReconcileNodeLifecycle(healthyReconcileView()); !errors.Is(err, ErrQuiesceRequiresRegistration) {
		t.Fatalf("downgraded reconcile error = %v", err)
	}
	assertEligibility(t, directory, id, false, IneligibleReasonPublicationInProgress)
}

func TestQuiesceMergeUpgradesToStrong(t *testing.T) {
	directory, id := quiesceFixture(t)
	directory.QuiesceNode("node-a", IneligibleReasonNodeLifecycleUnavailable)
	directory.QuiesceNode("node-a", IneligibleReasonCommitOutcomeUnknown)
	if got := directory.NodeQuiesceReason("node-a"); got != IneligibleReasonCommitOutcomeUnknown {
		t.Fatalf("recoverable was not upgraded to %q: %q", IneligibleReasonCommitOutcomeUnknown, got)
	}
	assertEligibility(t, directory, id, false, IneligibleReasonCommitOutcomeUnknown)
}

func TestRepeatedRecoverableQuiesceIsIdempotent(t *testing.T) {
	directory, id := quiesceFixture(t)
	for iteration := 0; iteration < 5; iteration++ {
		directory.QuiesceNode("node-a", IneligibleReasonRepositoryReadFailure)
		assertEligibility(t, directory, id, false, IneligibleReasonRepositoryReadFailure)
	}
	if err := directory.ReconcileNodeLifecycle(healthyReconcileView()); err != nil {
		t.Fatalf("ReconcileNodeLifecycle() error = %v", err)
	}
	assertEligibility(t, directory, id, true, IneligibleReasonNone)
}

func TestReconcileRejectsStaleFences(t *testing.T) {
	directory := New()
	descriptor := mustDescriptor(t, "resource-stale", "node-a", "temperature_sensor", []string{"read"}, nil, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 5, "reg-new", descriptor))
	mustUpdateLifecycle(t, directory, lifecycleView("node-a", 5, "reg-new", node.StatusActive, true, true))
	directory.QuiesceNode("node-a", IneligibleReasonPersistenceFailure)
	assertEligibility(t, directory, descriptor.ID, false, IneligibleReasonPersistenceFailure)

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
			err := directory.ReconcileNodeLifecycle(test.view)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ReconcileNodeLifecycle() error = %v, want %v", err, test.wantErr)
			}
			assertEligibility(t, directory, descriptor.ID, false, IneligibleReasonPersistenceFailure)
			assertGeneration(t, directory, descriptor.ID, 1)
		})
	}
}

func TestReconcileRequiresHealthyRuntimeState(t *testing.T) {
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
			directory.QuiesceNode("node-a", IneligibleReasonNodeLifecycleUnavailable)
			err := directory.ReconcileNodeLifecycle(test.view)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("ReconcileNodeLifecycle() error = %v, want %v", err, test.wantErr)
			}
			assertEligibility(t, directory, id, false, IneligibleReasonNodeLifecycleUnavailable)
			assertGeneration(t, directory, id, 1)
		})
	}
}

func TestReconcilePreservesTombstone(t *testing.T) {
	directory := New()
	descriptor := mustDescriptor(t, "resource-tombstone", "node-a", "temperature_sensor", []string{"read"}, nil, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", descriptor))
	mustUpdateLifecycle(t, directory, lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, true))
	if err := directory.WithdrawNodeResources("node-a", 1, "registration-a"); err != nil {
		t.Fatal(err)
	}
	assertStateAndGeneration(t, directory, descriptor.ID, PublicationStateWithdrawn, 1)
	directory.QuiesceNode("node-a", IneligibleReasonPersistenceFailure)
	if err := directory.ReconcileNodeLifecycle(lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, true)); err != nil {
		t.Fatal(err)
	}
	got, err := directory.GetByID(descriptor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.PublicationState != PublicationStateWithdrawn || got.Descriptor.Generation != 1 || got.Eligible {
		t.Fatalf("reconcile changed tombstone: %#v", got)
	}
}

func TestReconcileOnlyRestoresTargetNode(t *testing.T) {
	directory := New()
	for _, nodeID := range []model.NodeID{"node-a", "node-b"} {
		descriptor := mustDescriptor(t, model.ResourceID("resource-"+string(nodeID)), nodeID, "temperature_sensor", []string{"read"}, nil, 1)
		mustApply(t, directory, snapshotOf(t, nodeID, 1, "registration-"+string(nodeID), descriptor))
		mustUpdateLifecycle(t, directory, lifecycleView(nodeID, 1, "registration-"+string(nodeID), node.StatusActive, true, true))
		directory.QuiesceNode(nodeID, IneligibleReasonNodeLifecycleUnavailable)
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("both nodes eligible after quiesce: %#v", got)
	}
	if err := directory.ReconcileNodeLifecycle(lifecycleView("node-a", 1, "registration-node-a", node.StatusActive, true, true)); err != nil {
		t.Fatal(err)
	}
	got := directory.ListEligible()
	if len(got) != 1 || got[0].Descriptor.OwnerNodeID != "node-a" {
		t.Fatalf("reconcile restored wrong nodes: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-b"); reason != IneligibleReasonNodeLifecycleUnavailable {
		t.Fatalf("node-b quiesce changed: %q", reason)
	}
}

func TestReconcileKeepsStableSorting(t *testing.T) {
	directory := New()
	descriptors := []model.ResourceDescriptor{
		mustDescriptor(t, "resource-z", "node-a", "temperature_sensor", []string{"read"}, nil, 1),
		mustDescriptor(t, "resource-a", "node-a", "temperature_sensor", []string{"read"}, nil, 1),
		mustDescriptor(t, "resource-m", "node-a", "temperature_sensor", []string{"read"}, nil, 1),
	}
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", descriptors...))
	mustUpdateLifecycle(t, directory, lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, true))
	directory.QuiesceNode("node-a", IneligibleReasonPersistenceFailure)
	if err := directory.ReconcileNodeLifecycle(healthyReconcileView()); err != nil {
		t.Fatal(err)
	}
	byNode := directory.ListByNode("node-a")
	if len(byNode) != 3 || byNode[0].Descriptor.ID != "resource-a" || byNode[1].Descriptor.ID != "resource-m" || byNode[2].Descriptor.ID != "resource-z" {
		t.Fatalf("ListByNode() after reconcile = %#v", byNode)
	}
	all := directory.ListAll()
	if len(all) != 3 || all[0].Descriptor.ID != byNode[0].Descriptor.ID {
		t.Fatalf("ListAll() after reconcile = %#v", all)
	}
	eligible := directory.ListEligible()
	if len(eligible) != 3 || eligible[0].Descriptor.ID != "resource-a" {
		t.Fatalf("ListEligible() after reconcile = %#v", eligible)
	}
}

func TestConcurrentQuiesceReconcileNeverLeaksEligible(t *testing.T) {
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
				directory.QuiesceNode("node-a", IneligibleReasonNodeLifecycleUnavailable)
				_ = directory.ReconcileNodeLifecycle(healthyReconcileView())
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		directory.QuiesceNode("node-a", IneligibleReasonCommitOutcomeUnknown)
		strongApplied.Store(true)
		for {
			select {
			case <-stop:
				return
			default:
			}
			directory.QuiesceNode("node-a", IneligibleReasonRepositoryReadFailure)
		}
	}()
	close(start)
	for !strongApplied.Load() {
		// spin until the strong isolation has been applied at least once
	}
	for iteration := 0; iteration < 2000; iteration++ {
		if got := directory.ListEligible(); len(got) != 0 {
			t.Fatalf("eligible leak after strong quiesce: %#v", got)
		}
	}
	close(stop)
	workers.Wait()
	if reason := directory.NodeQuiesceReason("node-a"); !IsRecoverableQuiesceReason(reason) && reason != IneligibleReasonCommitOutcomeUnknown {
		t.Fatalf("unexpected final reason: %q", reason)
	}
	assertEligibility(t, directory, id, false, directory.NodeQuiesceReason("node-a"))
	active := healthyReconcileView()
	if err := directory.ActivateNodeLifecycle(active); err != nil {
		t.Fatal(err)
	}
	assertEligibility(t, directory, id, true, IneligibleReasonNone)
}

func TestUpdateNodeLifecycleNeverClearsQuiesce(t *testing.T) {
	directory, id := quiesceFixture(t)
	directory.QuiesceNode("node-a", IneligibleReasonRepositoryReadFailure)
	for iteration := 0; iteration < 3; iteration++ {
		if err := directory.UpdateNodeLifecycle(healthyReconcileView()); err != nil {
			t.Fatal(err)
		}
		assertEligibility(t, directory, id, false, IneligibleReasonRepositoryReadFailure)
	}
	if got := directory.NodeQuiesceReason("node-a"); got != IneligibleReasonRepositoryReadFailure {
		t.Fatalf("UpdateNodeLifecycle changed reason: %q", got)
	}
	if err := directory.ReconcileNodeLifecycle(healthyReconcileView()); err != nil {
		t.Fatal(err)
	}
	assertEligibility(t, directory, id, true, IneligibleReasonNone)
}

func TestQuiesceNodeOnUnknownNodeThenPublishFailsClosed(t *testing.T) {
	directory := New()
	directory.QuiesceNode("node-future", IneligibleReasonRepositoryReadFailure)
	if got := directory.NodeQuiesceReason("node-future"); got != IneligibleReasonRepositoryReadFailure {
		t.Fatalf("unknown node quiesce reason = %q", got)
	}
	descriptor := mustDescriptor(t, "resource-future", "node-future", "temperature_sensor", []string{"read"}, nil, 1)
	mustApply(t, directory, snapshotOf(t, "node-future", 1, "registration-future", descriptor))
	mustUpdateLifecycle(t, directory, lifecycleView("node-future", 1, "registration-future", node.StatusActive, true, true))
	assertEligibility(t, directory, descriptor.ID, false, IneligibleReasonRepositoryReadFailure)
	if err := directory.ReconcileNodeLifecycle(lifecycleView("node-future", 1, "registration-future", node.StatusActive, true, true)); err != nil {
		t.Fatal(err)
	}
	assertEligibility(t, directory, descriptor.ID, true, IneligibleReasonNone)
}
