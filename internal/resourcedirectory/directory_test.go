package resourcedirectory

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"

	"dtm/internal/model"
	"dtm/internal/node"
)

func TestDirectoryEmptyAndInputValidation(t *testing.T) {
	directory := New()
	if len(directory.ListAll()) != 0 || len(directory.ListEligible()) != 0 || len(directory.ListIneligible()) != 0 || len(directory.ListWithdrawn()) != 0 {
		t.Fatal("new directory is not empty")
	}
	if _, err := directory.GetByID("missing"); !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("GetByID() error = %v, want %v", err, ErrResourceNotFound)
	}

	for _, test := range []struct {
		name string
		view NodeLifecycleView
	}{
		{name: "empty node", view: NodeLifecycleView{NodeGeneration: 1, RegistrationID: "r", Status: node.StatusActive}},
		{name: "zero generation", view: NodeLifecycleView{NodeID: "n", RegistrationID: "r", Status: node.StatusActive}},
		{name: "trimmed registration forbidden", view: NodeLifecycleView{NodeID: "n", NodeGeneration: 1, RegistrationID: " r", Status: node.StatusActive}},
		{name: "unknown status", view: NodeLifecycleView{NodeID: "n", NodeGeneration: 1, RegistrationID: "r", Status: "unknown"}},
	} {
		t.Run("lifecycle/"+test.name, func(t *testing.T) {
			if err := directory.UpdateNodeLifecycle(test.view); !errors.Is(err, ErrInvalidNodeLifecycleView) {
				t.Fatalf("UpdateNodeLifecycle() error = %v, want %v", err, ErrInvalidNodeLifecycleView)
			}
		})
	}

	valid := mustDescriptor(t, "resource-1", "node-1", "temperature_sensor", []string{"read_temperature"}, map[string]string{"zone": "a"}, 1)
	invalid := valid.Clone()
	invalid.Operations = nil
	otherOwner := valid.Clone()
	otherOwner.OwnerNodeID = "node-2"
	for _, test := range []struct {
		name     string
		snapshot NodeResourceSnapshot
	}{
		{name: "empty node", snapshot: NodeResourceSnapshot{NodeGeneration: 1, RegistrationID: "r"}},
		{name: "zero generation", snapshot: NodeResourceSnapshot{NodeID: "node-1", RegistrationID: "r"}},
		{name: "empty registration", snapshot: NodeResourceSnapshot{NodeID: "node-1", NodeGeneration: 1}},
		{name: "invalid descriptor", snapshot: NodeResourceSnapshot{NodeID: "node-1", NodeGeneration: 1, RegistrationID: "r", Resources: []model.ResourceDescriptor{invalid}}},
		{name: "owner mismatch", snapshot: NodeResourceSnapshot{NodeID: "node-1", NodeGeneration: 1, RegistrationID: "r", Resources: []model.ResourceDescriptor{otherOwner}}},
		{name: "duplicate id", snapshot: NodeResourceSnapshot{NodeID: "node-1", NodeGeneration: 1, RegistrationID: "r", Resources: []model.ResourceDescriptor{valid, valid}}},
	} {
		t.Run("snapshot/"+test.name, func(t *testing.T) {
			if _, err := directory.ApplyNodeSnapshot(test.snapshot); !errors.Is(err, ErrInvalidResourceSnapshot) {
				t.Fatalf("ApplyNodeSnapshot() error = %v, want %v", err, ErrInvalidResourceSnapshot)
			}
		})
	}
}

func TestDirectoryEmptyValuedAttributeSemantics(t *testing.T) {
	t.Run("requirement matching distinguishes missing keys", func(t *testing.T) {
		tests := []struct {
			name               string
			requiredAttributes map[string]string
			resourceAttributes map[string]string
			wantMatches        int
		}{
			{name: "missing required empty-valued key", requiredAttributes: map[string]string{"required": ""}, resourceAttributes: map[string]string{"other": ""}},
			{name: "present required empty-valued key", requiredAttributes: map[string]string{"required": ""}, resourceAttributes: map[string]string{"required": ""}, wantMatches: 1},
			{name: "empty resource attributes", requiredAttributes: map[string]string{"required": ""}, resourceAttributes: map[string]string{}},
			{name: "matching value permits extra empty attribute", requiredAttributes: map[string]string{"required": "x"}, resourceAttributes: map[string]string{"required": "x", "extra": ""}, wantMatches: 1},
			{name: "different value", requiredAttributes: map[string]string{"required": "x"}, resourceAttributes: map[string]string{"required": ""}},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				directory := New()
				mustUpdateLifecycle(t, directory, lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, true))
				descriptor := mustDescriptor(t, "resource-a", "node-a", "temperature_sensor", []string{"read_temperature"}, test.resourceAttributes, 1)
				mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", descriptor))
				requirement, err := model.NewResourceRequirement(model.ResourceKindCapability, "temperature_sensor", "read_temperature", test.requiredAttributes)
				if err != nil {
					t.Fatal(err)
				}
				matches, err := directory.MatchRequirement(requirement)
				if err != nil {
					t.Fatal(err)
				}
				if len(matches) != test.wantMatches {
					t.Fatalf("MatchRequirement() returned %d resources, want %d", len(matches), test.wantMatches)
				}
			})
		}
	})

	t.Run("descriptor changes distinguish empty-valued keys", func(t *testing.T) {
		directory := New()
		left := mustDescriptor(t, "resource-a", "node-a", "temperature_sensor", []string{"read_temperature"}, map[string]string{"left": ""}, 1)
		mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", left))

		right := mustDescriptor(t, "resource-a", "node-a", "temperature_sensor", []string{"read_temperature"}, map[string]string{"right": ""}, 1)
		mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", right))
		assertGeneration(t, directory, right.ID, 2)
		got, err := directory.GetByID(right.ID)
		if err != nil {
			t.Fatal(err)
		}
		if _, exists := got.Descriptor.Attributes["right"]; !exists {
			t.Fatalf("committed attributes = %v, want right key", got.Descriptor.Attributes)
		}
		if _, exists := got.Descriptor.Attributes["left"]; exists {
			t.Fatalf("committed attributes = %v, left key must be absent", got.Descriptor.Attributes)
		}

		replay := right.Clone()
		replay.Generation = 2
		mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", replay))
		assertGeneration(t, directory, right.ID, 2)

		withoutAttributes := right.Clone()
		withoutAttributes.Generation = 2
		withoutAttributes.Attributes = map[string]string{}
		mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", withoutAttributes))
		assertGeneration(t, directory, right.ID, 3)
	})
}

func TestDirectorySupportsMultipleSameSemanticResourceInstances(t *testing.T) {
	directory := New()
	mustUpdateLifecycle(t, directory, lifecycleView("node-1", 1, "registration-1", node.StatusActive, true, true))
	sensorA := mustDescriptor(t, "sensor-a", "node-1", "temperature_sensor", []string{"read_temperature"}, map[string]string{"channel": "a"}, 1)
	sensorB := mustDescriptor(t, "sensor-b", "node-1", "temperature_sensor", []string{"read_temperature"}, map[string]string{"channel": "b"}, 1)
	snapshot := snapshotOf(t, "node-1", 1, "registration-1", sensorB, sensorA)
	views := mustApply(t, directory, snapshot)
	if len(views) != 2 || views[0].Descriptor.ID != "sensor-a" || views[1].Descriptor.ID != "sensor-b" {
		t.Fatalf("published views = %#v, want sensor-a then sensor-b", views)
	}
	for _, view := range views {
		if view.Descriptor.Generation != 1 {
			t.Fatalf("first generation for %q = %d, want 1", view.Descriptor.ID, view.Descriptor.Generation)
		}
	}
	if got := directory.ListByNode("node-1"); len(got) != 2 || got[0].Descriptor.ID != "sensor-a" || got[1].Descriptor.ID != "sensor-b" {
		t.Fatalf("ListByNode() = %#v", got)
	}
	requirement, err := model.NewResourceRequirement(model.ResourceKindCapability, "temperature_sensor", "read_temperature", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := directory.MatchRequirement(requirement); err != nil || len(got) != 2 || got[0].Descriptor.ID != "sensor-a" || got[1].Descriptor.ID != "sensor-b" {
		t.Fatalf("MatchRequirement() = %#v, %v", got, err)
	}

	sensorAChanged := sensorA.Clone()
	sensorAChanged.Attributes["channel"] = "a2"
	sensorBReplay := sensorB.Clone()
	mustApply(t, directory, snapshotOf(t, "node-1", 1, "registration-1", sensorAChanged, sensorBReplay))
	assertGeneration(t, directory, "sensor-a", 2)
	assertGeneration(t, directory, "sensor-b", 1)

	sensorACurrent := sensorAChanged.Clone()
	sensorACurrent.Generation = 2
	mustApply(t, directory, snapshotOf(t, "node-1", 1, "registration-1", sensorACurrent))
	assertStateAndGeneration(t, directory, "sensor-a", PublicationStatePublished, 2)
	assertStateAndGeneration(t, directory, "sensor-b", PublicationStateWithdrawn, 1)
	if got := directory.ListByNode("node-1"); len(got) != 2 {
		t.Fatalf("withdrawing one instance changed sibling visibility: %#v", got)
	}
	if got, err := directory.MatchRequirement(requirement); err != nil || len(got) != 1 || got[0].Descriptor.ID != "sensor-a" {
		t.Fatalf("MatchRequirement() after withdrawal = %#v, %v", got, err)
	}
}

func TestDirectoryMultipleInstanceSnapshotFailureIsAtomic(t *testing.T) {
	directory := New()
	sensorA := mustDescriptor(t, "sensor-a", "node-1", "temperature_sensor", []string{"read_temperature"}, map[string]string{"channel": "a"}, 1)
	sensorB := mustDescriptor(t, "sensor-b", "node-1", "temperature_sensor", []string{"read_temperature"}, map[string]string{"channel": "b"}, 1)
	mustApply(t, directory, snapshotOf(t, "node-1", 1, "registration-1", sensorA, sensorB))

	sensorAChanged := sensorA.Clone()
	sensorAChanged.Attributes["channel"] = "a2"
	sensorBStale := sensorB.Clone()
	sensorBStale.Generation = 2
	if _, err := directory.ApplyNodeSnapshot(snapshotOf(t, "node-1", 1, "registration-1", sensorAChanged, sensorBStale)); !errors.Is(err, ErrStaleResourceGeneration) {
		t.Fatalf("ApplyNodeSnapshot() error = %v, want %v", err, ErrStaleResourceGeneration)
	}
	assertGeneration(t, directory, "sensor-a", 1)
	assertGeneration(t, directory, "sensor-b", 1)
	if got, _ := directory.GetByID("sensor-a"); got.Descriptor.Attributes["channel"] != "a" {
		t.Fatalf("failed snapshot changed sensor-a: %#v", got)
	}

	duplicate := NodeResourceSnapshot{
		NodeID:         "node-1",
		NodeGeneration: 1,
		RegistrationID: "registration-1",
		Resources:      []model.ResourceDescriptor{sensorA.Clone(), sensorB.Clone(), sensorA.Clone()},
	}
	if _, err := directory.ApplyNodeSnapshot(duplicate); !errors.Is(err, ErrInvalidResourceSnapshot) {
		t.Fatalf("duplicate ID error = %v, want %v", err, ErrInvalidResourceSnapshot)
	}
	assertGeneration(t, directory, "sensor-a", 1)
	assertGeneration(t, directory, "sensor-b", 1)
}

func TestDirectoryFirstPublicationStableQueriesAndDefensiveCopies(t *testing.T) {
	directory := New()
	mustUpdateLifecycle(t, directory, lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, true))
	temperature := mustLegacyDescriptor(t, "node-a", "temperature_sensor", 1)
	cooling := mustLegacyDescriptor(t, "node-a", "cooling_control", 1)
	temperature.Attributes["zone"] = "north"
	input := []model.ResourceDescriptor{temperature, cooling}
	snapshot, err := NewNodeResourceSnapshot("node-a", 1, "registration-a", input)
	if err != nil {
		t.Fatal(err)
	}
	input[0].Attributes["zone"] = "mutated-input"
	input[0].Operations[0].InputSchemaID = "mutated-input"

	views, err := directory.ApplyNodeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if len(views) != 2 || !views[0].Eligible || !views[1].Eligible {
		t.Fatalf("published views = %#v", views)
	}
	if views[0].Descriptor.ID > views[1].Descriptor.ID {
		t.Fatalf("views not sorted by ResourceID: %q > %q", views[0].Descriptor.ID, views[1].Descriptor.ID)
	}
	for _, view := range views {
		if view.Descriptor.Generation != 1 || view.PublicationState != PublicationStatePublished {
			t.Fatalf("first publication = %#v", view)
		}
	}

	got, err := directory.GetByID(temperature.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Descriptor.Attributes["zone"] != "north" || got.Descriptor.Operations[0].InputSchemaID != "" {
		t.Fatalf("constructor input aliased into directory: %#v", got.Descriptor)
	}
	got.Descriptor.Attributes["zone"] = "mutated-output"
	got.Descriptor.Operations[0].InputSchemaID = "mutated-output"
	again, _ := directory.GetByID(temperature.ID)
	if again.Descriptor.Attributes["zone"] != "north" || again.Descriptor.Operations[0].InputSchemaID != "" {
		t.Fatalf("query output aliases directory: %#v", again.Descriptor)
	}

	all := directory.ListAll()
	byNode := directory.ListByNode("node-a")
	if len(all) != 2 || len(byNode) != 2 || all[0].Descriptor.ID != byNode[0].Descriptor.ID {
		t.Fatalf("stable lists differ: all=%#v byNode=%#v", all, byNode)
	}
}

func TestDirectoryGenerationReplayAndSemanticChanges(t *testing.T) {
	directory := New()
	descriptor := mustDescriptor(t, "custom-resource", "node-a", "custom_sensor", []string{"read_value", "reset_value"}, map[string]string{"rack": "1", "zone": "a"}, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", descriptor))

	reordered := mustDescriptor(t, "custom-resource", "node-a", "custom_sensor", []string{"reset_value", "read_value"}, map[string]string{"zone": "a", "rack": "1"}, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", reordered))
	assertGeneration(t, directory, descriptor.ID, 1)

	changed := mustDescriptor(t, "custom-resource", "node-a", "custom_sensor", []string{"read_value", "reset_value"}, map[string]string{"rack": "1", "zone": "b"}, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", changed))
	assertGeneration(t, directory, descriptor.ID, 2)

	replay := changed.Clone()
	replay.Generation = 2
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", replay))
	assertGeneration(t, directory, descriptor.ID, 2)

	stale := changed.Clone()
	stale.Generation = 1
	if _, err := directory.ApplyNodeSnapshot(snapshotOf(t, "node-a", 1, "registration-a", stale)); !errors.Is(err, ErrStaleResourceGeneration) {
		t.Fatalf("stale resource generation error = %v", err)
	}
	assertGeneration(t, directory, descriptor.ID, 2)

	nextRegistration := changed.Clone()
	nextRegistration.Generation = 2
	mustApply(t, directory, snapshotOf(t, "node-a", 2, "registration-b", nextRegistration))
	assertGeneration(t, directory, descriptor.ID, 3)

	if _, err := directory.ApplyNodeSnapshot(snapshotOf(t, "node-a", 2, "registration-old", withGeneration(nextRegistration, 3))); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("same-generation registration error = %v", err)
	}
	if _, err := directory.ApplyNodeSnapshot(snapshotOf(t, "node-a", 1, "registration-a", withGeneration(nextRegistration, 3))); !errors.Is(err, ErrStaleNodeGeneration) {
		t.Fatalf("old node generation error = %v", err)
	}
}

func TestDirectorySnapshotDiffWithdrawAndRepublish(t *testing.T) {
	directory := New()
	a := mustDescriptor(t, "resource-a", "node-a", "sensor_a", []string{"read_a"}, nil, 1)
	b := mustDescriptor(t, "resource-b", "node-a", "sensor_b", []string{"read_b"}, map[string]string{"version": "1"}, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", a, b))

	bChanged := mustDescriptor(t, "resource-b", "node-a", "sensor_b", []string{"read_b"}, map[string]string{"version": "2"}, 1)
	c := mustDescriptor(t, "resource-c", "node-a", "sensor_c", []string{"read_c"}, nil, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", bChanged, c))
	assertStateAndGeneration(t, directory, a.ID, PublicationStateWithdrawn, 1)
	assertStateAndGeneration(t, directory, b.ID, PublicationStatePublished, 2)
	assertStateAndGeneration(t, directory, c.ID, PublicationStatePublished, 1)

	aRepublished := a.Clone()
	aRepublished.Generation = 1
	bCurrent := bChanged.Clone()
	bCurrent.Generation = 2
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", c, aRepublished, bCurrent))
	assertStateAndGeneration(t, directory, a.ID, PublicationStatePublished, 2)
	if len(directory.ListWithdrawn()) != 0 {
		t.Fatalf("withdrawn resources remain after full republish: %#v", directory.ListWithdrawn())
	}
}

func TestDirectorySnapshotFailureIsAtomic(t *testing.T) {
	directory := New()
	a := mustDescriptor(t, "resource-a", "node-a", "sensor_a", []string{"read_a"}, map[string]string{"version": "1"}, 1)
	b := mustDescriptor(t, "resource-b", "node-a", "sensor_b", []string{"read_b"}, nil, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", a, b))

	aChanged := mustDescriptor(t, "resource-a", "node-a", "sensor_a", []string{"read_a"}, map[string]string{"version": "2"}, 1)
	bStale := b.Clone()
	bStale.Generation = 2
	newResource := mustDescriptor(t, "resource-c", "node-a", "sensor_c", []string{"read_c"}, nil, 1)
	if _, err := directory.ApplyNodeSnapshot(snapshotOf(t, "node-a", 1, "registration-a", aChanged, bStale, newResource)); !errors.Is(err, ErrStaleResourceGeneration) {
		t.Fatalf("ApplyNodeSnapshot() error = %v, want stale generation", err)
	}
	assertStateAndGeneration(t, directory, a.ID, PublicationStatePublished, 1)
	assertStateAndGeneration(t, directory, b.ID, PublicationStatePublished, 1)
	if _, err := directory.GetByID(newResource.ID); !errors.Is(err, ErrResourceNotFound) {
		t.Fatalf("failed snapshot partially added resource: %v", err)
	}
}

func TestDirectoryOwnerConflictFailsClosed(t *testing.T) {
	directory := New()
	original := mustDescriptor(t, "shared-id", "node-a", "sensor_a", []string{"read_a"}, nil, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", original))
	conflict := mustDescriptor(t, "shared-id", "node-b", "sensor_b", []string{"read_b"}, nil, 1)
	if _, err := directory.ApplyNodeSnapshot(snapshotOf(t, "node-b", 1, "registration-b", conflict)); !errors.Is(err, ErrResourceOwnerConflict) {
		t.Fatalf("owner conflict error = %v", err)
	}
	got, _ := directory.GetByID(original.ID)
	if got.Descriptor.OwnerNodeID != "node-a" || got.Descriptor.Generation != 1 {
		t.Fatalf("owner conflict changed record: %#v", got)
	}
	if len(directory.ListByNode("node-b")) != 0 {
		t.Fatalf("owner conflict partially published node-b: %#v", directory.ListByNode("node-b"))
	}
}

func TestDirectoryGenerationOverflowFailsClosed(t *testing.T) {
	directory := New()
	max := mustDescriptor(t, "resource-max", "node-a", "sensor_a", []string{"read_a"}, map[string]string{"version": "1"}, model.ResourceGeneration(math.MaxUint64))
	directory.records[max.ID] = resourceRecord{
		descriptor: max, nodeGeneration: 1, registrationID: "registration-a", publicationState: PublicationStatePublished,
	}
	directory.nodeFences["node-a"] = nodeFence{generation: 1, registrationID: "registration-a"}

	changed := mustDescriptor(t, "resource-max", "node-a", "sensor_a", []string{"read_a"}, map[string]string{"version": "2"}, model.ResourceGeneration(math.MaxUint64))
	if _, err := directory.ApplyNodeSnapshot(snapshotOf(t, "node-a", 1, "registration-a", changed)); !errors.Is(err, ErrResourceGenerationOverflow) {
		t.Fatalf("overflow error = %v", err)
	}
	got, _ := directory.GetByID(max.ID)
	if got.Descriptor.Generation != model.ResourceGeneration(math.MaxUint64) || got.Descriptor.Attributes["version"] != "1" {
		t.Fatalf("overflow changed state: %#v", got)
	}
}

func TestDirectoryLifecycleEligibilityAndFencing(t *testing.T) {
	directory := New()
	descriptor := mustLegacyDescriptor(t, "node-a", "temperature_sensor", 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", descriptor))
	assertEligibility(t, directory, descriptor.ID, false, IneligibleReasonNodeLifecycleUnavailable)

	for _, test := range []struct {
		name     string
		status   node.Status
		lease    bool
		endpoint bool
		eligible bool
		reason   IneligibleReason
	}{
		{name: "active", status: node.StatusActive, lease: true, endpoint: true, eligible: true},
		{name: "registered", status: node.StatusRegistered, lease: true, endpoint: true, reason: IneligibleReasonNodeNotActive},
		{name: "stale", status: node.StatusStale, lease: true, endpoint: true, reason: IneligibleReasonNodeNotActive},
		{name: "suspect", status: node.StatusSuspect, lease: true, endpoint: true, reason: IneligibleReasonNodeNotActive},
		{name: "offline", status: node.StatusOffline, lease: true, endpoint: true, reason: IneligibleReasonNodeNotActive},
		{name: "recovering", status: node.StatusRecovering, lease: true, endpoint: true, reason: IneligibleReasonNodeNotActive},
		{name: "invalid lease", status: node.StatusActive, endpoint: true, reason: IneligibleReasonLeaseInvalid},
		{name: "inactive endpoint", status: node.StatusActive, lease: true, reason: IneligibleReasonEndpointInactive},
	} {
		t.Run(test.name, func(t *testing.T) {
			mustUpdateLifecycle(t, directory, lifecycleView("node-a", 1, "registration-a", test.status, test.lease, test.endpoint))
			assertEligibility(t, directory, descriptor.ID, test.eligible, test.reason)
			assertGeneration(t, directory, descriptor.ID, 1)
		})
	}

	mustUpdateLifecycle(t, directory, lifecycleView("node-a", 2, "registration-b", node.StatusActive, true, true))
	assertEligibility(t, directory, descriptor.ID, false, IneligibleReasonNodeGenerationMismatch)
	if err := directory.UpdateNodeLifecycle(lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, true)); !errors.Is(err, ErrStaleNodeGeneration) {
		t.Fatalf("old lifecycle generation error = %v", err)
	}
	if err := directory.UpdateNodeLifecycle(lifecycleView("node-a", 2, "registration-old", node.StatusActive, true, true)); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("old lifecycle registration error = %v", err)
	}

	next := descriptor.Clone()
	next.Generation = 1
	mustApply(t, directory, snapshotOf(t, "node-a", 2, "registration-b", next))
	assertEligibility(t, directory, descriptor.ID, true, IneligibleReasonNone)
	assertGeneration(t, directory, descriptor.ID, 2)
}

func TestDirectoryWithdrawFencing(t *testing.T) {
	directory := New()
	descriptor := mustLegacyDescriptor(t, "node-a", "temperature_sensor", 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", descriptor))
	next := descriptor.Clone()
	next.Generation = 1
	mustApply(t, directory, snapshotOf(t, "node-a", 2, "registration-b", next))

	if err := directory.WithdrawNodeResources("node-a", 1, "registration-a"); !errors.Is(err, ErrStaleNodeGeneration) {
		t.Fatalf("old generation withdraw error = %v", err)
	}
	if err := directory.WithdrawNodeResources("node-a", 2, "registration-old"); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("old registration withdraw error = %v", err)
	}
	assertStateAndGeneration(t, directory, descriptor.ID, PublicationStatePublished, 2)
	if err := directory.WithdrawNodeResources("node-a", 2, "registration-b"); err != nil {
		t.Fatal(err)
	}
	assertStateAndGeneration(t, directory, descriptor.ID, PublicationStateWithdrawn, 2)
	assertEligibility(t, directory, descriptor.ID, false, IneligibleReasonResourceWithdrawn)
}

func TestDirectoryRequirementMatchingIsEligibleExactAndStable(t *testing.T) {
	directory := New()
	for _, id := range []model.NodeID{"node-b", "node-a"} {
		mustUpdateLifecycle(t, directory, lifecycleView(id, 1, "registration-"+string(id), node.StatusActive, true, true))
		descriptor := mustLegacyDescriptor(t, id, "temperature_sensor", 1)
		descriptor.Attributes["zone"] = "north"
		mustApply(t, directory, snapshotOf(t, id, 1, "registration-"+string(id), descriptor))
	}
	requirement, err := model.NewResourceRequirement(model.ResourceKindCapability, "temperature_sensor", "read_temperature", map[string]string{"zone": "north"})
	if err != nil {
		t.Fatal(err)
	}
	matches, err := directory.MatchRequirement(requirement)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 2 || matches[0].Descriptor.ID > matches[1].Descriptor.ID {
		t.Fatalf("stable matches = %#v", matches)
	}

	missingAttribute, _ := model.NewResourceRequirement(model.ResourceKindCapability, "temperature_sensor", "read_temperature", map[string]string{"zone": "south"})
	if got, err := directory.MatchRequirement(missingAttribute); err != nil || len(got) != 0 {
		t.Fatalf("attribute mismatch = %#v, %v", got, err)
	}
	wrongType, _ := model.NewResourceRequirement(model.ResourceKindCapability, "cooling_control", "read_temperature", nil)
	if got, err := directory.MatchRequirement(wrongType); err != nil || len(got) != 0 {
		t.Fatalf("type mismatch = %#v, %v", got, err)
	}
	wrongOperation, _ := model.NewResourceRequirement(model.ResourceKindCapability, "temperature_sensor", "set_target_temperature", nil)
	if got, err := directory.MatchRequirement(wrongOperation); err != nil || len(got) != 0 {
		t.Fatalf("operation mismatch = %#v, %v", got, err)
	}
	mustUpdateLifecycle(t, directory, lifecycleView("node-a", 1, "registration-node-a", node.StatusOffline, false, false))
	matches, _ = directory.MatchRequirement(requirement)
	if len(matches) != 1 || matches[0].Descriptor.OwnerNodeID != "node-b" {
		t.Fatalf("ineligible resource matched: %#v", matches)
	}
	if err := directory.WithdrawNodeResources("node-b", 1, "registration-node-b"); err != nil {
		t.Fatal(err)
	}
	matches, _ = directory.MatchRequirement(requirement)
	if len(matches) != 0 {
		t.Fatalf("withdrawn resource matched: %#v", matches)
	}
}

func TestDirectoryConcurrentOperations(t *testing.T) {
	directory := New()
	for index := 0; index < 4; index++ {
		nodeID := model.NodeID(fmt.Sprintf("node-%d", index))
		registrationID := fmt.Sprintf("registration-%d", index)
		mustUpdateLifecycle(t, directory, lifecycleView(nodeID, 1, registrationID, node.StatusActive, true, true))
		descriptor := mustDescriptor(t, model.ResourceID(fmt.Sprintf("resource-%d", index)), nodeID, "temperature_sensor", []string{"read_temperature"}, map[string]string{"node": fmt.Sprint(index)}, 1)
		mustApply(t, directory, snapshotOf(t, nodeID, 1, registrationID, descriptor))
	}
	mustUpdateLifecycle(t, directory, lifecycleView("node-withdraw", 1, "registration-withdraw", node.StatusActive, true, true))
	withdrawnDescriptor := mustDescriptor(t, "resource-withdraw", "node-withdraw", "temperature_sensor", []string{"read_temperature"}, nil, 1)
	mustApply(t, directory, snapshotOf(t, "node-withdraw", 1, "registration-withdraw", withdrawnDescriptor))
	requirement, _ := model.NewResourceRequirement(model.ResourceKindCapability, "temperature_sensor", "read_temperature", nil)
	start := make(chan struct{})
	var workers sync.WaitGroup

	for reader := 0; reader < 6; reader++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for iteration := 0; iteration < 200; iteration++ {
				_ = directory.ListAll()
				_ = directory.ListByNode("node-0")
				_, _ = directory.GetByID("resource-0")
				_, _ = directory.MatchRequirement(requirement)
			}
		}()
	}
	for index := 1; index < 4; index++ {
		index := index
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			nodeID := model.NodeID(fmt.Sprintf("node-%d", index))
			for iteration := 0; iteration < 100; iteration++ {
				leaseValid := iteration%2 == 0
				if err := directory.UpdateNodeLifecycle(lifecycleView(nodeID, 1, fmt.Sprintf("registration-%d", index), node.StatusActive, leaseValid, true)); err != nil {
					t.Errorf("UpdateNodeLifecycle() error = %v", err)
					return
				}
			}
		}()
	}
	workers.Add(3)
	go func() {
		defer workers.Done()
		<-start
		generation := model.ResourceGeneration(1)
		for nodeGeneration := int64(2); nodeGeneration <= 50; nodeGeneration++ {
			descriptor := mustDescriptor(t, "resource-0", "node-0", "temperature_sensor", []string{"read_temperature"}, map[string]string{"node": "0"}, generation)
			views, err := directory.ApplyNodeSnapshot(snapshotOf(t, "node-0", nodeGeneration, fmt.Sprintf("registration-new-%d", nodeGeneration), descriptor))
			if err != nil {
				t.Errorf("new snapshot generation %d: %v", nodeGeneration, err)
				return
			}
			generation = views[0].Descriptor.Generation
			if err := directory.UpdateNodeLifecycle(lifecycleView("node-0", nodeGeneration, fmt.Sprintf("registration-new-%d", nodeGeneration), node.StatusActive, true, true)); err != nil {
				t.Errorf("new lifecycle generation %d: %v", nodeGeneration, err)
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		generation := model.ResourceGeneration(1)
		for iteration := 0; iteration < 100; iteration++ {
			if err := directory.WithdrawNodeResources("node-withdraw", 1, "registration-withdraw"); err != nil {
				t.Errorf("WithdrawNodeResources() error = %v", err)
				return
			}
			descriptor := withdrawnDescriptor.Clone()
			descriptor.Generation = generation
			views, err := directory.ApplyNodeSnapshot(snapshotOf(t, "node-withdraw", 1, "registration-withdraw", descriptor))
			if err != nil {
				t.Errorf("republish after withdraw: %v", err)
				return
			}
			generation = views[0].Descriptor.Generation
		}
	}()
	go func() {
		defer workers.Done()
		<-start
		old := mustDescriptor(t, "resource-0", "node-0", "temperature_sensor", []string{"read_temperature"}, map[string]string{"node": "0"}, 1)
		for iteration := 0; iteration < 100; iteration++ {
			_, err := directory.ApplyNodeSnapshot(snapshotOf(t, "node-0", 1, "registration-0", old))
			if err != nil && !errors.Is(err, ErrStaleNodeGeneration) && !errors.Is(err, ErrStaleResourceGeneration) {
				t.Errorf("old snapshot error = %v", err)
				return
			}
		}
	}()

	close(start)
	workers.Wait()
	got, err := directory.GetByID("resource-0")
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeGeneration != 50 || got.Descriptor.Generation != 50 || !got.Eligible {
		t.Fatalf("final concurrent record = %#v", got)
	}
}

func TestDirectoryConcurrentMultipleSameSemanticResourceInstances(t *testing.T) {
	directory := New()
	mustUpdateLifecycle(t, directory, lifecycleView("node-multi", 1, "registration-multi", node.StatusActive, true, true))
	sensorA := mustDescriptor(t, "sensor-a", "node-multi", "temperature_sensor", []string{"read_temperature"}, map[string]string{"channel": "a-0"}, 1)
	sensorB := mustDescriptor(t, "sensor-b", "node-multi", "temperature_sensor", []string{"read_temperature"}, map[string]string{"channel": "b"}, 1)
	mustApply(t, directory, snapshotOf(t, "node-multi", 1, "registration-multi", sensorA, sensorB))
	requirement, err := model.NewResourceRequirement(model.ResourceKindCapability, "temperature_sensor", "read_temperature", nil)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	var workers sync.WaitGroup
	for reader := 0; reader < 8; reader++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for iteration := 0; iteration < 200; iteration++ {
				matches, matchErr := directory.MatchRequirement(requirement)
				if matchErr != nil {
					t.Errorf("MatchRequirement() error = %v", matchErr)
					return
				}
				if len(matches) != 2 || matches[0].Descriptor.ID != "sensor-a" || matches[1].Descriptor.ID != "sensor-b" {
					t.Errorf("concurrent matches = %#v", matches)
					return
				}
				if got := directory.ListByNode("node-multi"); len(got) != 2 {
					t.Errorf("concurrent ListByNode() = %#v", got)
					return
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		generation := model.ResourceGeneration(1)
		for iteration := 1; iteration <= 100; iteration++ {
			currentA := sensorA.Clone()
			currentA.Generation = generation
			currentA.Attributes["channel"] = fmt.Sprintf("a-%d", iteration)
			currentB := sensorB.Clone()
			snapshot := NodeResourceSnapshot{
				NodeID:         "node-multi",
				NodeGeneration: 1,
				RegistrationID: "registration-multi",
				Resources:      []model.ResourceDescriptor{currentB, currentA},
			}
			views, applyErr := directory.ApplyNodeSnapshot(snapshot)
			if applyErr != nil {
				t.Errorf("ApplyNodeSnapshot() iteration %d error = %v", iteration, applyErr)
				return
			}
			for _, view := range views {
				if view.Descriptor.ID == "sensor-a" {
					generation = view.Descriptor.Generation
				}
			}
		}
	}()

	close(start)
	workers.Wait()
	assertGeneration(t, directory, "sensor-a", 101)
	assertGeneration(t, directory, "sensor-b", 1)
}

func mustLegacyDescriptor(t *testing.T, nodeID model.NodeID, capability model.Capability, generation model.ResourceGeneration) model.ResourceDescriptor {
	t.Helper()
	descriptor, err := model.AdaptLegacyCapability(nodeID, capability, generation)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func mustDescriptor(
	t *testing.T,
	id model.ResourceID,
	nodeID model.NodeID,
	resourceType model.ResourceType,
	operations []string,
	attributes map[string]string,
	generation model.ResourceGeneration,
) model.ResourceDescriptor {
	t.Helper()
	describedOperations := make([]model.OperationDescriptor, len(operations))
	for index, operationID := range operations {
		operation, err := model.NewOperationDescriptor(model.OperationID(operationID), model.IdempotencyIdempotent, "", "")
		if err != nil {
			t.Fatal(err)
		}
		describedOperations[index] = operation
	}
	descriptor, err := model.NewResourceDescriptor(id, model.ResourceKindCapability, resourceType, nodeID, generation, describedOperations, attributes)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func snapshotOf(t *testing.T, nodeID model.NodeID, generation int64, registrationID string, descriptors ...model.ResourceDescriptor) NodeResourceSnapshot {
	t.Helper()
	snapshot, err := NewNodeResourceSnapshot(nodeID, generation, registrationID, descriptors)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func lifecycleView(nodeID model.NodeID, generation int64, registrationID string, status node.Status, leaseValid, endpointActive bool) NodeLifecycleView {
	return NodeLifecycleView{
		NodeID: nodeID, NodeGeneration: generation, RegistrationID: registrationID,
		Status: status, LeaseValid: leaseValid, EndpointActive: endpointActive,
	}
}

func mustApply(t *testing.T, directory *Directory, snapshot NodeResourceSnapshot) []ResourceRecordView {
	t.Helper()
	views, err := directory.ApplyNodeSnapshot(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	return views
}

func mustUpdateLifecycle(t *testing.T, directory *Directory, view NodeLifecycleView) {
	t.Helper()
	if err := directory.UpdateNodeLifecycle(view); err != nil {
		t.Fatal(err)
	}
}

func assertGeneration(t *testing.T, directory *Directory, id model.ResourceID, generation model.ResourceGeneration) {
	t.Helper()
	got, err := directory.GetByID(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Descriptor.Generation != generation {
		t.Fatalf("resource %q generation = %d, want %d", id, got.Descriptor.Generation, generation)
	}
}

func assertStateAndGeneration(t *testing.T, directory *Directory, id model.ResourceID, state PublicationState, generation model.ResourceGeneration) {
	t.Helper()
	got, err := directory.GetByID(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.PublicationState != state || got.Descriptor.Generation != generation {
		t.Fatalf("resource %q = %#v, want state=%q generation=%d", id, got, state, generation)
	}
}

func assertEligibility(t *testing.T, directory *Directory, id model.ResourceID, eligible bool, reason IneligibleReason) {
	t.Helper()
	got, err := directory.GetByID(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.Eligible != eligible || got.IneligibleReason != reason {
		t.Fatalf("resource %q eligibility = %v/%q, want %v/%q", id, got.Eligible, got.IneligibleReason, eligible, reason)
	}
}

func withGeneration(descriptor model.ResourceDescriptor, generation model.ResourceGeneration) model.ResourceDescriptor {
	clone := descriptor.Clone()
	clone.Generation = generation
	return clone
}

func TestPublishTransitionQuiescesUntilExplicitActivation(t *testing.T) {
	directory := New()
	first := mustDescriptor(t, "resource-a", "node-a", "temperature_sensor", []string{"read"}, nil, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", first))
	active := lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, true)
	mustUpdateLifecycle(t, directory, active)
	assertEligibility(t, directory, first.ID, true, IneligibleReasonNone)

	second := mustDescriptor(t, "resource-b", "node-a", "cooling_control", []string{"write"}, nil, 1)
	plan, err := PlanNodeResourceSnapshot(directory.ListAll(), snapshotOf(t, "node-a", 1, "registration-a", first, second))
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.PublishTransition(plan); err != nil {
		t.Fatal(err)
	}
	if got := len(directory.ListEligible()); got != 0 {
		t.Fatalf("eligible immediately after publication = %d, want 0", got)
	}
	if err := directory.UpdateNodeLifecycle(active); err != nil {
		t.Fatal(err)
	}
	if got := len(directory.ListEligible()); got != 0 {
		t.Fatalf("ordinary lifecycle update cleared quiescence: %d", got)
	}
	if err := directory.ActivateNodeLifecycle(active); err != nil {
		t.Fatal(err)
	}
	if got := len(directory.ListEligible()); got != 2 {
		t.Fatalf("eligible after activation = %d, want 2", got)
	}
}

func TestQuiesceNodePreservesResourceState(t *testing.T) {
	directory := New()
	descriptor := mustDescriptor(t, "resource-a", "node-a", "temperature_sensor", []string{"read"}, map[string]string{"empty": ""}, 1)
	mustApply(t, directory, snapshotOf(t, "node-a", 1, "registration-a", descriptor))
	mustUpdateLifecycle(t, directory, lifecycleView("node-a", 1, "registration-a", node.StatusActive, true, true))
	directory.QuiesceNode("node-a", IneligibleReasonPersistenceFailure)
	got, err := directory.GetByID(descriptor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Eligible || got.IneligibleReason != IneligibleReasonPersistenceFailure || got.Descriptor.Generation != 1 || got.Descriptor.Attributes["empty"] != "" {
		t.Fatalf("quiesced resource = %#v", got)
	}
}
