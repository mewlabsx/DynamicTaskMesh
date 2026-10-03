package mapper

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/planner"
)

type fakeDiscovery struct {
	nodes map[model.Capability][]node.Node
	calls []model.Capability
}

type fakeEligibility struct {
	eligible map[model.NodeID]bool
}

func (eligibility *fakeEligibility) Eligible(nodeID model.NodeID) bool {
	return eligibility.eligible[nodeID]
}

func (discovery *fakeDiscovery) Discover(capability model.Capability) []node.Node {
	discovery.calls = append(discovery.calls, capability)
	return discovery.nodes[capability]
}

func TestNewRejectsNilDiscovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		discovery Discovery
	}{
		{name: "nil interface", discovery: nil},
		{name: "typed nil", discovery: (*fakeDiscovery)(nil)},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			mapper, err := New(tt.discovery)
			if !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("New() error = %v, want %v", err, ErrInvalidPlan)
			}
			if mapper != nil {
				t.Fatalf("New() mapper = %#v, want nil", mapper)
			}
		})
	}
}

func TestMapChoosesLexicographicallySmallestNode(t *testing.T) {
	t.Parallel()

	discovery := &fakeDiscovery{
		nodes: map[model.Capability][]node.Node{
			"temperature_sensor": {
				mustNode(t, "node-z", "temperature_sensor"),
				mustNode(t, "node-a", "temperature_sensor"),
				mustNode(t, "node-m", "temperature_sensor"),
			},
			"cooling_control": {
				mustNode(t, "node-c", "cooling_control"),
				mustNode(t, "node-b", "cooling_control"),
			},
		},
	}
	mapper, err := New(discovery)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	got, err := mapper.Map(validPlan())
	if err != nil {
		t.Fatalf("Map() error = %v", err)
	}

	want := MappedPlan{
		TaskID: "task-1",
		Steps: []MappedStep{
			{ID: "step-1", Capability: "temperature_sensor", NodeID: "node-a"},
			{ID: "step-2", Capability: "cooling_control", NodeID: "node-b"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Map() = %#v, want %#v", got, want)
	}
}

func TestMapFiltersCandidatesWithoutValidLease(t *testing.T) {
	discovery := &fakeDiscovery{
		nodes: map[model.Capability][]node.Node{
			"temperature_sensor": {
				mustNode(t, "node-a", "temperature_sensor"),
				mustNode(t, "node-b", "temperature_sensor"),
			},
		},
	}
	mapper, err := New(
		discovery,
		WithEligibility(&fakeEligibility{
			eligible: map[model.NodeID]bool{"node-b": true},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	plan := planner.Plan{
		TaskID: "task-1",
		Steps: []planner.Step{{
			ID:         "step-1",
			Capability: "temperature_sensor",
		}},
	}

	mapped, err := mapper.Map(plan)
	if err != nil {
		t.Fatal(err)
	}
	if mapped.Steps[0].NodeID != "node-b" {
		t.Fatalf("mapped node = %q, want node-b", mapped.Steps[0].NodeID)
	}
}

func TestMapReturnsUnavailableWhenAllCandidateLeasesExpired(t *testing.T) {
	discovery := &fakeDiscovery{
		nodes: map[model.Capability][]node.Node{
			"temperature_sensor": {
				mustNode(t, "node-a", "temperature_sensor"),
			},
		},
	}
	mapper, err := New(
		discovery,
		WithEligibility(&fakeEligibility{eligible: map[model.NodeID]bool{}}),
	)
	if err != nil {
		t.Fatal(err)
	}
	plan := planner.Plan{
		TaskID: "task-1",
		Steps: []planner.Step{{
			ID:         "step-1",
			Capability: "temperature_sensor",
		}},
	}

	if _, err := mapper.Map(plan); !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("Map() error = %v, want %v", err, ErrCapabilityUnavailable)
	}
}

func TestRemapExcludesFailedNodesAndPreservesStep(t *testing.T) {
	discovery := &fakeDiscovery{
		nodes: map[model.Capability][]node.Node{
			"temperature_sensor": {
				mustNode(t, "node-a", "temperature_sensor"),
				mustNode(t, "node-b", "temperature_sensor"),
				mustNode(t, "node-c", "temperature_sensor"),
			},
		},
	}
	instance, err := New(discovery)
	if err != nil {
		t.Fatal(err)
	}
	step := MappedStep{
		ID:         "step-1",
		Capability: "temperature_sensor",
		NodeID:     "node-a",
		Inputs:     map[string]string{"operation": "read"},
	}

	got, err := instance.Remap(step, map[model.NodeID]struct{}{"node-a": {}})
	if err != nil {
		t.Fatal(err)
	}
	if got.NodeID != "node-b" || got.ID != step.ID || got.Capability != step.Capability ||
		!reflect.DeepEqual(got.Inputs, step.Inputs) {
		t.Fatalf("Remap() = %#v, want step on node-b", got)
	}
	got.Inputs["operation"] = "changed"
	if step.Inputs["operation"] != "read" {
		t.Fatal("Remap() did not copy inputs")
	}
}

func TestRemapReturnsUnavailableWhenNoAlternativeExists(t *testing.T) {
	discovery := &fakeDiscovery{
		nodes: map[model.Capability][]node.Node{
			"temperature_sensor": {
				mustNode(t, "node-a", "temperature_sensor"),
			},
		},
	}
	instance, err := New(discovery)
	if err != nil {
		t.Fatal(err)
	}
	_, err = instance.Remap(
		MappedStep{ID: "step-1", Capability: "temperature_sensor", NodeID: "node-a"},
		map[model.NodeID]struct{}{"node-a": {}},
	)
	if !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("Remap() error = %v, want %v", err, ErrCapabilityUnavailable)
	}
}

func TestMapCopiesStepInputs(t *testing.T) {
	t.Parallel()

	discovery := &fakeDiscovery{
		nodes: map[model.Capability][]node.Node{
			"temperature_sensor": {
				mustNode(t, "node-a", "temperature_sensor"),
			},
		},
	}
	mapper, err := New(discovery)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	inputs := map[string]string{"operation": "read_temperature"}
	plan := planner.Plan{
		TaskID: "task-1",
		Steps: []planner.Step{{
			ID:         "step-1",
			Capability: "temperature_sensor",
			Inputs:     inputs,
		}},
	}

	got, err := mapper.Map(plan)
	if err != nil {
		t.Fatalf("Map() error = %v", err)
	}
	if !reflect.DeepEqual(got.Steps[0].Inputs, inputs) {
		t.Fatalf("Map() inputs = %v, want %v", got.Steps[0].Inputs, inputs)
	}

	inputs["operation"] = "tampered"
	if got.Steps[0].Inputs["operation"] != "read_temperature" {
		t.Fatalf("mapped inputs changed after source mutation: %v", got.Steps[0].Inputs)
	}
	got.Steps[0].Inputs["operation"] = "changed by caller"
	if plan.Steps[0].Inputs["operation"] != "tampered" {
		t.Fatalf("source inputs changed after mapped plan mutation: %v", plan.Steps[0].Inputs)
	}
}

func TestMapRejectsInvalidPlanBeforeDiscovery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		plan planner.Plan
	}{
		{name: "blank task ID", plan: planner.Plan{TaskID: " ", Steps: validPlan().Steps}},
		{name: "empty steps", plan: planner.Plan{TaskID: "task-1"}},
		{name: "blank step ID", plan: planner.Plan{
			TaskID: "task-1",
			Steps:  []planner.Step{{ID: " ", Capability: "temperature_sensor"}},
		}},
		{name: "duplicate step ID", plan: planner.Plan{
			TaskID: "task-1",
			Steps: []planner.Step{
				{ID: "step-1", Capability: "temperature_sensor"},
				{ID: "step-1", Capability: "cooling_control"},
			},
		}},
		{name: "blank capability", plan: planner.Plan{
			TaskID: "task-1",
			Steps:  []planner.Step{{ID: "step-1", Capability: " "}},
		}},
		{name: "blank input key", plan: planner.Plan{
			TaskID: "task-1",
			Steps: []planner.Step{{
				ID:         "step-1",
				Capability: "temperature_sensor",
				Inputs:     map[string]string{" ": "read_temperature"},
			}},
		}},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			discovery := &fakeDiscovery{}
			mapper, err := New(discovery)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			got, err := mapper.Map(tt.plan)
			if !errors.Is(err, ErrInvalidPlan) {
				t.Fatalf("Map() error = %v, want %v", err, ErrInvalidPlan)
			}
			if !reflect.DeepEqual(got, MappedPlan{}) {
				t.Fatalf("Map() = %#v, want zero plan", got)
			}
			if len(discovery.calls) != 0 {
				t.Fatalf("Discover() calls = %v, want none", discovery.calls)
			}
		})
	}
}

func TestMapReturnsNoPartialPlanWhenCapabilityUnavailable(t *testing.T) {
	t.Parallel()

	discovery := &fakeDiscovery{
		nodes: map[model.Capability][]node.Node{
			"temperature_sensor": {
				mustNode(t, "node-a", "temperature_sensor"),
			},
		},
	}
	mapper, err := New(discovery)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	got, err := mapper.Map(validPlan())
	if !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("Map() error = %v, want %v", err, ErrCapabilityUnavailable)
	}
	if !strings.Contains(err.Error(), "cooling_control") {
		t.Fatalf("Map() error = %v, want missing capability context", err)
	}
	if !reflect.DeepEqual(got, MappedPlan{}) {
		t.Fatalf("Map() = %#v, want zero plan", got)
	}
}

func validPlan() planner.Plan {
	return planner.Plan{
		TaskID: "task-1",
		Steps: []planner.Step{
			{ID: "step-1", Capability: "temperature_sensor"},
			{ID: "step-2", Capability: "cooling_control"},
		},
	}
}

func mustNode(t *testing.T, id model.NodeID, capability model.Capability) node.Node {
	t.Helper()

	result, err := node.New(id, []model.Capability{capability}, node.StatusOnline)
	if err != nil {
		t.Fatalf("node.New() error = %v", err)
	}
	return result
}
