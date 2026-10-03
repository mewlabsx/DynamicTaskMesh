package mapper

import (
	"errors"
	"testing"

	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/planner"
	"dtm/internal/resourcedirectory"
)

// panicDiscovery proves Resource Scheduling never touches the Legacy
// Capability Registry path: any call fails the test.
type panicDiscovery struct{}

func (panicDiscovery) Discover(model.Capability) []node.Node {
	panic("legacy discovery must not be called in resource scheduling mode")
}

// stubCandidateSource returns fixed views or a fixed error and records every
// query for assertions.
type stubCandidateSource struct {
	views   []resourcedirectory.ResourceRecordView
	err     error
	queries []ResourceCandidateQuery
}

func (source *stubCandidateSource) Query(query ResourceCandidateQuery) ([]resourcedirectory.ResourceRecordView, error) {
	source.queries = append(source.queries, query)
	if source.err != nil {
		return nil, source.err
	}
	cloned := make([]resourcedirectory.ResourceRecordView, len(source.views))
	for index := range source.views {
		cloned[index] = source.views[index].Clone()
	}
	return cloned, nil
}

func mustResourceMapper(t *testing.T, source ResourceCandidateSource) *Mapper {
	t.Helper()
	mapperInstance, err := New(panicDiscovery{}, WithResourceCandidateSource(source))
	if err != nil {
		t.Fatal(err)
	}
	return mapperInstance
}

// mustRealResourceSource builds the production-style Candidate Source over a
// fake Directory reader so ordering and exclusion semantics are exercised by
// the real implementation.
func mustRealResourceSource(t *testing.T, views []resourcedirectory.ResourceRecordView) ResourceCandidateSource {
	t.Helper()
	source, err := NewResourceCandidateSource(&fakeDirectoryReader{views: views}, &testReadiness{ready: true})
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func mustLegacyMapper(t *testing.T, discovery Discovery) *Mapper {
	t.Helper()
	mapperInstance, err := New(discovery)
	if err != nil {
		t.Fatal(err)
	}
	return mapperInstance
}

func resourcePlan() planner.Plan {
	return planner.Plan{TaskID: "task-resource-map", Steps: []planner.Step{
		{ID: "step-1", Capability: "temperature_sensor", IdempotencyMode: model.IdempotencyIdempotent, Inputs: map[string]string{"operation": "read_temperature"}},
		{ID: "step-2", Capability: "cooling_control", IdempotencyMode: model.IdempotencyNonIdempotent, Inputs: map[string]string{"target_temperature": "26"}},
	}}
}

func assertZeroResourceRef(t *testing.T, step MappedStep, context string) {
	t.Helper()
	if step.ResourceRef != (model.ResourceRef{}) {
		t.Fatalf("%s: ResourceRef = %+v, want zero value", context, step.ResourceRef)
	}
}

func assertValidResourceRef(t *testing.T, step MappedStep, context string) {
	t.Helper()
	if err := step.ResourceRef.Validate(); err != nil {
		t.Fatalf("%s: ResourceRef.Validate() = %v (%+v)", context, err, step.ResourceRef)
	}
	if step.ResourceRef.OwnerNodeID != step.NodeID {
		t.Fatalf("%s: ResourceRef.OwnerNodeID %q != NodeID %q", context, step.ResourceRef.OwnerNodeID, step.NodeID)
	}
	if step.ResourceRef == (model.ResourceRef{}) {
		t.Fatalf("%s: ResourceRef is zero value in resource mode", context)
	}
}

func TestWithResourceCandidateSourceRejectsNil(t *testing.T) {
	var typedNil *stubCandidateSource
	for _, test := range []struct {
		name   string
		source ResourceCandidateSource
	}{
		{name: "nil interface", source: nil},
		{name: "typed nil", source: typedNil},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := New(panicDiscovery{}, WithResourceCandidateSource(test.source)); err == nil {
				t.Fatal("New() accepted nil candidate source; Map would panic later")
			}
		})
	}
}

func TestResourceMapSingleResourceProducesCompleteResourceRef(t *testing.T) {
	source := &stubCandidateSource{views: []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
	}}
	mapperInstance := mustResourceMapper(t, source)
	mapped, err := mapperInstance.Map(resourcePlan())
	if err != nil {
		t.Fatal(err)
	}
	if len(mapped.Steps) != 2 {
		t.Fatalf("mapped steps = %d", len(mapped.Steps))
	}
	for _, step := range mapped.Steps {
		assertValidResourceRef(t, step, "resource map")
		if step.NodeID != "node-a" {
			t.Fatalf("step %q NodeID = %q, want node-a", step.ID, step.NodeID)
		}
	}
	if len(source.queries) != 2 {
		t.Fatalf("candidate queries = %d, want 2", len(source.queries))
	}
	for index, requirement := range []model.ResourceRequirement{
		{Kind: model.ResourceKindCapability, Type: "temperature_sensor", OperationID: "read_temperature"},
		{Kind: model.ResourceKindCapability, Type: "cooling_control", OperationID: "set_target_temperature"},
	} {
		if !source.queries[index].Requirement.Equal(requirement) {
			t.Fatalf("query %d requirement = %+v, want %+v (must derive from the frozen mapping)", index, source.queries[index].Requirement, requirement)
		}
	}
}

func TestResourceMapUsesCandidateSourceOrdering(t *testing.T) {
	t.Run("same node multiple resources", func(t *testing.T) {
		source := mustRealResourceSource(t, []resourcedirectory.ResourceRecordView{
			candidateView(t, "resource-b", "node-a", 1, "registration-a", 1),
			candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
		})
		mapperInstance := mustResourceMapper(t, source)
		mapped, err := mapperInstance.Map(resourcePlan())
		if err != nil {
			t.Fatal(err)
		}
		if mapped.Steps[0].ResourceRef.ResourceID != "resource-a" {
			t.Fatalf("selected %q, want first sorted candidate resource-a", mapped.Steps[0].ResourceRef.ResourceID)
		}
	})
	t.Run("multiple nodes", func(t *testing.T) {
		source := mustRealResourceSource(t, []resourcedirectory.ResourceRecordView{
			candidateView(t, "resource-b", "node-b", 1, "registration-b", 1),
			candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
		})
		mapperInstance := mustResourceMapper(t, source)
		mapped, err := mapperInstance.Map(resourcePlan())
		if err != nil {
			t.Fatal(err)
		}
		if mapped.Steps[0].NodeID != "node-a" || mapped.Steps[0].ResourceRef.OwnerNodeID != "node-a" {
			t.Fatalf("selected node %q, want node-a (ProviderNodeID ordering)", mapped.Steps[0].NodeID)
		}
	})
}

func TestResourceMapNewGenerationBuildsNewResourceRef(t *testing.T) {
	// Old mapping fence: generation 1. Current authoritative view: same
	// ResourceID with generation 2; the Candidate Source must not exclude the
	// ResourceID and the new ResourceRef must carry generation 2.
	source := &stubCandidateSource{views: []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 2, "registration-new", 2),
	}}
	mapperInstance := mustResourceMapper(t, source)
	mapped, err := mapperInstance.Map(resourcePlan())
	if err != nil {
		t.Fatal(err)
	}
	ref := mapped.Steps[0].ResourceRef
	if ref.ResourceID != "resource-a" || ref.ResourceGeneration != 2 || ref.OwnerNodeGeneration != 2 || ref.RegistrationID != "registration-new" {
		t.Fatalf("new generation ResourceRef = %+v", ref)
	}
	assertValidResourceRef(t, mapped.Steps[0], "new generation")
}

func TestResourceMapInvalidCandidateFenceFailsClosed(t *testing.T) {
	view := candidateView(t, "resource-a", "node-a", 1, "registration-a", 1)
	view.NodeGeneration = 0 // cannot form a valid ResourceRef
	source := &stubCandidateSource{views: []resourcedirectory.ResourceRecordView{view}}
	mapperInstance := mustResourceMapper(t, source)
	if _, err := mapperInstance.Map(resourcePlan()); !errors.Is(err, ErrResourceCandidateQueryFailed) {
		t.Fatalf("Map() error = %v, want %v (fail closed, no legacy fallback)", err, ErrResourceCandidateQueryFailed)
	}
}

func TestResourceMapNeverFallsBackToLegacy(t *testing.T) {
	tests := []struct {
		name      string
		sourceErr error
		wantErr   error
	}{
		{name: "unavailable", sourceErr: resourceUnavailableError(model.ResourceRequirement{Kind: model.ResourceKindCapability, Type: "temperature_sensor", OperationID: "read_temperature"}), wantErr: ErrResourceUnavailable},
		{name: "not ready", sourceErr: ErrResourceCandidateSourceNotReady, wantErr: ErrResourceCandidateSourceNotReady},
		{name: "query failed", sourceErr: ErrResourceCandidateQueryFailed, wantErr: ErrResourceCandidateQueryFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := &stubCandidateSource{err: test.sourceErr}
			mapperInstance := mustResourceMapper(t, source)
			_, err := mapperInstance.Map(resourcePlan())
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("Map() error = %v, want %v", err, test.wantErr)
			}
		})
	}
	t.Run("unavailable is capability-compatible", func(t *testing.T) {
		source := &stubCandidateSource{err: resourceUnavailableError(model.ResourceRequirement{Kind: model.ResourceKindCapability, Type: "temperature_sensor", OperationID: "read_temperature"})}
		mapperInstance := mustResourceMapper(t, source)
		_, err := mapperInstance.Map(resourcePlan())
		if !errors.Is(err, ErrCapabilityUnavailable) {
			t.Fatalf("Map() error = %v must be compatible with %v", err, ErrCapabilityUnavailable)
		}
	})
}

func TestResourceMapUnsupportedCapabilityFailsClosed(t *testing.T) {
	source := &stubCandidateSource{}
	mapperInstance := mustResourceMapper(t, source)
	plan := planner.Plan{TaskID: "task-unsupported", Steps: []planner.Step{{ID: "step-1", Capability: "unknown_capability"}}}
	if _, err := mapperInstance.Map(plan); !errors.Is(err, model.ErrUnsupportedLegacyCapability) {
		t.Fatalf("Map() error = %v, want %v", err, model.ErrUnsupportedLegacyCapability)
	}
	if len(source.queries) != 0 {
		t.Fatalf("candidate queries = %d, want 0 (unsupported capability must not query)", len(source.queries))
	}
}

func TestResourceMapIsAtomicAcrossSteps(t *testing.T) {
	source := &stubCandidateSource{views: []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
	}}
	// step-2 (cooling_control) cannot find a candidate.
	failing := &failingSecondQuerySource{source}
	mapperInstance := mustResourceMapper(t, failing)
	if _, err := mapperInstance.Map(resourcePlan()); err == nil {
		t.Fatal("Map() error = nil, want failure on the second step")
	}
}

// failingSecondQuerySource fails every query after the first, simulating an
// unavailable candidate for step-2 while step-1 succeeds.
type failingSecondQuerySource struct {
	*stubCandidateSource
}

func (source *failingSecondQuerySource) Query(query ResourceCandidateQuery) ([]resourcedirectory.ResourceRecordView, error) {
	if len(source.queries) >= 1 {
		return nil, resourceUnavailableError(query.Requirement)
	}
	return source.stubCandidateSource.Query(query)
}

func TestResourceRemapReplacesResourceRef(t *testing.T) {
	source := mustRealResourceSource(t, []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
		candidateView(t, "resource-b", "node-b", 1, "registration-b", 1),
	})
	mapperInstance := mustResourceMapper(t, source)
	old := MappedStep{
		ID: "step-1", Capability: "temperature_sensor", IdempotencyMode: model.IdempotencyIdempotent,
		AttemptOffset: 3, MaxAttempts: 5, NodeID: "node-a",
		ResourceRef: model.ResourceRef{ResourceID: "resource-a", ResourceGeneration: 1, OwnerNodeID: "node-a", OwnerNodeGeneration: 1, RegistrationID: "registration-a"},
		Inputs:      map[string]string{"operation": "read_temperature"},
	}
	remapped, err := mapperInstance.Remap(old, map[model.NodeID]struct{}{"node-a": {}})
	if err != nil {
		t.Fatal(err)
	}
	if remapped.NodeID != "node-b" || remapped.ResourceRef.ResourceID != "resource-b" || remapped.ResourceRef.OwnerNodeID != "node-b" {
		t.Fatalf("remapped = %+v, want node-b resource-b", remapped)
	}
	assertValidResourceRef(t, remapped, "resource remap")
	if remapped.ID != old.ID || remapped.Capability != old.Capability || remapped.IdempotencyMode != old.IdempotencyMode ||
		remapped.AttemptOffset != old.AttemptOffset || remapped.MaxAttempts != old.MaxAttempts {
		t.Fatalf("remap did not preserve step fields: %+v", remapped)
	}
	remapped.Inputs["mutated"] = "yes"
	if _, exists := old.Inputs["mutated"]; exists {
		t.Fatalf("remap inputs alias the original: %v", old.Inputs)
	}
}

func TestResourceRemapAllExcludedIsUnavailable(t *testing.T) {
	source := mustRealResourceSource(t, []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
		candidateView(t, "resource-b", "node-b", 1, "registration-b", 1),
	})
	mapperInstance := mustResourceMapper(t, source)
	step := MappedStep{ID: "step-1", Capability: "temperature_sensor", NodeID: "node-a", Inputs: map[string]string{}}
	if _, err := mapperInstance.Remap(step, map[model.NodeID]struct{}{"node-a": {}, "node-b": {}}); !errors.Is(err, ErrResourceUnavailable) {
		t.Fatalf("Remap() error = %v, want %v", err, ErrResourceUnavailable)
	}
}

func TestLegacyMapAndRemapProduceZeroResourceRef(t *testing.T) {
	registry := newTestDiscovery(t)
	mapperInstance := mustLegacyMapper(t, registry)
	mapped, err := mapperInstance.Map(planner.Plan{TaskID: "task-legacy", Steps: []planner.Step{{ID: "step-1", Capability: "temperature_sensor"}}})
	if err != nil {
		t.Fatal(err)
	}
	assertZeroResourceRef(t, mapped.Steps[0], "legacy map")
	remapped, err := mapperInstance.Remap(mapped.Steps[0], map[model.NodeID]struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	assertZeroResourceRef(t, remapped, "legacy remap")
}

func TestResourceMappingModeDetermination(t *testing.T) {
	// No option -> Legacy; option present -> Resource.
	legacyMapper := mustLegacyMapper(t, newTestDiscovery(t))
	if legacyMapper.resourceCandidates != nil {
		t.Fatal("legacy mapper has a candidate source")
	}
	source := &stubCandidateSource{views: []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
	}}
	resourceMapper := mustResourceMapper(t, source)
	if resourceMapper.resourceCandidates == nil {
		t.Fatal("resource mapper has no candidate source")
	}
	if _, err := resourceMapper.Map(resourcePlan()); err != nil {
		t.Fatal(err)
	}
}

// newTestDiscovery returns the deterministic node-a/node-b discovery used by
// the existing Legacy mapper tests.
func newTestDiscovery(t *testing.T) *fakeDiscovery {
	return &fakeDiscovery{nodes: map[model.Capability][]node.Node{
		"temperature_sensor": {
			mustNode(t, "node-a", "temperature_sensor"),
			mustNode(t, "node-b", "temperature_sensor"),
		},
	}}
}
