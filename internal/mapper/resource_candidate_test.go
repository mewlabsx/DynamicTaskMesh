package mapper

import (
	"errors"
	"testing"

	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
)

func testRequirement(t *testing.T) model.ResourceRequirement {
	t.Helper()
	requirement, err := model.NewResourceRequirement(model.ResourceKindCapability, "temperature_sensor", "read_temperature", nil)
	if err != nil {
		t.Fatal(err)
	}
	return requirement
}

func invalidRequirement() model.ResourceRequirement {
	return model.ResourceRequirement{Kind: model.ResourceKindCapability, Type: "temperature_sensor", OperationID: ""}
}

type testReadiness struct {
	ready bool
}

func (r *testReadiness) Ready() bool { return r.ready }

type fakeDirectoryReader struct {
	views []resourcedirectory.ResourceRecordView
	err   error
	calls int
}

func (reader *fakeDirectoryReader) MatchRequirement(requirement model.ResourceRequirement) ([]resourcedirectory.ResourceRecordView, error) {
	reader.calls++
	if reader.err != nil {
		return nil, reader.err
	}
	cloned := make([]resourcedirectory.ResourceRecordView, len(reader.views))
	for index := range reader.views {
		cloned[index] = reader.views[index].Clone()
	}
	return cloned, nil
}

func candidateView(t *testing.T, id string, nodeID string, generation int64, registrationID string, resourceGeneration uint64) resourcedirectory.ResourceRecordView {
	t.Helper()
	descriptor, err := model.NewResourceDescriptor(
		model.ResourceID(id), model.ResourceKindCapability, "temperature_sensor",
		model.NodeID(nodeID), model.ResourceGeneration(resourceGeneration),
		[]model.OperationDescriptor{{ID: "read_temperature", IdempotencyMode: model.IdempotencyIdempotent}},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return resourcedirectory.ResourceRecordView{
		Descriptor: descriptor, NodeGeneration: generation, RegistrationID: registrationID,
		PublicationState: resourcedirectory.PublicationStatePublished, Eligible: true,
	}
}

func mustCandidateSource(t *testing.T, reader ResourceDirectoryReader, readiness ResourceCandidateReadiness) ResourceCandidateSource {
	t.Helper()
	source, err := NewResourceCandidateSource(reader, readiness)
	if err != nil {
		t.Fatal(err)
	}
	return source
}

func TestResourceCandidateQueryValidation(t *testing.T) {
	valid := ResourceCandidateQuery{Requirement: testRequirement(t)}
	if err := valid.validate(); err != nil {
		t.Fatalf("valid query rejected: %v", err)
	}

	tests := []struct {
		name  string
		query ResourceCandidateQuery
	}{
		{name: "invalid requirement", query: ResourceCandidateQuery{Requirement: invalidRequirement()}},
		{name: "zero requirement", query: ResourceCandidateQuery{}},
		{name: "whitespace owner node", query: ResourceCandidateQuery{Requirement: testRequirement(t), OwnerNodeID: " node-a"}},
		{name: "empty excluded resource", query: ResourceCandidateQuery{Requirement: testRequirement(t), ExcludedResourceIDs: []model.ResourceID{""}}},
		{name: "empty excluded node", query: ResourceCandidateQuery{Requirement: testRequirement(t), ExcludedNodeIDs: []model.NodeID{" "}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.query.validate(); !errors.Is(err, ErrInvalidResourceCandidateQuery) {
				t.Fatalf("validate() error = %v, want %v", err, ErrInvalidResourceCandidateQuery)
			}
		})
	}
}

func TestInvalidQueryDoesNotTouchReader(t *testing.T) {
	reader := &fakeDirectoryReader{}
	source := mustCandidateSource(t, reader, &testReadiness{ready: true})
	if _, err := source.Query(ResourceCandidateQuery{Requirement: invalidRequirement()}); !errors.Is(err, ErrInvalidResourceCandidateQuery) {
		t.Fatalf("Query() error = %v, want %v", err, ErrInvalidResourceCandidateQuery)
	}
	if reader.calls != 0 {
		t.Fatalf("reader called %d times for invalid query", reader.calls)
	}
}
