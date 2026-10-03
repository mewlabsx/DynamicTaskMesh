package mapper

import (
	"errors"
	"fmt"
	"testing"

	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
)

func TestCandidateSourceReadinessGate(t *testing.T) {
	reader := &fakeDirectoryReader{views: []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
	}}
	source := mustCandidateSource(t, reader, &testReadiness{ready: false})
	if _, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t)}); !errors.Is(err, ErrResourceCandidateSourceNotReady) {
		t.Fatalf("Query() error = %v, want %v", err, ErrResourceCandidateSourceNotReady)
	}
	if reader.calls != 0 {
		t.Fatalf("reader called while not ready: %d", reader.calls)
	}
}

func TestCandidateSourceReadyAllowsQuery(t *testing.T) {
	views := []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
	}
	reader := &fakeDirectoryReader{views: views}
	source := mustCandidateSource(t, reader, &testReadiness{ready: true})
	got, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t)})
	if err != nil {
		t.Fatal(err)
	}
	if reader.calls != 1 || len(got) != 1 || got[0].Descriptor.ID != "resource-a" {
		t.Fatalf("calls=%d got=%#v", reader.calls, got)
	}
}

func TestCandidateSourceEmptyResultIsAuthoritativeUnavailable(t *testing.T) {
	reader := &fakeDirectoryReader{views: nil}
	source := mustCandidateSource(t, reader, &testReadiness{ready: true})
	_, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t)})
	if !errors.Is(err, ErrResourceUnavailable) {
		t.Fatalf("Query() error = %v, want %v", err, ErrResourceUnavailable)
	}
	if !errors.Is(err, ErrCapabilityUnavailable) {
		t.Fatalf("Query() error = %v must also match %v", err, ErrCapabilityUnavailable)
	}
}

func TestCandidateSourceReaderErrorIsQueryFailed(t *testing.T) {
	cause := errors.New("directory internal failure")
	reader := &fakeDirectoryReader{err: cause}
	source := mustCandidateSource(t, reader, &testReadiness{ready: true})
	_, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t)})
	if !errors.Is(err, ErrResourceCandidateQueryFailed) {
		t.Fatalf("Query() error = %v, want %v", err, ErrResourceCandidateQueryFailed)
	}
	if !errors.Is(err, cause) {
		t.Fatalf("Query() error = %v must preserve underlying cause %v", err, cause)
	}
	if errors.Is(err, ErrResourceUnavailable) {
		t.Fatalf("reader failure must not be classified as unavailable: %v", err)
	}
}

func TestCandidateSourceRejectsIneligibleViewsAsInvariantBreak(t *testing.T) {
	view := candidateView(t, "resource-a", "node-a", 1, "registration-a", 1)
	view.Eligible = false
	reader := &fakeDirectoryReader{views: []resourcedirectory.ResourceRecordView{view}}
	source := mustCandidateSource(t, reader, &testReadiness{ready: true})
	if _, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t)}); !errors.Is(err, ErrResourceCandidateQueryFailed) {
		t.Fatalf("Query() error = %v, want %v", err, ErrResourceCandidateQueryFailed)
	}
}

func TestCandidateSourceFiltering(t *testing.T) {
	views := []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
		candidateView(t, "resource-b", "node-a", 1, "registration-a", 1),
		candidateView(t, "resource-c", "node-b", 1, "registration-b", 1),
		candidateView(t, "resource-d", "node-b", 1, "registration-b", 1),
	}
	tests := []struct {
		name  string
		query ResourceCandidateQuery
		want  []string
	}{
		{name: "no filter", query: ResourceCandidateQuery{Requirement: testRequirement(t)}, want: []string{"resource-a", "resource-b", "resource-c", "resource-d"}},
		{name: "owner node", query: ResourceCandidateQuery{Requirement: testRequirement(t), OwnerNodeID: "node-a"}, want: []string{"resource-a", "resource-b"}},
		{name: "exclude one resource", query: ResourceCandidateQuery{Requirement: testRequirement(t), ExcludedResourceIDs: []model.ResourceID{"resource-a"}}, want: []string{"resource-b", "resource-c", "resource-d"}},
		{name: "exclude multiple resources", query: ResourceCandidateQuery{Requirement: testRequirement(t), ExcludedResourceIDs: []model.ResourceID{"resource-a", "resource-c"}}, want: []string{"resource-b", "resource-d"}},
		{name: "exclude one node", query: ResourceCandidateQuery{Requirement: testRequirement(t), ExcludedNodeIDs: []model.NodeID{"node-a"}}, want: []string{"resource-c", "resource-d"}},
		{name: "exclude resource and node", query: ResourceCandidateQuery{Requirement: testRequirement(t), ExcludedResourceIDs: []model.ResourceID{"resource-c"}, ExcludedNodeIDs: []model.NodeID{"node-a"}}, want: []string{"resource-d"}},
		{name: "exclude all", query: ResourceCandidateQuery{Requirement: testRequirement(t), ExcludedNodeIDs: []model.NodeID{"node-a", "node-b"}}, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			reader := &fakeDirectoryReader{views: views}
			source := mustCandidateSource(t, reader, &testReadiness{ready: true})
			got, err := source.Query(test.query)
			if test.want == nil {
				if !errors.Is(err, ErrResourceUnavailable) {
					t.Fatalf("Query() error = %v, want %v", err, ErrResourceUnavailable)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			ids := make([]string, len(got))
			for index := range got {
				ids[index] = string(got[index].Descriptor.ID)
			}
			if fmt.Sprint(ids) != fmt.Sprint(test.want) {
				t.Fatalf("ids = %v, want %v", ids, test.want)
			}
		})
	}
}

func TestCandidateSourceFilteringDoesNotMutateInputs(t *testing.T) {
	views := []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
		candidateView(t, "resource-b", "node-b", 1, "registration-b", 1),
	}
	before := len(views)
	query := ResourceCandidateQuery{
		Requirement:         testRequirement(t),
		ExcludedResourceIDs: []model.ResourceID{"resource-a"},
		ExcludedNodeIDs:     []model.NodeID{"node-b"},
	}
	excludedResourceBefore := append([]model.ResourceID(nil), query.ExcludedResourceIDs...)
	excludedNodeBefore := append([]model.NodeID(nil), query.ExcludedNodeIDs...)
	reader := &fakeDirectoryReader{views: views}
	source := mustCandidateSource(t, reader, &testReadiness{ready: true})
	if _, err := source.Query(query); !errors.Is(err, ErrResourceUnavailable) {
		t.Fatalf("Query() error = %v", err)
	}
	if len(reader.views) != before || len(query.ExcludedResourceIDs) != 1 || len(query.ExcludedNodeIDs) != 1 ||
		query.ExcludedResourceIDs[0] != excludedResourceBefore[0] || query.ExcludedNodeIDs[0] != excludedNodeBefore[0] {
		t.Fatalf("query inputs mutated: views=%d excluded=%v/%v", len(reader.views), query.ExcludedResourceIDs, query.ExcludedNodeIDs)
	}
}

func TestCandidateSourceSortingProviderNodeThenResourceID(t *testing.T) {
	shuffled := []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-z", "node-a", 1, "registration-a", 1),
		candidateView(t, "resource-a", "node-b", 1, "registration-b", 1),
		candidateView(t, "resource-m", "node-b", 1, "registration-b", 1),
		candidateView(t, "resource-b", "node-a", 1, "registration-a", 1),
		candidateView(t, "resource-c", "node-a", 1, "registration-a", 1),
	}
	reader := &fakeDirectoryReader{views: shuffled}
	source := mustCandidateSource(t, reader, &testReadiness{ready: true})
	got, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t)})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"resource-b", "resource-c", "resource-z", "resource-a", "resource-m"}
	ids := make([]string, len(got))
	for index := range got {
		ids[index] = string(got[index].Descriptor.ID)
	}
	if fmt.Sprint(ids) != fmt.Sprint(want) {
		t.Fatalf("sorted ids = %v, want %v", ids, want)
	}
	second, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t)})
	if err != nil {
		t.Fatal(err)
	}
	secondIDs := make([]string, len(second))
	for index := range second {
		secondIDs[index] = string(second[index].Descriptor.ID)
	}
	if fmt.Sprint(ids) != fmt.Sprint(secondIDs) {
		t.Fatalf("unstable sort: %v vs %v", ids, secondIDs)
	}
}

func TestCandidateSourceSortingAfterOwnerFilterAndExclusion(t *testing.T) {
	views := []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-b", 1, "registration-b", 1),
		candidateView(t, "resource-b", "node-a", 1, "registration-a", 1),
		candidateView(t, "resource-c", "node-b", 1, "registration-b", 1),
	}
	reader := &fakeDirectoryReader{views: views}
	source := mustCandidateSource(t, reader, &testReadiness{ready: true})
	got, err := source.Query(ResourceCandidateQuery{
		Requirement:         testRequirement(t),
		ExcludedResourceIDs: []model.ResourceID{"resource-c"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Descriptor.ID != "resource-b" || got[1].Descriptor.ID != "resource-a" {
		t.Fatalf("got = %#v, want resource-b then resource-a", got)
	}
}

func TestCandidateSourceStaleMappingDoesNotExcludeResourceID(t *testing.T) {
	// Old mapping fence: resource-stale Generation=1, registration-old.
	// Current authoritative view: same ResourceID, Generation=2, registration-new.
	current := candidateView(t, "resource-stale", "node-a", 2, "registration-new", 2)
	reader := &fakeDirectoryReader{views: []resourcedirectory.ResourceRecordView{current}}
	source := mustCandidateSource(t, reader, &testReadiness{ready: true})
	query := ResourceCandidateQuery{Requirement: testRequirement(t)}
	got, err := source.Query(query)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Descriptor.ID != "resource-stale" || got[0].Descriptor.Generation != 2 {
		t.Fatalf("stale mapping excluded the same ResourceID with a newer generation: %#v", got)
	}
}

func TestCandidateSourceResourceFailureExcludesResourceID(t *testing.T) {
	views := []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
		candidateView(t, "resource-b", "node-a", 1, "registration-a", 1),
	}
	reader := &fakeDirectoryReader{views: views}
	source := mustCandidateSource(t, reader, &testReadiness{ready: true})
	got, err := source.Query(ResourceCandidateQuery{
		Requirement:         testRequirement(t),
		ExcludedResourceIDs: []model.ResourceID{"resource-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Descriptor.ID != "resource-b" {
		t.Fatalf("resource failure exclusion failed: %#v", got)
	}
}

func TestCandidateSourceNodeFailureExcludesNodeResources(t *testing.T) {
	views := []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
		candidateView(t, "resource-b", "node-a", 1, "registration-a", 1),
		candidateView(t, "resource-c", "node-b", 1, "registration-b", 1),
	}
	reader := &fakeDirectoryReader{views: views}
	source := mustCandidateSource(t, reader, &testReadiness{ready: true})
	got, err := source.Query(ResourceCandidateQuery{
		Requirement:     testRequirement(t),
		ExcludedNodeIDs: []model.NodeID{"node-a"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Descriptor.ID != "resource-c" || got[0].Descriptor.OwnerNodeID != "node-b" {
		t.Fatalf("node failure exclusion failed: %#v", got)
	}
}

func TestCandidateSourceRecoveryOrientedQueries(t *testing.T) {
	views := []resourcedirectory.ResourceRecordView{
		candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
		candidateView(t, "resource-b", "node-b", 1, "registration-b", 1),
		candidateView(t, "resource-c", "node-b", 1, "registration-b", 1),
	}
	t.Run("owner node unique candidate", func(t *testing.T) {
		reader := &fakeDirectoryReader{views: views}
		source := mustCandidateSource(t, reader, &testReadiness{ready: true})
		got, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t), OwnerNodeID: "node-a"})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].Descriptor.ID != "resource-a" {
			t.Fatalf("got = %#v", got)
		}
	})
	t.Run("owner node no candidates but others exist", func(t *testing.T) {
		reader := &fakeDirectoryReader{views: views}
		source := mustCandidateSource(t, reader, &testReadiness{ready: true})
		_, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t), OwnerNodeID: "node-zz"})
		if !errors.Is(err, ErrResourceUnavailable) {
			t.Fatalf("Query() error = %v, want %v (empty result is authoritative unavailable)", err, ErrResourceUnavailable)
		}
	})
	t.Run("owner node multiple candidates", func(t *testing.T) {
		reader := &fakeDirectoryReader{views: views}
		source := mustCandidateSource(t, reader, &testReadiness{ready: true})
		got, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t), OwnerNodeID: "node-b"})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 || got[0].Descriptor.ID != "resource-b" || got[1].Descriptor.ID != "resource-c" {
			t.Fatalf("got = %#v", got)
		}
	})
	t.Run("directory not ready", func(t *testing.T) {
		reader := &fakeDirectoryReader{views: views}
		source := mustCandidateSource(t, reader, &testReadiness{ready: false})
		if _, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t), OwnerNodeID: "node-a"}); !errors.Is(err, ErrResourceCandidateSourceNotReady) {
			t.Fatalf("Query() error = %v", err)
		}
	})
}

func TestCandidateSourceViewConstructsValidResourceRef(t *testing.T) {
	view := candidateView(t, "resource-a", "node-a", 7, "registration-a", 3)
	ref, err := model.NewResourceRef(
		view.Descriptor.ID,
		view.Descriptor.Generation,
		view.Descriptor.OwnerNodeID,
		view.NodeGeneration,
		view.RegistrationID,
	)
	if err != nil {
		t.Fatalf("construct ResourceRef from view: %v", err)
	}
	if err := ref.Validate(); err != nil {
		t.Fatalf("ResourceRef.Validate() = %v", err)
	}
	if ref.ResourceID != view.Descriptor.ID || ref.ResourceGeneration != view.Descriptor.Generation ||
		ref.OwnerNodeID != view.Descriptor.OwnerNodeID || ref.OwnerNodeGeneration != view.NodeGeneration ||
		ref.RegistrationID != view.RegistrationID {
		t.Fatalf("ref = %+v does not match view %+v", ref, view)
	}
}

func TestCandidateSourceViewOutputIsDefensiveCopy(t *testing.T) {
	view := candidateView(t, "resource-a", "node-a", 1, "registration-a", 1)
	reader := &fakeDirectoryReader{views: []resourcedirectory.ResourceRecordView{view}}
	source := mustCandidateSource(t, reader, &testReadiness{ready: true})
	got, err := source.Query(ResourceCandidateQuery{Requirement: testRequirement(t)})
	if err != nil {
		t.Fatal(err)
	}
	got[0].Descriptor.Attributes = map[string]string{"mutated": "yes"}
	if len(reader.views[0].Descriptor.Attributes) != 0 {
		t.Fatalf("query output aliases the reader's views: %v", reader.views[0].Descriptor.Attributes)
	}
}
