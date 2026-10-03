package mapper

import (
	"context"
	"errors"
	"sort"
	"testing"

	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
)

type recoveryCandidateSource struct {
	byNode map[model.NodeID][]resourcedirectory.ResourceRecordView
	err    error
	query  []ResourceCandidateQuery
}

func (source *recoveryCandidateSource) Query(query ResourceCandidateQuery) ([]resourcedirectory.ResourceRecordView, error) {
	source.query = append(source.query, query)
	if source.err != nil {
		return nil, source.err
	}
	views := source.byNode[query.OwnerNodeID]
	if query.OwnerNodeID == "" {
		for _, nodeViews := range source.byNode {
			views = append(views, nodeViews...)
		}
	}
	result := make([]resourcedirectory.ResourceRecordView, 0, len(views))
	excluded := make(map[model.NodeID]struct{}, len(query.ExcludedNodeIDs))
	for _, nodeID := range query.ExcludedNodeIDs {
		excluded[nodeID] = struct{}{}
	}
	for _, view := range views {
		if _, skip := excluded[view.Descriptor.OwnerNodeID]; skip {
			continue
		}
		result = append(result, view.Clone())
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Descriptor.OwnerNodeID != result[j].Descriptor.OwnerNodeID {
			return result[i].Descriptor.OwnerNodeID < result[j].Descriptor.OwnerNodeID
		}
		return result[i].Descriptor.ID < result[j].Descriptor.ID
	})
	return result, nil
}

func recoveryStep() MappedStep {
	return MappedStep{ID: "step-1", Capability: "temperature_sensor", NodeID: "node-a", Inputs: map[string]string{"operation": "read_temperature"}}
}

func TestRecoverResourceMappingRebuildsUniqueHistoricalCandidate(t *testing.T) {
	source := &recoveryCandidateSource{byNode: map[model.NodeID][]resourcedirectory.ResourceRecordView{
		"node-a": {candidateView(t, "resource-a", "node-a", 3, "registration-2", 2)},
	}}
	instance := mustResourceMapper(t, source)
	resolved, remapped, err := instance.RecoverResourceMapping(context.Background(), recoveryStep())
	if err != nil || remapped {
		t.Fatalf("RecoverResourceMapping() = %+v, %v, remapped=%v", resolved, err, remapped)
	}
	if resolved.ResourceRef.ResourceID != "resource-a" || resolved.ResourceRef.ResourceGeneration != 2 || resolved.ResourceRef.OwnerNodeGeneration != 3 || resolved.ResourceRef.RegistrationID != "registration-2" {
		t.Fatalf("resolved ResourceRef = %+v", resolved.ResourceRef)
	}
	if len(source.query) != 1 || source.query[0].OwnerNodeID != "node-a" {
		t.Fatalf("queries = %+v, want one historical-node query", source.query)
	}
}

func TestRecoverResourceMappingRemapsWhenHistoricalNodeIsUnresolved(t *testing.T) {
	source := &recoveryCandidateSource{byNode: map[model.NodeID][]resourcedirectory.ResourceRecordView{
		"node-b": {candidateView(t, "resource-b", "node-b", 1, "registration-b", 1)},
	}}
	instance := mustResourceMapper(t, source)
	resolved, remapped, err := instance.RecoverResourceMapping(context.Background(), recoveryStep())
	if err != nil || !remapped {
		t.Fatalf("RecoverResourceMapping() = %+v, %v, remapped=%v", resolved, err, remapped)
	}
	if resolved.NodeID != "node-b" || resolved.ResourceRef.ResourceID != "resource-b" {
		t.Fatalf("resolved = %+v, want node-b/resource-b", resolved)
	}
	if len(source.query) != 2 || source.query[1].OwnerNodeID != "" || len(source.query[1].ExcludedNodeIDs) != 0 {
		t.Fatalf("queries = %+v, want requirement-only global query", source.query)
	}
}

func TestRecoverResourceMappingDoesNotGuessAmongMultipleHistoricalCandidates(t *testing.T) {
	source := &recoveryCandidateSource{byNode: map[model.NodeID][]resourcedirectory.ResourceRecordView{
		"node-a": {
			candidateView(t, "resource-a", "node-a", 1, "registration-a", 1),
			candidateView(t, "resource-b", "node-a", 1, "registration-a", 1),
		},
		"node-b": {candidateView(t, "resource-c", "node-b", 1, "registration-b", 1)},
	}}
	instance := mustResourceMapper(t, source)
	resolved, remapped, err := instance.RecoverResourceMapping(context.Background(), recoveryStep())
	if err != nil || !remapped {
		t.Fatalf("RecoverResourceMapping() = %+v, %v, remapped=%v", resolved, err, remapped)
	}
	if resolved.ResourceRef == (model.ResourceRef{}) || resolved.ResourceRef.OwnerNodeID != resolved.NodeID {
		t.Fatalf("resolved ResourceRef = %+v, want complete remap", resolved.ResourceRef)
	}
}

func TestRecoverResourceMappingReturnsPendingCauseWhenNoGlobalCandidate(t *testing.T) {
	source := &recoveryCandidateSource{byNode: map[model.NodeID][]resourcedirectory.ResourceRecordView{}}
	instance := mustResourceMapper(t, source)
	_, _, err := instance.RecoverResourceMapping(context.Background(), recoveryStep())
	if !errors.Is(err, ErrResourceUnavailable) {
		t.Fatalf("RecoverResourceMapping() error = %v, want ErrResourceUnavailable", err)
	}
}
