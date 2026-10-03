package resourcedirectory

import (
	"fmt"
	"math"

	"dtm/internal/model"
)

// TransitionPlan is the deterministic result of applying one complete Node
// resource snapshot to an existing set of persisted or in-memory records.
// Eligibility is intentionally absent from the plan because it is derived
// from the current Node lifecycle view.
type TransitionPlan struct {
	NodeID         model.NodeID
	NodeGeneration int64
	RegistrationID string
	Records        []ResourceRecordView
}

func (plan TransitionPlan) Clone() TransitionPlan {
	clone := plan
	clone.Records = cloneRecordViews(plan.Records)
	return clone
}

// PlanNodeResourceSnapshot owns all Resource Generation allocation rules used
// by both Directory and persistence. It performs no I/O and reads no clock.
func PlanNodeResourceSnapshot(current []ResourceRecordView, snapshot NodeResourceSnapshot) (TransitionPlan, error) {
	if err := snapshot.Validate(); err != nil {
		return TransitionPlan{}, err
	}
	snapshot = snapshot.Clone()
	records, err := recordsFromViews(current)
	if err != nil {
		return TransitionPlan{}, err
	}
	nextRecords := cloneRecords(records)
	incoming := make(map[model.ResourceID]struct{}, len(snapshot.Resources))
	for _, supplied := range snapshot.Resources {
		descriptor, err := normalizeDescriptor(supplied)
		if err != nil {
			return TransitionPlan{}, fmt.Errorf("%w: normalize resource %q: %v", ErrInvalidResourceSnapshot, supplied.ID, err)
		}
		incoming[descriptor.ID] = struct{}{}
		currentRecord, exists := records[descriptor.ID]
		if exists && currentRecord.descriptor.OwnerNodeID != descriptor.OwnerNodeID {
			return TransitionPlan{}, fmt.Errorf("%w: resource %q belongs to %q, not %q", ErrResourceOwnerConflict, descriptor.ID, currentRecord.descriptor.OwnerNodeID, descriptor.OwnerNodeID)
		}
		if exists && currentRecord.descriptor.Kind != descriptor.Kind {
			return TransitionPlan{}, fmt.Errorf("%w: resource %q kind is %q, not %q", ErrResourceKindConflict, descriptor.ID, currentRecord.descriptor.Kind, descriptor.Kind)
		}
		generation, err := committedGeneration(currentRecord, exists, descriptor, snapshot.NodeGeneration, snapshot.RegistrationID)
		if err != nil {
			return TransitionPlan{}, fmt.Errorf("resource %q: %w", descriptor.ID, err)
		}
		descriptor.Generation = generation
		nextRecords[descriptor.ID] = resourceRecord{
			descriptor: descriptor, nodeGeneration: snapshot.NodeGeneration,
			registrationID: snapshot.RegistrationID, publicationState: PublicationStatePublished,
		}
	}
	for id, currentRecord := range nextRecords {
		if currentRecord.descriptor.OwnerNodeID != snapshot.NodeID || currentRecord.publicationState != PublicationStatePublished {
			continue
		}
		if _, exists := incoming[id]; exists {
			continue
		}
		currentRecord.nodeGeneration = snapshot.NodeGeneration
		currentRecord.registrationID = snapshot.RegistrationID
		currentRecord.publicationState = PublicationStateWithdrawn
		nextRecords[id] = currentRecord
	}
	return TransitionPlan{
		NodeID: snapshot.NodeID, NodeGeneration: snapshot.NodeGeneration,
		RegistrationID: snapshot.RegistrationID, Records: viewsFromRecords(nextRecords),
	}, nil
}

func validateTransitionPlan(plan TransitionPlan) (map[model.ResourceID]resourceRecord, error) {
	if err := validateNodeFence(plan.NodeID, plan.NodeGeneration, plan.RegistrationID); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidResourceSnapshot, err)
	}
	return recordsFromViews(plan.Records)
}

func recordsFromViews(views []ResourceRecordView) (map[model.ResourceID]resourceRecord, error) {
	records := make(map[model.ResourceID]resourceRecord, len(views))
	for _, view := range views {
		descriptor, err := normalizeDescriptor(view.Descriptor)
		if err != nil {
			return nil, fmt.Errorf("%w: persisted descriptor %q: %v", ErrInvalidResourceSnapshot, view.Descriptor.ID, err)
		}
		if _, exists := records[descriptor.ID]; exists {
			return nil, fmt.Errorf("%w: duplicate resource id %q", ErrInvalidResourceSnapshot, descriptor.ID)
		}
		if err := validateNodeFence(descriptor.OwnerNodeID, view.NodeGeneration, view.RegistrationID); err != nil {
			return nil, fmt.Errorf("%w: resource %q fence: %v", ErrInvalidResourceSnapshot, descriptor.ID, err)
		}
		if view.PublicationState != PublicationStatePublished && view.PublicationState != PublicationStateWithdrawn {
			return nil, fmt.Errorf("%w: resource %q publication state %q", ErrInvalidResourceSnapshot, descriptor.ID, view.PublicationState)
		}
		records[descriptor.ID] = resourceRecord{
			descriptor: descriptor, nodeGeneration: view.NodeGeneration,
			registrationID: view.RegistrationID, publicationState: view.PublicationState,
		}
	}
	return records, nil
}

func viewsFromRecords(records map[model.ResourceID]resourceRecord) []ResourceRecordView {
	views := make([]ResourceRecordView, 0, len(records))
	for _, record := range records {
		views = append(views, ResourceRecordView{
			Descriptor: record.descriptor.Clone(), NodeGeneration: record.nodeGeneration,
			RegistrationID: record.registrationID, PublicationState: record.publicationState,
		})
	}
	sortRecordViews(views)
	return views
}

func cloneRecordViews(values []ResourceRecordView) []ResourceRecordView {
	result := make([]ResourceRecordView, len(values))
	for index := range values {
		result[index] = values[index].Clone()
	}
	return result
}

func committedGeneration(current resourceRecord, exists bool, incoming model.ResourceDescriptor, nodeGeneration int64, registrationID string) (model.ResourceGeneration, error) {
	if !exists {
		if incoming.Generation != 1 {
			return 0, fmt.Errorf("%w: first publication expects generation 1, got %d", ErrStaleResourceGeneration, incoming.Generation)
		}
		return 1, nil
	}
	if incoming.Generation != current.descriptor.Generation {
		return 0, fmt.Errorf("%w: expected %d, got %d", ErrStaleResourceGeneration, current.descriptor.Generation, incoming.Generation)
	}
	advance := current.publicationState == PublicationStateWithdrawn ||
		current.nodeGeneration != nodeGeneration || current.registrationID != registrationID ||
		!descriptorSemanticallyEqual(current.descriptor, incoming)
	if !advance {
		return current.descriptor.Generation, nil
	}
	if current.descriptor.Generation == model.ResourceGeneration(math.MaxUint64) {
		return 0, ErrResourceGenerationOverflow
	}
	return current.descriptor.Generation + 1, nil
}
