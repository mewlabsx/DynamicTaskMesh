package mapper

import (
	"fmt"
	"reflect"

	"dtm/internal/model"
	"dtm/internal/planner"
	"dtm/internal/resourcedirectory"
)

// mapResources is the Resource Scheduling Map path. Every step derives its
// ResourceRequirement from the frozen legacyCapabilityMappings (via
// model.LegacyCapabilityRequirement), queries the Candidate Source, selects
// the first candidate (already deterministically sorted by the Source) and
// records a complete ResourceRef. The whole plan succeeds atomically or
// returns an error; a partially mapped plan is never returned.
func (mapper *Mapper) mapResources(plan planner.Plan) (MappedPlan, error) {
	steps := make([]MappedStep, 0, len(plan.Steps))
	for _, step := range plan.Steps {
		requirement, err := model.LegacyCapabilityRequirement(step.Capability)
		if err != nil {
			return MappedPlan{}, fmt.Errorf("capability %q: %w", step.Capability, err)
		}
		candidates, err := mapper.resourceCandidates.Query(ResourceCandidateQuery{Requirement: requirement})
		if err != nil {
			return MappedPlan{}, fmt.Errorf("capability %q: %w", step.Capability, err)
		}
		if len(candidates) == 0 {
			return MappedPlan{}, fmt.Errorf("capability %q: %w", step.Capability, ErrResourceUnavailable)
		}
		ref, err := resourceRefFromCandidate(candidates[0])
		if err != nil {
			return MappedPlan{}, fmt.Errorf("capability %q: %w", step.Capability, err)
		}
		steps = append(steps, MappedStep{
			ID:              step.ID,
			Capability:      step.Capability,
			IdempotencyMode: step.IdempotencyMode,
			NodeID:          ref.OwnerNodeID,
			ResourceRef:     ref,
			Inputs:          copyInputs(step.Inputs),
		})
	}
	return MappedPlan{TaskID: plan.TaskID, Steps: steps}, nil
}

// remapResources is the Resource Scheduling Remap path. The existing Node
// exclusion signal is forwarded as ExcludedNodeIDs; the selected candidate's
// complete ResourceRef replaces the old one, and every other step field is
// preserved with Inputs deep-copied.
func (mapper *Mapper) remapResources(step MappedStep, excluded map[model.NodeID]struct{}) (MappedStep, error) {
	requirement, err := model.LegacyCapabilityRequirement(step.Capability)
	if err != nil {
		return MappedStep{}, fmt.Errorf("capability %q: %w", step.Capability, err)
	}
	excludedNodeIDs := make([]model.NodeID, 0, len(excluded))
	for nodeID := range excluded {
		excludedNodeIDs = append(excludedNodeIDs, nodeID)
	}
	candidates, err := mapper.resourceCandidates.Query(ResourceCandidateQuery{
		Requirement:     requirement,
		ExcludedNodeIDs: excludedNodeIDs,
	})
	if err != nil {
		return MappedStep{}, fmt.Errorf("capability %q: %w", step.Capability, err)
	}
	if len(candidates) == 0 {
		return MappedStep{}, fmt.Errorf("capability %q: %w", step.Capability, ErrResourceUnavailable)
	}
	ref, err := resourceRefFromCandidate(candidates[0])
	if err != nil {
		return MappedStep{}, fmt.Errorf("capability %q: %w", step.Capability, err)
	}
	remapped := step
	remapped.NodeID = ref.OwnerNodeID
	remapped.ResourceRef = ref
	remapped.Inputs = copyInputs(step.Inputs)
	return remapped, nil
}

// resourceRefFromCandidate builds a complete ResourceRef from an eligible
// ResourceRecordView. Views that cannot form a valid ref are an invariant
// break of the Candidate Source / Directory contract and fail closed with the
// underlying cause preserved.
func resourceRefFromCandidate(view resourcedirectory.ResourceRecordView) (model.ResourceRef, error) {
	ref, err := model.NewResourceRef(
		view.Descriptor.ID,
		view.Descriptor.Generation,
		view.Descriptor.OwnerNodeID,
		view.NodeGeneration,
		view.RegistrationID,
	)
	if err != nil {
		return model.ResourceRef{}, fmt.Errorf(
			"%w: construct resource ref from candidate %q: %w",
			ErrResourceCandidateQueryFailed,
			view.Descriptor.ID,
			err,
		)
	}
	return ref, nil
}

// isNilResourceCandidateSource detects true nil interfaces and typed-nil
// implementations before the reflect-based check.
func isNilResourceCandidateSource(source ResourceCandidateSource) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
