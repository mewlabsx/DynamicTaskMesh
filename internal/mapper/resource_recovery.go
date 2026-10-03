package mapper

import (
	"context"
	"errors"
	"fmt"

	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
)

// ResourceRecoveryMapper resolves the ResourceRef omitted from persisted v0.3
// task-step rows. The bool reports that the historical OwnerNodeID could not
// be uniquely reused and a fresh global candidate was selected, so callers
// can persist the new NodeID as a recovery remap.
type ResourceRecoveryMapper interface {
	RecoverResourceMapping(context.Context, MappedStep) (MappedStep, bool, error)
}

// RecoverResourceMapping rebuilds a complete current ResourceRef from the
// persisted Capability + NodeID pair. Exactly one eligible candidate on the
// historical node is authoritative. Zero candidates or multiple candidates
// are unresolved; in either case a fresh global query may select a current
// mapping. The global query intentionally uses only the Requirement: the
// Candidate Source already filters to authoritative eligible views, and a
// remap may legitimately select a different Resource on the same Node. The
// method never falls back to the Legacy registry and never treats a
// multiple-candidate historical query as a recovered identity.
func (mapper *Mapper) RecoverResourceMapping(ctx context.Context, step MappedStep) (MappedStep, bool, error) {
	if err := ctx.Err(); err != nil {
		return MappedStep{}, false, err
	}
	if mapper.resourceCandidates == nil {
		return copyMappedStepForRecovery(step), false, nil
	}
	if stringsTrimmedEmpty(string(step.ID)) || stringsTrimmedEmpty(string(step.Capability)) || stringsTrimmedEmpty(string(step.NodeID)) {
		return MappedStep{}, false, fmt.Errorf("%w: persisted Resource mapping fields are incomplete", ErrInvalidResourceMapping)
	}
	requirement, err := model.LegacyCapabilityRequirement(step.Capability)
	if err != nil {
		return MappedStep{}, false, err
	}

	sameNode, sameErr := mapper.resourceCandidates.Query(ResourceCandidateQuery{
		Requirement: requirement,
		OwnerNodeID: step.NodeID,
	})
	if sameErr == nil && len(sameNode) == 1 {
		return mappedStepWithCandidate(step, sameNode[0])
	}
	if sameErr != nil && !errors.Is(sameErr, ErrResourceUnavailable) {
		return MappedStep{}, false, sameErr
	}

	// No unique historical mapping exists. Re-query the global candidate set;
	// selecting its deterministic first candidate is a fresh Remap, not a claim
	// that it was the historical Resource.
	global, globalErr := mapper.resourceCandidates.Query(ResourceCandidateQuery{
		Requirement: requirement,
	})
	if globalErr != nil {
		return MappedStep{}, false, globalErr
	}
	if len(global) == 0 {
		return MappedStep{}, false, fmt.Errorf("%w: no recovery candidate for capability %q", ErrResourceUnavailable, step.Capability)
	}
	remapped, _, err := mappedStepWithCandidate(step, global[0])
	if err != nil {
		return MappedStep{}, false, err
	}
	return remapped, true, nil
}

// ResolveResourceMapping is a context-free convenience for tests and small
// in-process callers. Production recovery uses RecoverResourceMapping so a
// cancelled recovery scan cannot issue another authoritative query.
func (mapper *Mapper) ResolveResourceMapping(step MappedStep) (MappedStep, bool, error) {
	return mapper.RecoverResourceMapping(context.Background(), step)
}

func mappedStepWithCandidate(step MappedStep, view resourcedirectory.ResourceRecordView) (MappedStep, bool, error) {
	ref, err := resourceRefFromCandidate(view)
	if err != nil {
		return MappedStep{}, false, err
	}
	resolved := copyMappedStepForRecovery(step)
	resolved.NodeID = ref.OwnerNodeID
	resolved.ResourceRef = ref
	return resolved, false, nil
}

func copyMappedStepForRecovery(step MappedStep) MappedStep {
	step.Inputs = copyInputs(step.Inputs)
	return step
}

func stringsTrimmedEmpty(value string) bool {
	for _, r := range value {
		if r != ' ' && r != '\t' && r != '\r' && r != '\n' {
			return false
		}
	}
	return true
}

var _ ResourceRecoveryMapper = (*Mapper)(nil)
