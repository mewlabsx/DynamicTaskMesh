package mapper

import (
	"fmt"
	"strings"

	"dtm/internal/model"
)

// ResourceCandidateQuery is the read-only scheduling query model frozen by
// M4-M0-R2/R3. It expresses ordinary mapping, remapping (by Resource or Node
// failure) and Core-restart recovery re-resolution (by OwnerNodeID).
type ResourceCandidateQuery struct {
	// Requirement is mandatory and must pass Validate() before any Directory
	// query.
	Requirement model.ResourceRequirement

	// OwnerNodeID optionally restricts candidates to exactly this Provider
	// Node. Zero value means unrestricted. Used by recovery re-resolution
	// against a persisted NodeID.
	OwnerNodeID model.NodeID

	// ExcludedResourceIDs is used only for explicit Resource-level failures
	// or explicit bans on a logical Resource. Mapping Fence staleness must
	// NOT be expressed here: a stale mapping re-queries without excluding the
	// ResourceID so a newer Generation or Registration of the same Resource
	// can be selected.
	ExcludedResourceIDs []model.ResourceID

	// ExcludedNodeIDs excludes every Resource provided by the given Nodes
	// (Node Offline, Lease invalid, Endpoint failure, Node-level execution
	// failure).
	ExcludedNodeIDs []model.NodeID
}

func (query ResourceCandidateQuery) validate() error {
	if err := query.Requirement.Validate(); err != nil {
		return fmt.Errorf("%w: requirement: %v", ErrInvalidResourceCandidateQuery, err)
	}
	if strings.TrimSpace(string(query.OwnerNodeID)) != string(query.OwnerNodeID) {
		return fmt.Errorf("%w: owner node id has surrounding whitespace", ErrInvalidResourceCandidateQuery)
	}
	for _, resourceID := range query.ExcludedResourceIDs {
		if strings.TrimSpace(string(resourceID)) == "" {
			return fmt.Errorf("%w: empty excluded resource id", ErrInvalidResourceCandidateQuery)
		}
	}
	for _, nodeID := range query.ExcludedNodeIDs {
		if strings.TrimSpace(string(nodeID)) == "" {
			return fmt.Errorf("%w: empty excluded node id", ErrInvalidResourceCandidateQuery)
		}
	}
	return nil
}
