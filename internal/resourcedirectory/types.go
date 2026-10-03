package resourcedirectory

import (
	"errors"
	"fmt"
	"sort"

	"dtm/internal/model"
	"dtm/internal/node"
)

var (
	ErrInvalidNodeLifecycleView    = errors.New("invalid node lifecycle view")
	ErrInvalidResourceSnapshot     = errors.New("invalid resource snapshot")
	ErrResourceNotFound            = errors.New("resource not found")
	ErrStaleNodeGeneration         = errors.New("stale node generation")
	ErrStaleRegistration           = errors.New("stale registration")
	ErrStaleResourceGeneration     = errors.New("stale resource generation")
	ErrResourceOwnerConflict       = errors.New("resource owner conflict")
	ErrResourceKindConflict        = errors.New("resource kind conflict")
	ErrResourceGenerationOverflow  = errors.New("resource generation overflow")
	ErrQuiesceRequiresRegistration = errors.New("quiesce requires complete registration")
	ErrNodeNotActive               = errors.New("node is not active")
	ErrLeaseInvalid                = errors.New("node lease is invalid")
	ErrEndpointInactive            = errors.New("node endpoint is inactive")
)

type PublicationState string

const (
	PublicationStatePublished PublicationState = "published"
	PublicationStateWithdrawn PublicationState = "withdrawn"
)

type IneligibleReason string

const (
	IneligibleReasonNone                      IneligibleReason = ""
	IneligibleReasonNodeLifecycleUnavailable  IneligibleReason = "node_lifecycle_unavailable"
	IneligibleReasonNodeNotActive             IneligibleReason = "node_not_active"
	IneligibleReasonLeaseInvalid              IneligibleReason = "lease_invalid"
	IneligibleReasonEndpointInactive          IneligibleReason = "endpoint_inactive"
	IneligibleReasonNodeGenerationMismatch    IneligibleReason = "node_generation_mismatch"
	IneligibleReasonRegistrationMismatch      IneligibleReason = "registration_mismatch"
	IneligibleReasonResourceWithdrawn         IneligibleReason = "resource_withdrawn"
	IneligibleReasonPublicationInProgress     IneligibleReason = "publication_in_progress"
	IneligibleReasonPersistenceFailure        IneligibleReason = "persistence_failure"
	IneligibleReasonPostCommitFailure         IneligibleReason = "post_commit_publication_failure"
	IneligibleReasonCommitOutcomeUnknown      IneligibleReason = "commit_outcome_unknown"
	IneligibleReasonRepositoryReadFailure     IneligibleReason = "repository_read_failure"
	IneligibleReasonSweepPersistenceFailure   IneligibleReason = "sweep_persistence_failure"
	IneligibleReasonOfflinePersistenceFailure IneligibleReason = "offline_persistence_failure"
	IneligibleReasonUnknownQuiesce            IneligibleReason = "unknown_quiesce_reason"
)

// quiesceRecoveryClass is the frozen M3-B isolation classification. Strong
// isolation can only be lifted by the final ActivateNodeLifecycle step of a
// complete Register; recoverable isolation can also be lifted by a successful
// authoritative ReconcileNodeLifecycle. Unknown reasons fail closed as strong.
type quiesceRecoveryClass int

const (
	quiesceClassNone quiesceRecoveryClass = iota
	quiesceClassRecoverable
	quiesceClassStrong
)

// IsRecoverableQuiesceReason reports whether the reason can be lifted by a
// successful authoritative reconciliation. Unknown or zero-value reasons are
// never automatically recoverable.
func IsRecoverableQuiesceReason(reason IneligibleReason) bool {
	return classifyQuiesceReason(reason) == quiesceClassRecoverable
}

func classifyQuiesceReason(reason IneligibleReason) quiesceRecoveryClass {
	switch reason {
	case IneligibleReasonNone:
		return quiesceClassNone
	case IneligibleReasonNodeLifecycleUnavailable,
		IneligibleReasonRepositoryReadFailure,
		IneligibleReasonPersistenceFailure,
		IneligibleReasonSweepPersistenceFailure,
		IneligibleReasonOfflinePersistenceFailure:
		return quiesceClassRecoverable
	case IneligibleReasonPublicationInProgress,
		IneligibleReasonPostCommitFailure,
		IneligibleReasonCommitOutcomeUnknown,
		IneligibleReasonUnknownQuiesce:
		return quiesceClassStrong
	default:
		return quiesceClassStrong
	}
}

// normalizeQuiesceReason maps a caller-supplied Quiesce reason onto the frozen
// M3-C state space. The zero value and every reason outside the frozen sets
// are normalized to the explicit unknown_quiesce_reason strong state so a
// degenerate or accidental input can never be stored as a placeholder that a
// later recoverable reason could replace. Known reasons pass through unchanged.
func normalizeQuiesceReason(reason IneligibleReason) IneligibleReason {
	switch reason {
	case IneligibleReasonNodeLifecycleUnavailable,
		IneligibleReasonRepositoryReadFailure,
		IneligibleReasonPersistenceFailure,
		IneligibleReasonSweepPersistenceFailure,
		IneligibleReasonOfflinePersistenceFailure,
		IneligibleReasonPublicationInProgress,
		IneligibleReasonPostCommitFailure,
		IneligibleReasonCommitOutcomeUnknown,
		IneligibleReasonUnknownQuiesce:
		return reason
	default:
		return IneligibleReasonUnknownQuiesce
	}
}

// mergeQuiesceReason applies the frozen quiesce priority: strong isolation is
// never downgraded, recoverable isolation is idempotent, and an unknown reason
// is never replaced by a weaker one. The result is never set back to none by
// an ordinary quiesce call.
func mergeQuiesceReason(current, incoming IneligibleReason) IneligibleReason {
	if classifyQuiesceReason(incoming) > classifyQuiesceReason(current) {
		return incoming
	}
	return current
}

// NodeLifecycleView is a caller-supplied projection of the authoritative Node
// lifecycle. It owns no timer, endpoint, connection, or persistence handle.
type NodeLifecycleView struct {
	NodeID         model.NodeID
	NodeGeneration int64
	RegistrationID string
	Status         node.Status
	LeaseValid     bool
	EndpointActive bool
}

func NewNodeLifecycleView(
	nodeID model.NodeID,
	nodeGeneration int64,
	registrationID string,
	status node.Status,
	leaseValid bool,
	endpointActive bool,
) (NodeLifecycleView, error) {
	view := NodeLifecycleView{
		NodeID: nodeID, NodeGeneration: nodeGeneration, RegistrationID: registrationID,
		Status: status, LeaseValid: leaseValid, EndpointActive: endpointActive,
	}
	if err := view.Validate(); err != nil {
		return NodeLifecycleView{}, err
	}
	return view, nil
}

func (view NodeLifecycleView) Validate() error {
	if err := validateNodeFence(view.NodeID, view.NodeGeneration, view.RegistrationID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidNodeLifecycleView, err)
	}
	if !knownNodeStatus(view.Status) {
		return fmt.Errorf("%w: unknown status %q", ErrInvalidNodeLifecycleView, view.Status)
	}
	return nil
}

// NodeResourceSnapshot is a complete publication for one Node registration.
// Descriptor generations are expected-generation fences; Directory computes
// and assigns every committed generation.
type NodeResourceSnapshot struct {
	NodeID         model.NodeID
	NodeGeneration int64
	RegistrationID string
	Resources      []model.ResourceDescriptor
}

func NewNodeResourceSnapshot(
	nodeID model.NodeID,
	nodeGeneration int64,
	registrationID string,
	resources []model.ResourceDescriptor,
) (NodeResourceSnapshot, error) {
	snapshot := NodeResourceSnapshot{
		NodeID: nodeID, NodeGeneration: nodeGeneration, RegistrationID: registrationID,
		Resources: cloneDescriptors(resources),
	}
	if err := snapshot.Validate(); err != nil {
		return NodeResourceSnapshot{}, err
	}
	return snapshot, nil
}

func (snapshot NodeResourceSnapshot) Validate() error {
	if err := validateNodeFence(snapshot.NodeID, snapshot.NodeGeneration, snapshot.RegistrationID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceSnapshot, err)
	}
	seenIDs := make(map[model.ResourceID]struct{}, len(snapshot.Resources))
	for _, descriptor := range snapshot.Resources {
		if err := descriptor.Validate(); err != nil {
			return fmt.Errorf("%w: descriptor %q: %v", ErrInvalidResourceSnapshot, descriptor.ID, err)
		}
		if descriptor.OwnerNodeID != snapshot.NodeID {
			return fmt.Errorf("%w: resource %q owner %q differs from node %q", ErrInvalidResourceSnapshot, descriptor.ID, descriptor.OwnerNodeID, snapshot.NodeID)
		}
		if _, exists := seenIDs[descriptor.ID]; exists {
			return fmt.Errorf("%w: duplicate resource id %q", ErrInvalidResourceSnapshot, descriptor.ID)
		}
		seenIDs[descriptor.ID] = struct{}{}
	}
	return nil
}

func (snapshot NodeResourceSnapshot) Clone() NodeResourceSnapshot {
	clone := snapshot
	clone.Resources = cloneDescriptors(snapshot.Resources)
	return clone
}

type ResourceRecordView struct {
	Descriptor       model.ResourceDescriptor
	NodeGeneration   int64
	RegistrationID   string
	PublicationState PublicationState
	Eligible         bool
	IneligibleReason IneligibleReason
}

func (view ResourceRecordView) Clone() ResourceRecordView {
	clone := view
	clone.Descriptor = view.Descriptor.Clone()
	return clone
}

func validateNodeFence(nodeID model.NodeID, nodeGeneration int64, registrationID string) error {
	_, err := model.NewResourceRef("lifecycle-validation", 1, nodeID, nodeGeneration, registrationID)
	return err
}

func knownNodeStatus(status node.Status) bool {
	switch status {
	case node.StatusRegistered, node.StatusActive, node.StatusSuspect, node.StatusOffline, node.StatusRecovering, node.StatusStale:
		return true
	default:
		return false
	}
}

func cloneDescriptors(values []model.ResourceDescriptor) []model.ResourceDescriptor {
	result := make([]model.ResourceDescriptor, len(values))
	for index := range values {
		result[index] = values[index].Clone()
	}
	return result
}

func sortRecordViews(values []ResourceRecordView) {
	sort.Slice(values, func(left, right int) bool {
		return values[left].Descriptor.ID < values[right].Descriptor.ID
	})
}
