package resourcedirectory

import (
	"fmt"
	"sync"

	"dtm/internal/model"
	"dtm/internal/node"
)

type nodeFence struct {
	generation     int64
	registrationID string
}

type nodeLifecycleState struct {
	view     NodeLifecycleView
	hasView  bool
	quiesced bool
	reason   IneligibleReason
}

type resourceRecord struct {
	descriptor       model.ResourceDescriptor
	nodeGeneration   int64
	registrationID   string
	publicationState PublicationState
}

type Directory struct {
	mu         sync.RWMutex
	records    map[model.ResourceID]resourceRecord
	nodeFences map[model.NodeID]nodeFence
	lifecycles map[model.NodeID]nodeLifecycleState
}

func New() *Directory {
	return &Directory{
		records:    make(map[model.ResourceID]resourceRecord),
		nodeFences: make(map[model.NodeID]nodeFence),
		lifecycles: make(map[model.NodeID]nodeLifecycleState),
	}
}

func (directory *Directory) ApplyNodeSnapshot(snapshot NodeResourceSnapshot) ([]ResourceRecordView, error) {
	if err := snapshot.Validate(); err != nil {
		return nil, err
	}
	snapshot = snapshot.Clone()

	directory.mu.Lock()
	defer directory.mu.Unlock()

	if err := directory.checkNodeFenceLocked(snapshot.NodeID, snapshot.NodeGeneration, snapshot.RegistrationID); err != nil {
		return nil, err
	}
	if lifecycle, exists := directory.lifecycles[snapshot.NodeID]; exists && lifecycle.hasView {
		if err := compareNodeFence(snapshot.NodeGeneration, snapshot.RegistrationID, lifecycle.view.NodeGeneration, lifecycle.view.RegistrationID); err != nil {
			return nil, err
		}
	}

	plan, err := PlanNodeResourceSnapshot(viewsFromRecords(directory.records), snapshot)
	if err != nil {
		return nil, err
	}
	nextRecords, err := validateTransitionPlan(plan)
	if err != nil {
		return nil, err
	}
	directory.records = nextRecords
	directory.nodeFences[snapshot.NodeID] = nodeFence{generation: snapshot.NodeGeneration, registrationID: snapshot.RegistrationID}
	return directory.listByNodeLocked(snapshot.NodeID), nil
}

func (directory *Directory) WithdrawNodeResources(nodeID model.NodeID, nodeGeneration int64, registrationID string) error {
	if err := validateNodeFence(nodeID, nodeGeneration, registrationID); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceSnapshot, err)
	}

	directory.mu.Lock()
	defer directory.mu.Unlock()
	if err := directory.checkNodeFenceLocked(nodeID, nodeGeneration, registrationID); err != nil {
		return err
	}
	if lifecycle, exists := directory.lifecycles[nodeID]; exists && lifecycle.hasView {
		if err := compareNodeFence(nodeGeneration, registrationID, lifecycle.view.NodeGeneration, lifecycle.view.RegistrationID); err != nil {
			return err
		}
	}

	plan, err := PlanNodeResourceSnapshot(viewsFromRecords(directory.records), NodeResourceSnapshot{
		NodeID: nodeID, NodeGeneration: nodeGeneration, RegistrationID: registrationID,
	})
	if err != nil {
		return err
	}
	nextRecords, err := validateTransitionPlan(plan)
	if err != nil {
		return err
	}
	directory.records = nextRecords
	directory.nodeFences[nodeID] = nodeFence{generation: nodeGeneration, registrationID: registrationID}
	return nil
}

// PublishTransition atomically publishes a plan that was already committed by
// the persistence coordinator. Publication atomically quiesces the owner until
// the registration path explicitly activates the matching lifecycle fence.
func (directory *Directory) PublishTransition(plan TransitionPlan) error {
	records, err := validateTransitionPlan(plan)
	if err != nil {
		return err
	}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	if err := directory.checkNodeFenceLocked(plan.NodeID, plan.NodeGeneration, plan.RegistrationID); err != nil {
		return err
	}
	directory.records = records
	directory.nodeFences[plan.NodeID] = nodeFence{generation: plan.NodeGeneration, registrationID: plan.RegistrationID}
	state := directory.lifecycles[plan.NodeID]
	state.quiesced = true
	state.reason = mergeQuiesceReason(state.reason, IneligibleReasonPublicationInProgress)
	directory.lifecycles[plan.NodeID] = state
	return nil
}

// RestoreRecords atomically replaces persisted records without recalculating
// Generation. Lifecycle views are cleared so restored resources fail closed.
func (directory *Directory) RestoreRecords(views []ResourceRecordView) error {
	records, err := recordsFromViews(cloneRecordViews(views))
	if err != nil {
		return err
	}
	fences := make(map[model.NodeID]nodeFence)
	for _, record := range records {
		current, exists := fences[record.descriptor.OwnerNodeID]
		if exists {
			if record.nodeGeneration < current.generation {
				continue
			}
			if record.nodeGeneration == current.generation && record.registrationID != current.registrationID {
				return fmt.Errorf("%w: owner %q has inconsistent persisted fences", ErrInvalidResourceSnapshot, record.descriptor.OwnerNodeID)
			}
		}
		fences[record.descriptor.OwnerNodeID] = nodeFence{generation: record.nodeGeneration, registrationID: record.registrationID}
	}
	directory.mu.Lock()
	directory.records = records
	directory.nodeFences = fences
	directory.lifecycles = make(map[model.NodeID]nodeLifecycleState)
	directory.mu.Unlock()
	return nil
}

func (directory *Directory) UpdateNodeLifecycle(view NodeLifecycleView) error {
	if err := view.Validate(); err != nil {
		return err
	}

	directory.mu.Lock()
	defer directory.mu.Unlock()
	if current, exists := directory.lifecycles[view.NodeID]; exists && current.hasView {
		if err := compareNodeFence(view.NodeGeneration, view.RegistrationID, current.view.NodeGeneration, current.view.RegistrationID); err != nil {
			return err
		}
	}
	if fence, exists := directory.nodeFences[view.NodeID]; exists {
		if err := compareNodeFence(view.NodeGeneration, view.RegistrationID, fence.generation, fence.registrationID); err != nil {
			return err
		}
	}
	state := directory.lifecycles[view.NodeID]
	state.view = view
	state.hasView = true
	directory.lifecycles[view.NodeID] = state
	return nil
}

// QuiesceNode immediately makes every Resource owned by nodeID ineligible.
// It is local, idempotent, and deliberately preserves descriptors and fences.
// The incoming reason is normalized (zero and unknown values become the
// explicit unknown_quiesce_reason strong state) and merged with the current
// one under the frozen priority: strong isolation is never downgraded,
// recoverable isolation stays recoverable, and ordinary calls can never clear
// an existing quiesce.
func (directory *Directory) QuiesceNode(nodeID model.NodeID, reason IneligibleReason) {
	directory.mu.Lock()
	defer directory.mu.Unlock()
	state := directory.lifecycles[nodeID]
	if state.quiesced {
		state.reason = mergeQuiesceReason(state.reason, normalizeQuiesceReason(reason))
	} else {
		state.reason = normalizeQuiesceReason(reason)
	}
	state.quiesced = true
	directory.lifecycles[nodeID] = state
}

// NodeQuiesceReason returns the current quiesce reason for nodeID, or
// IneligibleReasonNone when the node is not quiesced. The returned reason
// drives the production routing between UpdateNodeLifecycle and
// ReconcileNodeLifecycle.
func (directory *Directory) NodeQuiesceReason(nodeID model.NodeID) IneligibleReason {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	state, exists := directory.lifecycles[nodeID]
	if !exists || !state.quiesced {
		return IneligibleReasonNone
	}
	return state.reason
}

// ReconcileNodeLifecycle is the authoritative reconciliation entry used only
// after a successful authoritative state collection (committed NodeRecord,
// runtime Lease and Endpoint). It atomically updates the lifecycle view and
// lifts recoverable isolation only when every fence and runtime condition is
// exactly satisfied. It never advances Generation, never modifies Descriptor,
// PublicationState or tombstones, and never lifts strong isolation.
func (directory *Directory) ReconcileNodeLifecycle(view NodeLifecycleView) error {
	if err := view.Validate(); err != nil {
		return err
	}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	fence, exists := directory.nodeFences[view.NodeID]
	if !exists {
		return fmt.Errorf("%w: node %q has no resource snapshot fence", ErrInvalidNodeLifecycleView, view.NodeID)
	}
	if err := compareNodeFence(view.NodeGeneration, view.RegistrationID, fence.generation, fence.registrationID); err != nil {
		return err
	}
	if view.NodeGeneration != fence.generation || view.RegistrationID != fence.registrationID {
		return fmt.Errorf("%w: lifecycle fence does not match resource snapshot", ErrInvalidNodeLifecycleView)
	}
	state := directory.lifecycles[view.NodeID]
	if !state.quiesced {
		state.view = view
		state.hasView = true
		directory.lifecycles[view.NodeID] = state
		return nil
	}
	if classifyQuiesceReason(state.reason) != quiesceClassRecoverable {
		return fmt.Errorf("%w: reason %q is not automatically reconcilable", ErrQuiesceRequiresRegistration, state.reason)
	}
	if view.Status != node.StatusActive {
		return fmt.Errorf("%w: node %q status %q", ErrNodeNotActive, view.NodeID, view.Status)
	}
	if !view.LeaseValid {
		return fmt.Errorf("%w: node %q", ErrLeaseInvalid, view.NodeID)
	}
	if !view.EndpointActive {
		return fmt.Errorf("%w: node %q", ErrEndpointInactive, view.NodeID)
	}
	state.view = view
	state.hasView = true
	state.quiesced = false
	state.reason = IneligibleReasonNone
	directory.lifecycles[view.NodeID] = state
	return nil
}

// ActivateNodeLifecycle is reserved for the final successful registration
// publication step. Unlike UpdateNodeLifecycle it may clear quiescence, so it
// additionally requires a fully healthy runtime View (Status ACTIVE,
// LeaseValid and EndpointActive) with an exactly matching Fence. A caller
// that knows the Fence but not the runtime health must not be able to lift
// strong isolation.
func (directory *Directory) ActivateNodeLifecycle(view NodeLifecycleView) error {
	if err := view.Validate(); err != nil {
		return err
	}
	directory.mu.Lock()
	defer directory.mu.Unlock()
	fence, exists := directory.nodeFences[view.NodeID]
	if !exists {
		return fmt.Errorf("%w: node %q has no resource fence", ErrInvalidNodeLifecycleView, view.NodeID)
	}
	if err := compareNodeFence(view.NodeGeneration, view.RegistrationID, fence.generation, fence.registrationID); err != nil {
		return err
	}
	if view.NodeGeneration != fence.generation || view.RegistrationID != fence.registrationID {
		return fmt.Errorf("%w: lifecycle fence does not match resource snapshot", ErrInvalidNodeLifecycleView)
	}
	if view.Status != node.StatusActive {
		return fmt.Errorf("%w: node %q status %q", ErrNodeNotActive, view.NodeID, view.Status)
	}
	if !view.LeaseValid {
		return fmt.Errorf("%w: node %q", ErrLeaseInvalid, view.NodeID)
	}
	if !view.EndpointActive {
		return fmt.Errorf("%w: node %q", ErrEndpointInactive, view.NodeID)
	}
	directory.lifecycles[view.NodeID] = nodeLifecycleState{view: view, hasView: true}
	return nil
}

func (directory *Directory) GetByID(id model.ResourceID) (ResourceRecordView, error) {
	if err := id.Validate(); err != nil {
		return ResourceRecordView{}, fmt.Errorf("%w: %v", ErrResourceNotFound, err)
	}
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	record, exists := directory.records[id]
	if !exists {
		return ResourceRecordView{}, fmt.Errorf("%w: %q", ErrResourceNotFound, id)
	}
	return directory.viewLocked(record), nil
}

func (directory *Directory) ListByNode(nodeID model.NodeID) []ResourceRecordView {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	return directory.listByNodeLocked(nodeID)
}

func (directory *Directory) ListAll() []ResourceRecordView {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	result := make([]ResourceRecordView, 0, len(directory.records))
	for _, record := range directory.records {
		result = append(result, directory.viewLocked(record))
	}
	sortRecordViews(result)
	return result
}

func (directory *Directory) ListEligible() []ResourceRecordView {
	return directory.listMatchingState(func(view ResourceRecordView) bool { return view.Eligible })
}

func (directory *Directory) ListIneligible() []ResourceRecordView {
	return directory.listMatchingState(func(view ResourceRecordView) bool {
		return view.PublicationState == PublicationStatePublished && !view.Eligible
	})
}

func (directory *Directory) ListWithdrawn() []ResourceRecordView {
	return directory.listMatchingState(func(view ResourceRecordView) bool {
		return view.PublicationState == PublicationStateWithdrawn
	})
}

func (directory *Directory) MatchRequirement(requirement model.ResourceRequirement) ([]ResourceRecordView, error) {
	if err := requirement.Validate(); err != nil {
		return nil, err
	}
	requirement = requirement.Clone()
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	result := make([]ResourceRecordView, 0)
	for _, record := range directory.records {
		view := directory.viewLocked(record)
		if !view.Eligible || !matchesRequirement(view.Descriptor, requirement) {
			continue
		}
		result = append(result, view)
	}
	sortRecordViews(result)
	return result, nil
}

func (directory *Directory) listMatchingState(matches func(ResourceRecordView) bool) []ResourceRecordView {
	directory.mu.RLock()
	defer directory.mu.RUnlock()
	result := make([]ResourceRecordView, 0)
	for _, record := range directory.records {
		view := directory.viewLocked(record)
		if matches(view) {
			result = append(result, view)
		}
	}
	sortRecordViews(result)
	return result
}

func (directory *Directory) listByNodeLocked(nodeID model.NodeID) []ResourceRecordView {
	result := make([]ResourceRecordView, 0)
	for _, record := range directory.records {
		if record.descriptor.OwnerNodeID == nodeID {
			result = append(result, directory.viewLocked(record))
		}
	}
	sortRecordViews(result)
	return result
}

func (directory *Directory) viewLocked(record resourceRecord) ResourceRecordView {
	eligible, reason := directory.eligibilityLocked(record)
	return ResourceRecordView{
		Descriptor: record.descriptor.Clone(), NodeGeneration: record.nodeGeneration,
		RegistrationID: record.registrationID, PublicationState: record.publicationState,
		Eligible: eligible, IneligibleReason: reason,
	}
}

func (directory *Directory) eligibilityLocked(record resourceRecord) (bool, IneligibleReason) {
	if record.publicationState != PublicationStatePublished {
		return false, IneligibleReasonResourceWithdrawn
	}
	lifecycle, exists := directory.lifecycles[record.descriptor.OwnerNodeID]
	if !exists || !lifecycle.hasView {
		return false, IneligibleReasonNodeLifecycleUnavailable
	}
	if lifecycle.quiesced {
		if lifecycle.reason == IneligibleReasonNone {
			return false, IneligibleReasonNodeLifecycleUnavailable
		}
		return false, lifecycle.reason
	}
	view := lifecycle.view
	if view.Status != node.StatusActive {
		return false, IneligibleReasonNodeNotActive
	}
	if !view.LeaseValid {
		return false, IneligibleReasonLeaseInvalid
	}
	if !view.EndpointActive {
		return false, IneligibleReasonEndpointInactive
	}
	if record.nodeGeneration != view.NodeGeneration {
		return false, IneligibleReasonNodeGenerationMismatch
	}
	if record.registrationID != view.RegistrationID {
		return false, IneligibleReasonRegistrationMismatch
	}
	return true, IneligibleReasonNone
}

func (directory *Directory) checkNodeFenceLocked(nodeID model.NodeID, generation int64, registrationID string) error {
	if current, exists := directory.nodeFences[nodeID]; exists {
		return compareNodeFence(generation, registrationID, current.generation, current.registrationID)
	}
	return nil
}

func compareNodeFence(incomingGeneration int64, incomingRegistration string, currentGeneration int64, currentRegistration string) error {
	if incomingGeneration < currentGeneration {
		return fmt.Errorf("%w: incoming %d is older than %d", ErrStaleNodeGeneration, incomingGeneration, currentGeneration)
	}
	if incomingGeneration == currentGeneration && incomingRegistration != currentRegistration {
		return fmt.Errorf("%w: registration %q is not current", ErrStaleRegistration, incomingRegistration)
	}
	return nil
}

func normalizeDescriptor(descriptor model.ResourceDescriptor) (model.ResourceDescriptor, error) {
	return model.NewResourceDescriptor(
		descriptor.ID, descriptor.Kind, descriptor.Type, descriptor.OwnerNodeID,
		descriptor.Generation, descriptor.Operations, descriptor.Attributes,
	)
}

func descriptorSemanticallyEqual(left, right model.ResourceDescriptor) bool {
	if left.ID != right.ID || left.Kind != right.Kind || left.Type != right.Type || left.OwnerNodeID != right.OwnerNodeID {
		return false
	}
	leftOperations := left.CanonicalOperations()
	rightOperations := right.CanonicalOperations()
	if len(leftOperations) != len(rightOperations) {
		return false
	}
	for index := range leftOperations {
		if leftOperations[index] != rightOperations[index] {
			return false
		}
	}
	if len(left.Attributes) != len(right.Attributes) {
		return false
	}
	for key, value := range left.Attributes {
		rightValue, exists := right.Attributes[key]
		if !exists || rightValue != value {
			return false
		}
	}
	return true
}

func cloneRecords(values map[model.ResourceID]resourceRecord) map[model.ResourceID]resourceRecord {
	clone := make(map[model.ResourceID]resourceRecord, len(values))
	for id, record := range values {
		record.descriptor = record.descriptor.Clone()
		clone[id] = record
	}
	return clone
}

func matchesRequirement(descriptor model.ResourceDescriptor, requirement model.ResourceRequirement) bool {
	if descriptor.Kind != requirement.Kind || descriptor.Type != requirement.Type {
		return false
	}
	foundOperation := false
	for _, operation := range descriptor.Operations {
		if operation.ID == requirement.OperationID {
			foundOperation = true
			break
		}
	}
	if !foundOperation {
		return false
	}
	for key, value := range requirement.RequiredAttributes {
		descriptorValue, exists := descriptor.Attributes[key]
		if !exists || descriptorValue != value {
			return false
		}
	}
	return true
}
