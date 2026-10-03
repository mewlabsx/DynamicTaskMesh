package execution

import "testing"

func TestExecutionOwnershipValuesValidateAndClonePayload(t *testing.T) {
	var invocationID InvocationID
	invocationID[0] = 1
	request := ExecutionRequest{
		InvocationID:       invocationID,
		RootTaskRef:        "root-1",
		ChildTaskRef:       "child-1",
		ExecutionContextID: ExecutionContextID("context-1"),
		CapabilityHandleID: CapabilityHandleID("handle-1"),
		CallerIdentity:     "subject",
		Scope:              "local",
		Operation:          "execute",
		Payload:            []byte("payload"),
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("ExecutionRequest.Validate() error = %v", err)
	}
	clone := request.Clone()
	clone.Payload[0] = 'X'
	if string(request.Payload) != "payload" {
		t.Fatalf("ExecutionRequest.Clone() shared payload: %q", request.Payload)
	}

	descriptor := ExecutionDescriptor{
		InvocationID:            invocationID,
		RootTaskRef:             request.RootTaskRef,
		ChildTaskRef:            request.ChildTaskRef,
		ExecutionContextID:      request.ExecutionContextID,
		CapabilityHandleID:      request.CapabilityHandleID,
		CapabilityInstanceID:    CapabilityInstanceID("instance-1"),
		CapabilityDeclarationID: CapabilityDeclarationID("declaration-1"),
		ResourceID:              ResourceID("resource-1"),
		CallerIdentity:          request.CallerIdentity,
		Scope:                   request.Scope,
		Operation:               request.Operation,
		Payload:                 request.Payload,
	}
	allocation := ExecutionAllocation{
		ID:               ExecutionAllocationID("allocation-1"),
		Lifecycle:        AllocationRunning,
		CurrentOccupancy: CurrentOccupancyNotEstablished,
		OwnershipFence:   OwnershipFence("fence-1"),
		Descriptor:       descriptor,
	}
	if err := allocation.Validate(); err != nil {
		t.Fatalf("ExecutionAllocation.Validate() error = %v", err)
	}
	allocation.Lifecycle = AllocationReleased
	allocation.CurrentOccupancy = CurrentOccupancyEnded
	if err := allocation.Validate(); err != nil {
		t.Fatalf("released ExecutionAllocation.Validate() error = %v", err)
	}
}

func TestExecutionObservationKeepsUnknownDimensionsExplicit(t *testing.T) {
	var invocationID InvocationID
	invocationID[0] = 1
	observation := ExecutionObservation{
		AllocationID: ExecutionAllocationID("allocation-1"),
		InvocationID: invocationID,
		Outcome:      ExecutionOutcomeUnknown,
		Occupancy:    OccupancyConclusionUnknown,
		Reason:       "transport uncertainty",
	}
	if err := observation.Validate(); err != nil {
		t.Fatalf("ExecutionObservation.Validate() error = %v", err)
	}
	if observation.Outcome != OutcomeUnknown || observation.Occupancy != OccupancyUnknown {
		t.Fatalf("observation = %#v, want UNKNOWN + UNKNOWN", observation)
	}
}
