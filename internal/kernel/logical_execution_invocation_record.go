package kernel

import "sync"

// logicalInvocationRecord owns the private correlation and completion state
// for one accepted invocation. The facade continues to own the invocation
// registry and all admission/orchestration sequencing.

type logicalInvocationStage string

const (
	logicalStageAccepted    logicalInvocationStage = "ACCEPTED"
	logicalStageAllocated   logicalInvocationStage = "ALLOCATED"
	logicalStageDispatching logicalInvocationStage = "DISPATCHING"
	logicalStageFinished    logicalInvocationStage = "FINISHED"
)

type logicalInvocationRecord struct {
	mu                        sync.RWMutex
	binding                   invocationBinding
	done                      chan struct{}
	dispatchDone              chan struct{}
	stage                     logicalInvocationStage
	allocationID              ExecutionAllocationID
	resourceID                ResourceID
	ownershipFence            OwnershipFence
	providerCrossingAvailable bool
	providerCrossing          ProviderCrossing
	result                    LogicalExecutionResult
	resolutionEventID         EventID
	resolutionMu              sync.Mutex
}

func (record *logicalInvocationRecord) setAllocation(allocation ExecutionAllocation) {
	record.mu.Lock()
	record.stage = logicalStageAllocated
	record.allocationID = allocation.ID
	record.resourceID = allocation.Descriptor.ResourceID
	record.ownershipFence = allocation.OwnershipFence
	record.providerCrossingAvailable = true
	record.providerCrossing = ProviderCrossingNotCrossedDefinite
	record.result.ExecutionAllocationID = allocation.ID
	record.result.ProviderCrossingAvailable = true
	record.result.ProviderCrossing = ProviderCrossingNotCrossedDefinite
	record.result.CurrentOccupancy = allocation.CurrentOccupancy
	record.result.Lifecycle = allocation.Lifecycle
	record.result.Status = LogicalExecutionAllocated
	record.result = record.result.Clone()
	record.mu.Unlock()
}

func (record *logicalInvocationRecord) setDispatching() {
	record.mu.Lock()
	record.stage = logicalStageDispatching
	record.mu.Unlock()
}

func (record *logicalInvocationRecord) setFinished(result LogicalExecutionResult) {
	record.mu.Lock()
	record.stage = logicalStageFinished
	record.providerCrossingAvailable = result.ProviderCrossingAvailable
	record.providerCrossing = result.ProviderCrossing
	record.result = result.Clone()
	record.mu.Unlock()
	closeIfOpen(record.dispatchDone)
	close(record.done)
}

func (record *logicalInvocationRecord) setResult(result LogicalExecutionResult) {
	record.mu.Lock()
	record.providerCrossingAvailable = result.ProviderCrossingAvailable
	record.providerCrossing = result.ProviderCrossing
	record.result = result.Clone()
	record.mu.Unlock()
}

func (record *logicalInvocationRecord) snapshot() (invocationBinding, logicalInvocationStage, ExecutionAllocationID, ResourceID, OwnershipFence, LogicalExecutionResult) {
	record.mu.RLock()
	binding := invocationBinding{
		ExecutionContextID: record.binding.ExecutionContextID,
		CapabilityHandleID: record.binding.CapabilityHandleID,
		Operation:          record.binding.Operation,
		Payload:            append([]byte(nil), record.binding.Payload...),
		RootTaskRef:        record.binding.RootTaskRef,
		ChildTaskRef:       record.binding.ChildTaskRef,
	}
	stage := record.stage
	allocationID := record.allocationID
	resourceID := record.resourceID
	fence := record.ownershipFence
	result := record.result.Clone()
	record.mu.RUnlock()
	return binding, stage, allocationID, resourceID, fence, result
}

func (record *logicalInvocationRecord) resolutionEvent() EventID {
	record.mu.RLock()
	eventID := record.resolutionEventID
	record.mu.RUnlock()
	return eventID
}

func (record *logicalInvocationRecord) setResolutionEvent(eventID EventID) {
	record.mu.Lock()
	record.resolutionEventID = eventID
	record.mu.Unlock()
}

func (record *logicalInvocationRecord) crossingFact() (bool, ProviderCrossing) {
	if record == nil {
		return false, ""
	}
	record.mu.RLock()
	available := record.providerCrossingAvailable
	crossing := record.providerCrossing
	record.mu.RUnlock()
	if !available {
		return false, ""
	}
	return true, crossing
}

func closeIfOpen(channel chan struct{}) {
	select {
	case <-channel:
	default:
		close(channel)
	}
}
