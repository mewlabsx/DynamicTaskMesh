package kernel

// requestExecutionAllocationStage performs the pre-provider allocation work
// for one newly accepted invocation. Registry cleanup remains in
// RequestExecution so this helper only owns the contiguous allocation and
// record-publication operations.
func (facade *LogicalExecutionFacade) requestExecutionAllocationStage(request LogicalExecutionRequest, record *logicalInvocationRecord) (TryAllocateResult, ExecutionAllocation, error) {
	allocationResult, allocationErr := facade.kernel.TryAllocateExecution(request.executionRequest())
	if allocationErr != nil || !allocationResult.Granted {
		return allocationResult, ExecutionAllocation{}, allocationErr
	}

	allocation := allocationResult.Allocation.Clone()
	record.setAllocation(allocation)
	record.setDispatching()
	return allocationResult, allocation, nil
}

// completeRequestExecution performs the post-provider authoritative reads
// and result publication for one dispatched invocation. Dispatch itself and
// the crossing fact remain in RequestExecution so the provider boundary stays
// explicit in the orchestration method.
func (facade *LogicalExecutionFacade) completeRequestExecution(request LogicalExecutionRequest, record *logicalInvocationRecord, allocation ExecutionAllocation, observation ExecutionObservation, dispatchErr error, providerCrossed bool) (LogicalExecutionResult, error) {
	current, currentErr := facade.kernel.GetExecutionAllocation(allocation.ID)
	if currentErr != nil {
		result, structuredErr := logicalExecutionFailure(request.InvocationID, request.RequestID, LogicalErrorInternalFailure, LogicalPhaseObservation, crossingFor(providerCrossed), RetrySafetyUnsafeOrUnknown, providerCrossed, currentErr)
		result.ExecutionAllocationID = allocation.ID
		result.ErrorInfo.ExecutionAllocationID = allocation.ID
		record.setFinished(result)
		return result, structuredErr
	}
	claimHeld, claimErr := facade.kernel.Resources.ExecutionOccupancyForAllocation(current.Descriptor.ResourceID, current.ID)
	if claimErr != nil {
		result, structuredErr := logicalExecutionFailure(request.InvocationID, request.RequestID, LogicalErrorInternalFailure, LogicalPhaseObservation, crossingFor(providerCrossed), RetrySafetyUnsafeOrUnknown, providerCrossed, claimErr)
		result.ExecutionAllocationID = allocation.ID
		result.CurrentOccupancy = current.CurrentOccupancy
		result.Lifecycle = current.Lifecycle
		result.ErrorInfo.ExecutionAllocationID = allocation.ID
		record.setFinished(result)
		return result, structuredErr
	}

	result := logicalDispatchResult(request, current, observation, claimHeld, providerCrossed, dispatchErr)
	record.setFinished(result)
	return result, logicalResultError(result)
}
