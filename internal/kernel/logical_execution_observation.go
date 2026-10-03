package kernel

import "errors"

// This file contains the bounded read-only correlation and observation
// projection used by LogicalExecutionFacade. The facade retains registry
// orchestration, lock/wait sequencing, admission, provider dispatch, and all
// resolution mutation. These helpers preserve the existing manager read order
// and use the record's existing methods as the owners of record locks.

func existingResultForRequest(record *logicalInvocationRecord, requestID string) LogicalExecutionResult {
	_, _, _, _, _, result := record.snapshot()
	result.RequestID = requestID
	if result.ErrorInfo != nil {
		result.ErrorInfo.RequestID = requestID
	}
	return result
}

func (facade *LogicalExecutionFacade) observeKnownInvocation(record *logicalInvocationRecord, allocationID ExecutionAllocationID, requestID string) (LogicalInvocationObservation, error) {
	_, stage, currentAllocationID, _, _, stored := record.snapshot()
	if allocationID == "" {
		allocationID = currentAllocationID
	}
	if allocationID == "" {
		if stage == logicalStageFinished && stored.ErrorInfo != nil {
			return LogicalInvocationObservation{LogicalExecutionResult: existingResultForRequest(record, requestID), State: InvocationObservationInvariantUnknown}, stored.ErrorInfo
		}
		stored.Status = LogicalExecutionAccepted
		stored.RequestID = requestID
		return LogicalInvocationObservation{LogicalExecutionResult: stored, State: InvocationObservationAcceptedNotAllocated}, nil
	}

	allocation, err := facade.kernel.GetExecutionAllocation(allocationID)
	if err != nil {
		result, structuredErr := logicalExecutionFailure(stored.InvocationID, requestID, LogicalErrorInvariantViolation, LogicalPhaseObservation, storedCrossing(stored), RetrySafetyUnsafeOrUnknown, true, err)
		result.ExecutionAllocationID = allocationID
		result.ErrorInfo.ExecutionAllocationID = allocationID
		return LogicalInvocationObservation{LogicalExecutionResult: result, State: InvocationObservationInvariantUnknown}, structuredErr
	}
	claimHeld, err := facade.kernel.Resources.ExecutionOccupancyForAllocation(allocation.Descriptor.ResourceID, allocation.ID)
	if err != nil {
		result, structuredErr := logicalExecutionFailure(stored.InvocationID, requestID, LogicalErrorInvariantViolation, LogicalPhaseObservation, storedCrossing(stored), RetrySafetyUnsafeOrUnknown, true, err)
		result.ExecutionAllocationID = allocationID
		result.CurrentOccupancy = allocation.CurrentOccupancy
		result.Lifecycle = allocation.Lifecycle
		result.ErrorInfo.ExecutionAllocationID = allocationID
		return LogicalInvocationObservation{LogicalExecutionResult: result, State: InvocationObservationInvariantUnknown}, structuredErr
	}

	observation, observationErr := facade.kernel.GetExecutionObservation(allocation.ID)
	if observationErr != nil && !errors.Is(observationErr, ErrAllocationNotRunning) {
		result, structuredErr := logicalExecutionFailure(stored.InvocationID, requestID, LogicalErrorInvariantViolation, LogicalPhaseObservation, storedCrossing(stored), RetrySafetyUnsafeOrUnknown, true, observationErr)
		result.ExecutionAllocationID = allocationID
		result.CurrentOccupancy = allocation.CurrentOccupancy
		result.Lifecycle = allocation.Lifecycle
		result.ErrorInfo.ExecutionAllocationID = allocationID
		return LogicalInvocationObservation{LogicalExecutionResult: result, State: InvocationObservationInvariantUnknown}, structuredErr
	}

	result := stored.Clone()
	result.RequestID = requestID
	result.ExecutionAllocationID = allocation.ID
	result.CurrentOccupancy = allocation.CurrentOccupancy
	result.Lifecycle = allocation.Lifecycle
	result.ObservationAvailable = observationErr == nil
	crossingAvailable, crossing := record.crossingFact()
	if observationErr == nil {
		result.Observation = observation.Clone()
		// A completed immutable observation proves that the dispatch boundary
		// was reached. The underlying execution owner is the source of the
		// crossing fact; the facade only carries it forward.
		crossingAvailable = true
		crossing = crossingFor(facade.kernel.executionProviderCrossed(allocation.ID))
	}
	result.ProviderCrossingAvailable = crossingAvailable
	result.ProviderCrossing = crossing

	switch {
	case allocation.Lifecycle == AllocationRunning && allocation.CurrentOccupancy == CurrentOccupancyNotEstablished && claimHeld && observationErr != nil:
		result.Status = LogicalExecutionRunning
		result.ProviderCrossing = ProviderCrossingNotCrossedDefinite
		result.ReconciliationRequired = false
		result.RetrySafetyFact = RetrySafetyNotDeclared
		return LogicalInvocationObservation{LogicalExecutionResult: result, State: InvocationObservationAllocatedNotCrossed}, nil
	case allocation.Lifecycle == AllocationRunning && allocation.CurrentOccupancy == CurrentOccupancyUnknown && claimHeld && observationErr == nil && observation.Occupancy == OccupancyConclusionUnknown:
		result.Status = LogicalExecutionUnknown
		result.ProviderCrossingAvailable = true
		result.ProviderCrossing = ProviderCrossingCrossed
		result.ReconciliationRequired = true
		result.RetrySafetyFact = RetrySafetyUnsafeOrUnknown
		result.ErrorInfo = newLogicalError(LogicalErrorExecutionUnresolved, LogicalPhaseObservation, result.ProviderCrossing, result.RetrySafetyFact, result, true, nil)
		result.Diagnostic = "authoritative observation remains UNKNOWN and the exact Resource claim is held"
		result.ErrorInfo.Diagnostic = result.Diagnostic
		return LogicalInvocationObservation{LogicalExecutionResult: result, State: InvocationObservationUnresolvedUnknown}, result.ErrorInfo
	case allocation.Lifecycle == AllocationReleased && allocation.CurrentOccupancy == CurrentOccupancyEnded && !claimHeld:
		if observationErr == nil && (observation.Occupancy == OccupancyConclusionEnded || observation.Occupancy == OccupancyConclusionUnknown) {
			switch observation.Outcome {
			case ExecutionOutcomeSuccess:
				result.Status = LogicalExecutionCompleted
			case ExecutionOutcomeFailed:
				result.Status = LogicalExecutionFailed
			default:
				result.Status = LogicalExecutionUnknown
				result.ReconciliationRequired = true
				result.RetrySafetyFact = RetrySafetyUnsafeOrUnknown
				result.ErrorInfo = nil
				result.Diagnostic = "occupancy is ENDED, but the historical execution outcome remains UNKNOWN"
			}
			if observation.Occupancy == OccupancyConclusionEnded {
				result.ReconciliationRequired = false
			}
			return LogicalInvocationObservation{LogicalExecutionResult: result, State: InvocationObservationEndedReleased}, logicalResultError(result)
		}
		result.Status = LogicalExecutionUnknown
		result.ReconciliationRequired = true
		result.RetrySafetyFact = RetrySafetyUnsafeOrUnknown
		result.ErrorInfo = newLogicalError(LogicalErrorInvariantViolation, LogicalPhaseObservation, result.ProviderCrossing, result.RetrySafetyFact, result, true, ErrExecutionOwnershipInvariant)
		result.Diagnostic = "released allocation has no definitive immutable dispatch observation"
		return LogicalInvocationObservation{LogicalExecutionResult: result, State: InvocationObservationInvariantUnknown}, result.ErrorInfo
	default:
		result.Status = LogicalExecutionUnknown
		result.ReconciliationRequired = true
		result.RetrySafetyFact = RetrySafetyUnsafeOrUnknown
		result.ErrorInfo = newLogicalError(LogicalErrorInvariantViolation, LogicalPhaseObservation, result.ProviderCrossing, result.RetrySafetyFact, result, true, ErrExecutionOwnershipInvariant)
		result.Diagnostic = "allocation lifecycle, CurrentOccupancy, observation, and exact Resource claim disagree"
		return LogicalInvocationObservation{LogicalExecutionResult: result, State: InvocationObservationInvariantUnknown}, result.ErrorInfo
	}
}

func recordCrossing(record *logicalInvocationRecord, fallback LogicalExecutionResult) ProviderCrossing {
	if available, crossing := record.crossingFact(); available {
		return crossing
	}
	return storedCrossing(fallback)
}

func (facade *LogicalExecutionFacade) resolutionAlreadyResolved(record *logicalInvocationRecord, request LogicalOccupancyResolutionRequest) (LogicalResolutionResult, error) {
	_, _, allocationID, _, _, stored := record.snapshot()
	crossing := recordCrossing(record, stored)
	allocation, err := facade.kernel.GetExecutionAllocation(allocationID)
	if err != nil {
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorInvariantViolation, err)
	}
	claimHeld, err := facade.kernel.Resources.ExecutionOccupancyForAllocation(allocation.Descriptor.ResourceID, allocation.ID)
	if err != nil {
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorInvariantViolation, err)
	}
	if allocation.Lifecycle != AllocationReleased || allocation.CurrentOccupancy != CurrentOccupancyEnded || claimHeld {
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorInvariantViolation, ErrExecutionOwnershipInvariant)
	}
	observation, err := facade.kernel.GetExecutionObservation(allocation.ID)
	if err != nil {
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorInvariantViolation, err)
	}
	if observation.Occupancy != OccupancyConclusionUnknown {
		return logicalResolutionFailureForAllocation(request, allocationID, crossing, LogicalErrorInvariantViolation, ErrExecutionOwnershipInvariant)
	}
	return LogicalResolutionResult{
		InvocationID:              request.InvocationID,
		RequestID:                 request.RequestID,
		ExecutionAllocationID:     allocationID,
		ResourceID:                allocation.Descriptor.ResourceID,
		Status:                    ResolutionStatusAlreadyResolved,
		ProviderCrossingAvailable: crossing != "",
		ProviderCrossing:          crossing,
		CurrentOccupancy:          CurrentOccupancyEnded,
		Lifecycle:                 AllocationReleased,
		EventID:                   record.resolutionEvent(),
		ReconciliationRequired:    false,
		RetrySafetyFact:           RetrySafetyUnsafeOrUnknown,
		Diagnostic:                "equivalent occupancy resolution already accepted",
	}, nil
}
