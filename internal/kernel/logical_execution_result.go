package kernel

import "errors"

// Private logical result projections consume supplied facts without owning state.

func logicalAllocationFailure(request LogicalExecutionRequest, allocation TryAllocateResult, cause error) (LogicalExecutionResult, error) {
	category := LogicalErrorInternalFailure
	phase := LogicalPhaseAllocation
	switch allocation.Reason {
	case AllocationReasonInvalidRequest:
		category = LogicalErrorInvalidRequest
		phase = LogicalPhaseRequestValidation
	case AllocationReasonAuthorityDenied, AllocationReasonResourceNotFound:
		category = LogicalErrorInvalidOrStaleAuthority
		phase = LogicalPhaseAuthorityValidation
	case AllocationReasonResourceUnavailable, AllocationReasonResourceBusy:
		category = LogicalErrorCapacityUnavailable
	case AllocationReasonExecutionConstraintViolation:
		category = LogicalErrorInvalidRequest
	default:
		if errors.Is(cause, ErrAuthorityDenied) || errors.Is(cause, ErrResourceNotFound) {
			category = LogicalErrorInvalidOrStaleAuthority
		} else if errors.Is(cause, ErrResourceBusy) || errors.Is(cause, ErrResourceUnavailable) {
			category = LogicalErrorCapacityUnavailable
		} else if errors.Is(cause, ErrInvalidExecutionRequest) || errors.Is(cause, ErrExecutionConstraintViolation) {
			category = LogicalErrorInvalidRequest
		}
	}
	// TryAllocateExecution never enters the Provider boundary. Expose that
	// definitive fact for authority and capacity rejections while retaining
	// unavailable applicability for generic validation/internal failures.
	crossing := ProviderCrossing("")
	if category == LogicalErrorInvalidOrStaleAuthority || category == LogicalErrorCapacityUnavailable {
		crossing = ProviderCrossingNotCrossedDefinite
	}
	return logicalExecutionFailure(request.InvocationID, request.RequestID, category, phase, crossing, RetrySafetyNotDeclared, false, cause)
}

func logicalDispatchResult(
	request LogicalExecutionRequest,
	allocation ExecutionAllocation,
	observation ExecutionObservation,
	claimHeld bool,
	providerCrossed bool,
	dispatchErr error,
) LogicalExecutionResult {
	result := LogicalExecutionResult{
		InvocationID:              request.InvocationID,
		RequestID:                 request.RequestID,
		ExecutionAllocationID:     allocation.ID,
		ProviderCrossingAvailable: true,
		ProviderCrossing:          crossingFor(providerCrossed),
		Observation:               observation.Clone(),
		ObservationAvailable:      true,
		CurrentOccupancy:          allocation.CurrentOccupancy,
		Lifecycle:                 allocation.Lifecycle,
		RetrySafetyFact:           RetrySafetyNotDeclared,
	}

	if allocation.Lifecycle == AllocationRunning && allocation.CurrentOccupancy == CurrentOccupancyUnknown &&
		claimHeld && observation.Occupancy == OccupancyConclusionUnknown && providerCrossed {
		result.Status = LogicalExecutionUnknown
		result.ReconciliationRequired = true
		result.RetrySafetyFact = RetrySafetyUnsafeOrUnknown
		result.ErrorInfo = newLogicalError(LogicalErrorExecutionUnresolved, LogicalPhasePostProvider, result.ProviderCrossing, result.RetrySafetyFact, result, true, dispatchErr)
		result.Diagnostic = observation.Reason
		if result.ErrorInfo.Diagnostic == "" {
			result.ErrorInfo.Diagnostic = result.Diagnostic
		}
		return result
	}

	if allocation.Lifecycle == AllocationReleased && allocation.CurrentOccupancy == CurrentOccupancyEnded && !claimHeld &&
		observation.Occupancy == OccupancyConclusionEnded {
		switch observation.Outcome {
		case ExecutionOutcomeSuccess:
			result.Status = LogicalExecutionCompleted
		case ExecutionOutcomeFailed:
			result.Status = LogicalExecutionFailed
		default:
			result.Status = LogicalExecutionUnknown
			result.ReconciliationRequired = true
			result.RetrySafetyFact = RetrySafetyUnsafeOrUnknown
		}
		result.Diagnostic = observation.Reason
		if dispatchErr != nil {
			if !providerCrossed {
				result.ErrorInfo = newLogicalError(classifyPreProviderError(dispatchErr), LogicalPhasePreProvider, result.ProviderCrossing, result.RetrySafetyFact, result, false, dispatchErr)
			} else if result.Status == LogicalExecutionFailed {
				// A provider-side diagnostic may accompany a definitive FAILED /
				// ENDED observation. The authoritative logical result is already
				// trustworthy, so preserve the diagnostic without returning a
				// Kernel INTERNAL_FAILURE classification.
				if result.Diagnostic == "" {
					result.Diagnostic = dispatchErr.Error()
				}
			} else if result.Status == LogicalExecutionUnknown {
				result.ErrorInfo = newLogicalError(LogicalErrorInternalFailure, LogicalPhasePostProvider, result.ProviderCrossing, result.RetrySafetyFact, result, true, dispatchErr)
			} else {
				result.ErrorInfo = newLogicalError(LogicalErrorInternalFailure, LogicalPhasePostProvider, result.ProviderCrossing, result.RetrySafetyFact, result, false, dispatchErr)
			}
		}
		return result
	}

	result.Status = LogicalExecutionUnknown
	result.ReconciliationRequired = true
	result.RetrySafetyFact = RetrySafetyUnsafeOrUnknown
	result.ErrorInfo = newLogicalError(LogicalErrorInvariantViolation, LogicalPhaseObservation, result.ProviderCrossing, result.RetrySafetyFact, result, true, ErrExecutionOwnershipInvariant)
	if dispatchErr != nil {
		result.Diagnostic = dispatchErr.Error()
	} else {
		result.Diagnostic = "authoritative allocation, observation, and Resource claim facts are inconsistent"
	}
	return result
}

func classifyPreProviderError(err error) LogicalErrorCategory {
	switch {
	case errors.Is(err, ErrProviderUnavailable):
		return LogicalErrorProviderNotAvailable
	case errors.Is(err, ErrAuthorityDenied), errors.Is(err, ErrBindingMismatch), errors.Is(err, ErrResourceNotFound), errors.Is(err, ErrResourceUnavailable):
		return LogicalErrorInvalidOrStaleAuthority
	case errors.Is(err, ErrInvalidExecutionAllocation), errors.Is(err, ErrOccupancyMismatch), errors.Is(err, ErrExecutionOwnershipInvariant):
		return LogicalErrorInvariantViolation
	default:
		return LogicalErrorInternalFailure
	}
}

func crossingFor(crossed bool) ProviderCrossing {
	if crossed {
		return ProviderCrossingCrossed
	}
	return ProviderCrossingNotCrossedDefinite
}

func storedCrossing(result LogicalExecutionResult) ProviderCrossing {
	if !result.ProviderCrossingAvailable {
		return ""
	}
	return result.ProviderCrossing
}

func logicalExecutionFailure(invocationID InvocationID, requestID string, category LogicalErrorCategory, phase LogicalErrorPhase, crossing ProviderCrossing, retry RetrySafetyFact, reconciliation bool, cause error) (LogicalExecutionResult, error) {
	result := LogicalExecutionResult{
		InvocationID:              invocationID,
		RequestID:                 requestID,
		Status:                    LogicalExecutionRejected,
		ProviderCrossingAvailable: crossing != "",
		ProviderCrossing:          crossing,
		ReconciliationRequired:    reconciliation,
		RetrySafetyFact:           retry,
	}
	result.ErrorInfo = newLogicalError(category, phase, crossing, retry, result, reconciliation, cause)
	result.Diagnostic = result.ErrorInfo.Diagnostic
	return result, result.ErrorInfo
}

func logicalResultError(result LogicalExecutionResult) error {
	if result.ErrorInfo == nil {
		return nil
	}
	return result.ErrorInfo
}

func newLogicalError(category LogicalErrorCategory, phase LogicalErrorPhase, crossing ProviderCrossing, retry RetrySafetyFact, result LogicalExecutionResult, reconciliation bool, cause error) *LogicalErrorInfo {
	diagnostic := ""
	if cause != nil {
		diagnostic = cause.Error()
	}
	return &LogicalErrorInfo{
		Category:                  category,
		Phase:                     phase,
		ProviderCrossingAvailable: crossing != "",
		ProviderCrossing:          crossing,
		RetrySafetyFact:           retry,
		InvocationID:              result.InvocationID,
		RequestID:                 result.RequestID,
		ExecutionAllocationID:     result.ExecutionAllocationID,
		ReconciliationRequired:    reconciliation,
		Diagnostic:                diagnostic,
		cause:                     cause,
	}
}

func logicalResolutionFailure(request LogicalOccupancyResolutionRequest, category LogicalErrorCategory, phase LogicalErrorPhase, crossing ProviderCrossing, retry RetrySafetyFact, reconciliation bool, cause error) (LogicalResolutionResult, error) {
	base := LogicalExecutionResult{InvocationID: request.InvocationID, RequestID: request.RequestID}
	info := newLogicalError(category, phase, crossing, retry, base, reconciliation, cause)
	result := LogicalResolutionResult{
		InvocationID:              request.InvocationID,
		RequestID:                 request.RequestID,
		Status:                    ResolutionStatusRejected,
		ProviderCrossingAvailable: crossing != "",
		ProviderCrossing:          crossing,
		RetrySafetyFact:           retry,
		ReconciliationRequired:    reconciliation,
		ErrorInfo:                 info,
		Diagnostic:                info.Diagnostic,
	}
	return result, info
}

func logicalResolutionFailureForAllocation(request LogicalOccupancyResolutionRequest, allocationID ExecutionAllocationID, crossing ProviderCrossing, category LogicalErrorCategory, cause error) (LogicalResolutionResult, error) {
	phase := LogicalPhaseResolution
	retry := RetrySafetyUnsafeOrUnknown
	reconciliation := true
	if category == LogicalErrorInvalidRequest {
		phase = LogicalPhaseRequestValidation
		retry = RetrySafetyNotDeclared
		reconciliation = false
	}
	base := LogicalExecutionResult{InvocationID: request.InvocationID, RequestID: request.RequestID, ExecutionAllocationID: allocationID}
	info := newLogicalError(category, phase, crossing, retry, base, reconciliation, cause)
	result := LogicalResolutionResult{
		InvocationID:              request.InvocationID,
		RequestID:                 request.RequestID,
		ExecutionAllocationID:     allocationID,
		Status:                    ResolutionStatusRejected,
		ProviderCrossingAvailable: crossing != "",
		ProviderCrossing:          crossing,
		RetrySafetyFact:           retry,
		ReconciliationRequired:    reconciliation,
		ErrorInfo:                 info,
		Diagnostic:                info.Diagnostic,
	}
	return result, info
}
