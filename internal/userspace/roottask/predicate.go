package roottask

import "dtm/internal/userspace/taskruntime"

// AllRequiredChildrenSucceeded is the concrete first-slice policy used by
// integration tests and simple callers. It is deliberately exposed as a
// separable predicate so the Root Runtime mechanism does not make this rule a
// universal interpretation of every goal.
//
// This policy freezes the precedence FAILED-class > UNKNOWN > SUCCEEDED. A
// required FAILED, REJECTED, or CANCELLED_BEFORE_EXECUTION Child is a
// definitive non-success and dominates any UNKNOWN evidence. UNKNOWN remains
// unresolved and dominates an otherwise successful set. The input is
// expected to contain one terminal projection per registered member; an
// incomplete input is unresolved.
func AllRequiredChildrenSucceeded(root RootTaskSnapshot, evidence []ChildEvidence) (ClosureDecision, error) {
	if len(evidence) != len(root.ChildTaskIDs) {
		return ClosureDecisionUnknown, ErrRootClosureNotReady
	}
	sawUnknown := false
	for _, child := range evidence {
		switch child.State {
		case taskruntime.ChildTaskStateSucceeded:
			continue
		case taskruntime.ChildTaskStateFailed,
			taskruntime.ChildTaskStateRejected,
			taskruntime.ChildTaskStateCancelledBeforeExecution:
			return ClosureDecisionFailed, nil
		case taskruntime.ChildTaskStateUnknown:
			sawUnknown = true
		default:
			return ClosureDecisionUnknown, ErrRootClosureNotReady
		}
	}
	if sawUnknown {
		return ClosureDecisionUnknown, nil
	}
	return ClosureDecisionSucceeded, nil
}

// AllRequiredChildrenSucceededPredicate returns the first-slice policy as a
// ClosurePredicate value.
func AllRequiredChildrenSucceededPredicate() ClosurePredicate {
	return ClosurePredicateFunc(AllRequiredChildrenSucceeded)
}
