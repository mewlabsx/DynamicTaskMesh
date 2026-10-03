package roottask

import (
	"errors"
	"fmt"
	"testing"

	"dtm/internal/userspace/taskruntime"
)

func TestAllRequiredChildrenSucceededFrozenPrecedence(t *testing.T) {
	tests := []struct {
		name   string
		states []taskruntime.ChildTaskState
		want   ClosureDecision
	}{
		{name: "unknown then failed", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateUnknown, taskruntime.ChildTaskStateFailed}, want: ClosureDecisionFailed},
		{name: "failed then unknown", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateFailed, taskruntime.ChildTaskStateUnknown}, want: ClosureDecisionFailed},
		{name: "unknown then rejected", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateUnknown, taskruntime.ChildTaskStateRejected}, want: ClosureDecisionFailed},
		{name: "rejected then unknown", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateRejected, taskruntime.ChildTaskStateUnknown}, want: ClosureDecisionFailed},
		{name: "unknown then cancelled", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateUnknown, taskruntime.ChildTaskStateCancelledBeforeExecution}, want: ClosureDecisionFailed},
		{name: "cancelled then unknown", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateCancelledBeforeExecution, taskruntime.ChildTaskStateUnknown}, want: ClosureDecisionFailed},
		{name: "succeeded then unknown", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateSucceeded, taskruntime.ChildTaskStateUnknown}, want: ClosureDecisionUnknown},
		{name: "unknown then succeeded", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateUnknown, taskruntime.ChildTaskStateSucceeded}, want: ClosureDecisionUnknown},
		{name: "all succeeded", states: []taskruntime.ChildTaskState{taskruntime.ChildTaskStateSucceeded, taskruntime.ChildTaskStateSucceeded}, want: ClosureDecisionSucceeded},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := evaluatePolicyStates(test.states)
			if err != nil || got != test.want {
				t.Fatalf("AllRequiredChildrenSucceeded(%v) = %s, error = %v, want %s", test.states, got, err, test.want)
			}
		})
	}
}

func TestAllRequiredChildrenSucceededIsOrderIndependentForSmallStateSpace(t *testing.T) {
	states := []taskruntime.ChildTaskState{
		taskruntime.ChildTaskStateSucceeded,
		taskruntime.ChildTaskStateFailed,
		taskruntime.ChildTaskStateRejected,
		taskruntime.ChildTaskStateUnknown,
		taskruntime.ChildTaskStateCancelledBeforeExecution,
	}
	for length := 1; length <= 3; length++ {
		sequence := make([]taskruntime.ChildTaskState, length)
		enumeratePolicySequences(t, states, sequence, 0)
	}
}

func enumeratePolicySequences(t *testing.T, states []taskruntime.ChildTaskState, sequence []taskruntime.ChildTaskState, index int) {
	t.Helper()
	if index == len(sequence) {
		want := referencePolicyDecision(sequence)
		checkPolicyPermutations(t, sequence, append([]taskruntime.ChildTaskState(nil), sequence...), 0, want)
		return
	}
	for _, state := range states {
		sequence[index] = state
		enumeratePolicySequences(t, states, sequence, index+1)
	}
}

func checkPolicyPermutations(t *testing.T, original, permutation []taskruntime.ChildTaskState, index int, want ClosureDecision) {
	t.Helper()
	if index == len(permutation) {
		got, err := evaluatePolicyStates(permutation)
		if err != nil || got != want {
			t.Fatalf("policy sequence %v permutation %v = %s, error = %v, want %s", original, permutation, got, err, want)
		}
		return
	}
	for position := index; position < len(permutation); position++ {
		permutation[index], permutation[position] = permutation[position], permutation[index]
		checkPolicyPermutations(t, original, permutation, index+1, want)
		permutation[index], permutation[position] = permutation[position], permutation[index]
	}
}

func TestAllRequiredChildrenSucceededPreservesEmptyAndNonTerminalBehavior(t *testing.T) {
	got, err := AllRequiredChildrenSucceeded(RootTaskSnapshot{}, nil)
	if err != nil || got != ClosureDecisionSucceeded {
		t.Fatalf("empty evidence = %s, error = %v, want SUCCEEDED", got, err)
	}
	root := RootTaskSnapshot{ChildTaskIDs: []taskruntime.ChildTaskID{"child-1"}}
	got, err = AllRequiredChildrenSucceeded(root, []ChildEvidence{{ChildTaskID: "child-1", State: taskruntime.ChildTaskStateExecuting}})
	if !errors.Is(err, ErrRootClosureNotReady) || got != ClosureDecisionUnknown {
		t.Fatalf("non-terminal evidence = %s, error = %v, want UNKNOWN/not-ready", got, err)
	}
}

func evaluatePolicyStates(states []taskruntime.ChildTaskState) (ClosureDecision, error) {
	root := RootTaskSnapshot{ChildTaskIDs: make([]taskruntime.ChildTaskID, len(states))}
	evidence := make([]ChildEvidence, len(states))
	for index, state := range states {
		childID := taskruntime.ChildTaskID(fmt.Sprintf("child-%d", index))
		root.ChildTaskIDs[index] = childID
		evidence[index] = ChildEvidence{ChildTaskID: childID, State: state}
	}
	return AllRequiredChildrenSucceeded(root, evidence)
}

func referencePolicyDecision(states []taskruntime.ChildTaskState) ClosureDecision {
	sawUnknown := false
	for _, state := range states {
		switch state {
		case taskruntime.ChildTaskStateFailed, taskruntime.ChildTaskStateRejected, taskruntime.ChildTaskStateCancelledBeforeExecution:
			return ClosureDecisionFailed
		case taskruntime.ChildTaskStateUnknown:
			sawUnknown = true
		}
	}
	if sawUnknown {
		return ClosureDecisionUnknown
	}
	return ClosureDecisionSucceeded
}
