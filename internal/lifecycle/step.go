package lifecycle

import (
	"errors"
	"strings"

	"dtm/internal/model"
)

type StepState string

const (
	StepStateCreated    StepState = "created"
	StepStateMapped     StepState = "mapped"
	StepStateDispatched StepState = "dispatched"
	StepStateRunning    StepState = "running"
	StepStateRetrying   StepState = "retrying"
	StepStateRemapped   StepState = "remapped"
	StepStateSuccess    StepState = "success"
	StepStateFailed     StepState = "failed"
	StepStateCancelled  StepState = "cancelled"
)

var ErrInvalidStepTransition = errors.New("invalid step lifecycle transition")

type StepLifecycle struct {
	stepID model.StepID
	state  StepState
}

func NewStep(stepID model.StepID) (*StepLifecycle, error) {
	return RestoreStep(stepID, StepStateCreated)
}

func RestoreStep(stepID model.StepID, state StepState) (*StepLifecycle, error) {
	if strings.TrimSpace(string(stepID)) == "" || !isKnownStepState(state) {
		return nil, ErrInvalidStepTransition
	}
	return &StepLifecycle{stepID: stepID, state: state}, nil
}

func (lifecycle *StepLifecycle) State() StepState {
	return lifecycle.state
}

func (lifecycle *StepLifecycle) Transition(next StepState) error {
	if !isKnownStepState(next) {
		return ErrInvalidStepTransition
	}
	if next == lifecycle.state {
		return nil
	}
	if !canTransitionStep(lifecycle.state, next) {
		return ErrInvalidStepTransition
	}
	lifecycle.state = next
	return nil
}

func isKnownStepState(state StepState) bool {
	switch state {
	case StepStateCreated,
		StepStateMapped,
		StepStateDispatched,
		StepStateRunning,
		StepStateRetrying,
		StepStateRemapped,
		StepStateSuccess,
		StepStateFailed,
		StepStateCancelled:
		return true
	default:
		return false
	}
}

func canTransitionStep(current, next StepState) bool {
	switch current {
	case StepStateCreated:
		return next == StepStateMapped ||
			next == StepStateFailed ||
			next == StepStateCancelled
	case StepStateMapped:
		return next == StepStateDispatched ||
			next == StepStateFailed ||
			next == StepStateCancelled
	case StepStateDispatched:
		return next == StepStateRunning ||
			next == StepStateRetrying ||
			next == StepStateRemapped ||
			next == StepStateFailed ||
			next == StepStateCancelled
	case StepStateRunning:
		return next == StepStateSuccess ||
			next == StepStateRetrying ||
			next == StepStateRemapped ||
			next == StepStateFailed ||
			next == StepStateCancelled
	case StepStateRetrying:
		return next == StepStateDispatched ||
			next == StepStateRemapped ||
			next == StepStateFailed ||
			next == StepStateCancelled
	case StepStateRemapped:
		return next == StepStateDispatched ||
			next == StepStateRunning ||
			next == StepStateRetrying ||
			next == StepStateFailed ||
			next == StepStateCancelled
	case StepStateFailed:
		return next == StepStateRetrying
	default:
		return false
	}
}
