package lifecycle

import (
	"errors"
	"strings"

	"dtm/internal/model"
)

type State string

const (
	StateCreated    State = "created"
	StatePlanning   State = "planning"
	StateMapped     State = "mapped"
	StateDispatched State = "dispatched"
	StateRunning    State = "running"
	StateRetrying   State = "retrying"
	StateRemapped   State = "remapped"
	StateSuccess    State = "success"
	StateFailed     State = "failed"
	StateCancelled  State = "cancelled"

	// StatePlanned and StateSucceeded keep source compatibility with V0.2.
	// Persisted V0.2 values are migrated to their V0.2.5 canonical forms.
	StatePlanned   = StatePlanning
	StateSucceeded = StateSuccess
)

var ErrInvalidTransition = errors.New("invalid lifecycle transition")

type Lifecycle struct {
	taskID model.TaskID
	state  State
}

func New(taskID model.TaskID) (*Lifecycle, error) {
	return Restore(taskID, StateCreated)
}

func Restore(taskID model.TaskID, state State) (*Lifecycle, error) {
	if strings.TrimSpace(string(taskID)) == "" {
		return nil, ErrInvalidTransition
	}
	if !isKnownState(state) {
		return nil, ErrInvalidTransition
	}

	return &Lifecycle{
		taskID: taskID,
		state:  state,
	}, nil
}

func (l *Lifecycle) State() State {
	return l.state
}

func (l *Lifecycle) Transition(next State) error {
	if !isKnownState(next) {
		return ErrInvalidTransition
	}
	if next == l.state {
		return nil
	}
	if !canTransition(l.state, next) {
		return ErrInvalidTransition
	}

	l.state = next
	return nil
}

func isKnownState(state State) bool {
	switch state {
	case StateCreated,
		StatePlanning,
		StateMapped,
		StateDispatched,
		StateRunning,
		StateRetrying,
		StateRemapped,
		StateSuccess,
		StateFailed,
		StateCancelled:
		return true
	default:
		return false
	}
}

func canTransition(current, next State) bool {
	switch current {
	case StateCreated:
		return next == StatePlanning || next == StateFailed || next == StateCancelled
	case StatePlanning:
		return next == StateMapped || next == StateFailed || next == StateCancelled
	case StateMapped:
		return next == StateDispatched || next == StateFailed || next == StateCancelled
	case StateDispatched:
		return next == StateRunning ||
			next == StateRetrying ||
			next == StateRemapped ||
			next == StateFailed ||
			next == StateCancelled
	case StateRunning:
		return next == StateSuccess ||
			next == StateRetrying ||
			next == StateRemapped ||
			next == StateFailed ||
			next == StateCancelled
	case StateRetrying:
		return next == StateDispatched ||
			next == StateRemapped ||
			next == StateFailed ||
			next == StateCancelled
	case StateRemapped:
		return next == StateDispatched ||
			next == StateRunning ||
			next == StateRetrying ||
			next == StateFailed ||
			next == StateCancelled
	case StateFailed:
		return next == StateRetrying
	default:
		return false
	}
}
