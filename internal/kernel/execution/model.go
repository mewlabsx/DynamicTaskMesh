package execution

import (
	"errors"
	"fmt"
	"strings"

	"dtm/internal/kernel/identity"
)

type ExecutionContextID = identity.ExecutionContextID

type LifecycleState = identity.LifecycleState
type State = LifecycleState
type AvailabilityState = identity.AvailabilityState
type EvaluationState = identity.EvaluationState

const (
	StateCreated    LifecycleState = identity.LifecycleCreated
	StateActive     LifecycleState = identity.LifecycleActive
	StateSuspended  LifecycleState = identity.LifecycleSuspended
	StateTerminated LifecycleState = identity.LifecycleTerminated

	AvailabilityAvailable   AvailabilityState = identity.AvailabilityAvailable
	AvailabilityDegraded    AvailabilityState = identity.AvailabilityDegraded
	AvailabilityUnavailable AvailabilityState = identity.AvailabilityUnavailable

	ExecutionStateCreated    = StateCreated
	ExecutionStateActive     = StateActive
	ExecutionStateSuspended  = StateSuspended
	ExecutionStateTerminated = StateTerminated
)

var (
	ErrInvalidContext    = errors.New("invalid execution context")
	ErrInvalidState      = errors.New("invalid execution context state")
	ErrInvalidTransition = errors.New("invalid execution context state transition")
	ErrContextNotFound   = errors.New("execution context not found")
	ErrContextTerminated = errors.New("execution context is terminated")
)

type ExecutionContext struct {
	ID           ExecutionContextID
	Subject      string
	Scope        string
	State        LifecycleState
	Availability AvailabilityState
}

func NewContext(id ExecutionContextID, subject, scope string, state LifecycleState) (ExecutionContext, error) {
	context := ExecutionContext{ID: id, Subject: subject, Scope: scope, State: state, Availability: availabilityForState(state)}
	if err := context.Validate(); err != nil {
		return ExecutionContext{}, err
	}
	return context, nil
}

func (context ExecutionContext) Validate() error {
	if err := context.ID.Validate(); err != nil {
		return fmt.Errorf("%w: identity: %v", ErrInvalidContext, err)
	}
	if strings.TrimSpace(context.Subject) == "" || strings.ContainsRune(context.Subject, '\x00') {
		return fmt.Errorf("%w: subject is required", ErrInvalidContext)
	}
	if strings.TrimSpace(context.Scope) == "" || strings.ContainsRune(context.Scope, '\x00') {
		return fmt.Errorf("%w: scope is required", ErrInvalidContext)
	}
	if !isValidState(context.State) {
		return fmt.Errorf("%w: %q", ErrInvalidState, context.State)
	}
	if err := context.Availability.Validate(); err != nil {
		return fmt.Errorf("%w: availability: %v", ErrInvalidContext, err)
	}
	return nil
}

func (context ExecutionContext) Clone() ExecutionContext {
	return context
}

func (context ExecutionContext) LifecycleState() LifecycleState {
	return context.State
}

func (context ExecutionContext) IsUsable() bool {
	return (context.State == StateCreated || context.State == StateActive) && context.Availability == AvailabilityAvailable
}

func (context ExecutionContext) Transition(next LifecycleState) (ExecutionContext, error) {
	if !isValidState(next) {
		return ExecutionContext{}, fmt.Errorf("%w: %q", ErrInvalidState, next)
	}
	if next == context.State {
		return context, nil
	}
	if !canTransition(context.State, next) {
		return ExecutionContext{}, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, context.State, next)
	}
	context.State = next
	context.Availability = availabilityForState(next)
	return context, nil
}

func isValidState(state LifecycleState) bool {
	switch state {
	case StateCreated, StateActive, StateSuspended, StateTerminated:
		return true
	default:
		return false
	}
}

func canTransition(current, next LifecycleState) bool {
	switch current {
	case StateCreated:
		return next == StateActive || next == StateSuspended || next == StateTerminated
	case StateActive:
		return next == StateSuspended || next == StateTerminated
	case StateSuspended:
		return next == StateActive || next == StateTerminated
	case StateTerminated:
		return false
	default:
		return false
	}
}

func availabilityForState(state LifecycleState) AvailabilityState {
	switch state {
	case StateCreated, StateActive:
		return AvailabilityAvailable
	default:
		return AvailabilityUnavailable
	}
}
