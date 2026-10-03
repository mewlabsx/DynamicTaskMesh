package lifecycle

import (
	"errors"
	"testing"

	"dtm/internal/model"
)

func TestNewRejectsBlankTaskID(t *testing.T) {
	for _, taskID := range []model.TaskID{"", " \t\n "} {
		if _, err := New(taskID); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("New(%q) error = %v, want %v", taskID, err, ErrInvalidTransition)
		}
	}
}

func TestNewStartsCreated(t *testing.T) {
	lifecycle, err := New("task-1")
	if err != nil {
		t.Fatalf("New() returned unexpected error: %v", err)
	}

	if got := lifecycle.State(); got != StateCreated {
		t.Fatalf("State() = %q, want %q", got, StateCreated)
	}
}

func TestTransitionAllowsLifecycleSequence(t *testing.T) {
	lifecycle, err := New("task-1")
	if err != nil {
		t.Fatalf("New() returned unexpected error: %v", err)
	}

	for _, next := range []State{
		StatePlanning,
		StateMapped,
		StateDispatched,
		StateRunning,
		StateSuccess,
	} {
		if err := lifecycle.Transition(next); err != nil {
			t.Fatalf("Transition(%q) returned unexpected error: %v", next, err)
		}
		if got := lifecycle.State(); got != next {
			t.Fatalf("State() = %q after transition, want %q", got, next)
		}
	}
}

func TestTransitionAllowsFailureFromEveryNonterminalState(t *testing.T) {
	tests := []struct {
		name    string
		advance []State
	}{
		{name: "created"},
		{name: "planning", advance: []State{StatePlanning}},
		{name: "mapped", advance: []State{StatePlanning, StateMapped}},
		{name: "dispatched", advance: []State{StatePlanning, StateMapped, StateDispatched}},
		{name: "running", advance: []State{StatePlanning, StateMapped, StateDispatched, StateRunning}},
		{name: "retrying", advance: []State{StatePlanning, StateMapped, StateDispatched, StateRetrying}},
		{name: "remapped", advance: []State{StatePlanning, StateMapped, StateDispatched, StateRemapped}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lifecycle, err := New("task-1")
			if err != nil {
				t.Fatalf("New() returned unexpected error: %v", err)
			}
			for _, next := range tt.advance {
				if err := lifecycle.Transition(next); err != nil {
					t.Fatalf("setup Transition(%q) returned unexpected error: %v", next, err)
				}
			}

			if err := lifecycle.Transition(StateFailed); err != nil {
				t.Fatalf("Transition(%q) returned unexpected error: %v", StateFailed, err)
			}
			if got := lifecycle.State(); got != StateFailed {
				t.Fatalf("State() = %q, want %q", got, StateFailed)
			}
		})
	}
}

func TestTransitionToSameStateIsIdempotent(t *testing.T) {
	lifecycle, err := New("task-1")
	if err != nil {
		t.Fatalf("New() returned unexpected error: %v", err)
	}

	if err := lifecycle.Transition(StateCreated); err != nil {
		t.Fatalf("Transition(%q) returned unexpected error: %v", StateCreated, err)
	}
	if got := lifecycle.State(); got != StateCreated {
		t.Fatalf("State() = %q, want %q", got, StateCreated)
	}
}

func TestTransitionRejectsSkippedAndUnknownStates(t *testing.T) {
	for _, next := range []State{
		StateMapped,
		StateDispatched,
		StateRunning,
		StateRetrying,
		StateRemapped,
		StateSuccess,
		State("unknown"),
		State(""),
	} {
		lifecycle, err := New("task-1")
		if err != nil {
			t.Fatalf("New() returned unexpected error: %v", err)
		}

		if err := lifecycle.Transition(next); !errors.Is(err, ErrInvalidTransition) {
			t.Fatalf("Transition(%q) error = %v, want %v", next, err, ErrInvalidTransition)
		}
		if got := lifecycle.State(); got != StateCreated {
			t.Fatalf("State() = %q after rejected transition, want %q", got, StateCreated)
		}
	}
}

func TestSuccessAndCancelledStatesCannotBeLeft(t *testing.T) {
	tests := []struct {
		name     string
		terminal State
		advance  []State
	}{
		{
			name:     "success",
			terminal: StateSuccess,
			advance:  []State{StatePlanning, StateMapped, StateDispatched, StateRunning, StateSuccess},
		},
		{
			name:     "cancelled",
			terminal: StateCancelled,
			advance:  []State{StateCancelled},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lifecycle, err := New("task-1")
			if err != nil {
				t.Fatalf("New() returned unexpected error: %v", err)
			}
			for _, next := range tt.advance {
				if err := lifecycle.Transition(next); err != nil {
					t.Fatalf("setup Transition(%q) returned unexpected error: %v", next, err)
				}
			}

			for _, next := range []State{
				StateCreated,
				StatePlanning,
				StateMapped,
				StateDispatched,
				StateRunning,
				StateRetrying,
				StateRemapped,
				StateSuccess,
				StateFailed,
				StateCancelled,
				State("unknown"),
			} {
				if next == tt.terminal {
					continue
				}
				if err := lifecycle.Transition(next); !errors.Is(err, ErrInvalidTransition) {
					t.Fatalf("Transition(%q) from %q error = %v, want %v", next, tt.terminal, err, ErrInvalidTransition)
				}
				if got := lifecycle.State(); got != tt.terminal {
					t.Fatalf("State() = %q after rejected transition, want %q", got, tt.terminal)
				}
			}
		})
	}
}

func TestFailureCanEnterRetryAndRemapFlow(t *testing.T) {
	lifecycle, err := New("task-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []State{
		StatePlanning,
		StateMapped,
		StateDispatched,
		StateRunning,
		StateFailed,
		StateRetrying,
		StateRemapped,
		StateRunning,
		StateSuccess,
	} {
		if err := lifecycle.Transition(next); err != nil {
			t.Fatalf("Transition(%q) error = %v", next, err)
		}
	}
}

func TestRestorePreservesPersistedState(t *testing.T) {
	lifecycle, err := Restore("task-1", StateRunning)
	if err != nil {
		t.Fatal(err)
	}
	if lifecycle.State() != StateRunning {
		t.Fatalf("State() = %q, want %q", lifecycle.State(), StateRunning)
	}
	if _, err := Restore("task-1", State("unknown")); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("Restore(unknown) error = %v", err)
	}
}
