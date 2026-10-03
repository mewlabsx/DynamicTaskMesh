package node

import (
	"errors"
	"testing"

	"dtm/internal/model"
)

func TestNewRejectsBlankID(t *testing.T) {
	for _, id := range []model.NodeID{"", " \t\n "} {
		if _, err := New(id, nil, StatusOnline); !errors.Is(err, ErrInvalidNode) {
			t.Fatalf("New(%q, nil, %q) error = %v, want %v", id, StatusOnline, err, ErrInvalidNode)
		}
	}
}

func TestNewRejectsInvalidStatus(t *testing.T) {
	for _, status := range []Status{"", "unknown"} {
		if _, err := New("node-1", nil, status); !errors.Is(err, ErrInvalidNode) {
			t.Fatalf("New() status %q error = %v, want %v", status, err, ErrInvalidNode)
		}
	}
}

func TestNewRejectsBlankCapability(t *testing.T) {
	for _, capability := range []model.Capability{"", " \t\n "} {
		if _, err := New("node-1", []model.Capability{capability}, StatusOnline); !errors.Is(err, ErrInvalidNode) {
			t.Fatalf("New() capability %q error = %v, want %v", capability, err, ErrInvalidNode)
		}
	}
}

func TestNewAllowsEmptyCapabilitySet(t *testing.T) {
	node, err := New("node-1", nil, StatusOffline)
	if err != nil {
		t.Fatalf("New() returned unexpected error: %v", err)
	}

	if got := node.Capabilities(); len(got) != 0 {
		t.Fatalf("Capabilities() = %v, want empty", got)
	}
}

func TestNewDeduplicatesCapabilitiesInFirstSeenOrder(t *testing.T) {
	node, err := New(
		"node-1",
		[]model.Capability{"temperature_sensor", "cooling_control", "temperature_sensor"},
		StatusOnline,
	)
	if err != nil {
		t.Fatalf("New() returned unexpected error: %v", err)
	}

	assertCapabilities(t, node.Capabilities(), []model.Capability{"temperature_sensor", "cooling_control"})
}

func TestNodeExposesIdentityStatusAndMembership(t *testing.T) {
	node, err := New("node-1", []model.Capability{"temperature_sensor"}, StatusOnline)
	if err != nil {
		t.Fatalf("New() returned unexpected error: %v", err)
	}

	if got := node.ID(); got != "node-1" {
		t.Fatalf("ID() = %q, want %q", got, "node-1")
	}
	if got := node.Status(); got != StatusOnline {
		t.Fatalf("Status() = %q, want %q", got, StatusOnline)
	}
	if !node.Has("temperature_sensor") {
		t.Fatal("Has(temperature_sensor) = false, want true")
	}
	if node.Has("cooling_control") {
		t.Fatal("Has(cooling_control) = true, want false")
	}
}

func TestNodeStatusLifecycleAllowsFailureAndRecovery(t *testing.T) {
	current, err := New("node-1", nil, StatusRegistered)
	if err != nil {
		t.Fatal(err)
	}
	for _, next := range []Status{
		StatusActive,
		StatusSuspect,
		StatusOffline,
		StatusRecovering,
		StatusActive,
	} {
		current, err = current.Transition(next)
		if err != nil {
			t.Fatalf("Transition(%q) error = %v", next, err)
		}
		if current.Status() != next {
			t.Fatalf("status = %q, want %q", current.Status(), next)
		}
	}
}

func TestNodeStatusLifecycleRejectsSkippedAndTerminalTransitions(t *testing.T) {
	registered, err := New("node-1", nil, StatusRegistered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := registered.Transition(StatusSuspect); !errors.Is(err, ErrInvalidStatusTransition) {
		t.Fatalf("registered -> suspect error = %v", err)
	}
	active, err := registered.Transition(StatusActive)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := active.Transition(StatusRecovering); !errors.Is(err, ErrInvalidStatusTransition) {
		t.Fatalf("active -> recovering error = %v", err)
	}
}

func TestNewAndCapabilitiesUseDefensiveCopies(t *testing.T) {
	supplied := []model.Capability{"temperature_sensor"}
	node, err := New("node-1", supplied, StatusOnline)
	if err != nil {
		t.Fatalf("New() returned unexpected error: %v", err)
	}

	supplied[0] = "cooling_control"
	returned := node.Capabilities()
	returned[0] = "cooling_control"

	assertCapabilities(t, node.Capabilities(), []model.Capability{"temperature_sensor"})
}

func assertCapabilities(t *testing.T, got, want []model.Capability) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("Capabilities() = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("Capabilities()[%d] = %q, want %q", index, got[index], want[index])
		}
	}
}
