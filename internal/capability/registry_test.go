package capability

import (
	"errors"
	"sync"
	"testing"

	"dtm/internal/model"
	"dtm/internal/node"
)

func TestRegisterMakesOnlineNodeDiscoverable(t *testing.T) {
	registry := NewRegistry()
	registry.Register(mustNode(t, "node-1", []model.Capability{"temperature_sensor"}, node.StatusOnline))

	got := registry.Discover("temperature_sensor")

	assertNodeIDs(t, got, []model.NodeID{"node-1"})
}

func TestRegisterReplacesNodeWithSameID(t *testing.T) {
	registry := NewRegistry()
	registry.Register(mustNode(t, "node-1", []model.Capability{"temperature_sensor"}, node.StatusOnline))
	registry.Register(mustNode(t, "node-1", []model.Capability{"cooling_control"}, node.StatusOnline))

	if got := registry.Discover("temperature_sensor"); len(got) != 0 {
		t.Fatalf("Discover(temperature_sensor) = %v, want empty", got)
	}
	assertNodeIDs(t, registry.Discover("cooling_control"), []model.NodeID{"node-1"})
}

func TestSetStatusRejectsUnknownNode(t *testing.T) {
	registry := NewRegistry()

	if err := registry.SetStatus("missing", node.StatusOffline); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("SetStatus() error = %v, want %v", err, ErrNodeNotFound)
	}
}

func TestDiscoverExcludesOfflineNodes(t *testing.T) {
	registry := NewRegistry()
	registry.Register(mustNode(t, "node-1", []model.Capability{"temperature_sensor"}, node.StatusOnline))

	if err := registry.SetStatus("node-1", node.StatusOffline); err != nil {
		t.Fatalf("SetStatus() returned unexpected error: %v", err)
	}

	if got := registry.Discover("temperature_sensor"); len(got) != 0 {
		t.Fatalf("Discover() = %v, want empty", got)
	}
}

func TestDiscoverOnlyIncludesActiveNodes(t *testing.T) {
	registry := NewRegistry()
	for _, status := range []node.Status{
		node.StatusRegistered,
		node.StatusSuspect,
		node.StatusOffline,
		node.StatusRecovering,
	} {
		registry.Register(mustNode(t, model.NodeID(status), []model.Capability{"temperature_sensor"}, status))
	}
	registry.Register(mustNode(t, "active", []model.Capability{"temperature_sensor"}, node.StatusActive))

	assertNodeIDs(t, registry.Discover("temperature_sensor"), []model.NodeID{"active"})
}

func TestDiscoverSortsMatchingNodesByID(t *testing.T) {
	registry := NewRegistry()
	for _, id := range []model.NodeID{"node-z", "node-a", "node-m"} {
		registry.Register(mustNode(t, id, []model.Capability{"temperature_sensor"}, node.StatusOnline))
	}

	assertNodeIDs(
		t,
		registry.Discover("temperature_sensor"),
		[]model.NodeID{"node-a", "node-m", "node-z"},
	)
}

func TestDiscoverResultsCannotMutateRegistry(t *testing.T) {
	registry := NewRegistry()
	registry.Register(mustNode(t, "node-1", []model.Capability{"temperature_sensor"}, node.StatusOnline))
	replacement := mustNode(t, "replacement", []model.Capability{"temperature_sensor"}, node.StatusOnline)

	first := registry.Discover("temperature_sensor")
	first[0] = replacement
	capabilities := registry.Discover("temperature_sensor")[0].Capabilities()
	capabilities[0] = "cooling_control"

	second := registry.Discover("temperature_sensor")
	assertNodeIDs(t, second, []model.NodeID{"node-1"})
	if !second[0].Has("temperature_sensor") {
		t.Fatal("stored node lost temperature_sensor after caller mutation")
	}
}

func TestRegistrySupportsConcurrentAccess(t *testing.T) {
	registry := NewRegistry()
	var workers sync.WaitGroup

	for index := 0; index < 20; index++ {
		workers.Add(2)
		go func() {
			defer workers.Done()
			registry.Register(mustNode(t, "node-1", []model.Capability{"temperature_sensor"}, node.StatusOnline))
		}()
		go func() {
			defer workers.Done()
			registry.Discover("temperature_sensor")
		}()
	}

	workers.Wait()
	assertNodeIDs(t, registry.Discover("temperature_sensor"), []model.NodeID{"node-1"})
}

func mustNode(t *testing.T, id model.NodeID, capabilities []model.Capability, status node.Status) node.Node {
	t.Helper()
	result, err := node.New(id, capabilities, status)
	if err != nil {
		t.Fatalf("node.New() returned unexpected error: %v", err)
	}
	return result
}

func assertNodeIDs(t *testing.T, got []node.Node, want []model.NodeID) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("node count = %d, want %d", len(got), len(want))
	}
	for index := range want {
		if got[index].ID() != want[index] {
			t.Fatalf("node[%d].ID() = %q, want %q", index, got[index].ID(), want[index])
		}
	}
}
