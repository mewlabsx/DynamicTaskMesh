package nodelifecycle

import (
	"errors"
	"testing"
	"time"

	"dtm/internal/capability"
	"dtm/internal/lease"
	"dtm/internal/model"
	"dtm/internal/node"
)

func TestControllerMakesNodeSchedulableOnlyWhileLeaseValid(t *testing.T) {
	now := time.Unix(100, 0)
	registry := capability.NewRegistry()
	endpoints := newRecordingEndpoints()
	leases, _ := lease.NewManager(5 * time.Second)
	controller, err := New(registry, endpoints, leases, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	registered := mustRegisteredNode(t, "node-1")

	if _, err := controller.Register(registered, "127.0.0.1:50061", "registration-1"); err != nil {
		t.Fatal(err)
	}
	if !controller.Eligible("node-1") {
		t.Fatal("Eligible() after registration = false")
	}
	if got := registry.Discover("temperature_sensor"); len(got) != 1 || got[0].Status() != node.StatusActive {
		t.Fatalf("active discovery = %v", got)
	}

	now = now.Add(5 * time.Second)
	if controller.Eligible("node-1") {
		t.Fatal("Eligible() at lease expiry before sweep = true")
	}
	if got := registry.Discover("temperature_sensor"); len(got) != 1 {
		t.Fatalf("registry before sweep = %v, want active snapshot", got)
	}

	count, err := controller.SweepExpired(now)
	if err != nil || count != 1 {
		t.Fatalf("SweepExpired() = %d, %v", count, err)
	}
	if got := registry.Discover("temperature_sensor"); len(got) != 0 {
		t.Fatalf("registry after sweep = %v", got)
	}
	if !endpoints.deleted["node-1"] {
		t.Fatal("expired endpoint was not deleted")
	}
}

func TestControllerHeartbeatRenewsEligibility(t *testing.T) {
	now := time.Unix(100, 0)
	registry := capability.NewRegistry()
	endpoints := newRecordingEndpoints()
	leases, _ := lease.NewManager(5 * time.Second)
	controller, _ := New(registry, endpoints, leases, func() time.Time { return now })
	if _, err := controller.Register(
		mustRegisteredNode(t, "node-1"),
		"127.0.0.1:50061",
		"registration-1",
	); err != nil {
		t.Fatal(err)
	}

	now = now.Add(4 * time.Second)
	record, err := controller.Heartbeat("node-1", "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	if !record.ExpiresAt.Equal(now.Add(5 * time.Second)) {
		t.Fatalf("renewed expiry = %v", record.ExpiresAt)
	}
	now = now.Add(4 * time.Second)
	if !controller.Eligible("node-1") {
		t.Fatal("Eligible() after renewed heartbeat = false")
	}
}

func TestControllerRejectsStaleOfflineRegistration(t *testing.T) {
	now := time.Unix(100, 0)
	registry := capability.NewRegistry()
	endpoints := newRecordingEndpoints()
	leases, _ := lease.NewManager(5 * time.Second)
	controller, _ := New(registry, endpoints, leases, func() time.Time { return now })
	registered := mustRegisteredNode(t, "node-1")
	if _, err := controller.Register(registered, "address-old", "old"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Millisecond)
	if _, err := controller.Register(registered, "address-new", "new"); err != nil {
		t.Fatal(err)
	}

	if err := controller.Offline("node-1", "old"); !errors.Is(err, ErrStaleRegistration) {
		t.Fatalf("Offline(old) error = %v", err)
	}
	if !controller.Eligible("node-1") || endpoints.values["node-1"] != "address-new" {
		t.Fatalf("replacement state: eligible=%v endpoint=%q", controller.Eligible("node-1"), endpoints.values["node-1"])
	}
}

func TestControllerReregistrationRecoversOfflineNode(t *testing.T) {
	now := time.Unix(100, 0)
	registry := capability.NewRegistry()
	endpoints := newRecordingEndpoints()
	leases, _ := lease.NewManager(time.Second)
	controller, _ := New(registry, endpoints, leases, func() time.Time { return now })
	registered := mustRegisteredNode(t, "node-1")
	if _, err := controller.Register(registered, "address-old", "old"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if _, err := controller.SweepExpired(now); err != nil {
		t.Fatal(err)
	}

	now = now.Add(time.Millisecond)
	if _, err := controller.Register(registered, "address-new", "new"); err != nil {
		t.Fatal(err)
	}
	current, exists := registry.Find("node-1")
	if !exists || current.Status() != node.StatusActive {
		t.Fatalf("recovered node = %#v/%v", current, exists)
	}
	if !controller.Eligible("node-1") || endpoints.values["node-1"] != "address-new" {
		t.Fatalf("recovered scheduling state: eligible=%v endpoint=%q", controller.Eligible("node-1"), endpoints.values["node-1"])
	}
}

func TestControllerSweepTraversesSuspectBeforeOffline(t *testing.T) {
	now := time.Unix(100, 0)
	registry := &recordingRegistry{}
	endpoints := newRecordingEndpoints()
	leases, _ := lease.NewManager(time.Second)
	controller, _ := New(registry, endpoints, leases, func() time.Time { return now })
	if _, err := controller.Register(
		mustRegisteredNode(t, "node-1"),
		"address",
		"registration-1",
	); err != nil {
		t.Fatal(err)
	}
	registry.statuses = nil

	if _, err := controller.SweepExpired(now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(registry.statuses) != 2 ||
		registry.statuses[0] != node.StatusSuspect ||
		registry.statuses[1] != node.StatusOffline {
		t.Fatalf("expiry statuses = %v", registry.statuses)
	}
}

func mustRegisteredNode(t *testing.T, id model.NodeID) node.Node {
	t.Helper()
	result, err := node.New(
		id,
		[]model.Capability{"temperature_sensor"},
		node.StatusRegistered,
	)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

type recordingEndpoints struct {
	values  map[model.NodeID]string
	pending map[model.NodeID]string
	deleted map[model.NodeID]bool
}

func newRecordingEndpoints() *recordingEndpoints {
	return &recordingEndpoints{
		values:  make(map[model.NodeID]string),
		pending: make(map[model.NodeID]string),
		deleted: make(map[model.NodeID]bool),
	}
}

func (endpoints *recordingEndpoints) Prepare(id model.NodeID, address string) error {
	endpoints.pending[id] = address
	return nil
}

func (endpoints *recordingEndpoints) Activate(id model.NodeID) {
	endpoints.values[id] = endpoints.pending[id]
	delete(endpoints.pending, id)
	delete(endpoints.deleted, id)
}

func (endpoints *recordingEndpoints) Available(id model.NodeID) bool {
	_, exists := endpoints.values[id]
	return exists
}

func (endpoints *recordingEndpoints) Delete(id model.NodeID) {
	delete(endpoints.values, id)
	delete(endpoints.pending, id)
	endpoints.deleted[id] = true
}

type recordingRegistry struct {
	statuses []node.Status
}

func (*recordingRegistry) Register(node.Node) {}

func (registry *recordingRegistry) SetStatus(_ model.NodeID, status node.Status) error {
	registry.statuses = append(registry.statuses, status)
	return nil
}
