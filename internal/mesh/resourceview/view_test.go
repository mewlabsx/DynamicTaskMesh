package resourceview_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"dtm/internal/mesh/handshake"
	"dtm/internal/mesh/membership"
	"dtm/internal/mesh/protocol"
	. "dtm/internal/mesh/resourceview"
	"dtm/internal/model"
)

var viewEpoch = time.Unix(100, 0)

func TestLocalSelfResourcesPresentAndCopyIsolated(t *testing.T) {
	table, view := newView(t, identity("node-b", "self"), descriptor("resource-b"))
	_ = table
	snapshot := view.Snapshot()
	if len(snapshot.Entries) != 1 || len(snapshot.ActiveResources) != 1 || snapshot.Entries[0].Owner.NodeID != "node-b" {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	snapshot.Entries[0].Descriptor.Attributes["mutated"] = "yes"
	if _, exists := view.Snapshot().Entries[0].Descriptor.Attributes["mutated"]; exists {
		t.Fatal("snapshot aliases internal attributes")
	}
}

func TestRemoteFullSnapshotAcceptedIdempotentAndReplacementRemovesOmitted(t *testing.T) {
	table, view := newView(t, identity("local", "self"))
	peer := identity("peer", "session")
	observeMember(t, table, peer, viewEpoch)
	first := Advertisement{Owner: peer, Resources: []Descriptor{descriptor("resource-z"), descriptor("resource-a")}}
	if err := view.Observe(first); err != nil {
		t.Fatal(err)
	}
	if err := view.Observe(first); err != nil {
		t.Fatal(err)
	}
	if got := len(view.Snapshot().Entries); got != 2 {
		t.Fatalf("entries=%d", got)
	}
	if err := view.Observe(Advertisement{Owner: peer, Resources: []Descriptor{descriptor("resource-z")}}); err != nil {
		t.Fatal(err)
	}
	snapshot := view.Snapshot()
	if len(snapshot.Entries) != 1 || snapshot.Entries[0].Descriptor.ID != "resource-z" {
		t.Fatalf("replacement=%+v", snapshot)
	}
}

func TestAdvertisementValidationRejectsWithoutMutation(t *testing.T) {
	table, view := newView(t, identity("local", "self"))
	peer := identity("peer", "session")
	unknown := peer
	unknown.RuntimeInstance = "unknown"
	wrongNamespace := peer
	wrongNamespace.MeshNamespace = "other"
	wrongMajor := peer
	wrongMajor.ProtocolMajor++
	observeMember(t, table, peer, viewEpoch)
	tests := []struct {
		name          string
		advertisement Advertisement
		want          error
	}{
		{"unknown", Advertisement{Owner: unknown, Resources: []Descriptor{descriptor("resource-a")}}, ErrUnknownRuntimeSession},
		{"namespace", Advertisement{Owner: wrongNamespace, Resources: []Descriptor{descriptor("resource-a")}}, ErrNamespaceMismatch},
		{"major", Advertisement{Owner: wrongMajor, Resources: []Descriptor{descriptor("resource-a")}}, ErrProtocolMajorMismatch},
		{"duplicate", Advertisement{Owner: peer, Resources: []Descriptor{descriptor("resource-a"), descriptor("resource-a")}}, ErrDuplicateResourceID},
		{"invalid", Advertisement{Owner: peer, Resources: []Descriptor{{ID: "", Kind: model.ResourceKindCapability, Type: "temperature_sensor", Operations: []model.OperationID{"read_temperature"}}}}, ErrInvalidAdvertisement},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := view.Snapshot()
			if err := view.Observe(test.advertisement); !errors.Is(err, test.want) {
				t.Fatalf("Observe=%v want=%v", err, test.want)
			}
			after := view.Snapshot()
			if len(after.Entries) != len(before.Entries) {
				t.Fatalf("mutated=%+v", after)
			}
		})
	}
	if got := len(table.Snapshot().Members); got != 2 {
		t.Fatalf("advertisement changed membership: %d", got)
	}
}

func TestMembershipStateGatesActiveResourcesWithoutDeletingDiagnostics(t *testing.T) {
	table, view := newView(t, identity("local", "self"))
	peer := identity("peer", "session")
	observeMember(t, table, peer, viewEpoch)
	if err := view.Observe(Advertisement{Owner: peer, Resources: []Descriptor{descriptor("resource-a")}}); err != nil {
		t.Fatal(err)
	}
	if len(view.Snapshot().ActiveResources) != 1 {
		t.Fatal("active resource absent")
	}
	_ = table.Tick(viewEpoch.Add(6 * time.Second))
	snapshot := view.Snapshot()
	if len(snapshot.Entries) != 1 || len(snapshot.ActiveResources) != 0 {
		t.Fatalf("suspect=%+v", snapshot)
	}
	_ = table.Tick(viewEpoch.Add(9 * time.Second))
	snapshot = view.Snapshot()
	if len(snapshot.Entries) != 1 || len(snapshot.ActiveResources) != 0 {
		t.Fatalf("expired=%+v", snapshot)
	}
	observeMember(t, table, peer, viewEpoch.Add(10*time.Second))
	if len(view.Snapshot().ActiveResources) != 1 {
		t.Fatal("reactivated owner did not reuse latest diagnostic snapshot")
	}
}

func TestIdentityConflictAndSessionReplacementFailClosed(t *testing.T) {
	table, view := newView(t, identity("local", "self"))
	x, y := identity("node-a", "x"), identity("node-a", "y")
	observeMember(t, table, x, viewEpoch)
	_ = view.Observe(Advertisement{Owner: x, Resources: []Descriptor{descriptor("resource-x")}})
	_ = table.Tick(viewEpoch.Add(9 * time.Second))
	observeMember(t, table, y, viewEpoch.Add(10*time.Second))
	_ = view.Observe(Advertisement{Owner: y, Resources: []Descriptor{descriptor("resource-y")}})
	snapshot := view.Snapshot()
	if len(snapshot.ActiveResources) != 1 || snapshot.ActiveResources[0].Owner.RuntimeInstance != "y" {
		t.Fatalf("replacement=%+v", snapshot)
	}
	observeMember(t, table, x, viewEpoch.Add(11*time.Second))
	snapshot = view.Snapshot()
	if len(snapshot.Entries) != 2 || len(snapshot.ActiveResources) != 0 {
		t.Fatalf("conflict=%+v", snapshot)
	}
	observeMember(t, table, y, viewEpoch.Add(15*time.Second))
	_ = table.Tick(viewEpoch.Add(17 * time.Second))
	snapshot = view.Snapshot()
	if len(snapshot.ActiveResources) != 1 || snapshot.ActiveResources[0].Owner.RuntimeInstance != "y" {
		t.Fatalf("recovery=%+v", snapshot)
	}
}

func TestSelfConflictExcludesLocalResources(t *testing.T) {
	self := identity("node-a", "self")
	table, view := newView(t, self, descriptor("resource-self"))
	remote := identity("node-a", "remote")
	observeMember(t, table, remote, viewEpoch)
	_ = view.Observe(Advertisement{Owner: remote, Resources: []Descriptor{descriptor("resource-remote")}})
	if snapshot := view.Snapshot(); len(snapshot.Entries) != 2 || len(snapshot.ActiveResources) != 0 {
		t.Fatalf("self conflict=%+v", snapshot)
	}
}

func TestDeterministicOrderingAndConcurrentObserveSnapshotClose(t *testing.T) {
	table, view := newView(t, identity("node-b", "self"), descriptor("resource-b"))
	a := identity("node-a", "z")
	c := identity("node-c", "a")
	observeMember(t, table, a, viewEpoch)
	observeMember(t, table, c, viewEpoch)
	_ = view.Observe(Advertisement{Owner: c, Resources: []Descriptor{descriptor("resource-c")}})
	_ = view.Observe(Advertisement{Owner: a, Resources: []Descriptor{descriptor("resource-z"), descriptor("resource-a")}})
	got := []model.ResourceID{}
	for _, entry := range view.Snapshot().Entries {
		got = append(got, entry.Descriptor.ID)
	}
	want := []model.ResourceID{"resource-a", "resource-z", "resource-b", "resource-c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order=%v", got)
		}
	}
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); <-start; _ = view.Snapshot() }()
		go func() {
			defer wg.Done()
			<-start
			_ = view.Observe(Advertisement{Owner: a, Resources: []Descriptor{descriptor("resource-a")}})
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); <-start; _ = view.Close() }()
	close(start)
	wg.Wait()
	if err := view.Observe(Advertisement{Owner: a}); !errors.Is(err, ErrClosed) {
		t.Fatalf("Observe after close=%v", err)
	}
}

func newView(t *testing.T, local protocol.Identity, resources ...Descriptor) (*membership.Table, *View) {
	t.Helper()
	table, err := membership.New(local, membership.Timing{SuspectAfter: 6 * time.Second, ExpireAfter: 9 * time.Second}, viewEpoch)
	if err != nil {
		t.Fatal(err)
	}
	view, err := New(local, func() MembershipSnapshot {
		s := table.Snapshot()
		r := MembershipSnapshot{Members: make([]protocol.Identity, len(s.Members)), ActiveMembers: make([]protocol.Identity, len(s.ActiveMembers))}
		for i, m := range s.Members {
			r.Members[i] = m.Identity
		}
		for i, m := range s.ActiveMembers {
			r.ActiveMembers[i] = m.Identity
		}
		return r
	}, resources)
	if err != nil {
		t.Fatal(err)
	}
	return table, view
}
func observeMember(t *testing.T, table *membership.Table, peer protocol.Identity, now time.Time) {
	t.Helper()
	if err := table.Observe(handshake.Result{Identity: peer}, now); err != nil {
		t.Fatal(err)
	}
}
func identity(node, session string) protocol.Identity {
	return protocol.Identity{MeshNamespace: "mesh", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion, NodeID: node, RuntimeInstance: session, ControlEndpoint: "127.0.0.1:45900"}
}
func descriptor(id string) Descriptor {
	return Descriptor{ID: model.ResourceID(id), Kind: model.ResourceKindCapability, Type: "temperature_sensor", Operations: []model.OperationID{"read_temperature"}, Attributes: map[string]string{"source": "test"}}
}
