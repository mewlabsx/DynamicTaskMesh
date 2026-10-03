package coordinator

import (
	"sync"
	"testing"
	"time"

	"dtm/internal/mesh/handshake"
	"dtm/internal/mesh/membership"
	"dtm/internal/mesh/protocol"
)

func identity(nodeID, session string) protocol.Identity {
	return protocol.Identity{MeshNamespace: "mesh", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion, NodeID: nodeID, RuntimeInstance: session, ControlEndpoint: "127.0.0.1:45901"}
}

func active(nodeID, session string) membership.Member {
	return membership.Member{Identity: identity(nodeID, session), State: membership.StateActive}
}

func TestDeterministicSelection(t *testing.T) {
	tests := []struct {
		name    string
		members []membership.Member
		want    string
		ok      bool
	}{
		{name: "OneCandidateSelectsItself", members: []membership.Member{active("node-a", "a")}, want: "node-a", ok: true},
		{name: "ThreeCandidatesSelectMinimumNodeID", members: []membership.Member{active("node-c", "c"), active("node-a", "a"), active("node-b", "b")}, want: "node-a", ok: true},
		{name: "InputOrderDoesNotAffectSelection", members: []membership.Member{active("node-b", "b"), active("node-c", "c"), active("node-a", "a")}, want: "node-a", ok: true},
		{name: "EmptyCandidatesReturnsNoCoordinator", ok: false},
		{name: "SuspectMemberNotCandidate", members: []membership.Member{{Identity: identity("node-a", "a"), State: membership.StateSuspect}, active("node-b", "b")}, want: "node-b", ok: true},
		{name: "ExpiredMemberNotCandidate", members: []membership.Member{{Identity: identity("node-a", "a"), State: membership.StateExpired}, active("node-b", "b")}, want: "node-b", ok: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := Select(membership.Snapshot{ActiveMembers: test.members})
			if ok != test.ok || got.NodeID != test.want {
				t.Fatalf("Select() = %+v,%v want node=%q ok=%v", got, ok, test.want, test.ok)
			}
		})
	}
}

func TestSelectionContainsRuntimeSessionAndEndpoint(t *testing.T) {
	got, ok := Select(membership.Snapshot{ActiveMembers: []membership.Member{active("node-a", "session-a")}})
	if !ok || got != (Selection{NodeID: "node-a", RuntimeInstanceID: "session-a", ControlEndpoint: "127.0.0.1:45901"}) {
		t.Fatalf("Select() = %+v,%v", got, ok)
	}
}

func TestRepeatedSelectionIsDeterministic(t *testing.T) {
	snapshot := membership.Snapshot{ActiveMembers: []membership.Member{
		active("node-c", "session-c"),
		active("node-a", "session-a"),
		active("node-b", "session-b"),
	}}
	want := Selection{NodeID: "node-a", RuntimeInstanceID: "session-a", ControlEndpoint: "127.0.0.1:45901"}
	for iteration := 0; iteration < 1000; iteration++ {
		got, ok := Select(snapshot)
		if !ok || got != want {
			t.Fatalf("iteration %d: Select() = %+v,%v want %+v,true", iteration, got, ok, want)
		}
	}
}

func TestIdentityConflictAndMalformedDuplicateFailClosed(t *testing.T) {
	t.Run("IdentityConflictNodeIDNotCandidate", func(t *testing.T) {
		got, ok := Select(membership.Snapshot{ActiveMembers: []membership.Member{active("node-b", "z")}, IdentityConflicts: []string{"node-a"}})
		if !ok || got.NodeID != "node-b" {
			t.Fatalf("Select() = %+v,%v", got, ok)
		}
	})
	t.Run("SameNodeIDDifferentSessionDoesNotUseSessionTieBreak", func(t *testing.T) {
		got, ok := Select(membership.Snapshot{ActiveMembers: []membership.Member{active("node-a", "x"), active("node-a", "y"), active("node-b", "z")}})
		if !ok || got.NodeID != "node-b" {
			t.Fatalf("Select() = %+v,%v", got, ok)
		}
	})
	t.Run("InvalidEndpointFailsClosed", func(t *testing.T) {
		bad := active("node-a", "a")
		bad.Identity.ControlEndpoint = ""
		got, ok := Select(membership.Snapshot{ActiveMembers: []membership.Member{bad, active("node-b", "b")}})
		if !ok || got.NodeID != "node-b" {
			t.Fatalf("Select() = %+v,%v", got, ok)
		}
	})
}

func TestRoleViewTransitionsAndLocalSessionMatch(t *testing.T) {
	var view View
	local := identity("node-a", "local")
	if got := view.Snapshot(); got.HasCoordinator {
		t.Fatal("zero View has coordinator")
	}
	got := view.Update(membership.Snapshot{ActiveMembers: []membership.Member{active("node-a", "other")}}, local)
	if !got.HasCoordinator || got.LocalIsCoordinator {
		t.Fatalf("same NodeID other session = %+v", got)
	}
	got = view.Update(membership.Snapshot{ActiveMembers: []membership.Member{active("node-a", "local")}}, local)
	if !got.LocalIsCoordinator {
		t.Fatalf("local session = %+v", got)
	}
	got = view.Update(membership.Snapshot{}, local)
	if got.HasCoordinator || got.LocalIsCoordinator {
		t.Fatalf("none = %+v", got)
	}
}

func TestRoleViewConcurrentReadUpdate(t *testing.T) {
	var view View
	local := identity("node-a", "a")
	var wait sync.WaitGroup
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for j := 0; j < 1000; j++ {
				_ = view.Snapshot()
			}
		}()
	}
	for i := 0; i < 1000; i++ {
		view.Update(membership.Snapshot{ActiveMembers: []membership.Member{active("node-a", "a"), active("node-b", "b")}}, local)
	}
	wait.Wait()
}

func TestResourceViewChangesDoNotAffectCoordinator(t *testing.T) {
	// Resource state is deliberately represented only as unrelated test data:
	// Select has no resourceview input or import.
	snapshot := membership.Snapshot{ActiveMembers: []membership.Member{active("node-a", "a"), active("node-b", "b"), active("node-c", "c")}}
	for _, resourceCounts := range [][]int{{0, 0, 0}, {0, 100, 1000}, {1000, 0, 1}} {
		_ = resourceCounts
		got, ok := Select(snapshot)
		if !ok || got.NodeID != "node-a" {
			t.Fatalf("Select() = %+v,%v", got, ok)
		}
	}
}

func TestMembershipTransitionsAndConflictRecovery(t *testing.T) {
	base := time.Unix(100, 0)
	local := identity("node-c", "c")
	table, err := membership.New(local, membership.Timing{SuspectAfter: 3 * time.Second, ExpireAfter: 6 * time.Second}, base)
	if err != nil {
		t.Fatal(err)
	}
	observe := func(value protocol.Identity, at time.Time) {
		if err := table.Observe(handshake.Result{Identity: value}, at); err != nil {
			t.Fatal(err)
		}
	}
	a := identity("node-a", "x")
	b := identity("node-b", "z")
	observe(a, base)
	observe(b, base)
	assertNode := func(want string) {
		t.Helper()
		got, ok := Select(table.Snapshot())
		if !ok || got.NodeID != want {
			t.Fatalf("Select() = %+v,%v want %s; snapshot=%+v", got, ok, want, table.Snapshot())
		}
	}
	assertNode("node-a")
	if err := table.Tick(base.Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	assertNode("node-c") // local remains active; a and b are suspect.
	observe(b, base.Add(3*time.Second))
	assertNode("node-b")
	observe(a, base.Add(4*time.Second))
	assertNode("node-a")
	duplicate := a
	duplicate.RuntimeInstance = "y"
	observe(duplicate, base.Add(4*time.Second))
	assertNode("node-b")
	observe(duplicate, base.Add(7*time.Second))
	if err := table.Tick(base.Add(8 * time.Second)); err != nil {
		t.Fatal(err)
	}
	assertNode("node-a")
}
