package membership

import (
	"errors"
	"sync"
	"testing"
	"time"

	"dtm/internal/mesh/handshake"
	"dtm/internal/mesh/protocol"
)

var epoch = time.Unix(100, 0)
var testTiming = Timing{SuspectAfter: 3 * time.Second, ExpireAfter: 6 * time.Second}

func TestSelfEntryPresentActiveAndDoesNotTimeout(t *testing.T) {
	table := newTable(t, identity("node-b", "self"))
	_ = table.Tick(epoch.Add(time.Hour))
	snapshot := table.Snapshot()
	if len(snapshot.Members) != 1 || len(snapshot.ActiveMembers) != 1 || !snapshot.Members[0].IsSelf || snapshot.Members[0].State != StateActive {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}
func TestSuccessfulHandshakeAddsAndRefreshesActivePeer(t *testing.T) {
	table := newTable(t, identity("local", "self"))
	peer := identity("node-a", "session-a")
	observe(t, table, peer, epoch.Add(time.Second))
	observe(t, table, peer, epoch.Add(2*time.Second))
	snapshot := table.Snapshot()
	if len(snapshot.Members) != 2 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	assertState(t, table, "node-a", StateActive)
}
func TestActiveSuspectExpiredAndRefresh(t *testing.T) {
	table := newTable(t, identity("local", "self"))
	peer := identity("peer", "session")
	observe(t, table, peer, epoch)
	_ = table.Tick(epoch.Add(3 * time.Second))
	assertState(t, table, "peer", StateSuspect)
	observe(t, table, peer, epoch.Add(4*time.Second))
	assertState(t, table, "peer", StateActive)
	_ = table.Tick(epoch.Add(10 * time.Second))
	assertState(t, table, "peer", StateExpired)
	if len(table.Snapshot().ActiveMembers) != 1 {
		t.Fatal("expired peer remained active")
	}
}
func TestHealthyPeerDoesNotBecomeSuspectBeforeNormalConfirmedRefresh(t *testing.T) {
	timing := Timing{SuspectAfter: 6 * time.Second, ExpireAfter: 9 * time.Second}
	table, err := New(identity("local", "self"), timing, epoch)
	if err != nil {
		t.Fatal(err)
	}
	peer := identity("peer", "session")
	observe(t, table, peer, epoch)
	// I=1s and H=2s gives the conservative normal confirmed refresh bound 5s.
	for _, refresh := range []time.Duration{5 * time.Second, 10 * time.Second, 15 * time.Second} {
		if err := table.Tick(epoch.Add(refresh)); err != nil {
			t.Fatal(err)
		}
		assertState(t, table, "peer", StateActive)
		observe(t, table, peer, epoch.Add(refresh))
	}
	if err := table.Tick(epoch.Add(21 * time.Second)); err != nil {
		t.Fatal(err)
	}
	assertState(t, table, "peer", StateSuspect)
	if err := table.Tick(epoch.Add(24 * time.Second)); err != nil {
		t.Fatal(err)
	}
	assertState(t, table, "peer", StateExpired)
}
func TestExpiredOldSessionThenNewSessionReplacement(t *testing.T) {
	table := newTable(t, identity("local", "self"))
	old := identity("peer", "old")
	observe(t, table, old, epoch)
	_ = table.Tick(epoch.Add(6 * time.Second))
	newSession := identity("peer", "new")
	observe(t, table, newSession, epoch.Add(7*time.Second))
	snapshot := table.Snapshot()
	if len(snapshot.Members) != 2 || !snapshot.IsUniquelyActive("peer") {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	observe(t, table, old, epoch.Add(8*time.Second))
	snapshot = table.Snapshot()
	if len(snapshot.IdentityConflicts) != 1 || snapshot.IdentityConflicts[0] != "peer" || snapshot.IsUniquelyActive("peer") {
		t.Fatalf("reappeared old session did not fail closed: %+v", snapshot)
	}
	if err := table.Tick(epoch.Add(14 * time.Second)); err != nil {
		t.Fatal(err)
	}
	snapshot = table.Snapshot()
	if len(snapshot.IdentityConflicts) != 0 || snapshot.IsUniquelyActive("peer") {
		t.Fatalf("both sessions should be expired: %+v", snapshot)
	}
}

func TestSupersededSessionReappearsThenConflictRecovers(t *testing.T) {
	table := newTable(t, identity("local", "self"))
	old := identity("peer", "z-old")
	newSession := identity("peer", "a-new")
	observe(t, table, old, epoch)
	_ = table.Tick(epoch.Add(6 * time.Second))
	observe(t, table, newSession, epoch.Add(7*time.Second))
	observe(t, table, old, epoch.Add(8*time.Second))
	snapshot := table.Snapshot()
	if len(snapshot.IdentityConflicts) != 1 || snapshot.IdentityConflicts[0] != "peer" || len(snapshot.ActiveMembers) != 1 {
		t.Fatalf("reappearance=%+v", snapshot)
	}
	// Lexical RuntimeInstanceID ordering must not choose a winner.
	if snapshot.IsUniquelyActive("peer") {
		t.Fatal("RuntimeInstanceID ordering selected a winner")
	}
	observe(t, table, newSession, epoch.Add(12*time.Second))
	_ = table.Tick(epoch.Add(14 * time.Second))
	snapshot = table.Snapshot()
	if len(snapshot.IdentityConflicts) != 0 || !snapshot.IsUniquelyActive("peer") {
		t.Fatalf("recovery=%+v", snapshot)
	}
}
func TestIdentityConflictAndRecovery(t *testing.T) {
	table := newTable(t, identity("local", "self"))
	observe(t, table, identity("peer", "z-session"), epoch.Add(4*time.Second))
	observe(t, table, identity("peer", "a-session"), epoch)
	snapshot := table.Snapshot()
	if len(snapshot.IdentityConflicts) != 1 || snapshot.IsUniquelyActive("peer") {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	_ = table.Tick(epoch.Add(6 * time.Second))
	snapshot = table.Snapshot()
	if len(snapshot.IdentityConflicts) != 0 || !snapshot.IsUniquelyActive("peer") {
		t.Fatalf("recovered=%+v", snapshot)
	}
}

func TestInvalidTimingRejected(t *testing.T) {
	for _, value := range []Timing{{}, {SuspectAfter: time.Second, ExpireAfter: time.Second}, {SuspectAfter: 2 * time.Second, ExpireAfter: time.Second}} {
		if _, err := New(identity("local", "self"), value, epoch); !errors.Is(err, ErrInvalidTiming) {
			t.Fatalf("New(%+v) = %v", value, err)
		}
	}
}
func TestSelfVsRemoteDuplicateNodeIDConflicts(t *testing.T) {
	table := newTable(t, identity("node-a", "self"))
	observe(t, table, identity("node-a", "remote"), epoch)
	snapshot := table.Snapshot()
	if len(snapshot.IdentityConflicts) != 1 || len(snapshot.ActiveMembers) != 0 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
}
func TestSelfDuplicateExpiresAndReappears(t *testing.T) {
	table := newTable(t, identity("node-a", "self"))
	duplicate := identity("node-a", "remote")
	observe(t, table, duplicate, epoch)
	_ = table.Tick(epoch.Add(6 * time.Second))
	if snapshot := table.Snapshot(); len(snapshot.IdentityConflicts) != 0 || !snapshot.IsUniquelyActive("node-a") {
		t.Fatalf("expired duplicate=%+v", snapshot)
	}
	observe(t, table, duplicate, epoch.Add(7*time.Second))
	if snapshot := table.Snapshot(); len(snapshot.IdentityConflicts) != 1 || snapshot.IsUniquelyActive("node-a") {
		t.Fatalf("reappeared duplicate=%+v", snapshot)
	}
}
func TestObserveRejectsIncompatiblePeerWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		peer protocol.Identity
		want error
	}{
		{name: "namespace", peer: identity("peer", "session"), want: ErrNamespaceMismatch},
		{name: "protocol major", peer: identity("peer", "session"), want: ErrProtocolMajorMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			table := newTable(t, identity("local", "self"))
			if test.want == ErrNamespaceMismatch {
				test.peer.MeshNamespace = "other"
			} else {
				test.peer.ProtocolMajor++
			}
			before := table.Snapshot()
			if err := table.Observe(handshake.Result{Identity: test.peer}, epoch); !errors.Is(err, test.want) {
				t.Fatalf("Observe()=%v want=%v", err, test.want)
			}
			after := table.Snapshot()
			if len(after.Members) != len(before.Members) || len(after.ActiveMembers) != len(before.ActiveMembers) || len(after.IdentityConflicts) != 0 {
				t.Fatalf("rejected observation mutated snapshot: before=%+v after=%+v", before, after)
			}
		})
	}
}
func TestObserveAllowsDiagnosticMinorAndVersionDifference(t *testing.T) {
	table := newTable(t, identity("local", "self"))
	peer := identity("peer", "session")
	peer.ProtocolMinor = 99
	peer.DTMVersion = "diagnostic-build"
	observe(t, table, peer, epoch)
	if !table.Snapshot().IsUniquelyActive("peer") {
		t.Fatal("diagnostic differences became compatibility boundaries")
	}
}
func TestReappearanceConflictConcurrentSnapshotTick(t *testing.T) {
	table := newTable(t, identity("local", "self"))
	old := identity("peer", "old")
	newSession := identity("peer", "new")
	observe(t, table, old, epoch)
	_ = table.Tick(epoch.Add(6 * time.Second))
	observe(t, table, newSession, epoch.Add(7*time.Second))
	var wg sync.WaitGroup
	for index := 0; index < 100; index++ {
		wg.Add(3)
		go func(offset int) {
			defer wg.Done()
			_ = table.Observe(handshake.Result{Identity: old}, epoch.Add(8*time.Second+time.Duration(offset)*time.Millisecond))
		}(index)
		go func(offset int) {
			defer wg.Done()
			_ = table.Tick(epoch.Add(8*time.Second + time.Duration(offset)*time.Millisecond))
		}(index)
		go func() { defer wg.Done(); _ = table.Snapshot() }()
	}
	wg.Wait()
	if snapshot := table.Snapshot(); len(snapshot.IdentityConflicts) != 1 || snapshot.IsUniquelyActive("peer") {
		t.Fatalf("concurrent reappearance=%+v", snapshot)
	}
}
func TestSnapshotOrderingCopyIsolationAndConcurrentReads(t *testing.T) {
	table := newTable(t, identity("node-b", "self"))
	observe(t, table, identity("node-c", "c"), epoch)
	observe(t, table, identity("node-a", "z"), epoch)
	observe(t, table, identity("node-a", "a"), epoch)
	snapshot := table.Snapshot()
	got := []string{}
	for _, member := range snapshot.Members {
		got = append(got, member.Identity.NodeID+"/"+member.Identity.RuntimeInstance)
	}
	want := []string{"node-a/a", "node-a/z", "node-b/self", "node-c/c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order=%v", got)
		}
	}
	snapshot.Members[0].Identity.NodeID = "mutated"
	if table.Snapshot().Members[0].Identity.NodeID == "mutated" {
		t.Fatal("snapshot aliases state")
	}
	var wg sync.WaitGroup
	for index := 0; index < 100; index++ {
		wg.Add(2)
		go func() { defer wg.Done(); _ = table.Snapshot() }()
		go func(offset int) { defer wg.Done(); _ = table.Tick(epoch.Add(time.Duration(offset) * time.Millisecond)) }(index)
	}
	wg.Wait()
}
func TestCloseDoubleCloseAndConcurrentUpdate(t *testing.T) {
	table := newTable(t, identity("local", "self"))
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_ = table.Observe(handshake.Result{Identity: identity("peer", "session")}, epoch)
	}()
	go func() { defer wg.Done(); <-start; _ = table.Close() }()
	close(start)
	wg.Wait()
	if err := table.Close(); err != nil {
		t.Fatal(err)
	}
	if err := table.Tick(epoch); !errors.Is(err, ErrClosed) {
		t.Fatalf("Tick=%v", err)
	}
}

func newTable(t *testing.T, local protocol.Identity) *Table {
	t.Helper()
	table, err := New(local, testTiming, epoch)
	if err != nil {
		t.Fatal(err)
	}
	return table
}
func identity(node, session string) protocol.Identity {
	return protocol.Identity{MeshNamespace: "mesh", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion, NodeID: node, RuntimeInstance: session, ControlEndpoint: "127.0.0.1:45900"}
}
func observe(t *testing.T, table *Table, peer protocol.Identity, now time.Time) {
	t.Helper()
	if err := table.Observe(handshake.Result{Identity: peer}, now); err != nil {
		t.Fatal(err)
	}
}
func assertState(t *testing.T, table *Table, node string, want State) {
	t.Helper()
	for _, member := range table.Snapshot().Members {
		if member.Identity.NodeID == node {
			if member.State != want {
				t.Fatalf("state=%s want=%s", member.State, want)
			}
			return
		}
	}
	t.Fatalf("member %s absent", node)
}
