package coordinator

import (
	"sync"

	"dtm/internal/mesh/membership"
	"dtm/internal/mesh/protocol"
)

// Selection identifies the concrete Runtime session selected for the
// coordinator role. NodeID is the only ordering key.
type Selection struct {
	NodeID            string
	RuntimeInstanceID string
	ControlEndpoint   string
}

// Select calculates the coordinator solely from a Runtime membership
// snapshot. Invalid, non-active, conflicted, and duplicate identities fail
// closed and are not candidates.
func Select(snapshot membership.Snapshot) (Selection, bool) {
	conflicted := make(map[string]struct{}, len(snapshot.IdentityConflicts))
	for _, nodeID := range snapshot.IdentityConflicts {
		conflicted[nodeID] = struct{}{}
	}
	counts := make(map[string]int, len(snapshot.ActiveMembers))
	for _, member := range snapshot.ActiveMembers {
		if member.State == membership.StateActive && member.Identity.Validate() == nil {
			counts[member.Identity.NodeID]++
		}
	}
	var selected protocol.Identity
	found := false
	for _, member := range snapshot.ActiveMembers {
		identity := member.Identity
		if member.State != membership.StateActive || identity.Validate() != nil || counts[identity.NodeID] != 1 {
			continue
		}
		if _, excluded := conflicted[identity.NodeID]; excluded {
			continue
		}
		if !found || identity.NodeID < selected.NodeID {
			selected = identity
			found = true
		}
	}
	if !found {
		return Selection{}, false
	}
	return Selection{NodeID: selected.NodeID, RuntimeInstanceID: selected.RuntimeInstance, ControlEndpoint: selected.ControlEndpoint}, true
}

type RoleSnapshot struct {
	Selection          Selection
	HasCoordinator     bool
	LocalIsCoordinator bool
}

// View is a concurrency-safe local observation of the current calculated
// role. It has no authority or Core lifecycle behavior.
type View struct {
	mu      sync.RWMutex
	current RoleSnapshot
}

func (view *View) Update(snapshot membership.Snapshot, local protocol.Identity) RoleSnapshot {
	selection, ok := Select(snapshot)
	current := RoleSnapshot{Selection: selection, HasCoordinator: ok}
	current.LocalIsCoordinator = ok && selection.NodeID == local.NodeID && selection.RuntimeInstanceID == local.RuntimeInstance
	view.mu.Lock()
	view.current = current
	view.mu.Unlock()
	return current
}

func (view *View) Snapshot() RoleSnapshot {
	view.mu.RLock()
	defer view.mu.RUnlock()
	return view.current
}
