package membership

import (
	"errors"
	"sort"
	"sync"
	"time"

	"dtm/internal/mesh/handshake"
	"dtm/internal/mesh/protocol"
)

type State string

const (
	StateActive  State = "active"
	StateSuspect State = "suspect"
	StateExpired State = "expired"

	DefaultSuspectAfter = 6 * time.Second
	DefaultExpireAfter  = 9 * time.Second
)

var (
	ErrInvalidTiming         = errors.New("membership timing is invalid")
	ErrClosed                = errors.New("membership is closed")
	ErrNamespaceMismatch     = errors.New("membership peer namespace does not match local namespace")
	ErrProtocolMajorMismatch = errors.New("membership peer protocol major does not match local protocol major")
)

type Timing struct{ SuspectAfter, ExpireAfter time.Duration }

type Member struct {
	Identity      protocol.Identity
	State         State
	FirstSeen     time.Time
	LastSeen      time.Time
	LastHandshake time.Time
	IsSelf        bool
}

type Snapshot struct {
	Members           []Member
	ActiveMembers     []Member
	IdentityConflicts []string
}

func (snapshot Snapshot) IsUniquelyActive(nodeID string) bool {
	for _, conflict := range snapshot.IdentityConflicts {
		if conflict == nodeID {
			return false
		}
	}
	for _, member := range snapshot.ActiveMembers {
		if member.Identity.NodeID == nodeID {
			return true
		}
	}
	return false
}

type record struct {
	Member
	superseded bool
}

type Table struct {
	mu      sync.RWMutex
	local   protocol.Identity
	timing  Timing
	members map[string]*record
	closed  bool
}

func New(local protocol.Identity, timing Timing, now time.Time) (*Table, error) {
	if err := local.Validate(); err != nil {
		return nil, err
	}
	if timing.SuspectAfter <= 0 || timing.ExpireAfter <= timing.SuspectAfter {
		return nil, ErrInvalidTiming
	}
	member := Member{Identity: local, State: StateActive, FirstSeen: now, LastSeen: now, LastHandshake: now, IsSelf: true}
	return &Table{local: local, timing: timing, members: map[string]*record{key(local): {Member: member}}}, nil
}

func (table *Table) Observe(result handshake.Result, now time.Time) error {
	identity := result.Identity
	if err := identity.Validate(); err != nil {
		return err
	}
	if identity.MeshNamespace != table.local.MeshNamespace {
		return ErrNamespaceMismatch
	}
	if identity.ProtocolMajor != table.local.ProtocolMajor {
		return ErrProtocolMajorMismatch
	}
	if identity.SameSession(table.local) {
		return nil
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.closed {
		return ErrClosed
	}
	entryKey := key(identity)
	if existing := table.members[entryKey]; existing != nil {
		// Superseded describes stale history, not a permanent ban. A newly
		// confirmed direct Handshake is fresh liveness evidence, so restore the
		// old session and let the active-set conflict rule fail closed.
		existing.superseded = false
		existing.Identity = identity
		existing.State = StateActive
		existing.LastSeen = now
		existing.LastHandshake = now
		return nil
	}
	// A new session replaces expired sessions for the same stable NodeID. Live
	// sessions are retained so duplicate stable identity fails closed.
	for _, existing := range table.members {
		if !existing.IsSelf && existing.Identity.NodeID == identity.NodeID && existing.State == StateExpired {
			existing.superseded = true
		}
	}
	table.members[entryKey] = &record{Member: Member{Identity: identity, State: StateActive, FirstSeen: now, LastSeen: now, LastHandshake: now}}
	return nil
}

func (table *Table) Tick(now time.Time) error {
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.closed {
		return ErrClosed
	}
	for _, member := range table.members {
		if member.IsSelf || member.superseded {
			continue
		}
		age := now.Sub(member.LastSeen)
		switch {
		case age >= table.timing.ExpireAfter:
			member.State = StateExpired
		case age >= table.timing.SuspectAfter:
			member.State = StateSuspect
		default:
			member.State = StateActive
		}
	}
	// Once a duplicate session expires while another remains active, it cannot
	// later displace that live session through a stale observation.
	activeByNode := map[string]int{}
	for _, member := range table.members {
		if !member.superseded && member.State == StateActive {
			activeByNode[member.Identity.NodeID]++
		}
	}
	for _, member := range table.members {
		if !member.IsSelf && member.State == StateExpired && activeByNode[member.Identity.NodeID] > 0 {
			member.superseded = true
		}
	}
	return nil
}

func (table *Table) Snapshot() Snapshot {
	table.mu.RLock()
	defer table.mu.RUnlock()
	members := make([]Member, 0, len(table.members))
	activeCounts := map[string]int{}
	for _, entry := range table.members {
		if entry.superseded {
			continue
		}
		copy := entry.Member
		members = append(members, copy)
		if copy.State == StateActive {
			activeCounts[copy.Identity.NodeID]++
		}
	}
	sortMembers(members)
	conflicts := make([]string, 0)
	for nodeID, count := range activeCounts {
		if count > 1 {
			conflicts = append(conflicts, nodeID)
		}
	}
	sort.Strings(conflicts)
	active := make([]Member, 0, len(members))
	for _, member := range members {
		if member.State == StateActive && activeCounts[member.Identity.NodeID] == 1 {
			active = append(active, member)
		}
	}
	return Snapshot{Members: members, ActiveMembers: active, IdentityConflicts: conflicts}
}

func (table *Table) Close() error {
	table.mu.Lock()
	table.closed = true
	table.mu.Unlock()
	return nil
}

func key(identity protocol.Identity) string {
	return identity.NodeID + "\x00" + identity.RuntimeInstance
}
func sortMembers(members []Member) {
	sort.Slice(members, func(i, j int) bool {
		if members[i].Identity.NodeID == members[j].Identity.NodeID {
			return members[i].Identity.RuntimeInstance < members[j].Identity.RuntimeInstance
		}
		return members[i].Identity.NodeID < members[j].Identity.NodeID
	})
}
