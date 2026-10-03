package ui

import (
	"context"
	"sort"
	"sync"
	"time"

	"dtm/internal/mesh/discovery"
	"dtm/internal/mesh/protocol"
)

type MemberObservation struct {
	Identity protocol.Identity
	LastSeen time.Time
	State    string
}

type MemberSource func() []MemberObservation

// DiscoveryCollector observes existing Runtime presence announcements. It
// does not publish presence and does not participate in membership decisions.
type DiscoveryCollector struct {
	transport   discovery.Transport
	now         func() time.Time
	staleAfter  time.Duration
	expireAfter time.Duration

	mu            sync.RWMutex
	namespace     string
	protocolMajor uint32
	members       map[string]MemberObservation
}

func NewDiscoveryCollector(transport discovery.Transport, staleAfter, expireAfter time.Duration) *DiscoveryCollector {
	if staleAfter <= 0 {
		staleAfter = 5 * time.Second
	}
	if expireAfter <= staleAfter {
		expireAfter = 10 * time.Second
	}
	return &DiscoveryCollector{
		transport: transport, now: time.Now, staleAfter: staleAfter, expireAfter: expireAfter,
		members: make(map[string]MemberObservation),
	}
}

func (collector *DiscoveryCollector) SetFilter(namespace string, protocolMajor uint32) {
	if collector == nil {
		return
	}
	collector.mu.Lock()
	collector.namespace = namespace
	collector.protocolMajor = protocolMajor
	collector.mu.Unlock()
}

func (collector *DiscoveryCollector) Run(ctx context.Context) {
	if collector == nil || collector.transport == nil {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case value, ok := <-collector.transport.Announcements():
			if !ok {
				return
			}
			identity, err := protocol.Decode(value)
			if err != nil {
				continue
			}
			collector.observe(identity)
		}
	}
}

func (collector *DiscoveryCollector) observe(identity protocol.Identity) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if collector.namespace != "" && identity.MeshNamespace != collector.namespace {
		return
	}
	if collector.protocolMajor != 0 && identity.ProtocolMajor != collector.protocolMajor {
		return
	}
	now := collector.now()
	key := identity.NodeID + "\x00" + identity.RuntimeInstance
	collector.members[key] = MemberObservation{Identity: identity, LastSeen: now, State: "ACTIVE"}
}

func (collector *DiscoveryCollector) Snapshot() []MemberObservation {
	if collector == nil {
		return nil
	}
	collector.mu.RLock()
	defer collector.mu.RUnlock()
	now := collector.now()
	result := make([]MemberObservation, 0, len(collector.members))
	for _, member := range collector.members {
		copy := member
		age := now.Sub(member.LastSeen)
		switch {
		case age >= collector.expireAfter:
			copy.State = "EXPIRED"
		case age >= collector.staleAfter:
			copy.State = "SUSPECT"
		default:
			copy.State = "ACTIVE"
		}
		result = append(result, copy)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Identity.NodeID != result[right].Identity.NodeID {
			return result[left].Identity.NodeID < result[right].Identity.NodeID
		}
		return result[left].Identity.RuntimeInstance < result[right].Identity.RuntimeInstance
	})
	return result
}

func StaticMemberSource(identities []protocol.Identity) MemberSource {
	values := append([]protocol.Identity(nil), identities...)
	return func() []MemberObservation {
		result := make([]MemberObservation, 0, len(values))
		for _, identity := range values {
			result = append(result, MemberObservation{Identity: identity, State: "ACTIVE"})
		}
		return result
	}
}
