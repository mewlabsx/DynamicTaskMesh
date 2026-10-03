package membership

import (
	"testing"
	"time"

	"dtm/internal/mesh/handshake"
	"dtm/internal/mesh/protocol"
)

const maxMembershipFuzzInput = 1 << 20

func FuzzObserveAndTick(f *testing.F) {
	local := protocol.Identity{
		MeshNamespace:   "mesh-a",
		ProtocolMajor:   1,
		ProtocolMinor:   0,
		DTMVersion:      protocol.DTMVersion,
		NodeID:          "node-a",
		RuntimeInstance: "session-a",
		ControlEndpoint: "127.0.0.1:45893",
	}
	peer := local
	peer.NodeID = "node-b"
	peer.RuntimeInstance = "session-b"
	peer.ControlEndpoint = "127.0.0.1:45894"
	encoded, err := protocol.Encode(peer)
	if err != nil {
		f.Fatalf("protocol.Encode(peer) = %v", err)
	}
	f.Add(encoded)
	f.Add([]byte{})
	f.Add([]byte("{}"))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxMembershipFuzzInput {
			return
		}
		identity, err := protocol.Decode(data)
		if err != nil {
			return
		}
		now := time.Unix(1_000, 0)
		table, err := New(local, Timing{SuspectAfter: time.Second, ExpireAfter: 2 * time.Second}, now)
		if err != nil {
			t.Fatalf("New(local) = %v", err)
		}
		_ = table.Observe(handshake.Result{Identity: identity}, now)
		if err := table.Tick(now.Add(3 * time.Second)); err != nil {
			t.Fatalf("Tick() = %v", err)
		}

		snapshot := table.Snapshot()
		for _, member := range snapshot.Members {
			if err := member.Identity.Validate(); err != nil {
				t.Fatalf("snapshot contains invalid identity: %v", err)
			}
			switch member.State {
			case StateActive, StateSuspect, StateExpired:
			default:
				t.Fatalf("snapshot contains unknown state %q", member.State)
			}
		}
	})
}
