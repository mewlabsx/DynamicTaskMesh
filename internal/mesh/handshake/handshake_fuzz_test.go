package handshake

import (
	"context"
	"testing"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/mesh/protocol"
	"google.golang.org/protobuf/proto"
)

const maxHandshakeFuzzInput = 1 << 20

func FuzzHandshakeRequest(f *testing.F) {
	local := protocol.Identity{
		MeshNamespace:   "mesh-a",
		ProtocolMajor:   1,
		ProtocolMinor:   0,
		DTMVersion:      protocol.DTMVersion,
		NodeID:          "node-a",
		RuntimeInstance: "session-a",
		ControlEndpoint: "127.0.0.1:45893",
	}
	server, err := NewServer(local)
	if err != nil {
		f.Fatalf("NewServer(local) = %v", err)
	}
	seed := &dtmv1.HandshakeRequest{Runtime: &dtmv1.RuntimeIdentity{
		MeshNamespace:             "mesh-a",
		ProtocolMajor:             1,
		ProtocolMinor:             0,
		DtmVersion:                protocol.DTMVersion,
		NodeId:                    "node-b",
		RuntimeInstanceId:         "session-b",
		AdvertisedControlEndpoint: "127.0.0.1:45894",
	}}
	encoded, err := proto.Marshal(seed)
	if err != nil {
		f.Fatalf("proto.Marshal(seed) = %v", err)
	}
	f.Add(encoded)
	f.Add([]byte{})
	f.Add([]byte{0x0a, 0x01, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxHandshakeFuzzInput {
			return
		}
		request := new(dtmv1.HandshakeRequest)
		if err := proto.Unmarshal(data, request); err != nil {
			return
		}
		response, err := server.Handshake(context.Background(), request)
		if err != nil {
			t.Fatalf("Handshake returned an unexpected error: %v", err)
		}
		if response == nil {
			t.Fatal("Handshake returned a nil response")
		}
		if !response.GetAccepted() {
			return
		}

		peer := fromProto(request.GetRuntime())
		if err := peer.Validate(); err != nil {
			t.Fatalf("accepted peer is invalid: %v", err)
		}
		if response.GetRejectionReason() != dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_UNSPECIFIED {
			t.Fatalf("accepted response has rejection reason %s", response.GetRejectionReason())
		}
	})
}
