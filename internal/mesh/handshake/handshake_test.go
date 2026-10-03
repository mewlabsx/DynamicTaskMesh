package handshake

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/mesh/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestHandshakeCompatibleRoundTrip(t *testing.T) {
	local, peer := identity("node-a", "session-a"), identity("node-b", "session-b")
	client, stop := testPair(t, local, peer)
	defer stop()
	result, err := client.Handshake(context.Background(), "bufnet")
	if err != nil {
		t.Fatal(err)
	}
	if result.Identity != peer {
		t.Fatalf("result = %+v", result)
	}
}

func TestHandshakeTypedRejections(t *testing.T) {
	local := identity("node-a", "session-a")
	tests := []struct {
		name   string
		peer   protocol.Identity
		reason dtmv1.HandshakeRejectionReason
	}{
		{"namespace", with(local, func(v *protocol.Identity) {
			v.NodeID = "node-b"
			v.RuntimeInstance = "session-b"
			v.MeshNamespace = "other"
		}), dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_NAMESPACE_MISMATCH},
		{"major", with(local, func(v *protocol.Identity) { v.NodeID = "node-b"; v.RuntimeInstance = "session-b"; v.ProtocolMajor = 2 }), dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_PROTOCOL_MAJOR_INCOMPATIBLE},
		{"identity", with(local, func(v *protocol.Identity) { v.NodeID = "" }), dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_MALFORMED_IDENTITY},
		{"endpoint", with(local, func(v *protocol.Identity) {
			v.NodeID = "node-b"
			v.RuntimeInstance = "session-b"
			v.ControlEndpoint = "bad"
		}), dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_INVALID_ENDPOINT},
		{"self", local, dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_SELF_SESSION},
	}
	server, _ := NewServer(local)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := server.Handshake(context.Background(), &dtmv1.HandshakeRequest{Runtime: toProto(test.peer)})
			if err != nil || response.GetAccepted() || response.GetRejectionReason() != test.reason {
				t.Fatalf("response=%+v err=%v", response, err)
			}
		})
	}
}

func TestHandshakeClientPreservesTypedRejection(t *testing.T) {
	local := identity("node-a", "session-a")
	tests := []struct {
		name   string
		peer   protocol.Identity
		reason dtmv1.HandshakeRejectionReason
	}{
		{"namespace", with(identity("node-b", "session-b"), func(v *protocol.Identity) { v.MeshNamespace = "other" }), dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_NAMESPACE_MISMATCH},
		{"major", with(identity("node-b", "session-b"), func(v *protocol.Identity) { v.ProtocolMajor = 2 }), dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_PROTOCOL_MAJOR_INCOMPATIBLE},
		{"malformed", with(identity("node-b", "session-b"), func(v *protocol.Identity) { v.DTMVersion = " " }), dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_MALFORMED_IDENTITY},
		{"endpoint", with(identity("node-b", "session-b"), func(v *protocol.Identity) { v.ControlEndpoint = "bad" }), dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_INVALID_ENDPOINT},
		{"self", local, dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_SELF_SESSION},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, stop := testPairUnchecked(t, local, test.peer)
			defer stop()
			_, err := client.Handshake(context.Background(), "bufnet")
			if !errors.Is(err, ErrRejected) {
				t.Fatalf("error=%v", err)
			}
			var rejection *RejectionError
			if !errors.As(err, &rejection) {
				t.Fatalf("error type=%T", err)
			}
			if rejection.Reason != test.reason {
				t.Fatalf("reason=%s", rejection.Reason)
			}
		})
	}
}

func TestHandshakeRejectsBlankDTMVersion(t *testing.T) {
	local := identity("node-a", "session-a")
	server, _ := NewServer(local)
	peer := identity("node-b", "session-b")
	peer.DTMVersion = " "
	response, err := server.Handshake(context.Background(), &dtmv1.HandshakeRequest{Runtime: toProto(peer)})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetAccepted() || response.GetRejectionReason() != dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_MALFORMED_IDENTITY {
		t.Fatalf("response=%+v", response)
	}
}

func TestHandshakeTimeoutAndCancellation(t *testing.T) {
	client, err := NewClient(identity("node-a", "session-a"), 20*time.Millisecond, func(ctx context.Context, _ string) (*grpc.ClientConn, error) { <-ctx.Done(); return nil, ctx.Err() })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Handshake(context.Background(), "blocked"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := client.Handshake(ctx, "blocked"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel = %v", err)
	}
}

func identity(node, session string) protocol.Identity {
	return protocol.Identity{MeshNamespace: "mesh-a", ProtocolMajor: 1, ProtocolMinor: 0, DTMVersion: protocol.DTMVersion, NodeID: node, RuntimeInstance: session, ControlEndpoint: "127.0.0.1:45893"}
}
func with(value protocol.Identity, change func(*protocol.Identity)) protocol.Identity {
	change(&value)
	return value
}
func testPair(t *testing.T, local, peer protocol.Identity) (*Client, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	service, err := NewServer(peer)
	if err != nil {
		t.Fatal(err)
	}
	dtmv1.RegisterRuntimeControlServiceServer(server, service)
	go server.Serve(listener)
	dial := func(ctx context.Context, _ string) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, "passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	client, err := NewClient(local, time.Second, dial)
	if err != nil {
		t.Fatal(err)
	}
	return client, func() { server.Stop(); listener.Close() }
}

func testPairUnchecked(t *testing.T, local, peer protocol.Identity) (*Client, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	dtmv1.RegisterRuntimeControlServiceServer(server, &Server{local: peer})
	go server.Serve(listener)
	dial := func(ctx context.Context, _ string) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, "passthrough:///bufnet", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	client, err := NewClient(local, time.Second, dial)
	if err != nil {
		t.Fatal(err)
	}
	return client, func() { server.Stop(); listener.Close() }
}
