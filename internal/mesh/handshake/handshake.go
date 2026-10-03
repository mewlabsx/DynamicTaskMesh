package handshake

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/mesh/protocol"
	"dtm/internal/mesh/resourceview"
	"dtm/internal/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const DefaultTimeout = 2 * time.Second

var ErrRejected = errors.New("runtime handshake rejected")

type RejectionError struct {
	Reason  dtmv1.HandshakeRejectionReason
	Message string
}

func (rejection *RejectionError) Error() string {
	if rejection.Message == "" {
		return fmt.Sprintf("%s: %s", ErrRejected, rejection.Reason)
	}
	return fmt.Sprintf("%s: %s: %s", ErrRejected, rejection.Reason, rejection.Message)
}

func (*RejectionError) Unwrap() error { return ErrRejected }

type Result struct {
	Identity protocol.Identity
}

type Server struct {
	dtmv1.UnimplementedRuntimeControlServiceServer
	local     protocol.Identity
	resources []resourceview.Descriptor
	statusMu  sync.RWMutex
	status    RuntimeStatusHandler
}

// RuntimeStatusHandler is installed by RuntimeHost before its execution
// server starts serving. It keeps the additive status RPC on the existing
// RuntimeControlService without attempting to register a service after Serve.
type RuntimeStatusHandler func(context.Context, *dtmv1.GetRuntimeStatusRequest) (*dtmv1.GetRuntimeStatusResponse, error)

func NewServer(local protocol.Identity, resources ...resourceview.Descriptor) (*Server, error) {
	if err := local.Validate(); err != nil {
		return nil, err
	}
	copy := make([]resourceview.Descriptor, len(resources))
	for i, descriptor := range resources {
		if descriptor.Validate() != nil {
			return nil, resourceview.ErrInvalidAdvertisement
		}
		copy[i] = descriptor.Clone()
	}
	return &Server{local: local, resources: copy}, nil
}

func (server *Server) SetRuntimeStatusHandler(handler RuntimeStatusHandler) {
	server.statusMu.Lock()
	server.status = handler
	server.statusMu.Unlock()
}

func (server *Server) GetResourceAdvertisement(_ context.Context, request *dtmv1.GetResourceAdvertisementRequest) (*dtmv1.GetResourceAdvertisementResponse, error) {
	requester := fromProto(request.GetRequester())
	if reason, message := validatePeer(server.local, requester); reason != dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_UNSPECIFIED {
		return nil, status.Error(codes.InvalidArgument, message)
	}
	resources := make([]*dtmv1.MeshResourceDescriptor, len(server.resources))
	for i, descriptor := range server.resources {
		resources[i] = resourceToProto(descriptor)
	}
	return &dtmv1.GetResourceAdvertisementResponse{Owner: toProto(server.local), Resources: resources}, nil
}

func (server *Server) Handshake(_ context.Context, request *dtmv1.HandshakeRequest) (*dtmv1.HandshakeResponse, error) {
	peer := fromProto(request.GetRuntime())
	reason, message := validatePeer(server.local, peer)
	return &dtmv1.HandshakeResponse{
		Runtime: toProto(server.local), Accepted: reason == dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_UNSPECIFIED,
		RejectionReason: reason, RejectionMessage: message,
	}, nil
}

func (server *Server) GetRuntimeStatus(ctx context.Context, request *dtmv1.GetRuntimeStatusRequest) (*dtmv1.GetRuntimeStatusResponse, error) {
	server.statusMu.RLock()
	handler := server.status
	server.statusMu.RUnlock()
	if handler == nil {
		return nil, status.Error(codes.Unimplemented, "runtime status is not configured")
	}
	return handler(ctx, request)
}

type DialFunc func(context.Context, string) (*grpc.ClientConn, error)

type Client struct {
	local   protocol.Identity
	dial    DialFunc
	timeout time.Duration
}

func NewClient(local protocol.Identity, timeout time.Duration, dial DialFunc) (*Client, error) {
	if err := local.Validate(); err != nil || timeout <= 0 {
		return nil, errors.New("invalid handshake client configuration")
	}
	if dial == nil {
		dial = func(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
			return grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		}
	}
	return &Client{local: local, dial: dial, timeout: timeout}, nil
}

func (client *Client) Handshake(ctx context.Context, endpoint string) (Result, error) {
	callCtx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	connection, err := client.dial(callCtx, endpoint)
	if err != nil {
		return Result{}, err
	}
	defer connection.Close()
	response, err := dtmv1.NewRuntimeControlServiceClient(connection).Handshake(callCtx, &dtmv1.HandshakeRequest{Runtime: toProto(client.local)})
	if err != nil {
		return Result{}, err
	}
	if !response.GetAccepted() {
		return Result{}, &RejectionError{Reason: response.GetRejectionReason(), Message: response.GetRejectionMessage()}
	}
	peer := fromProto(response.GetRuntime())
	if reason, _ := validatePeer(client.local, peer); reason != dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_UNSPECIFIED {
		return Result{}, &RejectionError{Reason: reason, Message: "invalid handshake response"}
	}
	return Result{Identity: peer}, nil
}

func (client *Client) GetResourceAdvertisement(ctx context.Context, endpoint string) (resourceview.Advertisement, error) {
	callCtx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	connection, err := client.dial(callCtx, endpoint)
	if err != nil {
		return resourceview.Advertisement{}, err
	}
	defer connection.Close()
	response, err := dtmv1.NewRuntimeControlServiceClient(connection).GetResourceAdvertisement(callCtx, &dtmv1.GetResourceAdvertisementRequest{Requester: toProto(client.local)})
	if err != nil {
		return resourceview.Advertisement{}, err
	}
	owner := fromProto(response.GetOwner())
	if reason, _ := validatePeer(client.local, owner); reason != dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_UNSPECIFIED {
		return resourceview.Advertisement{}, &RejectionError{Reason: reason, Message: "invalid resource advertisement owner"}
	}
	resources := make([]resourceview.Descriptor, len(response.GetResources()))
	for i, wire := range response.GetResources() {
		descriptor, convertErr := resourceFromProto(wire)
		if convertErr != nil {
			return resourceview.Advertisement{}, convertErr
		}
		resources[i] = descriptor
	}
	return resourceview.Advertisement{Owner: owner, Resources: resources}, nil
}

func (client *Client) GetRuntimeStatus(ctx context.Context, endpoint string) (*dtmv1.GetRuntimeStatusResponse, error) {
	callCtx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()
	connection, err := client.dial(callCtx, endpoint)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return dtmv1.NewRuntimeControlServiceClient(connection).GetRuntimeStatus(callCtx, &dtmv1.GetRuntimeStatusRequest{Requester: toProto(client.local)})
}

func resourceToProto(descriptor resourceview.Descriptor) *dtmv1.MeshResourceDescriptor {
	operations := make([]string, len(descriptor.Operations))
	for i, operation := range descriptor.Operations {
		operations[i] = operation.String()
	}
	attributes := make(map[string]string, len(descriptor.Attributes))
	for key, value := range descriptor.Attributes {
		attributes[key] = value
	}
	return &dtmv1.MeshResourceDescriptor{ResourceId: descriptor.ID.String(), Kind: descriptor.Kind.String(), Type: descriptor.Type.String(), OperationIds: operations, Attributes: attributes}
}
func resourceFromProto(wire *dtmv1.MeshResourceDescriptor) (resourceview.Descriptor, error) {
	if wire == nil {
		return resourceview.Descriptor{}, resourceview.ErrInvalidAdvertisement
	}
	descriptor := resourceview.Descriptor{ID: model.ResourceID(wire.GetResourceId()), Kind: model.ResourceKind(wire.GetKind()), Type: model.ResourceType(wire.GetType()), Operations: make([]model.OperationID, len(wire.GetOperationIds())), Attributes: map[string]string{}}
	for i, operation := range wire.GetOperationIds() {
		descriptor.Operations[i] = model.OperationID(operation)
	}
	for key, value := range wire.GetAttributes() {
		descriptor.Attributes[key] = value
	}
	if err := descriptor.Validate(); err != nil {
		return resourceview.Descriptor{}, err
	}
	return descriptor, nil
}

func validatePeer(local, peer protocol.Identity) (dtmv1.HandshakeRejectionReason, string) {
	if strings.TrimSpace(peer.DTMVersion) == "" || strings.TrimSpace(peer.NodeID) == "" || strings.TrimSpace(peer.RuntimeInstance) == "" {
		return dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_MALFORMED_IDENTITY, "DTM version, node and runtime identities are required"
	}
	if local.MeshNamespace != peer.MeshNamespace {
		return dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_NAMESPACE_MISMATCH, "mesh namespace mismatch"
	}
	if local.ProtocolMajor != peer.ProtocolMajor {
		return dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_PROTOCOL_MAJOR_INCOMPATIBLE, "protocol major mismatch"
	}
	if local.SameSession(peer) {
		return dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_SELF_SESSION, "self session"
	}
	host, portText, err := net.SplitHostPort(peer.ControlEndpoint)
	port, portErr := strconv.Atoi(portText)
	if err != nil || strings.TrimSpace(host) == "" || portErr != nil || port < 1 || port > 65535 {
		return dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_INVALID_ENDPOINT, "invalid advertised control endpoint"
	}
	return dtmv1.HandshakeRejectionReason_HANDSHAKE_REJECTION_REASON_UNSPECIFIED, ""
}

func toProto(identity protocol.Identity) *dtmv1.RuntimeIdentity {
	return &dtmv1.RuntimeIdentity{MeshNamespace: identity.MeshNamespace, ProtocolMajor: identity.ProtocolMajor, ProtocolMinor: identity.ProtocolMinor, DtmVersion: identity.DTMVersion, NodeId: identity.NodeID, RuntimeInstanceId: identity.RuntimeInstance, AdvertisedControlEndpoint: identity.ControlEndpoint}
}

func fromProto(identity *dtmv1.RuntimeIdentity) protocol.Identity {
	if identity == nil {
		return protocol.Identity{}
	}
	return protocol.Identity{MeshNamespace: identity.GetMeshNamespace(), ProtocolMajor: identity.GetProtocolMajor(), ProtocolMinor: identity.GetProtocolMinor(), DTMVersion: identity.GetDtmVersion(), NodeID: identity.GetNodeId(), RuntimeInstance: identity.GetRuntimeInstanceId(), ControlEndpoint: identity.GetAdvertisedControlEndpoint()}
}
