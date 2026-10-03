package grpcapi

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/capability"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/planner"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestRegistryServiceOverGRPC(t *testing.T) {
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	service, err := NewRegistryServer(registry, endpoints)
	if err != nil {
		t.Fatal(err)
	}
	client := newBufconnRegistryClient(t, service)
	ctx := context.Background()

	response, err := client.RegisterNode(ctx, validRegisterRequest())
	if err != nil {
		t.Fatalf("RegisterNode() error = %v", err)
	}
	if !response.GetAccepted() {
		t.Fatalf("RegisterNode() response = %#v, want accepted", response)
	}
	if address, err := endpoints.Resolve(model.NodeID("node-1")); err != nil || address != "127.0.0.1:50061" {
		t.Fatalf("registered endpoint = %q, %v", address, err)
	}

	invalidRegistrationStatuses := []struct {
		name   string
		status dtmv1.NodeStatus
	}{
		{name: "default", status: dtmv1.NodeStatus_NODE_STATUS_UNSPECIFIED},
		{name: "offline", status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE},
		{name: "unknown", status: dtmv1.NodeStatus(99)},
	}
	for _, tt := range invalidRegistrationStatuses {
		t.Run("register "+tt.name+" status", func(t *testing.T) {
			_, err := client.RegisterNode(ctx, registerRequest(
				"invalid-"+tt.name,
				[]string{"temperature_sensor"},
				tt.status,
				"127.0.0.1:50062",
			))
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("RegisterNode() code = %v, want InvalidArgument (error %v)", status.Code(err), err)
			}
		})
	}

	invalidUpdateStatuses := []struct {
		name   string
		status dtmv1.NodeStatus
	}{
		{name: "default", status: dtmv1.NodeStatus_NODE_STATUS_UNSPECIFIED},
		{name: "online", status: dtmv1.NodeStatus_NODE_STATUS_ONLINE},
		{name: "unknown", status: dtmv1.NodeStatus(99)},
	}
	for _, tt := range invalidUpdateStatuses {
		t.Run("update "+tt.name+" status", func(t *testing.T) {
			_, err := client.UpdateNodeStatus(ctx, &dtmv1.UpdateNodeStatusRequest{
				NodeId:         "node-1",
				Status:         tt.status,
				RegistrationId: "registration-1",
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("UpdateNodeStatus() code = %v, want InvalidArgument (error %v)", status.Code(err), err)
			}
		})
	}

	offlineResponse, err := client.UpdateNodeStatus(ctx, &dtmv1.UpdateNodeStatusRequest{
		NodeId:         "node-1",
		Status:         dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		RegistrationId: "registration-1",
	})
	if err != nil {
		t.Fatalf("UpdateNodeStatus() error = %v", err)
	}
	if !offlineResponse.GetAccepted() {
		t.Fatalf("UpdateNodeStatus() response = %#v, want accepted", offlineResponse)
	}
	if _, err := endpoints.Resolve(model.NodeID("node-1")); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("Resolve() after offline error = %v, want ErrEndpointNotFound", err)
	}

	_, err = client.UpdateNodeStatus(ctx, &dtmv1.UpdateNodeStatusRequest{
		NodeId:         "missing",
		Status:         dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		RegistrationId: "registration-missing",
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("UpdateNodeStatus() unknown node code = %v, want NotFound (error %v)", status.Code(err), err)
	}
}

func TestRegistryServiceRejectsOldRegistrationOfflineOverGRPC(t *testing.T) {
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	service, err := NewRegistryServer(registry, endpoints)
	if err != nil {
		t.Fatal(err)
	}
	client := newBufconnRegistryClient(t, service)
	ctx := context.Background()

	first := registerRequest("node-1", []string{"capability-a"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "address-a")
	first.RegistrationId = "registration-old"
	if _, err := client.RegisterNode(ctx, first); err != nil {
		t.Fatal(err)
	}
	if _, err := client.UpdateNodeStatus(ctx, &dtmv1.UpdateNodeStatusRequest{NodeId: "node-1", RegistrationId: "registration-old", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	second := registerRequest("node-1", []string{"capability-b"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "address-b")
	second.RegistrationId = "registration-new"
	if _, err := client.RegisterNode(ctx, second); err != nil {
		t.Fatal(err)
	}

	response, err := client.UpdateNodeStatus(ctx, &dtmv1.UpdateNodeStatusRequest{
		NodeId:         "node-1",
		Status:         dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		RegistrationId: "registration-old",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old UpdateNodeStatus() code = %v, want FailedPrecondition (error %v)", status.Code(err), err)
	}
	if response != nil {
		t.Fatalf("old UpdateNodeStatus() response = %#v, want nil", response)
	}
	if address, err := endpoints.Resolve(model.NodeID("node-1")); err != nil || address != "address-b" {
		t.Fatalf("replacement endpoint = %q, %v; want address-b", address, err)
	}
	if got := registry.Discover(model.Capability("capability-b")); len(got) != 1 || got[0].ID() != model.NodeID("node-1") {
		t.Fatalf("replacement online nodes = %v", got)
	}

	response, err = client.UpdateNodeStatus(ctx, &dtmv1.UpdateNodeStatusRequest{
		NodeId:         "node-1",
		Status:         dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		RegistrationId: "registration-new",
	})
	if err != nil || response == nil || !response.GetAccepted() {
		t.Fatalf("new UpdateNodeStatus() response = %#v, error = %v", response, err)
	}
}

func TestRegistryHeartbeatRenewsLeaseAndTimeoutMarksNodeOffline(t *testing.T) {
	now := time.Unix(100, 0)
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	service, err := NewRegistryServer(
		registry,
		endpoints,
		WithLeaseTTL(5*time.Second),
		WithRegistryClock(func() time.Time { return now }),
	)
	if err != nil {
		t.Fatal(err)
	}
	client := newBufconnRegistryClient(t, service)
	ctx := context.Background()

	if _, err := client.RegisterNode(ctx, validRegisterRequest()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(4 * time.Second)
	heartbeat, err := client.Heartbeat(ctx, &dtmv1.HeartbeatRequest{
		NodeId:           "node-1",
		RegistrationId:   "registration-1",
		SentAtUnixMillis: 1,
	})
	if err != nil {
		t.Fatalf("Heartbeat() error = %v", err)
	}
	if heartbeat.GetStatus() != dtmv1.NodeStatus_NODE_STATUS_ONLINE ||
		heartbeat.GetLeaseExpiresUnixMillis() != now.Add(5*time.Second).UnixMilli() {
		t.Fatalf("Heartbeat() response = %+v", heartbeat)
	}

	if count, err := service.SweepExpired(now.Add(4 * time.Second)); err != nil || count != 0 {
		t.Fatalf("SweepExpired() before renewed TTL = %d, %v", count, err)
	}
	if count, err := service.SweepExpired(now.Add(5 * time.Second)); err != nil || count != 1 {
		t.Fatalf("SweepExpired() at renewed TTL = %d, %v", count, err)
	}
	if got := registry.Discover("temperature_sensor"); len(got) != 0 {
		t.Fatalf("Discover() after lease expiry = %v, want offline", got)
	}
	if _, err := endpoints.Resolve("node-1"); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("Resolve() after lease expiry error = %v", err)
	}
}

func TestExpiredLeaseCannotBeMappedBeforeSweep(t *testing.T) {
	now := time.Unix(100, 0)
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	service, err := NewRegistryServer(
		registry,
		endpoints,
		WithLeaseTTL(5*time.Second),
		WithRegistryClock(func() time.Time { return now }),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RegisterNode(context.Background(), validRegisterRequest()); err != nil {
		t.Fatal(err)
	}
	meshMapper, err := mapper.New(registry, mapper.WithEligibility(service))
	if err != nil {
		t.Fatal(err)
	}
	plan := planner.Plan{
		TaskID: "task-1",
		Steps: []planner.Step{{
			ID:         "step-1",
			Capability: "temperature_sensor",
		}},
	}
	if _, err := meshMapper.Map(plan); err != nil {
		t.Fatalf("Map() with valid lease error = %v", err)
	}

	now = now.Add(5 * time.Second)
	if got := registry.Discover("temperature_sensor"); len(got) != 1 {
		t.Fatalf("registry before sweep = %v, want active snapshot", got)
	}
	if _, err := meshMapper.Map(plan); !errors.Is(err, mapper.ErrCapabilityUnavailable) {
		t.Fatalf("Map() at lease expiry error = %v, want %v", err, mapper.ErrCapabilityUnavailable)
	}
}

func TestRegistryHeartbeatRejectsMissingAndStaleRegistration(t *testing.T) {
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	service, err := NewRegistryServer(registry, endpoints)
	if err != nil {
		t.Fatal(err)
	}
	client := newBufconnRegistryClient(t, service)
	ctx := context.Background()

	if _, err := client.Heartbeat(ctx, &dtmv1.HeartbeatRequest{NodeId: "missing", RegistrationId: "registration"}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing Heartbeat() code = %v, error = %v", status.Code(err), err)
	}
	if _, err := client.RegisterNode(ctx, validRegisterRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.UpdateNodeStatus(ctx, &dtmv1.UpdateNodeStatusRequest{NodeId: "node-1", RegistrationId: "registration-1", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	replacement := validRegisterRequest()
	replacement.RegistrationId = "registration-new"
	if _, err := client.RegisterNode(ctx, replacement); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Heartbeat(ctx, &dtmv1.HeartbeatRequest{
		NodeId:         "node-1",
		RegistrationId: "registration-1",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old Heartbeat() code = %v, error = %v", status.Code(err), err)
	}
	if _, err := client.Heartbeat(ctx, &dtmv1.HeartbeatRequest{NodeId: "node-1"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid Heartbeat() code = %v, error = %v", status.Code(err), err)
	}
}

func newBufconnRegistryClient(t *testing.T, service dtmv1.NodeRegistryServiceServer) dtmv1.NodeRegistryServiceClient {
	t.Helper()

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	dtmv1.RegisterNodeRegistryServiceServer(grpcServer, service)
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		_ = listener.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		grpcServer.Stop()
		_ = listener.Close()
	})
	return dtmv1.NewNodeRegistryServiceClient(connection)
}
