package main

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/capability"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/transport/grpcapi"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type scriptedReregistrationClient struct {
	mu             sync.Mutex
	registerErrors []error
	registrations  chan string
	registerIDs    []string
	heartbeatIDs   []string
	offlineRequest *dtmv1.UpdateNodeStatusRequest
}

func (client *scriptedReregistrationClient) RegisterNode(_ context.Context, request *dtmv1.RegisterNodeRequest, _ ...grpc.CallOption) (*dtmv1.RegisterNodeResponse, error) {
	client.registrations <- request.GetRegistrationId()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.registerIDs = append(client.registerIDs, request.GetRegistrationId())
	if len(client.registerErrors) > 0 {
		err := client.registerErrors[0]
		client.registerErrors = client.registerErrors[1:]
		if err != nil {
			return nil, err
		}
	}
	return &dtmv1.RegisterNodeResponse{Accepted: true}, nil
}

func (client *scriptedReregistrationClient) Heartbeat(_ context.Context, request *dtmv1.HeartbeatRequest, _ ...grpc.CallOption) (*dtmv1.HeartbeatResponse, error) {
	client.mu.Lock()
	client.heartbeatIDs = append(client.heartbeatIDs, request.GetRegistrationId())
	client.mu.Unlock()
	return nil, status.Error(codes.NotFound, "runtime lease missing")
}

func (client *scriptedReregistrationClient) UpdateNodeStatus(_ context.Context, request *dtmv1.UpdateNodeStatusRequest, _ ...grpc.CallOption) (*dtmv1.UpdateNodeStatusResponse, error) {
	client.mu.Lock()
	client.offlineRequest = request
	client.mu.Unlock()
	return &dtmv1.UpdateNodeStatusResponse{Accepted: true}, nil
}

func registrationRequestForAgentTest(registrationID string) *dtmv1.RegisterNodeRequest {
	return &dtmv1.RegisterNodeRequest{
		Node:           &dtmv1.Node{Id: "agent-fenced", Capabilities: []string{"temperature_sensor"}, Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, ExecutionAddress: "endpoint-agent"},
		RegistrationId: registrationID,
	}
}

func runReregistrationLoopForTest(t *testing.T, client dtmv1.NodeRegistryServiceClient, registrationID string) (<-chan error, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runHeartbeatLoop(ctx, client, "agent-fenced", registrationID, time.Millisecond, registrationRequestForAgentTest(registrationID))
	}()
	return done, cancel
}

func TestAgentStopsWhenReregistrationReturnsFailedPrecondition(t *testing.T) {
	client := &scriptedReregistrationClient{registerErrors: []error{status.Error(codes.FailedPrecondition, "owned by another registration")}, registrations: make(chan string, 4)}
	done, cancel := runReregistrationLoopForTest(t, client, "registration-old")
	defer cancel()
	select {
	case err := <-done:
		if status.Code(errors.Unwrap(err)) != codes.FailedPrecondition {
			t.Fatalf("runHeartbeatLoop() error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Agent did not stop on FailedPrecondition")
	}
	assertTerminalReregistrationCalls(t, client, "registration-old")
}

func TestAgentStopsWhenReregistrationRequestIsInvalid(t *testing.T) {
	client := &scriptedReregistrationClient{registerErrors: []error{status.Error(codes.InvalidArgument, "invalid registration")}, registrations: make(chan string, 4)}
	done, cancel := runReregistrationLoopForTest(t, client, "registration-invalid")
	defer cancel()
	select {
	case err := <-done:
		if status.Code(errors.Unwrap(err)) != codes.InvalidArgument {
			t.Fatalf("runHeartbeatLoop() error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Agent did not stop on InvalidArgument")
	}
	assertTerminalReregistrationCalls(t, client, "registration-invalid")
}

func assertTerminalReregistrationCalls(t *testing.T, client *scriptedReregistrationClient, registrationID string) {
	t.Helper()
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.heartbeatIDs) != 1 || len(client.registerIDs) != 1 || client.heartbeatIDs[0] != registrationID || client.registerIDs[0] != registrationID {
		t.Fatalf("terminal recovery calls: heartbeats=%v registrations=%v", client.heartbeatIDs, client.registerIDs)
	}
}

func TestAgentRetriesTransientReregistrationFailure(t *testing.T) {
	client := &scriptedReregistrationClient{registerErrors: []error{status.Error(codes.Unavailable, "core starting"), nil}, registrations: make(chan string, 8)}
	done, cancel := runReregistrationLoopForTest(t, client, "registration-retry")
	defer cancel()
	for index := 0; index < 2; index++ {
		select {
		case registrationID := <-client.registrations:
			if registrationID != "registration-retry" {
				t.Fatalf("registration ID=%q", registrationID)
			}
		case <-time.After(time.Second):
			t.Fatal("Agent did not retry transient registration failure")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("runHeartbeatLoop() after cancel error=%v", err)
	}
}

func TestAgentDoesNotRegenerateRegistrationIDDuringRecovery(t *testing.T) {
	client := &scriptedReregistrationClient{registerErrors: []error{
		status.Error(codes.Unavailable, "starting"),
		status.Error(codes.DeadlineExceeded, "busy"),
		status.Error(codes.Aborted, "retry"),
	}, registrations: make(chan string, 8)}
	done, cancel := runReregistrationLoopForTest(t, client, "registration-stable")
	defer cancel()
	for index := 0; index < 3; index++ {
		select {
		case registrationID := <-client.registrations:
			if registrationID != "registration-stable" {
				t.Fatalf("attempt %d registration ID=%q", index+1, registrationID)
			}
		case <-time.After(time.Second):
			t.Fatal("Agent did not continue recovery attempts")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("runHeartbeatLoop() after cancel error=%v", err)
	}
	client.mu.Lock()
	heartbeatIDs := append([]string(nil), client.heartbeatIDs...)
	registerIDs := append([]string(nil), client.registerIDs...)
	client.mu.Unlock()
	for _, ids := range [][]string{heartbeatIDs, registerIDs} {
		for _, registrationID := range ids {
			if registrationID != "registration-stable" {
				t.Fatalf("recovery IDs: heartbeats=%v registrations=%v", heartbeatIDs, registerIDs)
			}
		}
	}
}

type registryServerClient struct {
	server *grpcapi.RegistryServer
}

func (client registryServerClient) RegisterNode(ctx context.Context, request *dtmv1.RegisterNodeRequest, _ ...grpc.CallOption) (*dtmv1.RegisterNodeResponse, error) {
	return client.server.RegisterNode(ctx, request)
}

func (client registryServerClient) Heartbeat(ctx context.Context, request *dtmv1.HeartbeatRequest, _ ...grpc.CallOption) (*dtmv1.HeartbeatResponse, error) {
	return client.server.Heartbeat(ctx, request)
}

func (client registryServerClient) UpdateNodeStatus(ctx context.Context, request *dtmv1.UpdateNodeStatusRequest, _ ...grpc.CallOption) (*dtmv1.UpdateNodeStatusResponse, error) {
	return client.server.UpdateNodeStatus(ctx, request)
}

type blockedReregistrationClient struct {
	registryServerClient
	registrationID string
	entered        chan struct{}
	release        chan struct{}
	mu             sync.Mutex
	registerCalls  int
}

func (client *blockedReregistrationClient) RegisterNode(ctx context.Context, request *dtmv1.RegisterNodeRequest, options ...grpc.CallOption) (*dtmv1.RegisterNodeResponse, error) {
	client.mu.Lock()
	client.registerCalls++
	call := client.registerCalls
	client.mu.Unlock()
	if request.GetRegistrationId() == client.registrationID && call == 1 {
		close(client.entered)
		select {
		case <-client.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return client.registryServerClient.RegisterNode(ctx, request, options...)
}

func TestAutomaticReregistrationCannotReplaceNewOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	initial := newPersistentRegistryBackend(t, path, nil)
	requestA := registrationRequestForAgentTest("registration-a")
	requestA.Node.Id = "automatic-race"
	requestA.Node.ExecutionAddress = "endpoint-a"
	if _, err := initial.server.RegisterNode(context.Background(), requestA); err != nil {
		t.Fatal(err)
	}
	if err := initial.repository.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := newPersistentRegistryBackend(t, path, nil)
	defer restarted.repository.Close()
	stale, err := restarted.repository.GetNode(context.Background(), "automatic-race")
	if err != nil {
		t.Fatal(err)
	}
	now := stale.UpdatedAt.Add(time.Second)
	restarted.registry = capability.NewRegistry()
	restarted.endpoints = grpcapi.NewEndpointDirectory()
	restarted.server, err = grpcapi.NewRegistryServer(
		restarted.registry,
		restarted.endpoints,
		grpcapi.WithLeaseTTL(250*time.Millisecond),
		grpcapi.WithRegistryClock(func() time.Time { return now }),
		grpcapi.WithNodeRepository(restarted.repository),
	)
	if err != nil {
		t.Fatal(err)
	}
	restarted.mapper, err = mapper.New(restarted.registry, mapper.WithEligibility(restarted.server))
	if err != nil {
		t.Fatal(err)
	}
	client := &blockedReregistrationClient{
		registryServerClient: registryServerClient{server: restarted.server},
		registrationID:       "registration-a",
		entered:              make(chan struct{}),
		release:              make(chan struct{}),
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runHeartbeatLoop(ctx, client, "automatic-race", "registration-a", time.Millisecond, requestA)
	}()
	select {
	case <-client.entered:
	case <-time.After(time.Second):
		t.Fatal("automatic re-registration did not reach barrier")
	}
	requestB := registrationRequestForAgentTest("registration-b")
	requestB.Node.Id = "automatic-race"
	requestB.Node.ExecutionAddress = "endpoint-b"
	clientB := registryServerClient{server: restarted.server}
	if _, err := clientB.RegisterNode(context.Background(), requestB); err != nil {
		t.Fatal(err)
	}
	if _, err := clientB.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "automatic-race", RegistrationId: "registration-b"}); err != nil {
		t.Fatalf("new owner lease is not valid: %v", err)
	}
	before, err := restarted.repository.GetNode(context.Background(), "automatic-race")
	if err != nil || before.RegistrationID != "registration-b" || before.Generation != 2 || before.Status != node.StatusActive {
		t.Fatalf("new owner before delayed registration: record=%#v error=%v", before, err)
	}
	if address, err := restarted.endpoints.Resolve("automatic-race"); err != nil || address != "endpoint-b" {
		t.Fatalf("new owner endpoint before delayed registration=%q error=%v", address, err)
	}
	if _, err := restarted.mapper.Map(singleCapabilityPlan("automatic-race")); err != nil {
		t.Fatalf("new owner mapping before delayed registration=%v", err)
	}
	close(client.release)
	select {
	case err := <-done:
		if status.Code(errors.Unwrap(err)) != codes.FailedPrecondition {
			t.Fatalf("automatic re-registration error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("old Agent did not stop after ownership fencing")
	}
	record, err := restarted.repository.GetNode(context.Background(), "automatic-race")
	if err != nil || record.RegistrationID != "registration-b" || record.Generation != 2 || record.Status != node.StatusActive || !restarted.server.Eligible("automatic-race") {
		t.Fatalf("new owner after automatic race: record=%#v eligible=%v error=%v", record, restarted.server.Eligible("automatic-race"), err)
	}
	if address, err := restarted.endpoints.Resolve("automatic-race"); err != nil || address != "endpoint-b" {
		t.Fatalf("new owner endpoint after delayed registration=%q error=%v", address, err)
	}
	if _, err := restarted.mapper.Map(singleCapabilityPlan("automatic-race")); err != nil {
		t.Fatalf("new owner mapping after delayed registration=%v", err)
	}
	client.mu.Lock()
	registerCalls := client.registerCalls
	client.mu.Unlock()
	if registerCalls != 1 {
		t.Fatalf("old Agent registration calls=%d", registerCalls)
	}
}

func TestOldAgentBestEffortOfflineCannotAffectNewOwner(t *testing.T) {
	backend := newPersistentRegistryBackend(t, filepath.Join(t.TempDir(), "core.db"), nil)
	defer backend.repository.Close()
	requestA := registrationRequestForAgentTest("registration-a")
	requestA.Node.Id = "offline-fence"
	requestA.Node.ExecutionAddress = "endpoint-a"
	if _, err := backend.server.RegisterNode(context.Background(), requestA); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "offline-fence", RegistrationId: "registration-a", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	requestB := registrationRequestForAgentTest("registration-b")
	requestB.Node.Id = "offline-fence"
	requestB.Node.ExecutionAddress = "endpoint-b"
	if _, err := backend.server.RegisterNode(context.Background(), requestB); err != nil {
		t.Fatal(err)
	}
	if err := bestEffortOffline(registryServerClient{server: backend.server}, "offline-fence", "registration-a"); err != nil {
		t.Fatalf("bestEffortOffline()=%v", err)
	}
	record, err := backend.repository.GetNode(context.Background(), model.NodeID("offline-fence"))
	if err != nil || record.RegistrationID != "registration-b" || record.Generation != 2 || record.Status != node.StatusActive || !backend.server.Eligible("offline-fence") {
		t.Fatalf("new owner after stale cleanup: record=%#v eligible=%v error=%v", record, backend.server.Eligible("offline-fence"), err)
	}
}

var _ dtmv1.NodeRegistryServiceClient = (*scriptedReregistrationClient)(nil)
var _ dtmv1.NodeRegistryServiceClient = registryServerClient{}
var _ dtmv1.NodeRegistryServiceClient = (*blockedReregistrationClient)(nil)
