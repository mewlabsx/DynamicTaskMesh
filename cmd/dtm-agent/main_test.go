package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/capability"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/planner"
	sqliteplatform "dtm/internal/platform/sqlite"
	storageport "dtm/internal/storage"
	"dtm/internal/transport/grpcapi"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type recordingRegistry struct {
	dtmv1.UnimplementedNodeRegistryServiceServer
	registered        chan *dtmv1.Node
	heartbeat         chan *dtmv1.HeartbeatRequest
	offline           chan struct{}
	mu                sync.Mutex
	registrationError error
	heartbeatError    error
	offlineError      error
	advertisedAddress string
	probeError        error
	registrationID    string
	offlineID         string
	blockRegistration bool
	registrationEnded chan struct{}
}

func (r *recordingRegistry) RegisterNode(ctx context.Context, request *dtmv1.RegisterNodeRequest) (*dtmv1.RegisterNodeResponse, error) {
	r.mu.Lock()
	err := r.registrationError
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.advertisedAddress = request.GetNode().GetExecutionAddress()
	r.registrationID = request.GetRegistrationId()
	r.mu.Unlock()
	r.registered <- request.GetNode()
	r.mu.Lock()
	block := r.blockRegistration
	ended := r.registrationEnded
	r.mu.Unlock()
	if block {
		<-ctx.Done()
		close(ended)
		return nil, ctx.Err()
	}
	return &dtmv1.RegisterNodeResponse{Accepted: true}, nil
}
func (r *recordingRegistry) Heartbeat(_ context.Context, request *dtmv1.HeartbeatRequest) (*dtmv1.HeartbeatResponse, error) {
	select {
	case r.heartbeat <- request:
	default:
	}
	r.mu.Lock()
	err := r.heartbeatError
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &dtmv1.HeartbeatResponse{
		Status:                 dtmv1.NodeStatus_NODE_STATUS_ONLINE,
		LeaseExpiresUnixMillis: time.Now().Add(time.Second).UnixMilli(),
	}, nil
}

func TestRunReregistersWhenCoreLosesLeaseState(t *testing.T) {
	registry, coreAddress, stopCore := startRegistry(t)
	defer stopCore()
	agentAddress := reserveAddress(t)
	path := writeAgentConfig(t, "node:\n  id: agent-1\n  capabilities: [temperature_sensor]\ncore:\n  address: "+coreAddress+"\nserver:\n  address: "+agentAddress+"\n  advertised_address: "+agentAddress+"\nheartbeat:\n  interval: 20ms\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- run(ctx, []string{"-config", path}, &stdout, &stderr) }()
	select {
	case <-registry.registered:
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not initially register")
	}
	registry.mu.Lock()
	registry.heartbeatError = status.Error(codes.NotFound, "lease state lost")
	registry.mu.Unlock()

	select {
	case registered := <-registry.registered:
		if registered.GetId() != "agent-1" {
			t.Fatalf("re-registered node = %#v", registered)
		}
		registry.mu.Lock()
		registry.heartbeatError = nil
		registry.mu.Unlock()
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not re-register after heartbeat NotFound")
	}
	select {
	case code := <-done:
		t.Fatalf("agent stopped during re-registration with code %d", code)
	default:
	}
	cancel()
	select {
	case <-registry.offline:
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not deregister")
	}
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("run code = %d; stderr=%s", code, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not stop")
	}
}
func (r *recordingRegistry) UpdateNodeStatus(_ context.Context, request *dtmv1.UpdateNodeStatusRequest) (*dtmv1.UpdateNodeStatusResponse, error) {
	if request.GetStatus() == dtmv1.NodeStatus_NODE_STATUS_OFFLINE {
		r.mu.Lock()
		address := r.advertisedAddress
		r.offlineID = request.GetRegistrationId()
		r.mu.Unlock()
		probeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		connection, err := grpc.DialContext(probeCtx, address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err == nil {
			_, err = dtmv1.NewAgentExecutionServiceClient(connection).ExecuteStep(probeCtx, &dtmv1.ExecuteStepRequest{TaskId: "probe", Step: &dtmv1.MappedStep{StepId: "probe", NodeId: request.GetNodeId(), Capability: "temperature_sensor", Inputs: map[string]string{"operation": "read_temperature"}}})
			_ = connection.Close()
		}
		cancel()
		r.mu.Lock()
		r.probeError = err
		r.mu.Unlock()
		close(r.offline)
	}
	r.mu.Lock()
	err := r.offlineError
	r.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &dtmv1.UpdateNodeStatusResponse{Accepted: true}, nil
}

func TestRunServesRegistersAndStops(t *testing.T) {
	registry, coreAddress, stopCore := startRegistry(t)
	defer stopCore()
	agentAddress := reserveAddress(t)
	path := writeAgentConfig(t, "node:\n  id: agent-1\n  capabilities: [temperature_sensor]\ncore:\n  address: "+coreAddress+"\nserver:\n  address: "+agentAddress+"\n  advertised_address: "+agentAddress+"\nheartbeat:\n  interval: 20ms\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- run(ctx, []string{"-config", path}, &stdout, &stderr) }()
	var registered *dtmv1.Node
	select {
	case registered = <-registry.registered:
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not register")
	}
	if registered.GetId() != "agent-1" || registered.GetStatus() != dtmv1.NodeStatus_NODE_STATUS_ONLINE || registered.GetExecutionAddress() != agentAddress {
		t.Fatalf("registered node = %#v", registered)
	}
	select {
	case heartbeat := <-registry.heartbeat:
		registry.mu.Lock()
		registrationID := registry.registrationID
		registry.mu.Unlock()
		if heartbeat.GetNodeId() != "agent-1" ||
			heartbeat.GetRegistrationId() != registrationID ||
			heartbeat.GetSentAtUnixMillis() == 0 {
			t.Fatalf("Heartbeat() request = %+v", heartbeat)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not send heartbeat")
	}
	probeCtx, cancelProbe := context.WithTimeout(context.Background(), time.Second)
	defer cancelProbe()
	conn, err := grpc.DialContext(probeCtx, agentAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatalf("dial agent: %v", err)
	}
	defer conn.Close()
	if _, err := dtmv1.NewAgentExecutionServiceClient(conn).ExecuteStep(probeCtx, &dtmv1.ExecuteStepRequest{TaskId: "task-1", Step: &dtmv1.MappedStep{StepId: "step-1", NodeId: "agent-1", Capability: "temperature_sensor", Inputs: map[string]string{"operation": "read_temperature"}}}); err != nil {
		t.Fatalf("agent execution unavailable at registration: %v", err)
	}
	select {
	case <-done:
		t.Fatal("run exited before cancellation")
	default:
	}
	cancel()
	select {
	case <-registry.offline:
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not deregister offline")
	}
	registry.mu.Lock()
	probeErr := registry.probeError
	registrationID := registry.registrationID
	offlineID := registry.offlineID
	registry.mu.Unlock()
	if probeErr != nil {
		t.Fatalf("agent server was unavailable before offline registration: %v", probeErr)
	}
	assertRegistrationOwnership(t, registrationID, offlineID)
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("run code = %d; stderr=%s", code, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not stop")
	}
	if !strings.Contains(stdout.String(), "agent starting node_id=agent-1 capabilities=temperature_sensor") ||
		!strings.Contains(stdout.String(), "register success node_id=agent-1 capabilities=temperature_sensor advertise_address="+agentAddress+" core_address="+coreAddress) {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunReturnsOneWhenRegistrationFails(t *testing.T) {
	registry, coreAddress, stopCore := startRegistry(t)
	defer stopCore()
	registry.registrationError = context.DeadlineExceeded
	agentAddress := reserveAddress(t)
	path := writeAgentConfig(t, "node:\n  id: agent-1\n  capabilities: [temperature_sensor]\ncore:\n  address: "+coreAddress+"\nserver:\n  address: "+agentAddress+"\n  advertised_address: "+agentAddress+"\n")
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"-config", path}, &stdout, &stderr); code != 1 {
		t.Fatalf("run code = %d; stderr=%s", code, stderr.String())
	}
	probeCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	connection, err := grpc.DialContext(probeCtx, agentAddress, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err == nil {
		_ = connection.Close()
		t.Fatal("agent listener remains reachable after registration failure")
	}
}

func TestRunCompensatesOnlineRegistrationWhenCanceledRegisterReturnsError(t *testing.T) {
	registry, coreAddress, stopCore := startRegistry(t)
	defer stopCore()
	registry.mu.Lock()
	registry.blockRegistration = true
	registry.registrationEnded = make(chan struct{})
	registry.mu.Unlock()
	agentAddress := reserveAddress(t)
	path := writeAgentConfig(t, "node:\n  id: agent-1\n  capabilities: [temperature_sensor]\ncore:\n  address: "+coreAddress+"\nserver:\n  address: "+agentAddress+"\n  advertised_address: "+agentAddress+"\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- run(ctx, []string{"-config", path}, &stdout, &stderr) }()
	select {
	case <-registry.registered:
	case <-time.After(3 * time.Second):
		t.Fatal("core did not record agent online")
	}
	cancel()
	select {
	case <-registry.registrationEnded:
	case <-time.After(3 * time.Second):
		t.Fatal("register request was not canceled")
	}
	select {
	case <-registry.offline:
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not compensate registration with offline")
	}
	registry.mu.Lock()
	probeErr := registry.probeError
	registrationID := registry.registrationID
	offlineID := registry.offlineID
	registry.mu.Unlock()
	if probeErr != nil {
		t.Fatalf("agent server was unavailable during compensation: %v", probeErr)
	}
	assertRegistrationOwnership(t, registrationID, offlineID)
	select {
	case code := <-done:
		if code != 1 {
			t.Fatalf("run code = %d, want 1", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not stop")
	}
}

func TestRunReturnsOneWhenOfflineRegistrationFails(t *testing.T) {
	registry, coreAddress, stopCore := startRegistry(t)
	defer stopCore()
	registry.offlineError = context.DeadlineExceeded
	agentAddress := reserveAddress(t)
	path := writeAgentConfig(t, "node:\n  id: agent-1\n  capabilities: [temperature_sensor]\ncore:\n  address: "+coreAddress+"\nserver:\n  address: "+agentAddress+"\n  advertised_address: "+agentAddress+"\n")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() { done <- run(ctx, []string{"-config", path}, &stdout, &stderr) }()
	select {
	case <-registry.registered:
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not register")
	}
	cancel()
	select {
	case code := <-done:
		if code != 1 || stderr.Len() == 0 {
			t.Fatalf("run = %d, stderr=%q; want cleanup failure", code, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("run did not stop")
	}
}

func TestRunRequiresConfigFlag(t *testing.T) {
	var stderr bytes.Buffer
	if code := run(context.Background(), nil, &bytes.Buffer{}, &stderr); code != 2 {
		t.Fatalf("run code = %d", code)
	}
}

func TestRunRejectsRuntimeModeWithoutStartingStaticRegistration(t *testing.T) {
	address := reserveAddress(t)
	path := writeAgentConfig(t, "mode: runtime\nnode:\n  id: agent-1\n  capabilities: [temperature_sensor]\n  advertise_address: "+address+"\nserver:\n  listen_address: "+address+"\nmesh:\n  namespace: test-mesh\n  control:\n    listen: "+address+"\n    advertise: "+address+"\n")
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"-config", path}, &stdout, &stderr); code != 2 {
		t.Fatalf("run code = %d; stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "dtm-agent requires static mode") {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func startRegistry(t *testing.T) (*recordingRegistry, string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	registry := &recordingRegistry{
		registered: make(chan *dtmv1.Node, 1),
		heartbeat:  make(chan *dtmv1.HeartbeatRequest, 1),
		offline:    make(chan struct{}),
	}
	dtmv1.RegisterNodeRegistryServiceServer(server, registry)
	go server.Serve(listener)
	return registry, listener.Addr().String(), func() { server.Stop(); listener.Close() }
}
func reserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	return address
}
func writeAgentConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func assertRegistrationOwnership(t *testing.T, registrationID, offlineID string) {
	t.Helper()
	if registrationID == "" {
		t.Fatal("RegisterNode() registration ID is blank")
	}
	if len(registrationID) != 32 {
		t.Fatalf("RegisterNode() registration ID length = %d, want 32 hex characters", len(registrationID))
	}
	if _, err := hex.DecodeString(registrationID); err != nil {
		t.Fatalf("RegisterNode() registration ID = %q, want hex: %v", registrationID, err)
	}
	if offlineID != registrationID {
		t.Fatalf("UpdateNodeStatus() registration ID = %q, want registration token %q", offlineID, registrationID)
	}
}

type registryProxyCall struct {
	method         string
	registrationID string
	address        string
}

type switchingRegistryProxy struct {
	dtmv1.UnimplementedNodeRegistryServiceServer
	mu      sync.RWMutex
	backend dtmv1.NodeRegistryServiceServer
	calls   chan registryProxyCall
	server  *grpc.Server
	listen  net.Listener
	stop    sync.Once
}

func (proxy *switchingRegistryProxy) Close() {
	if proxy == nil {
		return
	}
	proxy.stop.Do(func() {
		proxy.server.Stop()
		_ = proxy.listen.Close()
	})
}

func (proxy *switchingRegistryProxy) setBackend(backend dtmv1.NodeRegistryServiceServer) {
	proxy.mu.Lock()
	proxy.backend = backend
	proxy.mu.Unlock()
}

func (proxy *switchingRegistryProxy) current() dtmv1.NodeRegistryServiceServer {
	proxy.mu.RLock()
	backend := proxy.backend
	proxy.mu.RUnlock()
	return backend
}

func (proxy *switchingRegistryProxy) RegisterNode(ctx context.Context, request *dtmv1.RegisterNodeRequest) (*dtmv1.RegisterNodeResponse, error) {
	proxy.calls <- registryProxyCall{method: "register", registrationID: request.GetRegistrationId(), address: request.GetNode().GetExecutionAddress()}
	backend := proxy.current()
	if backend == nil {
		return nil, status.Error(codes.Unavailable, "core starting")
	}
	return backend.RegisterNode(ctx, request)
}
func (proxy *switchingRegistryProxy) Heartbeat(ctx context.Context, request *dtmv1.HeartbeatRequest) (*dtmv1.HeartbeatResponse, error) {
	proxy.calls <- registryProxyCall{method: "heartbeat", registrationID: request.GetRegistrationId()}
	backend := proxy.current()
	if backend == nil {
		return nil, status.Error(codes.Unavailable, "core starting")
	}
	return backend.Heartbeat(ctx, request)
}
func (proxy *switchingRegistryProxy) UpdateNodeStatus(ctx context.Context, request *dtmv1.UpdateNodeStatusRequest) (*dtmv1.UpdateNodeStatusResponse, error) {
	proxy.calls <- registryProxyCall{method: "offline", registrationID: request.GetRegistrationId()}
	backend := proxy.current()
	if backend == nil {
		return nil, status.Error(codes.Unavailable, "core starting")
	}
	return backend.UpdateNodeStatus(ctx, request)
}

type persistentRegistryBackend struct {
	repository *sqliteplatform.Repository
	registry   *capability.Registry
	endpoints  *grpcapi.EndpointDirectory
	server     *grpcapi.RegistryServer
	mapper     *mapper.Mapper
}

func newPersistentRegistryBackend(t *testing.T, path string, repository grpcapi.NodeRepository) *persistentRegistryBackend {
	t.Helper()
	var database *sqliteplatform.Repository
	if repository == nil {
		var err error
		database, err = sqliteplatform.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		repository = database
	} else {
		var ok bool
		database, ok = repository.(*sqliteplatform.Repository)
		if !ok {
			if wrapper, wrapped := repository.(*agentInjectedNodeRepository); wrapped {
				database = wrapper.Repository
			}
		}
	}
	if database == nil {
		t.Fatal("persistent backend database is required")
	}
	if _, err := database.MarkNodesStale(context.Background(), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	registry := capability.NewRegistry()
	endpoints := grpcapi.NewEndpointDirectory()
	server, err := grpcapi.NewRegistryServer(registry, endpoints, grpcapi.WithLeaseTTL(250*time.Millisecond), grpcapi.WithNodeRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	meshMapper, err := mapper.New(registry, mapper.WithEligibility(server))
	if err != nil {
		t.Fatal(err)
	}
	return &persistentRegistryBackend{repository: database, registry: registry, endpoints: endpoints, server: server, mapper: meshMapper}
}

func startSwitchingRegistryProxy(t *testing.T, backend dtmv1.NodeRegistryServiceServer) (*switchingRegistryProxy, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	proxy := &switchingRegistryProxy{backend: backend, calls: make(chan registryProxyCall, 128), server: server, listen: listener}
	dtmv1.RegisterNodeRegistryServiceServer(server, proxy)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(proxy.Close)
	return proxy, listener.Addr().String()
}

func agentConfigForTest(t *testing.T, nodeID, coreAddress, agentAddress string) string {
	t.Helper()
	return writeAgentConfig(t, "node:\n  id: "+nodeID+"\n  capabilities: [temperature_sensor]\ncore:\n  address: "+coreAddress+"\nserver:\n  address: "+agentAddress+"\n  advertised_address: "+agentAddress+"\nheartbeat:\n  interval: 20ms\n")
}

func waitProxyCall(t *testing.T, proxy *switchingRegistryProxy, predicate func(registryProxyCall) bool) registryProxyCall {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case call := <-proxy.calls:
			if predicate(call) {
				return call
			}
		case <-deadline.C:
			t.Fatal("timed out waiting for registry call")
		}
	}
}

func waitForNode(t *testing.T, backend *persistentRegistryBackend, nodeID model.NodeID, predicate func(storageport.NodeRecord) bool) storageport.NodeRecord {
	t.Helper()
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		record, err := backend.repository.GetNode(context.Background(), nodeID)
		if err == nil && predicate(record) {
			return record
		}
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("timed out waiting for node %q, last error=%v record=%#v", nodeID, err, record)
		}
	}
}

func singleCapabilityPlan(nodeID string) planner.Plan {
	return planner.Plan{TaskID: model.TaskID("task-" + nodeID), Steps: []planner.Step{{ID: "step-1", Capability: "temperature_sensor"}}}
}

func TestAgentReregistersAfterCoreRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	first := newPersistentRegistryBackend(t, path, nil)
	proxy, coreAddress := startSwitchingRegistryProxy(t, first.server)
	agentAddress := reserveAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"-config", agentConfigForTest(t, "agent-restart", coreAddress, agentAddress)}, &stdout, &stderr)
	}()
	initialCall := waitProxyCall(t, proxy, func(call registryProxyCall) bool { return call.method == "register" })
	initial := waitForNode(t, first, "agent-restart", func(record storageport.NodeRecord) bool {
		return record.Generation == 1 && record.Status == node.StatusActive
	})
	if _, err := first.mapper.Map(singleCapabilityPlan("restart")); err != nil {
		t.Fatalf("Map() before restart = %v", err)
	}
	if err := first.repository.Close(); err != nil {
		t.Fatal(err)
	}
	proxy.setBackend(nil)
	second := newPersistentRegistryBackend(t, path, nil)
	defer second.repository.Close()
	proxy.setBackend(second.server)
	restartedCall := waitProxyCall(t, proxy, func(call registryProxyCall) bool { return call.method == "register" })
	if restartedCall.registrationID != initialCall.registrationID || restartedCall.registrationID != initial.RegistrationID {
		t.Fatalf("reregistration ID changed: initial=%q restarted=%q stored=%q", initialCall.registrationID, restartedCall.registrationID, initial.RegistrationID)
	}
	restarted := waitForNode(t, second, "agent-restart", func(record storageport.NodeRecord) bool {
		return record.Generation == 2 && record.Status == node.StatusActive
	})
	if restarted.RegistrationID != initial.RegistrationID {
		t.Fatalf("restarted record = %#v", restarted)
	}
	if _, err := second.mapper.Map(singleCapabilityPlan("restart")); err != nil {
		t.Fatalf("Map() after automatic reregistration = %v", err)
	}
	select {
	case code := <-done:
		t.Fatalf("agent exited during core restart with code %d, stderr=%s", code, stderr.String())
	default:
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("agent exit=%d stderr=%s", code, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not stop")
	}
}

func TestAgentStopsWhenRegistrationOwnershipChanges(t *testing.T) {
	directory, err := os.MkdirTemp("", "dtm-agent-ownership-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		var removeErr error
		for attempt := 0; attempt < 20; attempt++ {
			removeErr = os.RemoveAll(directory)
			if removeErr == nil {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Errorf("remove test directory: %v", removeErr)
	})
	path := filepath.Join(directory, "core.db")
	backend := newPersistentRegistryBackend(t, path, nil)
	// Register repository cleanup before the proxy cleanup below. Testing runs
	// cleanups in LIFO order, so the gRPC server stops before SQLite closes and
	// the TempDir remover runs only after both have released their handles.
	t.Cleanup(func() {
		if err := backend.repository.Close(); err != nil {
			t.Error(err)
		}
	})
	proxy, coreAddress := startSwitchingRegistryProxy(t, backend.server)
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()
	var outA, errA, outB, errB bytes.Buffer
	doneA, doneB := make(chan int, 1), make(chan int, 1)
	addressA, addressB := reserveAddress(t), reserveAddress(t)
	go func() {
		doneA <- run(ctxA, []string{"-config", agentConfigForTest(t, "shared-agent", coreAddress, addressA)}, &outA, &errA)
	}()
	callA := waitProxyCall(t, proxy, func(call registryProxyCall) bool { return call.method == "register" && call.address == addressA })
	waitForNode(t, backend, "shared-agent", func(record storageport.NodeRecord) bool { return record.RegistrationID == callA.registrationID })
	go func() {
		doneB <- run(ctxB, []string{"-config", agentConfigForTest(t, "shared-agent", coreAddress, addressB)}, &outB, &errB)
	}()
	callB := waitProxyCall(t, proxy, func(call registryProxyCall) bool { return call.method == "register" && call.address == addressB })
	if callA.registrationID == callB.registrationID {
		t.Fatal("independent agents reused registration ID")
	}
	select {
	case code := <-doneB:
		if code != 1 {
			t.Fatalf("conflicting agent exit=%d stderr=%s", code, errB.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("conflicting agent did not stop after ownership rejection")
	}
	current := waitForNode(t, backend, "shared-agent", func(record storageport.NodeRecord) bool {
		return record.RegistrationID == callA.registrationID && record.Status == node.StatusActive
	})
	if current.RegistrationID != callA.registrationID || current.Generation != 1 || !backend.server.Eligible("shared-agent") {
		t.Fatalf("current owner disturbed: %#v", current)
	}
	select {
	case code := <-doneA:
		t.Fatalf("current owner exited with code %d stderr=%s", code, errA.String())
	default:
	}
	cancelA()
	select {
	case code := <-doneA:
		if code != 0 {
			t.Fatalf("current owner exit=%d stderr=%s", code, errA.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("current owner did not stop")
	}
	// Close the proxy and repository before returning so Windows TempDir cleanup
	// never races a gRPC handler or SQLite connection under race instrumentation.
	proxy.Close()
	if err := backend.repository.Close(); err != nil {
		t.Fatal(err)
	}
}

type agentInjectedNodeRepository struct {
	*sqliteplatform.Repository
	mu        sync.Mutex
	failRenew bool
}

func (repository *agentInjectedNodeRepository) RenewNodeLease(ctx context.Context, request storageport.RenewNodeLeaseRequest) error {
	repository.mu.Lock()
	fail := repository.failRenew
	if fail {
		repository.failRenew = false
	}
	repository.mu.Unlock()
	if fail {
		return errors.New("injected heartbeat persistence failure")
	}
	return repository.Repository.RenewNodeLease(ctx, request)
}

func TestAgentRecoversAfterHeartbeatPersistenceFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	database, err := sqliteplatform.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	injected := &agentInjectedNodeRepository{Repository: database}
	backend := newPersistentRegistryBackend(t, path, injected)
	defer backend.repository.Close()
	proxy, coreAddress := startSwitchingRegistryProxy(t, backend.server)
	agentAddress := reserveAddress(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- run(ctx, []string{"-config", agentConfigForTest(t, "agent-heartbeat-recovery", coreAddress, agentAddress)}, &stdout, &stderr)
	}()
	initialCall := waitProxyCall(t, proxy, func(call registryProxyCall) bool { return call.method == "register" })
	waitForNode(t, backend, "agent-heartbeat-recovery", func(record storageport.NodeRecord) bool {
		return record.Generation == 1 && record.Status == node.StatusActive
	})
	injected.mu.Lock()
	injected.failRenew = true
	injected.mu.Unlock()
	waitProxyCall(t, proxy, func(call registryProxyCall) bool {
		return call.method == "heartbeat" && call.registrationID == initialCall.registrationID
	})
	reregister := waitProxyCall(t, proxy, func(call registryProxyCall) bool { return call.method == "register" })
	if reregister.registrationID != initialCall.registrationID {
		t.Fatalf("heartbeat recovery changed registration ID: %q -> %q", initialCall.registrationID, reregister.registrationID)
	}
	recovered := waitForNode(t, backend, "agent-heartbeat-recovery", func(record storageport.NodeRecord) bool {
		return record.Generation == 2 && record.Status == node.StatusActive
	})
	if recovered.RegistrationID != initialCall.registrationID || !backend.server.Eligible("agent-heartbeat-recovery") {
		t.Fatalf("heartbeat recovery record=%#v eligible=%v", recovered, backend.server.Eligible("agent-heartbeat-recovery"))
	}
	if _, err := backend.mapper.Map(singleCapabilityPlan("heartbeat-recovery")); err != nil {
		t.Fatalf("Map() after heartbeat recovery = %v", err)
	}
	select {
	case code := <-done:
		t.Fatalf("agent exited during heartbeat recovery with code %d stderr=%s", code, stderr.String())
	default:
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("agent exit=%d stderr=%s", code, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not stop")
	}
}

type coreStartingRegistry struct {
	dtmv1.UnimplementedNodeRegistryServiceServer
	mu       sync.Mutex
	register int
	calls    chan string
}

func (registry *coreStartingRegistry) RegisterNode(_ context.Context, request *dtmv1.RegisterNodeRequest) (*dtmv1.RegisterNodeResponse, error) {
	registry.mu.Lock()
	registry.register++
	attempt := registry.register
	registry.mu.Unlock()
	registry.calls <- request.GetRegistrationId()
	if attempt == 2 {
		return nil, status.Error(codes.Unavailable, "core migration still running")
	}
	return &dtmv1.RegisterNodeResponse{Accepted: true}, nil
}
func (registry *coreStartingRegistry) Heartbeat(context.Context, *dtmv1.HeartbeatRequest) (*dtmv1.HeartbeatResponse, error) {
	registry.mu.Lock()
	attempts := registry.register
	registry.mu.Unlock()
	if attempts < 3 {
		return nil, status.Error(codes.NotFound, "runtime lease missing")
	}
	return &dtmv1.HeartbeatResponse{Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, LeaseExpiresUnixMillis: time.Now().Add(time.Second).UnixMilli()}, nil
}
func (registry *coreStartingRegistry) UpdateNodeStatus(context.Context, *dtmv1.UpdateNodeStatusRequest) (*dtmv1.UpdateNodeStatusResponse, error) {
	return &dtmv1.UpdateNodeStatusResponse{Accepted: true}, nil
}

func TestAgentRetriesReregistrationWhileCoreIsStarting(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	registry := &coreStartingRegistry{calls: make(chan string, 8)}
	dtmv1.RegisterNodeRegistryServiceServer(server, registry)
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	defer listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	agentAddress := reserveAddress(t)
	go func() {
		done <- run(ctx, []string{"-config", agentConfigForTest(t, "agent-starting", listener.Addr().String(), agentAddress)}, &stdout, &stderr)
	}()
	ids := make([]string, 0, 3)
	for len(ids) < 3 {
		select {
		case id := <-registry.calls:
			ids = append(ids, id)
		case <-time.After(3 * time.Second):
			t.Fatal("agent did not retry registration")
		}
	}
	if ids[0] == "" || ids[0] != ids[1] || ids[1] != ids[2] {
		t.Fatalf("registration IDs across retry = %v", ids)
	}
	select {
	case code := <-done:
		t.Fatalf("agent exited during registration retry with code %d stderr=%s", code, stderr.String())
	default:
	}
	cancel()
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("agent exit=%d stderr=%s", code, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("agent did not stop")
	}
}
