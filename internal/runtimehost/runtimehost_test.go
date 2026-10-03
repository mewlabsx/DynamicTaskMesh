package runtimehost

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/agent"
	"dtm/internal/config"
	"dtm/internal/invocation"
	"dtm/internal/mapper"
	"dtm/internal/mesh/discovery"
	"dtm/internal/mesh/handshake"
	"dtm/internal/mesh/membership"
	"dtm/internal/mesh/protocol"
	"dtm/internal/mesh/resourceview"
	"dtm/internal/model"
	storageport "dtm/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type testHandler struct{ capability model.Capability }

func (handler testHandler) Capability() model.Capability { return handler.capability }
func (handler testHandler) Execute(context.Context, map[string]string) (map[string]any, error) {
	return map[string]any{"temperature": 23.5}, nil
}

func TestResourceExecutionCapabilityServesExistingAgentRPCAndCloses(t *testing.T) {
	capability, err := NewResourceExecutionCapability(
		executionConfig("node-a", ":memory:"),
		testHandler{capability: "temperature_sensor"},
	)
	if err != nil {
		t.Fatal(err)
	}
	if capability.Ready() {
		t.Fatal("constructed capability is ready before Start")
	}
	listener := bufconn.Listen(1024 * 1024)
	if err := capability.Start(listener); err != nil {
		t.Fatal(err)
	}
	connection, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	response, err := dtmv1.NewAgentExecutionServiceClient(connection).ExecuteStep(context.Background(), &dtmv1.ExecuteStepRequest{
		TaskId: "task-1",
		Step:   &dtmv1.MappedStep{StepId: "step-1", NodeId: "node-a", Capability: "temperature_sensor", Inputs: map[string]string{"operation": "read_temperature"}},
	})
	if err != nil || response.GetResult().GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED {
		t.Fatalf("ExecuteStep() = %#v, %v", response, err)
	}
	ref, err := model.NewResourceRef("resource-1", 1, "node-a", 1, "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	adapter := invocation.NewLegacyCapabilityAdapter()
	payload, err := adapter.EncodeRequest(mapper.MappedStep{
		ID: "step-native", Capability: "temperature_sensor", NodeID: "node-a", ResourceRef: ref,
		Inputs: map[string]string{"operation": "read_temperature"},
	}, "read_temperature")
	if err != nil {
		t.Fatal(err)
	}
	id, err := invocation.NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	nativeResponse, err := dtmv1.NewResourceInvocationServiceClient(connection).InvokeResource(context.Background(), &dtmv1.InvocationRequest{
		InvocationId: id.Bytes(),
		ResourceRef:  &dtmv1.ResourceRef{ResourceId: string(ref.ResourceID), ResourceGeneration: uint64(ref.ResourceGeneration), OwnerNodeId: string(ref.OwnerNodeID), OwnerNodeGeneration: ref.OwnerNodeGeneration, RegistrationId: ref.RegistrationID},
		OperationId:  "read_temperature", Payload: payload,
		Metadata: &dtmv1.InvocationMetadata{TaskId: "task-native", StepId: "step-native", Attempt: 1, IdempotencyKey: "native-key"},
	})
	_ = connection.Close()
	if err != nil || nativeResponse.GetResult() == nil {
		t.Fatalf("InvokeResource() = %#v, %v", nativeResponse, err)
	}
	if err := capability.Close(); err != nil {
		t.Fatal(err)
	}
	if capability.Ready() {
		t.Fatal("closed capability remains ready")
	}
	if err := capability.ExecutionRepository.Health(context.Background()); !errors.Is(err, storageport.ErrClosed) {
		t.Fatalf("repository health after close = %v, want closed", err)
	}
	if err := capability.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
}

func TestResourceExecutionCapabilityFailsClosedWhenRepositoryCannotOpen(t *testing.T) {
	_, err := NewResourceExecutionCapability(
		executionConfig("node-a", t.TempDir()),
		testHandler{capability: "temperature_sensor"},
	)
	if err == nil {
		t.Fatal("NewResourceExecutionCapability() error = nil")
	}
}

func TestCoreCapabilityLifecycle(t *testing.T) {
	capability, err := NewCoreCapability(context.Background(), coreConfig(":memory:"), CoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if native, ok := capability.InvocationTransport.(*invocation.NativeGrpcInvocationTransport); !ok || native.TransportProfile() != invocation.TransportProfileNative {
		t.Fatalf("core invocation transport = %T, want native grpc", capability.InvocationTransport)
	}
	if capability.Ready() {
		t.Fatal("constructed CoreCapability is ready before activation")
	}
	listener := bufconn.Listen(1024 * 1024)
	if err := capability.Activate(context.Background(), listener); err != nil {
		t.Fatal(err)
	}
	if !capability.Ready() {
		t.Fatal("activated CoreCapability is not ready")
	}
	duplicateListener := bufconn.Listen(1024)
	defer duplicateListener.Close()
	if err := capability.Activate(context.Background(), duplicateListener); !errors.Is(err, ErrCapabilityActive) {
		t.Fatalf("second Activate() = %v, want ErrCapabilityActive", err)
	}
	connection, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dtmv1.NewCoreServiceClient(connection).SubmitTask(context.Background(), nil); err == nil {
		t.Fatal("existing CoreService was not registered")
	}
	_ = connection.Close()
	if err := capability.Close(); err != nil {
		t.Fatal(err)
	}
	if capability.Ready() {
		t.Fatal("closed CoreCapability remains ready")
	}
	if err := capability.Repository.Health(context.Background()); !errors.Is(err, storageport.ErrClosed) {
		t.Fatalf("repository health after close = %v, want closed", err)
	}
	if err := capability.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
}

func TestCoreCapabilityConstructionFailureLeavesNoDatabaseLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	broken := coreConfig(path)
	broken.Retry.MaxAttempts = 0
	if _, err := NewCoreCapability(context.Background(), broken, CoreOptions{}); err == nil {
		t.Fatal("NewCoreCapability() error = nil")
	}
	recovered, err := NewCoreCapability(context.Background(), coreConfig(path), CoreOptions{})
	if err != nil {
		t.Fatalf("reopen after construction failure: %v", err)
	}
	if err := recovered.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeHostExecutionOnlyNeedsNoCoreAddress(t *testing.T) {
	execution, err := NewResourceExecutionCapability(executionConfig("node-a", ":memory:"), testHandler{capability: "temperature_sensor"})
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	host, err := New(Options{
		NodeID: "node-a", Execution: execution,
		ExecutionAddress: "execution", Listen: singleListener(listener),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	if !host.Ready() || host.CoreReady() || host.Core() != nil {
		t.Fatalf("execution-only readiness = host:%v core:%v value:%v", host.Ready(), host.CoreReady(), host.Core())
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeHostExplicitCoreActivationAndReverseCleanup(t *testing.T) {
	directory := t.TempDir()
	execution, err := NewResourceExecutionCapability(executionConfig("node-a", filepath.Join(directory, "agent.db")), testHandler{capability: "temperature_sensor"})
	if err != nil {
		t.Fatal(err)
	}
	executionListener := bufconn.Listen(1024 * 1024)
	coreListener := bufconn.Listen(1024 * 1024)
	listeners := []net.Listener{executionListener, coreListener}
	host, err := New(Options{
		NodeID: "node-a", Execution: execution, ExecutionAddress: "execution", CoreAddress: "core",
		Listen: func(string, string) (net.Listener, error) {
			listener := listeners[0]
			listeners = listeners[1:]
			return listener, nil
		},
		CoreFactory: func(ctx context.Context) (*CoreCapability, error) {
			return NewCoreCapability(ctx, coreConfig(filepath.Join(directory, "core.db")), CoreOptions{})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	if err := host.ActivateCore(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !host.Ready() || !host.CoreReady() {
		t.Fatalf("host/core ready = %v/%v", host.Ready(), host.CoreReady())
	}
	if err := host.ActivateCore(context.Background()); !errors.Is(err, ErrCapabilityActive) {
		t.Fatalf("second ActivateCore() = %v", err)
	}
	core := host.Core()
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	if host.Ready() || host.CoreReady() {
		t.Fatal("closed RuntimeHost remains ready")
	}
	if err := core.Repository.Health(context.Background()); !errors.Is(err, storageport.ErrClosed) {
		t.Fatalf("core repository after host close = %v", err)
	}
	if err := execution.ExecutionRepository.Health(context.Background()); !errors.Is(err, storageport.ErrClosed) {
		t.Fatalf("execution repository after host close = %v", err)
	}
	if err := host.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
}

func TestRuntimeHostCoreConstructionFailureCleansExecution(t *testing.T) {
	execution, err := NewResourceExecutionCapability(executionConfig("node-a", ":memory:"), testHandler{capability: "temperature_sensor"})
	if err != nil {
		t.Fatal(err)
	}
	host, err := New(Options{
		NodeID: "node-a", Execution: execution, ExecutionAddress: "execution",
		Listen:      singleListener(bufconn.Listen(1024 * 1024)),
		CoreFactory: func(context.Context) (*CoreCapability, error) { return nil, errors.New("core construction failed") },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	if err := host.ActivateCore(context.Background()); err == nil {
		t.Fatal("ActivateCore() error = nil")
	}
	if host.Ready() || host.CoreReady() {
		t.Fatal("failed RuntimeHost remains ready")
	}
	if err := execution.ExecutionRepository.Health(context.Background()); !errors.Is(err, storageport.ErrClosed) {
		t.Fatalf("execution repository after failure = %v", err)
	}
}

func TestRuntimeHostNilCoreFactoryResultCleansExecution(t *testing.T) {
	execution, err := NewResourceExecutionCapability(executionConfig("node-a", filepath.Join(t.TempDir(), "agent.db")), testHandler{capability: "temperature_sensor"})
	if err != nil {
		t.Fatal(err)
	}
	host, err := New(Options{
		NodeID: "node-a", Execution: execution, ExecutionAddress: "execution",
		Listen:      singleListener(bufconn.Listen(1024 * 1024)),
		CoreFactory: func(context.Context) (*CoreCapability, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	if err := host.ActivateCore(context.Background()); err == nil {
		t.Fatal("ActivateCore() error = nil")
	}
	if host.Ready() || host.CoreReady() {
		t.Fatal("failed RuntimeHost remains ready")
	}
}

func TestRuntimeHostRejectsConcurrentCoreActivation(t *testing.T) {
	execution, err := NewResourceExecutionCapability(executionConfig("node-a", filepath.Join(t.TempDir(), "agent.db")), testHandler{capability: "temperature_sensor"})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	host, err := New(Options{
		NodeID: "node-a", Execution: execution, ExecutionAddress: "execution", CoreAddress: "core",
		Listen: func(_ string, address string) (net.Listener, error) {
			return bufconn.Listen(1024 * 1024), nil
		},
		CoreFactory: func(ctx context.Context) (*CoreCapability, error) {
			close(entered)
			<-release
			return NewCoreCapability(ctx, coreConfig(":memory:"), CoreOptions{})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	activationResult := make(chan error, 1)
	go func() { activationResult <- host.ActivateCore(context.Background()) }()
	<-entered
	if err := host.ActivateCore(context.Background()); !errors.Is(err, ErrCapabilityActive) {
		t.Fatalf("concurrent ActivateCore() = %v, want ErrCapabilityActive", err)
	}
	close(release)
	if err := <-activationResult; err != nil {
		t.Fatal(err)
	}
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
}

func executionConfig(nodeID, path string) config.Agent {
	return config.Agent{
		Mode:    config.ModeRuntime,
		Node:    config.Node{ID: nodeID, Capabilities: []string{"temperature_sensor"}},
		Storage: config.Storage{Path: path},
	}
}

func coreConfig(path string) config.Core {
	return config.Core{
		Lease:   config.Lease{TTL: config.Duration{Duration: time.Second}, SweepInterval: config.Duration{Duration: time.Second}},
		Storage: config.Storage{Path: path},
		Retry:   config.Retry{MaxAttempts: 1, Backoff: config.Duration{Duration: time.Millisecond}},
	}
}

func singleListener(listener net.Listener) ListenFunc {
	return func(string, string) (net.Listener, error) { return listener, nil }
}

var _ agent.Handler = testHandler{}

func TestRuntimeHostRejectsMismatchedStableNodeIdentity(t *testing.T) {
	execution, err := NewResourceExecutionCapability(executionConfig("node-a", filepath.Join(t.TempDir(), "agent.db")), testHandler{capability: "temperature_sensor"})
	if err != nil {
		t.Fatal(err)
	}
	defer execution.Close()
	if _, err := New(Options{NodeID: "node-b", Execution: execution}); !errors.Is(err, ErrInvalidNodeIdentity) {
		t.Fatalf("New() = %v, want ErrInvalidNodeIdentity", err)
	}
}

func TestThreeRuntimeHostsObserveCompatiblePeerHandshakes(t *testing.T) {
	network := discovery.NewMemoryNetwork()
	type runtime struct {
		host     *RuntimeHost
		listener *bufconn.Listener
	}
	runtimes := make([]runtime, 0, 3)
	dialers := make(map[string]*bufconn.Listener)
	for index, nodeID := range []string{"node-a", "node-b", "node-c"} {
		address := fmt.Sprintf("127.0.0.1:%d", 45901+index)
		listener := bufconn.Listen(1024 * 1024)
		dialers[address] = listener
		execution, err := NewResourceExecutionCapability(executionConfig(nodeID, filepath.Join(t.TempDir(), nodeID+".db")), testHandler{capability: "temperature_sensor"})
		if err != nil {
			_ = execution.Close()
			t.Fatal(err)
		}
		identity := protocol.Identity{MeshNamespace: "mesh-a", ProtocolMajor: 1, ProtocolMinor: uint32(index), DTMVersion: protocol.DTMVersion, NodeID: nodeID, RuntimeInstance: "session-" + nodeID, ControlEndpoint: address}
		host, err := New(Options{NodeID: nodeID, Execution: execution, ExecutionAddress: address, Listen: singleListener(listener), Mesh: &MeshOptions{Identity: identity, Transport: network.NewTransport(), AnnouncementInterval: 10 * time.Millisecond, HandshakeTimeout: time.Second, HandshakeDial: func(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
			target := dialers[endpoint]
			if target == nil {
				return nil, fmt.Errorf("unknown endpoint %s", endpoint)
			}
			return grpc.DialContext(ctx, "passthrough:///"+endpoint, grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return target.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
		}}})
		if err != nil {
			t.Fatal(err)
		}
		runtimes = append(runtimes, runtime{host: host, listener: listener})
	}
	for _, runtime := range runtimes {
		if err := runtime.host.Start(); err != nil {
			t.Fatal(err)
		}
		defer runtime.host.Close()
	}
	for _, runtime := range runtimes {
		seen := map[string]bool{}
		deadline := time.After(3 * time.Second)
		for len(seen) < 2 {
			select {
			case result := <-runtime.host.PeerHandshakes():
				seen[result.Identity.NodeID] = true
			case <-deadline:
				t.Fatalf("%s saw %v", runtime.host.nodeID, seen)
			}
		}
		if runtime.host.Core() != nil || runtime.host.CoreReady() {
			t.Fatal("discovery activated Core")
		}
		role := runtime.host.CoordinatorRoleSnapshot()
		if !role.HasCoordinator || role.Selection.NodeID != "node-a" || role.Selection.RuntimeInstanceID != "session-node-a" {
			t.Fatalf("%s coordinator role = %+v", runtime.host.nodeID, role)
		}
		if role.LocalIsCoordinator != (runtime.host.nodeID == "node-a") {
			t.Fatalf("%s local coordinator = %+v", runtime.host.nodeID, role)
		}
		snapshot := runtime.host.MembershipSnapshot()
		if len(snapshot.ActiveMembers) != 3 {
			t.Fatalf("%s membership = %+v", runtime.host.nodeID, snapshot)
		}
		for index, want := range []string{"node-a", "node-b", "node-c"} {
			if snapshot.ActiveMembers[index].Identity.NodeID != want {
				t.Fatalf("%s order = %+v", runtime.host.nodeID, snapshot.ActiveMembers)
			}
		}
		var resources resourceview.Snapshot
		resourceDeadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(resourceDeadline) {
			resources = runtime.host.ResourceViewSnapshot()
			if len(resources.ActiveResources) == 3 {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if len(resources.ActiveResources) != 3 {
			t.Fatalf("%s resources did not converge: %+v", runtime.host.nodeID, resources)
		}
		for index, want := range []string{"node-a", "node-b", "node-c"} {
			if resources.ActiveResources[index].Owner.NodeID != want {
				t.Fatalf("%s resource order = %+v", runtime.host.nodeID, resources.ActiveResources)
			}
		}
	}
}

type blockingAdvertisementClient struct {
	owners  map[string]protocol.Identity
	started chan protocol.Identity
	release chan struct{}
	failure error
}

func (client *blockingAdvertisementClient) GetResourceAdvertisement(ctx context.Context, endpoint string) (resourceview.Advertisement, error) {
	owner, ok := client.owners[endpoint]
	if !ok {
		return resourceview.Advertisement{}, fmt.Errorf("unknown endpoint %s", endpoint)
	}
	select {
	case client.started <- owner:
	default:
	}
	select {
	case <-ctx.Done():
		return resourceview.Advertisement{}, ctx.Err()
	case <-client.release:
	}
	if client.failure != nil {
		return resourceview.Advertisement{}, client.failure
	}
	return resourceview.Advertisement{Owner: owner}, nil
}

func TestSlowResourcePullDoesNotBlockMembershipObservation(t *testing.T) {
	network := discovery.NewMemoryNetwork()
	identities := []protocol.Identity{
		{MeshNamespace: "mesh-a", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion, NodeID: "node-a", RuntimeInstance: "session-a", ControlEndpoint: "127.0.0.1:45911"},
		{MeshNamespace: "mesh-a", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion, NodeID: "node-b", RuntimeInstance: "session-b", ControlEndpoint: "127.0.0.1:45912"},
		{MeshNamespace: "mesh-a", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion, NodeID: "node-c", RuntimeInstance: "session-c", ControlEndpoint: "127.0.0.1:45913"},
	}
	listeners := map[string]*bufconn.Listener{}
	for _, identity := range identities {
		listeners[identity.ControlEndpoint] = bufconn.Listen(1024 * 1024)
	}
	failure := errors.New("resource pull failed")
	blocking := &blockingAdvertisementClient{
		owners: map[string]protocol.Identity{
			identities[1].ControlEndpoint: identities[1], identities[2].ControlEndpoint: identities[2],
		},
		started: make(chan protocol.Identity, 2), release: make(chan struct{}), failure: failure,
	}
	hosts := make([]*RuntimeHost, 0, len(identities))
	for index, identity := range identities {
		execution, err := NewResourceExecutionCapability(executionConfig(identity.NodeID, filepath.Join(t.TempDir(), identity.NodeID+".db")), testHandler{capability: "temperature_sensor"})
		if err != nil {
			t.Fatal(err)
		}
		mesh := &MeshOptions{
			Identity: identity, Transport: network.NewTransport(), AnnouncementInterval: 10 * time.Millisecond, HandshakeTimeout: time.Second,
			HandshakeDial: func(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
				listener := listeners[endpoint]
				if listener == nil {
					return nil, fmt.Errorf("unknown endpoint %s", endpoint)
				}
				return grpc.DialContext(ctx, "passthrough:///"+endpoint, grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }), grpc.WithTransportCredentials(insecure.NewCredentials()))
			},
		}
		if index == 0 {
			mesh.ResourceClient = blocking
		}
		host, err := New(Options{NodeID: identity.NodeID, Execution: execution, ExecutionAddress: identity.ControlEndpoint, Listen: singleListener(listeners[identity.ControlEndpoint]), Mesh: mesh})
		if err != nil {
			t.Fatal(err)
		}
		hosts = append(hosts, host)
	}
	for _, host := range hosts {
		if err := host.Start(); err != nil {
			t.Fatal(err)
		}
		defer host.Close()
	}
	select {
	case <-blocking.started:
	case <-time.After(3 * time.Second):
		t.Fatal("resource pull did not start")
	}
	seen := map[string]bool{}
	deadline := time.After(time.Second)
	for len(seen) < 2 {
		select {
		case result := <-hosts[0].PeerHandshakes():
			seen[result.Identity.NodeID] = true
		case <-deadline:
			t.Fatalf("blocked resource pulls stalled the membership result consumer: seen=%v", seen)
		}
	}
	if active := hosts[0].MembershipSnapshot().ActiveMembers; len(active) != 3 {
		t.Fatalf("membership did not advance while resource pull was blocked: %+v", active)
	}
	tickDone := make(chan error, 1)
	go func() { tickDone <- hosts[0].Membership().Tick(time.Now()) }()
	select {
	case err := <-tickDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("blocked resource pull stalled Membership.Tick")
	}
	close(blocking.release)
	select {
	case err := <-hosts[0].ResourceSyncErrors():
		if !errors.Is(err, failure) {
			t.Fatalf("resource sync error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("resource sync failure was not observable")
	}
	if active := hosts[0].MembershipSnapshot().ActiveMembers; len(active) != 3 {
		t.Fatalf("resource sync failure mutated membership: %+v", active)
	}
}

func TestRuntimeMembershipSessionRestartAndIdentityConflict(t *testing.T) {
	local := protocol.Identity{MeshNamespace: "mesh-a", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion, NodeID: "node-a", RuntimeInstance: "local", ControlEndpoint: "127.0.0.1:45901"}
	table, err := membership.New(local, membership.Timing{SuspectAfter: 3 * time.Second, ExpireAfter: 6 * time.Second}, time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	old := protocol.Identity{MeshNamespace: "mesh-a", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion, NodeID: "node-b", RuntimeInstance: "session-old", ControlEndpoint: "127.0.0.1:45902"}
	newSession := old
	newSession.RuntimeInstance = "session-new"
	if err := table.Observe(handshake.Result{Identity: old}, time.Unix(100, 0)); err != nil {
		t.Fatal(err)
	}
	if err := table.Tick(time.Unix(106, 0)); err != nil {
		t.Fatal(err)
	}
	if err := table.Observe(handshake.Result{Identity: newSession}, time.Unix(107, 0)); err != nil {
		t.Fatal(err)
	}
	if !table.Snapshot().IsUniquelyActive("node-b") {
		t.Fatal("new session did not replace expired old session")
	}
	duplicate := newSession
	duplicate.RuntimeInstance = "session-duplicate"
	if err := table.Observe(handshake.Result{Identity: duplicate}, time.Unix(108, 0)); err != nil {
		t.Fatal(err)
	}
	snapshot := table.Snapshot()
	if len(snapshot.IdentityConflicts) != 1 || snapshot.IdentityConflicts[0] != "node-b" || snapshot.IsUniquelyActive("node-b") {
		t.Fatalf("conflict=%+v", snapshot)
	}
	if snapshot.Members[1].Identity.RuntimeInstance > snapshot.Members[2].Identity.RuntimeInstance && snapshot.IsUniquelyActive("node-b") {
		t.Fatal("RuntimeInstanceID ordering selected a winner")
	}
	if err := table.Observe(handshake.Result{Identity: duplicate}, time.Unix(112, 0)); err != nil {
		t.Fatal(err)
	}
	if err := table.Tick(time.Unix(113, 0)); err != nil {
		t.Fatal(err)
	}
	snapshot = table.Snapshot()
	if len(snapshot.IdentityConflicts) != 0 || !snapshot.IsUniquelyActive("node-b") {
		t.Fatalf("recovery=%+v", snapshot)
	}
}

func TestRuntimeHostConcurrentStartCloseWithMembership(t *testing.T) {
	for iteration := 0; iteration < 50; iteration++ {
		execution, err := NewResourceExecutionCapability(executionConfig("node-a", filepath.Join(t.TempDir(), fmt.Sprintf("agent-%d.db", iteration))), testHandler{capability: "temperature_sensor"})
		if err != nil {
			t.Fatal(err)
		}
		listener := bufconn.Listen(1024 * 1024)
		network := discovery.NewMemoryNetwork()
		identity := protocol.Identity{MeshNamespace: "mesh", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion, NodeID: "node-a", RuntimeInstance: fmt.Sprintf("session-%d", iteration), ControlEndpoint: "127.0.0.1:45901"}
		host, err := New(Options{NodeID: "node-a", Execution: execution, ExecutionAddress: "execution", Listen: singleListener(listener), Mesh: &MeshOptions{Identity: identity, Transport: network.NewTransport(), AnnouncementInterval: time.Millisecond, HandshakeTimeout: time.Millisecond, MembershipTiming: membership.Timing{SuspectAfter: 5 * time.Millisecond, ExpireAfter: 8 * time.Millisecond}}})
		if err != nil {
			t.Fatal(err)
		}
		start := make(chan struct{})
		done := make(chan error, 2)
		go func() { <-start; done <- host.Start() }()
		go func() { <-start; done <- host.Close() }()
		close(start)
		first, second := <-done, <-done
		for _, result := range []error{first, second} {
			if result != nil && !errors.Is(result, ErrCapabilityClosed) && !errors.Is(result, discovery.ErrDiscoveryClosed) {
				t.Fatalf("iteration %d result=%v", iteration, result)
			}
		}
		if err := host.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRuntimeHostCloseBeforeStartClosesMembershipObservations(t *testing.T) {
	execution, err := NewResourceExecutionCapability(executionConfig("node-a", filepath.Join(t.TempDir(), "agent.db")), testHandler{capability: "temperature_sensor"})
	if err != nil {
		t.Fatal(err)
	}
	identity := protocol.Identity{MeshNamespace: "mesh", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion, NodeID: "node-a", RuntimeInstance: "session", ControlEndpoint: "127.0.0.1:45901"}
	host, err := New(Options{NodeID: "node-a", Execution: execution, Mesh: &MeshOptions{Identity: identity, Transport: discovery.NewMemoryNetwork().NewTransport(), MembershipTiming: membership.Timing{SuspectAfter: 6 * time.Second, ExpireAfter: 9 * time.Second}}})
	if err != nil {
		t.Fatal(err)
	}
	observations := host.PeerHandshakes()
	if err := host.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case _, ok := <-observations:
		if ok {
			t.Fatal("observation channel remains open")
		}
	case <-time.After(time.Second):
		t.Fatal("observation channel did not close")
	}
}

func TestRuntimeHostRejectsMembershipTimingAtCustomConfirmedRefreshBoundary(t *testing.T) {
	execution, err := NewResourceExecutionCapability(executionConfig("node-a", filepath.Join(t.TempDir(), "agent.db")), testHandler{capability: "temperature_sensor"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = execution.Close() })
	identity := protocol.Identity{MeshNamespace: "mesh", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion, NodeID: "node-a", RuntimeInstance: "session", ControlEndpoint: "127.0.0.1:45901"}
	_, err = New(Options{NodeID: "node-a", Execution: execution, Mesh: &MeshOptions{Identity: identity, Transport: discovery.NewMemoryNetwork().NewTransport(), AnnouncementInterval: time.Second, HandshakeTimeout: 500 * time.Millisecond, MembershipTiming: membership.Timing{SuspectAfter: 3500 * time.Millisecond, ExpireAfter: 6 * time.Second}}})
	if !errors.Is(err, membership.ErrInvalidTiming) {
		t.Fatalf("New() = %v, want %v", err, membership.ErrInvalidTiming)
	}
}
