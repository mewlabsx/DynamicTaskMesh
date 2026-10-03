package ui

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/mesh/protocol"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testRuntimeControl struct {
	dtmv1.UnimplementedRuntimeControlServiceServer
	status    *dtmv1.GetRuntimeStatusResponse
	statusErr error
	resources []*dtmv1.MeshResourceDescriptor
}

func (server *testRuntimeControl) GetRuntimeStatus(context.Context, *dtmv1.GetRuntimeStatusRequest) (*dtmv1.GetRuntimeStatusResponse, error) {
	if server.statusErr != nil {
		return nil, server.statusErr
	}
	return server.status, nil
}

func (server *testRuntimeControl) GetResourceAdvertisement(context.Context, *dtmv1.GetResourceAdvertisementRequest) (*dtmv1.GetResourceAdvertisementResponse, error) {
	return &dtmv1.GetResourceAdvertisementResponse{Owner: server.status.GetRuntime(), Resources: server.resources}, nil
}

type testCoreServer struct {
	dtmv1.UnimplementedCoreServiceServer
	task       *dtmv1.TaskDetails
	executions []*dtmv1.TaskExecution
}

func (server *testCoreServer) ListTasks(context.Context, *dtmv1.ListTasksRequest) (*dtmv1.ListTasksResponse, error) {
	return &dtmv1.ListTasksResponse{Tasks: []*dtmv1.TaskSummary{{
		TaskId: server.task.GetTaskId(), Intent: server.task.GetIntent(), Status: server.task.GetStatus(),
		CreatedAt: server.task.GetCreatedAt(), UpdatedAt: server.task.GetUpdatedAt(),
	}}}, nil
}

func (server *testCoreServer) GetTask(context.Context, *dtmv1.GetTaskRequest) (*dtmv1.GetTaskResponse, error) {
	return &dtmv1.GetTaskResponse{Task: server.task}, nil
}

func (server *testCoreServer) GetTaskExecutions(context.Context, *dtmv1.GetTaskExecutionsRequest) (*dtmv1.GetTaskExecutionsResponse, error) {
	return &dtmv1.GetTaskExecutionsResponse{Executions: server.executions}, nil
}

func startTestGRPC(t *testing.T, register func(*grpc.Server)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	register(server)
	go server.Serve(listener)
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func TestM7VISObservesThreeRuntimeMesh(t *testing.T) {
	now := timestamppb.New(time.Unix(100, 0).UTC())
	coreTask := &dtmv1.TaskDetails{
		TaskId: "m7-greenhouse", Intent: "cool_environment", Status: dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED,
		CreatedAt: now, UpdatedAt: now,
		Steps: []*dtmv1.TaskStepDetails{
			{StepId: "step-1", Sequence: 1, Capability: "temperature_sensor", Status: dtmv1.StepStatus_STEP_STATUS_SUCCEEDED, AssignedNodeId: "node-a"},
			{StepId: "step-2", Sequence: 2, Capability: "cooling_control", Status: dtmv1.StepStatus_STEP_STATUS_SUCCEEDED, AssignedNodeId: "node-b"},
		},
	}
	temperature, _ := structpb.NewStruct(map[string]any{"temperature": 30})
	cooling, _ := structpb.NewStruct(map[string]any{"cooling_started": true})
	executions := []*dtmv1.TaskExecution{
		{StepId: "step-1", NodeId: "node-a", Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED, UpdatedAt: now, Response: temperature},
		{StepId: "step-2", NodeId: "node-b", Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED, UpdatedAt: now, Response: cooling},
	}
	coreEndpoint := startTestGRPC(t, func(server *grpc.Server) {
		dtmv1.RegisterCoreServiceServer(server, &testCoreServer{task: coreTask, executions: executions})
	})

	identities := []protocol.Identity{
		{MeshNamespace: "m7-vis", ProtocolMajor: 1, ProtocolMinor: 0, DTMVersion: protocol.DTMVersion, NodeID: "node-a", RuntimeInstance: "session-a", ControlEndpoint: "127.0.0.1:0"},
		{MeshNamespace: "m7-vis", ProtocolMajor: 1, ProtocolMinor: 0, DTMVersion: protocol.DTMVersion, NodeID: "node-b", RuntimeInstance: "session-b", ControlEndpoint: "127.0.0.1:0"},
		{MeshNamespace: "m7-vis", ProtocolMajor: 1, ProtocolMinor: 0, DTMVersion: protocol.DTMVersion, NodeID: "node-c", RuntimeInstance: "session-c", ControlEndpoint: "127.0.0.1:0"},
	}
	servers := make([]string, len(identities))
	for index := range identities {
		listenerEndpoint := startTestGRPC(t, func(server *grpc.Server) {})
		servers[index] = listenerEndpoint
		identities[index].ControlEndpoint = listenerEndpoint
	}
	identities[0].ControlEndpoint = servers[0]
	coordinator := &dtmv1.RuntimeIdentity{NodeId: "node-a", RuntimeInstanceId: "session-a", AdvertisedControlEndpoint: servers[0]}
	for index, identity := range identities {
		status := &dtmv1.GetRuntimeStatusResponse{
			Runtime:     &dtmv1.RuntimeIdentity{MeshNamespace: identity.MeshNamespace, ProtocolMajor: identity.ProtocolMajor, ProtocolMinor: identity.ProtocolMinor, DtmVersion: identity.DTMVersion, NodeId: identity.NodeID, RuntimeInstanceId: identity.RuntimeInstance, AdvertisedControlEndpoint: identity.ControlEndpoint},
			Coordinator: coordinator, LocalIsCoordinator: index == 0, CoreReady: index == 0, AuthorityReady: index == 0, IngressReady: index == 0,
			CoreAddress: func() string {
				if index == 0 {
					return coreEndpoint
				}
				return ""
			}(), AuthorityStatus: map[bool]string{true: "ready", false: "not_required"}[index != 2],
		}
		resources := []*dtmv1.MeshResourceDescriptor{}
		if index == 0 {
			resources = append(resources, &dtmv1.MeshResourceDescriptor{ResourceId: "sensor-a", Kind: "capability", Type: "temperature_sensor", OperationIds: []string{"read_temperature"}})
		}
		if index == 1 {
			resources = append(resources, &dtmv1.MeshResourceDescriptor{ResourceId: "cooling-b", Kind: "capability", Type: "cooling_control", OperationIds: []string{"set_temperature"}})
		}
		// Replace the temporary empty server with a server that owns this status.
		endpoint := startTestGRPC(t, func(server *grpc.Server) {
			dtmv1.RegisterRuntimeControlServiceServer(server, &testRuntimeControl{status: status, resources: resources})
		})
		identities[index].ControlEndpoint = endpoint
		status.GetRuntime().AdvertisedControlEndpoint = endpoint
		if index == 0 {
			coordinator.AdvertisedControlEndpoint = endpoint
			status.GetCoordinator().AdvertisedControlEndpoint = endpoint
		}
	}
	// The first loop created endpoint identities before the final servers were
	// assigned; rebuild the static source from the actual advertised endpoints.
	// The test intentionally uses the real gRPC bootstrap and query path.
	actual := make([]protocol.Identity, len(identities))
	copy(actual, identities)
	observer, err := NewObserver(ObserverOptions{BootstrapEndpoint: actual[1].ControlEndpoint, Members: StaticMemberSource(actual), QueryTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := observer.Snapshot(context.Background())
	if snapshot.Mesh.RuntimeCount != 3 || snapshot.Mesh.CoordinatorCount != 1 || snapshot.Mesh.Coordinator != "node-a" || snapshot.Mesh.CoreState != "ACTIVE" {
		t.Fatalf("mesh snapshot = %#v", snapshot.Mesh)
	}
	if len(snapshot.Resources) != 2 || len(snapshot.Tasks) != 1 || len(snapshot.Tasks[0].Steps) != 2 {
		t.Fatalf("resources/tasks = %d/%#v", len(snapshot.Resources), snapshot.Tasks)
	}
	if snapshot.Tasks[0].Steps[0].Result["temperature"] != float64(30) || snapshot.Tasks[0].Steps[1].Result["cooling_started"] != true {
		t.Fatalf("task results = %#v", snapshot.Tasks[0].Steps)
	}
	if len(snapshot.Events) == 0 || snapshot.Tasks[0].Steps[0].ResourceRefHistory != "PARTIAL" {
		t.Fatalf("events/history = %d/%q", len(snapshot.Events), snapshot.Tasks[0].Steps[0].ResourceRefHistory)
	}
}

func TestObserverDoesNotRelabelExpiredRuntimeAfterRestart(t *testing.T) {
	current := &dtmv1.RuntimeIdentity{
		MeshNamespace: "m7-vis", ProtocolMajor: 1, ProtocolMinor: 0, DtmVersion: protocol.DTMVersion,
		NodeId: "node-a", RuntimeInstanceId: "current-session",
	}
	control := &testRuntimeControl{
		status: &dtmv1.GetRuntimeStatusResponse{
			Runtime: current, Coordinator: current, LocalIsCoordinator: true,
			AuthorityReady: true, IngressReady: true, AuthorityStatus: "ready",
		},
		resources: []*dtmv1.MeshResourceDescriptor{{ResourceId: "sensor-a", Kind: "capability", Type: "temperature_sensor"}},
	}
	endpoint := startTestGRPC(t, func(server *grpc.Server) {
		dtmv1.RegisterRuntimeControlServiceServer(server, control)
	})
	current.AdvertisedControlEndpoint = endpoint
	old := proto.Clone(current).(*dtmv1.RuntimeIdentity)
	old.RuntimeInstanceId = "expired-session"

	observer, err := NewObserver(ObserverOptions{
		BootstrapEndpoint: endpoint,
		Members: func() []MemberObservation {
			return []MemberObservation{
				{Identity: identityFromProto(old), State: "EXPIRED"},
				{Identity: identityFromProto(current), State: "ACTIVE"},
			}
		},
		QueryTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := observer.Snapshot(context.Background())
	if snapshot.Mesh.CoordinatorCount != 1 || snapshot.Mesh.CoordinatorState != "OK" {
		t.Fatalf("mesh after runtime restart = %#v", snapshot.Mesh)
	}
	if snapshot.Mesh.RuntimeCount != 1 {
		t.Fatalf("active runtime count after restart = %d, want 1", snapshot.Mesh.RuntimeCount)
	}
	if len(snapshot.Runtimes) != 2 {
		t.Fatalf("runtimes after runtime restart = %#v", snapshot.Runtimes)
	}
	var expired, active int
	for _, runtime := range snapshot.Runtimes {
		switch runtime.RuntimeInstanceID {
		case "expired-session":
			expired++
			if runtime.MembershipState != "EXPIRED" || runtime.Coordinator {
				t.Fatalf("expired runtime view = %#v", runtime)
			}
		case "current-session":
			active++
			if runtime.MembershipState != "ACTIVE" || !runtime.Coordinator {
				t.Fatalf("active runtime view = %#v", runtime)
			}
		}
	}
	if expired != 1 || active != 1 {
		t.Fatalf("runtime identities after restart = %#v", snapshot.Runtimes)
	}
	if len(snapshot.Resources) != 1 || snapshot.Resources[0].OwnerRuntimeInstanceID != "current-session" {
		t.Fatalf("resources after runtime restart = %#v", snapshot.Resources)
	}
}

func TestObserverRejectsMismatchedRuntimeIdentity(t *testing.T) {
	current := &dtmv1.RuntimeIdentity{
		MeshNamespace: "m7-vis", ProtocolMajor: 1, ProtocolMinor: 0, DtmVersion: protocol.DTMVersion,
		NodeId: "node-a", RuntimeInstanceId: "current-session",
	}
	control := &testRuntimeControl{status: &dtmv1.GetRuntimeStatusResponse{Runtime: current, LocalIsCoordinator: true}}
	endpoint := startTestGRPC(t, func(server *grpc.Server) {
		dtmv1.RegisterRuntimeControlServiceServer(server, control)
	})
	current.AdvertisedControlEndpoint = endpoint
	observed := proto.Clone(current).(*dtmv1.RuntimeIdentity)
	observed.RuntimeInstanceId = "different-session"

	observer, err := NewObserver(ObserverOptions{
		BootstrapEndpoint: endpoint,
		Members: func() []MemberObservation {
			return []MemberObservation{{Identity: identityFromProto(observed), State: "SUSPECT"}}
		},
		QueryTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := observer.Snapshot(context.Background())
	if snapshot.Mesh.CoordinatorCount != 1 || snapshot.Mesh.CoordinatorState != "OK" {
		t.Fatalf("mesh after identity mismatch = %#v", snapshot.Mesh)
	}
	for _, runtime := range snapshot.Runtimes {
		if runtime.RuntimeInstanceID == "different-session" && runtime.Coordinator {
			t.Fatalf("mismatched runtime was treated as coordinator: %#v", runtime)
		}
	}
}

func TestObserverBootstrapFailureIsBounded(t *testing.T) {
	observer, err := NewObserver(ObserverOptions{BootstrapEndpoint: "127.0.0.1:1", QueryTimeout: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	snapshot := observer.Snapshot(context.Background())
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("bootstrap failure took %s", elapsed)
	}
	if snapshot.Status != "UNAVAILABLE" || snapshot.Error == "" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestObserverFallsBackToHealthyRuntimeSeed(t *testing.T) {
	current := &dtmv1.RuntimeIdentity{
		MeshNamespace: "m7-vis", ProtocolMajor: 1, ProtocolMinor: 0, DtmVersion: protocol.DTMVersion,
		NodeId: "node-b", RuntimeInstanceId: "session-b",
	}
	control := &testRuntimeControl{status: &dtmv1.GetRuntimeStatusResponse{
		Runtime: current, Coordinator: current, LocalIsCoordinator: true,
		CoreReady: true, AuthorityReady: true, IngressReady: true, AuthorityStatus: "ready",
	}}
	endpoint := startTestGRPC(t, func(server *grpc.Server) {
		dtmv1.RegisterRuntimeControlServiceServer(server, control)
	})
	current.AdvertisedControlEndpoint = endpoint

	observer, err := NewObserver(ObserverOptions{
		BootstrapEndpoint:  "127.0.0.1:1",
		BootstrapEndpoints: []string{"127.0.0.1:1", endpoint},
		QueryTimeout:       100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := observer.Snapshot(context.Background())
	if snapshot.Observer.Endpoint != endpoint || snapshot.Observer.NodeID != "node-b" || snapshot.Observer.State != "FALLBACK" || snapshot.Observer.Source != "SEED" {
		t.Fatalf("observer fallback = %#v", snapshot.Observer)
	}
	if snapshot.Mesh.Coordinator != "node-b" || snapshot.Mesh.CoordinatorCount != 1 {
		t.Fatalf("mesh after observer fallback = %#v", snapshot.Mesh)
	}
}

func TestObserverEnumeratesConfiguredSeedsWithoutMulticast(t *testing.T) {
	primary := &testRuntimeControl{status: &dtmv1.GetRuntimeStatusResponse{
		Runtime:            &dtmv1.RuntimeIdentity{MeshNamespace: "m8-vis", ProtocolMajor: 1, ProtocolMinor: 0, DtmVersion: protocol.DTMVersion, NodeId: "node-a", RuntimeInstanceId: "session-a"},
		LocalIsCoordinator: true, CoreReady: true, AuthorityReady: true, IngressReady: true, AuthorityStatus: "ready",
	}}
	primaryEndpoint := startTestGRPC(t, func(server *grpc.Server) {
		dtmv1.RegisterRuntimeControlServiceServer(server, primary)
	})
	primary.status.Runtime.AdvertisedControlEndpoint = primaryEndpoint

	secondary := &testRuntimeControl{status: &dtmv1.GetRuntimeStatusResponse{
		Runtime:            &dtmv1.RuntimeIdentity{MeshNamespace: "m8-vis", ProtocolMajor: 1, ProtocolMinor: 0, DtmVersion: protocol.DTMVersion, NodeId: "node-b", RuntimeInstanceId: "session-b"},
		LocalIsCoordinator: false, AuthorityStatus: "ready",
	}}
	secondaryEndpoint := startTestGRPC(t, func(server *grpc.Server) {
		dtmv1.RegisterRuntimeControlServiceServer(server, secondary)
	})
	secondary.status.Runtime.AdvertisedControlEndpoint = secondaryEndpoint

	observer, err := NewObserver(ObserverOptions{
		BootstrapEndpoint:  primaryEndpoint,
		BootstrapEndpoints: []string{secondaryEndpoint},
		QueryTimeout:       time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot := observer.Snapshot(context.Background())
	if snapshot.Mesh.RuntimeCount != 2 || snapshot.Mesh.CoordinatorCount != 1 || snapshot.Mesh.Coordinator != "node-a" {
		t.Fatalf("seed-only mesh snapshot = %#v runtimes=%#v", snapshot.Mesh, snapshot.Runtimes)
	}
	seen := map[string]bool{}
	for _, runtime := range snapshot.Runtimes {
		seen[runtime.NodeID] = true
	}
	if !seen["node-a"] || !seen["node-b"] {
		t.Fatalf("configured seeds were not enumerated: %#v", snapshot.Runtimes)
	}
}

func TestObserverFallsBackToDiscoveredRuntime(t *testing.T) {
	current := &dtmv1.RuntimeIdentity{
		MeshNamespace: "m7-vis", ProtocolMajor: 1, ProtocolMinor: 0, DtmVersion: protocol.DTMVersion,
		NodeId: "node-c", RuntimeInstanceId: "session-c",
	}
	control := &testRuntimeControl{status: &dtmv1.GetRuntimeStatusResponse{
		Runtime: current, Coordinator: current, LocalIsCoordinator: true,
		CoreReady: true, AuthorityReady: true, IngressReady: true, AuthorityStatus: "ready",
	}}
	endpoint := startTestGRPC(t, func(server *grpc.Server) {
		dtmv1.RegisterRuntimeControlServiceServer(server, control)
	})
	current.AdvertisedControlEndpoint = endpoint

	observer, err := NewObserver(ObserverOptions{
		BootstrapEndpoint: "127.0.0.1:1",
		Members: StaticMemberSource([]protocol.Identity{{
			MeshNamespace: "m7-vis", ProtocolMajor: 1, ProtocolMinor: 0, DTMVersion: protocol.DTMVersion,
			NodeID: "node-c", RuntimeInstance: "session-c", ControlEndpoint: endpoint,
		}}),
		QueryTimeout: 100 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	snapshot := observer.Snapshot(context.Background())
	if snapshot.Observer.Endpoint != endpoint || snapshot.Observer.NodeID != "node-c" || snapshot.Observer.State != "FALLBACK" || snapshot.Observer.Source != "DISCOVERED_MEMBER" {
		t.Fatalf("observer discovered fallback = %#v", snapshot.Observer)
	}
}
func TestObserverPreservesLastSnapshotAsStale(t *testing.T) {
	current := &dtmv1.RuntimeIdentity{
		MeshNamespace: "m7-vis", ProtocolMajor: 1, ProtocolMinor: 0, DtmVersion: protocol.DTMVersion,
		NodeId: "node-a", RuntimeInstanceId: "session-a",
	}
	control := &testRuntimeControl{status: &dtmv1.GetRuntimeStatusResponse{
		Runtime: current, Coordinator: current, LocalIsCoordinator: true,
		CoreReady: true, AuthorityReady: true, IngressReady: true, AuthorityStatus: "ready",
	}}
	endpoint := startTestGRPC(t, func(server *grpc.Server) {
		dtmv1.RegisterRuntimeControlServiceServer(server, control)
	})
	current.AdvertisedControlEndpoint = endpoint
	observer, err := NewObserver(ObserverOptions{BootstrapEndpoint: endpoint, QueryTimeout: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	ready := observer.Snapshot(context.Background())
	if ready.Status == "UNAVAILABLE" || ready.LastSuccessfulSnapshot == nil || ready.Observer.State != "ACTIVE" {
		t.Fatalf("initial snapshot = %#v", ready)
	}
	lastSuccessful := ready.LastSuccessfulSnapshot
	control.statusErr = errors.New("runtime stopped")
	stale := observer.Snapshot(context.Background())
	if stale.Status != "STALE" || stale.Error == "" || stale.LastSuccessfulSnapshot == nil {
		t.Fatalf("stale snapshot = %#v", stale)
	}
	if lastSuccessful == nil || !stale.LastSuccessfulSnapshot.Equal(*lastSuccessful) {
		t.Fatalf("last successful time changed: ready=%v stale=%v", lastSuccessful, stale.LastSuccessfulSnapshot)
	}
	if stale.Observer.State != "STALE" || stale.Observer.NodeID != "node-a" || stale.Mesh.CoordinatorState != "UNKNOWN" || stale.Mesh.CoreEndpoint != "" {
		t.Fatalf("stale observer/mesh = %#v/%#v", stale.Observer, stale.Mesh)
	}
}
