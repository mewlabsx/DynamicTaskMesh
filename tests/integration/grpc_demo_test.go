package integration_test

import (
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/agent"
	"dtm/internal/application"
	"dtm/internal/capability"
	"dtm/internal/demo"
	"dtm/internal/lifecycle"
	"dtm/internal/mapper"
	"dtm/internal/planner"
	"dtm/internal/runtime"
	"dtm/internal/transport/grpcapi"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGRPCDemoSucceeds(t *testing.T) {
	mesh := newTestMesh(t)
	mesh.startAgent(t, "sensor-node-001", demo.NewTemperatureHandler())
	cooling := mesh.startAgent(t, "cooling-node-001", demo.NewCoolingHandler())

	response := mesh.submit(t, "task-demo-1")
	if response.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED {
		t.Fatalf("status = %s, error = %q", response.GetStatus(), response.GetError())
	}
	if got := response.GetTaskId(); got != "task-demo-1" {
		t.Fatalf("task ID = %q", got)
	}
	if len(response.GetResults()) != 2 {
		t.Fatalf("result count = %d", len(response.GetResults()))
	}
	if result := response.GetResults()[0]; result.GetNodeId() != "sensor-node-001" || result.GetOutput().AsMap()["temperature"] != float64(30) {
		t.Fatalf("sensor result = %#v", result)
	}
	if result := response.GetResults()[1]; result.GetNodeId() != "cooling-node-001" || result.GetOutput().AsMap()["cooling_started"] != true {
		t.Fatalf("cooling result = %#v", result)
	}
	if got := cooling.taskIDs(); len(got) != 1 || got[0] != "task-demo-1" {
		t.Fatalf("cooling agent task IDs = %v", got)
	}
}

func TestGRPCDemoReplacesCoolingNode(t *testing.T) {
	mesh := newTestMesh(t)
	mesh.startAgent(t, "sensor-node-001", demo.NewTemperatureHandler())
	first := mesh.startAgent(t, "cooling-node-001", demo.NewCoolingHandler())
	if response := mesh.submit(t, "task-before-replacement"); response.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED || len(response.GetResults()) != 2 || response.GetResults()[1].GetNodeId() != "cooling-node-001" {
		t.Fatalf("first response = %#v", response)
	}

	mesh.offline(t, "cooling-node-001")
	first.stop()
	mesh.startAgent(t, "cooling-node-002", demo.NewCoolingHandler())
	response := mesh.submit(t, "task-after-replacement")
	if response.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED {
		t.Fatalf("status = %s, error = %q", response.GetStatus(), response.GetError())
	}
	if len(response.GetResults()) != 2 {
		t.Fatalf("result count = %d", len(response.GetResults()))
	}
	if got := response.GetResults()[1].GetNodeId(); got != "cooling-node-002" {
		t.Fatalf("cooling node = %q", got)
	}
	for _, result := range response.GetResults() {
		if result.GetNodeId() == "cooling-node-001" {
			t.Fatalf("replacement response retained offline node: %#v", response)
		}
	}
}

func TestGRPCDemoFailsWithoutCoolingCapability(t *testing.T) {
	mesh := newTestMesh(t)
	sensor := mesh.startAgent(t, "sensor-node-001", demo.NewTemperatureHandler())

	response := mesh.submit(t, "task-without-cooling")
	if response.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_FAILED {
		t.Fatalf("status = %s, error = %q", response.GetStatus(), response.GetError())
	}
	if !strings.Contains(response.GetError(), mapper.ErrCapabilityUnavailable.Error()) {
		t.Fatalf("error = %q, want capability-unavailable category", response.GetError())
	}
	if len(response.GetResults()) != 0 {
		t.Fatalf("results = %#v, want none", response.GetResults())
	}
	if got := sensor.callCount(); got != 0 {
		t.Fatalf("sensor execution count = %d, want 0", got)
	}
}

type testMesh struct {
	registryClient dtmv1.NodeRegistryServiceClient
	coreClient     dtmv1.CoreServiceClient
	registrations  map[string]string
}

func newTestMesh(t *testing.T) *testMesh {
	t.Helper()
	registry := capability.NewRegistry()
	endpoints := grpcapi.NewEndpointDirectory()
	executor, err := grpcapi.NewRemoteExecutor(endpoints, networkDialer)
	if err != nil {
		t.Fatal(err)
	}
	runner, err := runtime.New(executor)
	if err != nil {
		t.Fatal(err)
	}
	mapperService, err := mapper.New(registry)
	if err != nil {
		t.Fatal(err)
	}
	service, err := application.NewTaskService(lifecycle.New, planner.New(), mapperService, runner)
	if err != nil {
		t.Fatal(err)
	}
	core, err := grpcapi.NewCoreServer(service)
	if err != nil {
		t.Fatal(err)
	}
	registryServer, err := grpcapi.NewRegistryServer(registry, endpoints)
	if err != nil {
		t.Fatal(err)
	}

	listener := listen(t)
	server := grpc.NewServer()
	dtmv1.RegisterCoreServiceServer(server, core)
	dtmv1.RegisterNodeRegistryServiceServer(server, registryServer)
	serve(t, server, listener)
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	connection := dial(t, listener.Addr().String())
	t.Cleanup(func() { _ = connection.Close() })
	return &testMesh{
		registryClient: dtmv1.NewNodeRegistryServiceClient(connection),
		coreClient:     dtmv1.NewCoreServiceClient(connection),
		registrations:  make(map[string]string),
	}
}

func (mesh *testMesh) startAgent(t *testing.T, id string, handler agent.Handler) *testAgent {
	t.Helper()
	instance := startTestAgent(t, mesh.registryClient, id, handler)
	mesh.registrations[id] = instance.registrationID
	return instance
}

func (mesh *testMesh) submit(t *testing.T, id string) *dtmv1.SubmitTaskResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := mesh.coreClient.SubmitTask(ctx, &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{Id: id, Intent: "cool_environment", Constraints: map[string]string{"target_temperature": "26"}}})
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func (mesh *testMesh) offline(t *testing.T, id string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := mesh.registryClient.UpdateNodeStatus(ctx, &dtmv1.UpdateNodeStatusRequest{
		NodeId:         id,
		Status:         dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		RegistrationId: mesh.registrations[id],
	})
	if err != nil || !response.GetAccepted() {
		t.Fatalf("offline %q: response=%#v error=%v", id, response, err)
	}
}

type testAgent struct {
	server         *grpc.Server
	listener       net.Listener
	registrationID string
	mu             sync.Mutex
	ids            []string
}

func startTestAgent(t *testing.T, registry dtmv1.NodeRegistryServiceClient, id string, handler agent.Handler) *testAgent {
	t.Helper()
	listener := listen(t)
	router, err := agent.NewRouter(handler)
	if err != nil {
		t.Fatal(err)
	}
	agentServer, err := grpcapi.NewAgentExecutionServer(router)
	if err != nil {
		t.Fatal(err)
	}
	instance := &testAgent{
		listener:       listener,
		registrationID: "test-registration-" + listener.Addr().String(),
	}
	instance.server = grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		if execute, ok := request.(*dtmv1.ExecuteStepRequest); ok {
			instance.mu.Lock()
			instance.ids = append(instance.ids, execute.GetTaskId())
			instance.mu.Unlock()
		}
		return next(ctx, request)
	}))
	dtmv1.RegisterAgentExecutionServiceServer(instance.server, agentServer)
	serve(t, instance.server, listener)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	response, err := registry.RegisterNode(ctx, &dtmv1.RegisterNodeRequest{
		Node: &dtmv1.Node{
			Id:               id,
			Capabilities:     []string{string(handler.Capability())},
			Status:           dtmv1.NodeStatus_NODE_STATUS_ONLINE,
			ExecutionAddress: listener.Addr().String(),
		},
		RegistrationId: instance.registrationID,
	})
	if err != nil || !response.GetAccepted() {
		t.Fatalf("register %q: response=%#v error=%v", id, response, err)
	}
	t.Cleanup(instance.stop)
	return instance
}

func (agent *testAgent) stop() { agent.server.Stop(); _ = agent.listener.Close() }
func (agent *testAgent) taskIDs() []string {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	return append([]string(nil), agent.ids...)
}
func (agent *testAgent) callCount() int { return len(agent.taskIDs()) }

func listen(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}
func serve(t *testing.T, server *grpc.Server, listener net.Listener) {
	t.Helper()
	go func() { _ = server.Serve(listener) }()
}
func dial(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	connection, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	return connection
}
func networkDialer(ctx context.Context, address string) (*grpc.ClientConn, error) {
	return grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
}
