package runtimehost

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/agent"
	"dtm/internal/authoritybinding"
	"dtm/internal/config"
	"dtm/internal/demo"
	"dtm/internal/mapper"
	"dtm/internal/mesh/discovery"
	"dtm/internal/mesh/membership"
	"dtm/internal/mesh/protocol"
	"dtm/internal/model"
	"dtm/internal/task"
	"dtm/internal/ui"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestM7IngressFailsClosedBeforeAuthorityReady(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := listener.Addr().String()
	agentDir := t.TempDir()
	execution, err := NewResourceExecutionCapability(config.Agent{
		Mode:    config.ModeRuntime,
		Node:    config.Node{ID: "m7-blocked", Capabilities: []string{"temperature_sensor"}},
		Storage: config.Storage{Path: filepath.Join(agentDir, "agent.db")},
	}, demo.NewTemperatureHandler())
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	blocked := &blockedAuthoritySession{}
	coreDir := t.TempDir()
	corePath := filepath.Join(coreDir, "core.db")
	identity := protocol.Identity{
		MeshNamespace: "m7-pre-ingress", ProtocolMajor: 1, ProtocolMinor: 0,
		DTMVersion: protocol.DTMVersion, NodeID: "m7-blocked", RuntimeInstance: "session-m7-blocked",
		ControlEndpoint: endpoint,
	}
	var host *RuntimeHost
	host, err = New(Options{
		NodeID: "m7-blocked", Execution: execution, ExecutionAddress: endpoint,
		AuthorityFactory: func() (AuthoritySession, error) { return blocked, nil },
		CoreFactory: func(ctx context.Context) (*CoreCapability, error) {
			return NewCoreCapability(ctx, coreConfig(corePath), CoreOptions{
				Readiness: func() bool { return host != nil && host.IngressReady() },
			})
		},
		Listen: func(network, address string) (net.Listener, error) {
			if network == "tcp" && address == endpoint {
				return listener, nil
			}
			return net.Listen(network, address)
		},
		Mesh: &MeshOptions{
			Identity: identity, Transport: discovery.NewMemoryNetwork().NewTransport(),
			AnnouncementInterval: 20 * time.Millisecond, HandshakeTimeout: 250 * time.Millisecond,
			MembershipTiming: membership.Timing{SuspectAfter: time.Second, ExpireAfter: 2 * time.Second},
		},
	})
	if err != nil {
		_ = listener.Close()
		_ = execution.Close()
		t.Fatal(err)
	}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = host.Close()
		cleanupRuntimeHostTestDirs(t, agentDir, coreDir)
	})

	m7WaitFor(t, 10*time.Second, func() bool {
		return host.CoreReady() && blocked.started.Load() && host.CoreAddress() != ""
	})
	if host.AuthorityReady() || host.IngressReady() {
		t.Fatalf("authority gate unexpectedly open: %+v", host.StatusSnapshot())
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	connection, err := grpc.DialContext(ctx, host.CoreAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_, err = dtmv1.NewCoreServiceClient(connection).SubmitTask(ctx, &dtmv1.SubmitTaskRequest{
		Task:  &dtmv1.Task{Id: "m7-before-authority", Intent: "cool_environment", Constraints: map[string]string{"target_temperature": "26"}},
		Async: true,
	})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("pre-authority SubmitTask() error = %v, code = %v, want Unavailable", err, status.Code(err))
	}
	t.Logf("pre_ingress_submit code=%s error=%q core_address=%s authority_status=%s", status.Code(err), err, host.CoreAddress(), host.StatusSnapshot().AuthorityStatus)
}

func TestM7SelfOrganizingGreenhouseClosedLoop(t *testing.T) {
	const (
		taskID    = "m7-greenhouse-closed-loop"
		namespace = "m7-greenhouse"
	)
	ids := []string{"node-a", "node-b", "node-c"}
	capabilities := map[string][]string{
		"node-a": {"temperature_sensor"},
		"node-b": {"cooling_control"},
		"node-c": nil,
	}
	testDirs := make([]string, 0, len(ids)*2)

	type runtime struct {
		host     *RuntimeHost
		endpoint string
	}
	network := discovery.NewMemoryNetwork()
	runtimes := make([]runtime, 0, len(ids))
	for _, nodeID := range ids {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		endpoint := listener.Addr().String()
		agentDir := t.TempDir()
		testDirs = append(testDirs, agentDir)
		configured := config.Agent{
			Mode:    config.ModeRuntime,
			Node:    config.Node{ID: nodeID, Capabilities: capabilities[nodeID], AdvertiseAddress: endpoint},
			Storage: config.Storage{Path: filepath.Join(agentDir, nodeID+"-agent.db")},
		}
		handlers := make([]agent.Handler, 0, len(capabilities[nodeID]))
		for _, capability := range capabilities[nodeID] {
			switch capability {
			case "temperature_sensor":
				handlers = append(handlers, demo.NewTemperatureHandler())
			case "cooling_control":
				handlers = append(handlers, demo.NewCoolingHandler())
			default:
				_ = listener.Close()
				t.Fatalf("unsupported M7 test capability %q", capability)
			}
		}
		execution, err := NewResourceExecutionCapability(configured, handlers...)
		if err != nil {
			_ = listener.Close()
			t.Fatal(err)
		}
		identity := protocol.Identity{
			MeshNamespace: namespace, ProtocolMajor: 1, ProtocolMinor: 0,
			DTMVersion: protocol.DTMVersion, NodeID: nodeID, RuntimeInstance: "session-" + nodeID,
			ControlEndpoint: endpoint,
		}
		corePath := ":memory:"
		if nodeID == "node-a" {
			coreDir := t.TempDir()
			testDirs = append(testDirs, coreDir)
			corePath = filepath.Join(coreDir, nodeID+"-core.db")
		}
		var host *RuntimeHost
		coreFactory := func(coreCtx context.Context) (*CoreCapability, error) {
			return NewCoreCapability(coreCtx, coreConfig(corePath), CoreOptions{
				Readiness: func() bool { return host != nil && host.IngressReady() },
			})
		}
		authorityFactory := func() (AuthoritySession, error) {
			return authoritybinding.New(authoritybinding.Options{
				NodeID: nodeID, Capabilities: capabilities[nodeID], ExecutionAddress: endpoint,
				HeartbeatInterval: 100 * time.Millisecond,
			})
		}
		host, err = New(Options{
			NodeID: nodeID, Execution: execution, ExecutionAddress: endpoint,
			CoreFactory: coreFactory, AuthorityFactory: authorityFactory,
			Listen: func(network, address string) (net.Listener, error) {
				if network == "tcp" && address == endpoint {
					return listener, nil
				}
				return net.Listen(network, address)
			},
			Mesh: &MeshOptions{
				Identity: identity, Transport: network.NewTransport(), AnnouncementInterval: 20 * time.Millisecond,
				HandshakeTimeout: 250 * time.Millisecond,
				MembershipTiming: membership.Timing{SuspectAfter: time.Second, ExpireAfter: 2 * time.Second},
			},
		})
		if err != nil {
			_ = listener.Close()
			_ = execution.Close()
			t.Fatal(err)
		}
		runtimes = append(runtimes, runtime{host: host, endpoint: endpoint})
	}
	for _, item := range runtimes {
		if err := item.host.Start(); err != nil {
			for _, started := range runtimes {
				_ = started.host.Close()
			}
			t.Fatal(err)
		}
	}
	// Register host shutdown after all per-runtime TempDir cleanups. Testing
	// executes cleanups in LIFO order, so every Core/Agent handle is released
	// before its temporary database directory is removed on Windows.
	t.Cleanup(func() {
		for _, item := range runtimes {
			_ = item.host.Close()
		}
		cleanupRuntimeHostTestDirs(t, testDirs...)
	})

	coordinatorIndex := -1
	lastAuthoritySignature := ""
	stableAuthoritySnapshots := 0
	m7WaitFor(t, 20*time.Second, func() bool {
		resetStability := func() bool {
			lastAuthoritySignature = ""
			stableAuthoritySnapshots = 0
			return false
		}
		selected := -1
		coordinatorNode := ""
		coordinatorRuntime := ""
		coordinatorEndpoint := ""
		for index, item := range runtimes {
			statusSnapshot := item.host.StatusSnapshot()
			if statusSnapshot.Role.LocalIsCoordinator {
				if selected != -1 {
					return resetStability()
				}
				selected = index
				coordinatorNode = string(statusSnapshot.Role.Selection.NodeID)
				coordinatorRuntime = statusSnapshot.Role.Selection.RuntimeInstanceID
				coordinatorEndpoint = statusSnapshot.Role.Selection.ControlEndpoint
			}
		}
		if selected == -1 {
			return resetStability()
		}
		for _, item := range runtimes {
			statusSnapshot := item.host.StatusSnapshot()
			if !statusSnapshot.Role.HasCoordinator || string(statusSnapshot.Role.Selection.NodeID) != coordinatorNode ||
				statusSnapshot.Role.Selection.RuntimeInstanceID != coordinatorRuntime ||
				statusSnapshot.Role.Selection.ControlEndpoint != coordinatorEndpoint {
				return resetStability()
			}
			if len(item.host.ResourceViewSnapshot().ActiveResources) != 2 {
				return resetStability()
			}
		}
		selectedStatus := runtimes[selected].host.StatusSnapshot()
		if !selectedStatus.CoreReady || !selectedStatus.AuthorityReady || !selectedStatus.IngressReady ||
			selectedStatus.AuthorityStatus != AuthorityStatusReady || runtimes[selected].host.Core() == nil {
			return resetStability()
		}
		selectedCore := runtimes[selected].host.Core()
		if len(selectedCore.Resources.ListEligible()) != 2 {
			return resetStability()
		}
		authoritySignature := fmt.Sprintf("coordinator=%s/%s/%s core=%s|", coordinatorNode, coordinatorRuntime, coordinatorEndpoint, runtimes[selected].host.CoreAddress())
		for _, nodeID := range []model.NodeID{"node-a", "node-b"} {
			record, err := selectedCore.Repository.GetNode(context.Background(), nodeID)
			if err != nil || record.Generation < 1 || record.RegistrationID == "" || record.Endpoint == "" ||
				!selectedCore.RegistryAPI.Eligible(nodeID) || !selectedCore.Endpoints.Available(nodeID) {
				return resetStability()
			}
			authoritySignature += fmt.Sprintf("%s/%d/%s/%s|", nodeID, record.Generation, record.RegistrationID, record.Endpoint)
		}
		if authoritySignature == lastAuthoritySignature {
			stableAuthoritySnapshots++
		} else {
			lastAuthoritySignature = authoritySignature
			stableAuthoritySnapshots = 1
		}
		if stableAuthoritySnapshots < 5 {
			return false
		}
		coordinatorIndex = selected
		return true
	})

	coordinator := runtimes[coordinatorIndex]
	core := coordinator.host.Core()
	if core == nil {
		t.Fatal("discovered coordinator has no dynamic Core")
	}
	t.Logf("coordinator node_id=%s runtime_instance_id=%s control_endpoint=%s dynamic_core_address=%s", coordinator.host.StatusSnapshot().Role.Selection.NodeID, coordinator.host.StatusSnapshot().Role.Selection.RuntimeInstanceID, coordinator.host.StatusSnapshot().Role.Selection.ControlEndpoint, coordinator.host.CoreAddress())

	uiMembers := make([]protocol.Identity, 0, len(runtimes))
	for _, item := range runtimes {
		uiMembers = append(uiMembers, item.host.meshIdentity)
	}
	uiBootstrap := runtimes[(coordinatorIndex+1)%len(runtimes)]
	observer, err := ui.NewObserver(ui.ObserverOptions{
		BootstrapEndpoint: uiBootstrap.endpoint,
		Members:           ui.StaticMemberSource(uiMembers),
		QueryTimeout:      time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	readySnapshot := observer.Snapshot(context.Background())
	if readySnapshot.Mesh.RuntimeCount != 3 || readySnapshot.Mesh.CoordinatorCount != 1 ||
		readySnapshot.Mesh.Coordinator != string(coordinator.host.meshIdentity.NodeID) ||
		readySnapshot.Mesh.CoreState != "ACTIVE" || readySnapshot.Mesh.AuthorityState != "READY" ||
		readySnapshot.Mesh.IngressState != "READY" || len(readySnapshot.Resources) != 2 {
		t.Fatalf("M7-VIS ready snapshot = %#v resources=%#v", readySnapshot.Mesh, readySnapshot.Resources)
	}
	t.Logf("m7_vis_ready runtime_count=%d coordinator=%s core=%s authority=%s ingress=%s resources=%d timeline_events=%d", readySnapshot.Mesh.RuntimeCount, readySnapshot.Mesh.Coordinator, readySnapshot.Mesh.CoreState, readySnapshot.Mesh.AuthorityState, readySnapshot.Mesh.IngressState, len(readySnapshot.Resources), len(readySnapshot.Events))

	for _, nodeID := range []model.NodeID{"node-a", "node-b"} {
		record, err := core.Repository.GetNode(context.Background(), nodeID)
		if err != nil {
			t.Fatalf("authoritative GetNode(%s): %v", nodeID, err)
		}
		if record.Generation < 1 || record.RegistrationID == "" || record.Endpoint == "" || !core.RegistryAPI.Eligible(nodeID) {
			t.Fatalf("authoritative node record %s = %+v eligible=%v", nodeID, record, core.RegistryAPI.Eligible(nodeID))
		}
		t.Logf("authority_node node_id=%s node_generation=%d registration_id=%s endpoint=%s eligible=%t", record.ID, record.Generation, record.RegistrationID, record.Endpoint, core.RegistryAPI.Eligible(nodeID))
	}

	input, err := task.New(taskID, "cool_environment", nil, task.Constraints{"target_temperature": "26"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := core.Planner.Plan(input)
	if err != nil {
		t.Fatalf("Planner.Plan(): %v", err)
	}
	if len(plan.Steps) != 2 || plan.Steps[0].ID != "step-1" || plan.Steps[1].ID != "step-2" {
		t.Fatalf("greenhouse plan = %+v", plan)
	}
	mapped, err := core.Mapper.Map(plan)
	if err != nil {
		t.Fatalf("authoritative Mapper.Map(): %v", err)
	}
	if len(mapped.Steps) != 2 {
		t.Fatalf("mapped greenhouse steps = %+v", mapped.Steps)
	}
	validator, err := mapper.NewResourceMappingValidator(core.Resources)
	if err != nil {
		t.Fatal(err)
	}
	mappedByStep := make(map[string]mapper.MappedStep, len(mapped.Steps))
	owners := make(map[model.NodeID]struct{}, len(mapped.Steps))
	for _, mappedStep := range mapped.Steps {
		if err := mappedStep.ResourceRef.Validate(); err != nil {
			t.Fatalf("mapped ResourceRef %s = %+v: %v", mappedStep.ID, mappedStep.ResourceRef, err)
		}
		if err := validator.Validate(mappedStep); err != nil {
			t.Fatalf("authoritative fence validation %s: %v", mappedStep.ID, err)
		}
		mappedByStep[string(mappedStep.ID)] = mappedStep
		owners[mappedStep.ResourceRef.OwnerNodeID] = struct{}{}
		t.Logf("resource_ref step_id=%s capability=%s resource_id=%s owner_node_id=%s owner_node_generation=%d resource_generation=%d registration_id=%s", mappedStep.ID, mappedStep.Capability, mappedStep.ResourceRef.ResourceID, mappedStep.ResourceRef.OwnerNodeID, mappedStep.ResourceRef.OwnerNodeGeneration, mappedStep.ResourceRef.ResourceGeneration, mappedStep.ResourceRef.RegistrationID)
	}
	if len(owners) != 2 {
		t.Fatalf("resource owners = %v, want two distinct remote resources", owners)
	}
	coordinatorNodeID := model.NodeID(coordinator.host.StatusSnapshot().Role.Selection.NodeID)
	outsideCoordinator := 0
	for owner := range owners {
		if owner != coordinatorNodeID {
			outsideCoordinator++
		}
	}
	if outsideCoordinator == 0 {
		t.Fatalf("all mapped resources are local to coordinator %s: %v", coordinatorNodeID, owners)
	}

	connectionContext, cancelConnection := context.WithTimeout(context.Background(), 5*time.Second)
	connection, err := grpc.DialContext(connectionContext, coordinator.host.CoreAddress(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	cancelConnection()
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	coreClient := dtmv1.NewCoreServiceClient(connection)
	submitContext, cancelSubmit := context.WithTimeout(context.Background(), 5*time.Second)
	submitted, err := coreClient.SubmitTask(submitContext, &dtmv1.SubmitTaskRequest{
		Task:  &dtmv1.Task{Id: taskID, Intent: "cool_environment", Constraints: map[string]string{"target_temperature": "26"}},
		Async: true, IdempotencyKey: "m7-greenhouse-idempotency-key",
	})
	cancelSubmit()
	if err != nil {
		t.Fatal(err)
	}
	if submitted.GetTaskId() != taskID || submitted.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_CREATED {
		t.Fatalf("SubmitTask() = %+v, want CREATED acceptance", submitted)
	}
	t.Logf("task_submitted task_id=%s status=%s", submitted.GetTaskId(), submitted.GetStatus())

	var completed *dtmv1.GetTaskStatusResponse
	m7WaitFor(t, 20*time.Second, func() bool {
		queryContext, cancelQuery := context.WithTimeout(context.Background(), 2*time.Second)
		response, queryErr := coreClient.GetTaskStatus(queryContext, &dtmv1.GetTaskStatusRequest{TaskId: taskID})
		cancelQuery()
		if queryErr != nil {
			return false
		}
		if response.GetStatus() == dtmv1.TaskStatus_TASK_STATUS_FAILED {
			t.Fatalf("greenhouse task failed: %+v", response)
		}
		if response.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED {
			return false
		}
		completed = response
		return true
	})
	if completed == nil || completed.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED || len(completed.GetResults()) != 2 {
		t.Fatalf("completed greenhouse task = %+v", completed)
	}

	resultsByStep := make(map[string]*dtmv1.StepResult, len(completed.GetResults()))
	for _, result := range completed.GetResults() {
		if result == nil {
			t.Fatal("greenhouse task returned nil StepResult")
		}
		if result.GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED {
			t.Fatalf("greenhouse step result = %+v", result)
		}
		resultsByStep[result.GetStepId()] = result
		mappedStep, exists := mappedByStep[result.GetStepId()]
		if !exists || result.GetNodeId() != string(mappedStep.NodeID) {
			t.Fatalf("step %s result node=%s mapped=%+v", result.GetStepId(), result.GetNodeId(), mappedStep)
		}
		t.Logf("remote_execution step_id=%s node_id=%s status=%s output=%s", result.GetStepId(), result.GetNodeId(), result.GetStatus(), result.GetOutput())
	}
	sensor := resultsByStep["step-1"]
	cooling := resultsByStep["step-2"]
	if sensor == nil || sensor.GetOutput().GetFields()["temperature"].GetNumberValue() != 30 {
		t.Fatalf("temperature sensor result = %+v", sensor)
	}
	if cooling == nil || !cooling.GetOutput().GetFields()["cooling_started"].GetBoolValue() {
		t.Fatalf("cooling result = %+v", cooling)
	}
	if sensor.GetNodeId() == cooling.GetNodeId() {
		t.Fatalf("greenhouse steps used the same owner: sensor=%s cooling=%s", sensor.GetNodeId(), cooling.GetNodeId())
	}
	if sensor.GetNodeId() == string(coordinatorNodeID) && cooling.GetNodeId() == string(coordinatorNodeID) {
		t.Fatalf("both greenhouse steps executed on Coordinator %s", coordinatorNodeID)
	}

	detail, err := coreClient.GetTask(context.Background(), &dtmv1.GetTaskRequest{TaskId: taskID})
	if err != nil || detail.GetTask().GetStatus() != dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED {
		t.Fatalf("GetTask() = %+v, %v", detail, err)
	}
	listed, err := coreClient.ListTasks(context.Background(), &dtmv1.ListTasksRequest{Status: dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED, Limit: 100})
	if err != nil {
		t.Fatalf("ListTasks() = %v", err)
	}
	listedTask := false
	for _, item := range listed.GetTasks() {
		if item.GetTaskId() == taskID {
			listedTask = true
			break
		}
	}
	if !listedTask {
		t.Fatalf("ListTasks() did not return %s: %+v", taskID, listed)
	}
	executions, err := coreClient.GetTaskExecutions(context.Background(), &dtmv1.GetTaskExecutionsRequest{TaskId: taskID})
	if err != nil || len(executions.GetExecutions()) != 2 {
		t.Fatalf("GetTaskExecutions() = %+v, %v", executions, err)
	}
	for _, execution := range executions.GetExecutions() {
		if execution.GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED {
			t.Fatalf("task execution = %+v", execution)
		}
	}

	taskSnapshot := observer.Snapshot(context.Background())
	if taskSnapshot.Status != "READY" || len(taskSnapshot.Tasks) == 0 {
		t.Fatalf("M7-VIS task snapshot = %#v", taskSnapshot)
	}
	var observedTask *ui.TaskView
	for index := range taskSnapshot.Tasks {
		if taskSnapshot.Tasks[index].TaskID == taskID {
			observedTask = &taskSnapshot.Tasks[index]
			break
		}
	}
	if observedTask == nil || observedTask.Status != "SUCCEEDED" || len(observedTask.Steps) != 2 ||
		observedTask.Steps[0].MappedNode == "" || observedTask.Steps[1].MappedNode == "" ||
		observedTask.Steps[0].Result["temperature"] != float64(30) || observedTask.Steps[1].Result["cooling_started"] != true ||
		observedTask.Steps[0].ResourceRefHistory != "PARTIAL" {
		t.Fatalf("M7-VIS task observation = %#v", observedTask)
	}
	t.Logf("m7_vis_task task_id=%s status=%s steps=%d timeline_events=%d mapping_history=%s", observedTask.TaskID, observedTask.Status, len(observedTask.Steps), len(taskSnapshot.Events), observedTask.Steps[0].ResourceRefHistory)

	for index, item := range runtimes {
		var bootstrap *dtmv1.GetRuntimeStatusResponse
		var bootstrapErr error
		m7WaitFor(t, 5*time.Second, func() bool {
			bootstrapContext, cancelBootstrap := context.WithTimeout(context.Background(), time.Second)
			bootstrap, bootstrapErr = item.host.runtimeClient.GetRuntimeStatus(bootstrapContext, item.endpoint)
			cancelBootstrap()
			return bootstrapErr == nil
		})
		if bootstrap.GetCoordinator().GetNodeId() != string(coordinatorNodeID) ||
			bootstrap.GetCoordinator().GetRuntimeInstanceId() != coordinator.host.RuntimeInstanceID() {
			t.Fatalf("runtime[%d] bootstrap coordinator = %+v, want node=%s runtime=%s", index, bootstrap.GetCoordinator(), coordinatorNodeID, coordinator.host.RuntimeInstanceID())
		}
		if index == coordinatorIndex {
			if bootstrap.GetReadiness() != dtmv1.RuntimeReadiness_RUNTIME_READINESS_READY || !bootstrap.GetIngressReady() || bootstrap.GetCoreAddress() != coordinator.host.CoreAddress() {
				t.Fatalf("coordinator bootstrap status = %+v", bootstrap)
			}
		} else if bootstrap.GetCoreAddress() != "" {
			t.Fatalf("non-coordinator runtime[%d] exposed Core address: %+v", index, bootstrap)
		}
		t.Logf("bootstrap_runtime index=%d node_id=%s readiness=%s coordinator=%s core_address=%s ingress_ready=%t", index, bootstrap.GetRuntime().GetNodeId(), bootstrap.GetReadiness(), bootstrap.GetCoordinator().GetNodeId(), bootstrap.GetCoreAddress(), bootstrap.GetIngressReady())
	}
}

func m7WaitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-ticker.C:
			if time.Now().After(deadline) {
				t.Fatal("timed out waiting for M7 runtime state")
			}
		case <-time.After(time.Until(deadline)):
			t.Fatal("timed out waiting for M7 runtime state")
		}
	}
}
