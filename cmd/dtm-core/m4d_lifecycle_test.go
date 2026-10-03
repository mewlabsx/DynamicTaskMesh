package main

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/agent"
	"dtm/internal/application"
	"dtm/internal/demo"
	"dtm/internal/execution"
	"dtm/internal/invocation"
	"dtm/internal/lifecycle"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/planner"
	sqliteplatform "dtm/internal/platform/sqlite"
	"dtm/internal/resourcedirectory"
	meshruntime "dtm/internal/runtime"
	"dtm/internal/storage"
	"dtm/internal/task"
	"dtm/internal/transport/grpcapi"

	"google.golang.org/grpc"
)

type m4dAgentCall struct {
	TaskID     model.TaskID
	StepID     model.StepID
	NodeID     model.NodeID
	Attempt    uint32
	ResourceID model.ResourceID
}

type m4dRecordingAgent struct {
	router *agent.Router

	mu    sync.Mutex
	calls []m4dAgentCall
}

func (handler *m4dRecordingAgent) Execute(
	ctx context.Context,
	step mapper.MappedStep,
) (execution.StepResult, error) {
	result, err := handler.router.Execute(ctx, step)
	handler.mu.Lock()
	handler.calls = append(handler.calls, m4dAgentCall{
		TaskID:     "",
		StepID:     step.ID,
		NodeID:     step.NodeID,
		Attempt:    0,
		ResourceID: step.ResourceRef.ResourceID,
	})
	handler.mu.Unlock()
	return result, err
}

func (handler *m4dRecordingAgent) count(stepID model.StepID) int {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	count := 0
	for _, call := range handler.calls {
		if call.StepID == stepID {
			count++
		}
	}
	return count
}

func (handler *m4dRecordingAgent) total() int {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	return len(handler.calls)
}

func startM4DAgent(t *testing.T) (*m4dRecordingAgent, string) {
	t.Helper()
	router, err := agent.NewRouter(demo.NewTemperatureHandler(), demo.NewCoolingHandler())
	if err != nil {
		t.Fatal(err)
	}
	recording := &m4dRecordingAgent{router: router}
	executionServer, err := grpcapi.NewAgentExecutionServer(recording,
		grpcapi.WithInvocationAdapter(invocation.NewLegacyCapabilityAdapter()),
	)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	dtmv1.RegisterAgentExecutionServiceServer(server, executionServer)
	dtmv1.RegisterResourceInvocationServiceServer(server, executionServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	address := listener.Addr().String()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		conn, dialErr := net.DialTimeout("tcp", address, 25*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			return recording, address
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("agent execution server did not become ready: %s", address)
	return nil, ""
}

func closeM4DDependencies(deps dependencies) {
	if deps.AsyncTasks != nil {
		deps.AsyncTasks.Close()
	}
	if deps.Repository != nil {
		_ = deps.Repository.Close()
	}
}

func registerM4DNode(
	t *testing.T,
	deps dependencies,
	nodeID model.NodeID,
	address string,
	registrationID string,
	capabilities ...string,
) {
	t.Helper()
	response, err := deps.RegistryAPI.RegisterNode(context.Background(), &dtmv1.RegisterNodeRequest{
		Node: &dtmv1.Node{
			Id:               string(nodeID),
			Capabilities:     append([]string(nil), capabilities...),
			Status:           dtmv1.NodeStatus_NODE_STATUS_ONLINE,
			ExecutionAddress: address,
		},
		RegistrationId: registrationID,
	})
	if err != nil || response == nil || !response.GetAccepted() {
		t.Fatalf("RegisterNode(%q, %q) response=%#v error=%v", nodeID, registrationID, response, err)
	}
}

func m4dSingleResourcePlan(taskID model.TaskID) planner.Plan {
	return planner.Plan{
		TaskID: taskID,
		Steps: []planner.Step{{
			ID:              "step-1",
			Capability:      "temperature_sensor",
			IdempotencyMode: model.IdempotencyIdempotent,
			Inputs:          map[string]string{"operation": "read_temperature"},
		}},
	}
}

func assertM4DCompleteRef(t *testing.T, step mapper.MappedStep) {
	t.Helper()
	if err := step.ResourceRef.Validate(); err != nil {
		t.Fatalf("step %q ResourceRef.Validate() = %v (%+v)", step.ID, err, step.ResourceRef)
	}
	if step.ResourceRef.OwnerNodeID != step.NodeID {
		t.Fatalf("step %q ResourceRef.OwnerNodeID=%q NodeID=%q", step.ID, step.ResourceRef.OwnerNodeID, step.NodeID)
	}
	if step.ResourceRef == (model.ResourceRef{}) {
		t.Fatalf("step %q has zero ResourceRef", step.ID)
	}
}

func m4dAdvanceTemperatureGeneration(
	directory *resourcedirectory.Directory,
	nodeID model.NodeID,
) (model.ResourceRef, resourcedirectory.ResourceRecordView, error) {
	views := directory.ListByNode(nodeID)
	if len(views) == 0 {
		return model.ResourceRef{}, resourcedirectory.ResourceRecordView{}, errors.New("no resource views for node")
	}
	var target resourcedirectory.ResourceRecordView
	descriptors := make([]model.ResourceDescriptor, 0, len(views))
	for _, view := range views {
		descriptor := view.Descriptor.Clone()
		if descriptor.Type == model.ResourceType("temperature_sensor") {
			target = view
			descriptor.Attributes["m4d_generation_probe"] = "generation-2"
		}
		descriptors = append(descriptors, descriptor)
	}
	if target.Descriptor.ID == "" {
		return model.ResourceRef{}, resourcedirectory.ResourceRecordView{}, errors.New("temperature resource not found")
	}
	oldRef, err := model.NewResourceRef(
		target.Descriptor.ID,
		target.Descriptor.Generation,
		target.Descriptor.OwnerNodeID,
		target.NodeGeneration,
		target.RegistrationID,
	)
	if err != nil {
		return model.ResourceRef{}, resourcedirectory.ResourceRecordView{}, err
	}
	snapshot, err := resourcedirectory.NewNodeResourceSnapshot(
		nodeID,
		target.NodeGeneration,
		target.RegistrationID,
		descriptors,
	)
	if err != nil {
		return model.ResourceRef{}, resourcedirectory.ResourceRecordView{}, err
	}
	if _, err := directory.ApplyNodeSnapshot(snapshot); err != nil {
		return model.ResourceRef{}, resourcedirectory.ResourceRecordView{}, err
	}
	current, err := directory.GetByID(target.Descriptor.ID)
	return oldRef, current, err
}

func m4dRepublishAllResources(
	directory *resourcedirectory.Directory,
	nodeID model.NodeID,
) (resourcedirectory.ResourceRecordView, error) {
	views := directory.ListByNode(nodeID)
	if len(views) == 0 {
		return resourcedirectory.ResourceRecordView{}, errors.New("no resources to republish")
	}
	descriptors := make([]model.ResourceDescriptor, 0, len(views))
	for _, view := range views {
		descriptors = append(descriptors, view.Descriptor.Clone())
	}
	snapshot, err := resourcedirectory.NewNodeResourceSnapshot(
		nodeID,
		views[0].NodeGeneration,
		views[0].RegistrationID,
		descriptors,
	)
	if err != nil {
		return resourcedirectory.ResourceRecordView{}, err
	}
	if _, err := directory.ApplyNodeSnapshot(snapshot); err != nil {
		return resourcedirectory.ResourceRecordView{}, err
	}
	return directory.GetByID(views[0].Descriptor.ID)
}

type m4dStaleAfterFirstQuerySource struct {
	delegate  mapper.ResourceCandidateSource
	directory *resourcedirectory.Directory
	nodeID    model.NodeID
	once      sync.Once
	err       error
}

func (source *m4dStaleAfterFirstQuerySource) Query(
	query mapper.ResourceCandidateQuery,
) ([]resourcedirectory.ResourceRecordView, error) {
	views, err := source.delegate.Query(query)
	if err != nil {
		return nil, err
	}
	source.once.Do(func() {
		_, _, source.err = m4dAdvanceTemperatureGeneration(source.directory, source.nodeID)
	})
	if source.err != nil {
		return nil, source.err
	}
	return views, nil
}

type m4dRecordingValidator struct {
	delegate *mapper.ResourceMappingValidator

	mu   sync.Mutex
	refs []model.ResourceRef
}

func (validator *m4dRecordingValidator) Validate(step mapper.MappedStep) error {
	validator.mu.Lock()
	validator.refs = append(validator.refs, step.ResourceRef)
	validator.mu.Unlock()
	return validator.delegate.Validate(step)
}

func (validator *m4dRecordingValidator) snapshot() []model.ResourceRef {
	validator.mu.Lock()
	defer validator.mu.Unlock()
	return append([]model.ResourceRef(nil), validator.refs...)
}

func newM4DResourceTaskService(
	t *testing.T,
	deps dependencies,
	source mapper.ResourceCandidateSource,
	validator meshruntime.MappingValidator,
) *application.TaskService {
	t.Helper()
	meshMapper, err := mapper.New(
		deps.Registry,
		mapper.WithEligibility(deps.RegistryAPI),
		mapper.WithResourceCandidateSource(source),
	)
	if err != nil {
		t.Fatal(err)
	}
	meshRuntime, err := meshruntime.New(
		deps.Executor,
		meshruntime.WithRetryPolicy(meshruntime.RetryPolicy{MaxAttempts: 1}),
		meshruntime.WithRemapping(meshMapper, meshruntime.DefaultRemappingPolicy),
		meshruntime.WithMappingValidator(validator),
	)
	if err != nil {
		t.Fatal(err)
	}
	service, err := application.NewTaskService(
		lifecycle.New,
		planner.New(),
		meshMapper,
		meshRuntime,
		application.WithTaskRepository(deps.Repository),
		application.WithRecoveryEligibility(deps.RegistryAPI),
		application.WithResourceRecoveryMapper(meshMapper),
	)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func TestM4DResourceLifecycleAcceptance(t *testing.T) {
	deps, err := compose()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeM4DDependencies(deps) })
	agentRecorder, address := startM4DAgent(t)
	const nodeID = model.NodeID("m4d-full-node")
	registerM4DNode(t, deps, nodeID, address, "m4d-registration-1", "temperature_sensor", "cooling_control")

	input := coolEnvironmentInput("m4d-full-lifecycle")
	plan, err := deps.Planner.Plan(input)
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := deps.Mapper.Map(plan)
	if err != nil {
		t.Fatalf("Resource Map() error = %v", err)
	}
	assertCompleteResourceRefs(t, mapped)
	if len(deps.Resources.ListEligible()) != 2 || !deps.Endpoints.Available(nodeID) {
		t.Fatalf("resource or endpoint readiness: eligible=%d endpoint=%v", len(deps.Resources.ListEligible()), deps.Endpoints.Available(nodeID))
	}

	outcome, err := deps.TaskService.Submit(context.Background(), input)
	if err != nil || outcome.State != lifecycle.StateSuccess {
		t.Fatalf("full Resource submit outcome=%#v error=%v", outcome, err)
	}
	record, err := deps.TaskService.Find(context.Background(), input.ID)
	if err != nil || record.State != lifecycle.StateSuccess {
		t.Fatalf("completed task record=%#v error=%v", record, err)
	}
	executions, err := deps.Repository.ListExecutionsByTask(context.Background(), input.ID)
	if err != nil || len(executions) != 2 {
		t.Fatalf("execution history=%#v error=%v", executions, err)
	}
	for _, executionRecord := range executions {
		if executionRecord.State != storage.ExecutionStateSucceeded || executionRecord.NodeID != nodeID {
			t.Fatalf("execution record=%#v, want succeeded on owner %q", executionRecord, nodeID)
		}
	}
	if agentRecorder.total() != 2 {
		t.Fatalf("Agent RPC calls=%d, want 2", agentRecorder.total())
	}
}

func TestM4DStaleMappingCompletesThroughApplicationAndRuntime(t *testing.T) {
	deps, err := compose()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeM4DDependencies(deps) })
	agentRecorder, address := startM4DAgent(t)
	const nodeID = model.NodeID("m4d-stale-node")
	registerM4DNode(t, deps, nodeID, address, "m4d-stale-registration-1", "temperature_sensor", "cooling_control")

	baseSource, err := mapper.NewResourceCandidateSource(deps.Resources, restoredResourceReadiness{})
	if err != nil {
		t.Fatal(err)
	}
	source := &m4dStaleAfterFirstQuerySource{delegate: baseSource, directory: deps.Resources, nodeID: nodeID}
	resourceValidator, err := mapper.NewResourceMappingValidator(deps.Resources)
	if err != nil {
		t.Fatal(err)
	}
	recordingValidator := &m4dRecordingValidator{delegate: resourceValidator}
	service := newM4DResourceTaskService(t, deps, source, recordingValidator)

	input := coolEnvironmentInput("m4d-stale-application")
	outcome, err := service.Submit(context.Background(), input)
	if err != nil || outcome.State != lifecycle.StateSuccess {
		t.Fatalf("stale Resource submit outcome=%#v error=%v", outcome, err)
	}
	refs := recordingValidator.snapshot()
	if len(refs) < 2 || refs[0].ResourceGeneration != 1 || refs[1].ResourceGeneration != 2 {
		t.Fatalf("fence validation refs=%#v, want old G1 then remapped G2", refs)
	}
	executions, err := deps.Repository.GetExecutions(context.Background(), input.ID, "step-1")
	if err != nil || len(executions) != 1 || executions[0].AttemptNo != 1 || executions[0].State != storage.ExecutionStateSucceeded {
		t.Fatalf("stale step execution history=%#v error=%v, want one successful attempt", executions, err)
	}
	if agentRecorder.count("step-1") != 1 {
		t.Fatalf("stale step Agent RPC calls=%d, want 1 after immediate remap", agentRecorder.count("step-1"))
	}
}

func TestM4DGenerationReregistrationWithdrawRepublishAndQuiesce(t *testing.T) {
	t.Run("resource_generation_stale", func(t *testing.T) {
		deps, err := compose()
		if err != nil {
			t.Fatal(err)
		}
		defer closeM4DDependencies(deps)
		agentRecorder, address := startM4DAgent(t)
		const nodeID = model.NodeID("m4d-generation-node")
		registerM4DNode(t, deps, nodeID, address, "m4d-generation-registration", "temperature_sensor")
		mapped, err := deps.Mapper.Map(m4dSingleResourcePlan("m4d-generation-task"))
		if err != nil {
			t.Fatal(err)
		}
		oldStep := mapped.Steps[0]
		oldRef, current, err := m4dAdvanceTemperatureGeneration(deps.Resources, nodeID)
		if err != nil {
			t.Fatal(err)
		}
		if oldRef != oldStep.ResourceRef || current.Descriptor.Generation != oldRef.ResourceGeneration+1 {
			t.Fatalf("generation transition old=%+v mapped=%+v current=%+v", oldRef, oldStep.ResourceRef, current)
		}
		validator, err := mapper.NewResourceMappingValidator(deps.Resources)
		if err != nil {
			t.Fatal(err)
		}
		if err := validator.Validate(oldStep); !errors.Is(err, mapper.ErrStaleResourceMapping) {
			t.Fatalf("old mapping validation=%v, want ErrStaleResourceMapping", err)
		}
		var progress []meshruntime.Progress
		result, err := deps.Runtime.ExecuteWithObserver(context.Background(), mapped, func(_ context.Context, value meshruntime.Progress) error {
			progress = append(progress, value)
			return nil
		})
		if err != nil || result.Status != execution.StatusSucceeded {
			t.Fatalf("stale generation execution result=%#v error=%v", result, err)
		}
		if len(progress) < 3 || progress[0].State != meshruntime.ProgressRemapped || progress[1].State != meshruntime.ProgressAttemptStarted || progress[1].Attempt != 1 {
			t.Fatalf("stale generation progress=%#v, want remap before attempt 1", progress)
		}
		if agentRecorder.count("step-1") != 1 {
			t.Fatalf("stale generation Agent RPC calls=%d, want 1", agentRecorder.count("step-1"))
		}
	})

	t.Run("node_reregistration_fence", func(t *testing.T) {
		deps, err := compose()
		if err != nil {
			t.Fatal(err)
		}
		defer closeM4DDependencies(deps)
		agentRecorder, address := startM4DAgent(t)
		const nodeID = model.NodeID("m4d-reregister-node")
		registerM4DNode(t, deps, nodeID, address, "m4d-reregister-registration-1", "temperature_sensor")
		mapped, err := deps.Mapper.Map(m4dSingleResourcePlan("m4d-reregister-task"))
		if err != nil {
			t.Fatal(err)
		}
		oldStep := mapped.Steps[0]
		if _, err := deps.RegistryAPI.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{
			NodeId: string(nodeID), RegistrationId: "m4d-reregister-registration-1", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		}); err != nil {
			t.Fatal(err)
		}
		registerM4DNode(t, deps, nodeID, address, "m4d-reregister-registration-2", "temperature_sensor")
		currentNode, err := deps.Repository.GetNode(context.Background(), nodeID)
		if err != nil {
			t.Fatal(err)
		}
		currentView, err := deps.Resources.GetByID(oldStep.ResourceRef.ResourceID)
		if err != nil {
			t.Fatal(err)
		}
		if currentNode.Generation == oldStep.ResourceRef.OwnerNodeGeneration || currentNode.RegistrationID == oldStep.ResourceRef.RegistrationID || currentView.Descriptor.Generation == oldStep.ResourceRef.ResourceGeneration {
			t.Fatalf("reregistration did not advance fence: node=%+v view=%+v old=%+v", currentNode, currentView, oldStep.ResourceRef)
		}
		var progress []meshruntime.Progress
		result, err := deps.Runtime.ExecuteWithObserver(context.Background(), mapped, func(_ context.Context, value meshruntime.Progress) error {
			progress = append(progress, value)
			return nil
		})
		if err != nil || result.Status != execution.StatusSucceeded {
			t.Fatalf("reregistration execution result=%#v error=%v", result, err)
		}
		if len(progress) < 3 || progress[0].State != meshruntime.ProgressRemapped || progress[1].State != meshruntime.ProgressAttemptStarted {
			t.Fatalf("reregistration progress=%#v, want immediate remap before attempt", progress)
		}
		if agentRecorder.count("step-1") != 1 {
			t.Fatalf("reregistration Agent RPC calls=%d, want 1", agentRecorder.count("step-1"))
		}
	})

	t.Run("withdraw_republish", func(t *testing.T) {
		deps, err := compose()
		if err != nil {
			t.Fatal(err)
		}
		defer closeM4DDependencies(deps)
		agentRecorder, address := startM4DAgent(t)
		const nodeID = model.NodeID("m4d-withdraw-node")
		registerM4DNode(t, deps, nodeID, address, "m4d-withdraw-registration", "temperature_sensor")
		plan := m4dSingleResourcePlan("m4d-withdraw-task")
		mapped, err := deps.Mapper.Map(plan)
		if err != nil {
			t.Fatal(err)
		}
		oldStep := mapped.Steps[0]
		if err := deps.Resources.WithdrawNodeResources(nodeID, oldStep.ResourceRef.OwnerNodeGeneration, oldStep.ResourceRef.RegistrationID); err != nil {
			t.Fatal(err)
		}
		if _, err := deps.Mapper.Map(plan); !errors.Is(err, mapper.ErrResourceUnavailable) {
			t.Fatalf("Map() while withdrawn=%v, want ErrResourceUnavailable", err)
		}
		if _, err := deps.Runtime.Execute(context.Background(), mapped); !errors.Is(err, mapper.ErrResourceUnavailable) {
			t.Fatalf("execute withdrawn mapping=%v, want ErrResourceUnavailable", err)
		}
		if agentRecorder.total() != 0 {
			t.Fatalf("withdrawn Resource Agent RPC calls=%d, want 0", agentRecorder.total())
		}
		current, err := m4dRepublishAllResources(deps.Resources, nodeID)
		if err != nil {
			t.Fatal(err)
		}
		if current.PublicationState != resourcedirectory.PublicationStatePublished || !current.Eligible || current.Descriptor.Generation <= oldStep.ResourceRef.ResourceGeneration {
			t.Fatalf("republished view=%+v, want eligible new generation", current)
		}
		remapped, err := deps.Mapper.Map(plan)
		if err != nil {
			t.Fatal(err)
		}
		assertM4DCompleteRef(t, remapped.Steps[0])
		result, err := deps.Runtime.Execute(context.Background(), remapped)
		if err != nil || result.Status != execution.StatusSucceeded {
			t.Fatalf("republished execution result=%#v error=%v", result, err)
		}
		if agentRecorder.count("step-1") != 1 {
			t.Fatalf("republished Agent RPC calls=%d, want 1", agentRecorder.count("step-1"))
		}
	})

	t.Run("quiesce_requires_new_fence", func(t *testing.T) {
		deps, err := compose()
		if err != nil {
			t.Fatal(err)
		}
		defer closeM4DDependencies(deps)
		agentRecorder, address := startM4DAgent(t)
		const nodeID = model.NodeID("m4d-quiesce-node")
		const registrationID = "m4d-quiesce-registration-1"
		registerM4DNode(t, deps, nodeID, address, registrationID, "temperature_sensor")
		plan := m4dSingleResourcePlan("m4d-quiesce-task")
		mapped, err := deps.Mapper.Map(plan)
		if err != nil {
			t.Fatal(err)
		}
		deps.Resources.QuiesceNode(nodeID, resourcedirectory.IneligibleReasonPublicationInProgress)
		if _, err := deps.Mapper.Map(plan); !errors.Is(err, mapper.ErrResourceUnavailable) {
			t.Fatalf("Map() while quiesced=%v, want ErrResourceUnavailable", err)
		}
		if _, err := deps.Runtime.Execute(context.Background(), mapped); !errors.Is(err, mapper.ErrResourceUnavailable) {
			t.Fatalf("execute quiesced mapping=%v, want ErrResourceUnavailable", err)
		}
		if agentRecorder.total() != 0 {
			t.Fatalf("quiesced Resource Agent RPC calls=%d, want 0", agentRecorder.total())
		}
		if _, err := deps.RegistryAPI.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{
			NodeId: string(nodeID), RegistrationId: registrationID, Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		}); err != nil {
			t.Fatal(err)
		}
		registerM4DNode(t, deps, nodeID, address, "m4d-quiesce-registration-2", "temperature_sensor")
		remapped, err := deps.Mapper.Map(plan)
		if err != nil {
			t.Fatal(err)
		}
		assertM4DCompleteRef(t, remapped.Steps[0])
		if remapped.Steps[0].ResourceRef.RegistrationID == mapped.Steps[0].ResourceRef.RegistrationID {
			t.Fatalf("quiesce recovery reused old registration fence: old=%+v new=%+v", mapped.Steps[0].ResourceRef, remapped.Steps[0].ResourceRef)
		}
		result, err := deps.Runtime.Execute(context.Background(), remapped)
		if err != nil || result.Status != execution.StatusSucceeded {
			t.Fatalf("post-quiesce execution result=%#v error=%v", result, err)
		}
		if agentRecorder.count("step-1") != 1 {
			t.Fatalf("post-quiesce Agent RPC calls=%d, want 1", agentRecorder.count("step-1"))
		}
	})
}

func TestM4DCoreRestartResourceRecoveryAndNonIdempotentSafety(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m4d-recovery.db")
	ctx := context.Background()
	first, err := composeWithConfig(ctx, resourceSchedulingConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	agentRecorder, address := startM4DAgent(t)
	const nodeID = model.NodeID("m4d-recovery-node")
	registerM4DNode(t, first, nodeID, address, "m4d-recovery-registration-1", "temperature_sensor")
	seedM4DInterruptedTask(t, first.Repository, "m4d-recovery-idempotent", nodeID, model.IdempotencyIdempotent)
	seedM4DInterruptedTask(t, first.Repository, "m4d-recovery-nonidempotent", nodeID, model.IdempotencyNonIdempotent)
	closeM4DDependencies(first)

	second, err := composeWithConfig(ctx, resourceSchedulingConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer closeM4DDependencies(second)
	plan := m4dSingleResourcePlan("m4d-recovery-probe")
	if _, err := second.Mapper.Map(plan); !errors.Is(err, mapper.ErrResourceUnavailable) {
		t.Fatalf("Map() before re-registration=%v, want ErrResourceUnavailable", err)
	}
	if len(second.Resources.ListEligible()) != 0 || len(second.Registry.Discover("temperature_sensor")) != 0 {
		t.Fatalf("restart restored an executable Legacy/resource candidate: eligible=%d discovery=%v", len(second.Resources.ListEligible()), second.Registry.Discover("temperature_sensor"))
	}
	registerM4DNode(t, second, nodeID, address, "m4d-recovery-registration-2", "temperature_sensor")

	if err := second.TaskService.Recover(ctx, "m4d-recovery-idempotent"); err != nil {
		t.Fatalf("idempotent Resource recovery error=%v", err)
	}
	recovered, err := second.TaskService.Find(ctx, "m4d-recovery-idempotent")
	if err != nil || recovered.State != lifecycle.StateSuccess {
		t.Fatalf("idempotent recovered task=%#v error=%v", recovered, err)
	}
	executions, err := second.Repository.GetExecutions(ctx, "m4d-recovery-idempotent", "step-1")
	if err != nil || len(executions) != 2 || executions[0].State != storage.ExecutionStateFailed || executions[1].State != storage.ExecutionStateSucceeded {
		t.Fatalf("idempotent recovery executions=%#v error=%v", executions, err)
	}
	if agentRecorder.count("step-1") != 1 {
		t.Fatalf("idempotent recovery Agent RPC calls=%d, want one retry", agentRecorder.count("step-1"))
	}

	if err := second.TaskService.Recover(ctx, "m4d-recovery-nonidempotent"); err != nil {
		t.Fatalf("non-idempotent Resource recovery error=%v", err)
	}
	nonIdempotent, err := second.TaskService.Find(ctx, "m4d-recovery-nonidempotent")
	if err != nil || nonIdempotent.State != lifecycle.StateFailed {
		t.Fatalf("non-idempotent recovered task=%#v error=%v", nonIdempotent, err)
	}
	nonIdempotentExecutions, err := second.Repository.GetExecutions(ctx, "m4d-recovery-nonidempotent", "step-1")
	if err != nil || len(nonIdempotentExecutions) != 1 || nonIdempotentExecutions[0].State != storage.ExecutionStateFailed {
		t.Fatalf("non-idempotent recovery executions=%#v error=%v", nonIdempotentExecutions, err)
	}
	if agentRecorder.total() != 1 {
		t.Fatalf("non-idempotent recovery sent an Agent RPC: total calls=%d", agentRecorder.total())
	}
}

func seedM4DInterruptedTask(
	t *testing.T,
	repository *sqliteplatform.Repository,
	taskID model.TaskID,
	nodeID model.NodeID,
	mode model.IdempotencyMode,
) {
	t.Helper()
	now := time.Now().UTC()
	stepID := model.StepID("step-1")
	if err := repository.CreateTask(context.Background(), storage.Task{
		ID: taskID, Intent: "cool_environment", Requirements: []model.Capability{"temperature_sensor"},
		Constraints: task.Constraints{"target_temperature": "26"}, State: lifecycle.StateRunning,
		CreatedAt: now, UpdatedAt: now, StartedAt: &now, Version: 1,
		Steps: []storage.TaskStep{{
			ID: stepID, TaskID: taskID, Sequence: 0, Capability: "temperature_sensor",
			IdempotencyMode: mode, Input: map[string]string{"operation": "read_temperature"},
			State: lifecycle.StepStateDispatched, AssignedNodeID: &nodeID, MaxAttempts: 2,
			CreatedAt: now, UpdatedAt: now, Version: 1,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.StartExecution(context.Background(), storage.StartExecutionRequest{
		ExecutionID: model.ExecutionID(string(taskID) + "-execution-1"), TaskID: taskID, StepID: stepID,
		AttemptNo: 1, NodeID: nodeID, Request: map[string]string{"operation": "read_temperature"},
		ExpectedStepState: lifecycle.StepStateDispatched, ExpectedStepVersion: 1, StartedAt: now,
		Event: storage.TaskEvent{TaskID: taskID, StepID: &stepID, Type: "execution_started",
			FromState: string(lifecycle.StepStateDispatched), ToState: string(lifecycle.StepStateRunning), CreatedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
}

type m4dReadiness bool

func (readiness m4dReadiness) Ready() bool { return bool(readiness) }

func TestM4DLegacyRegressionAndResourceNoFallback(t *testing.T) {
	deps, err := compose()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeM4DDependencies(deps) })
	const nodeA = model.NodeID("m4d-sort-node-a")
	const nodeB = model.NodeID("m4d-sort-node-b")
	registerM4DNode(t, deps, nodeB, "127.0.0.1:5012", "m4d-sort-registration-b", "temperature_sensor")
	registerM4DNode(t, deps, nodeA, "127.0.0.1:5011", "m4d-sort-registration-a", "temperature_sensor")
	plan := m4dSingleResourcePlan("m4d-legacy-regression")

	resourceMapped, err := deps.Mapper.Map(plan)
	if err != nil {
		t.Fatal(err)
	}
	assertM4DCompleteRef(t, resourceMapped.Steps[0])
	if resourceMapped.Steps[0].NodeID != nodeA {
		t.Fatalf("Resource candidate order selected %q, want ProviderNodeID-first %q", resourceMapped.Steps[0].NodeID, nodeA)
	}

	legacyMapper, err := mapper.New(deps.Registry)
	if err != nil {
		t.Fatal(err)
	}
	legacyMapped, err := legacyMapper.Map(plan)
	if err != nil {
		t.Fatal(err)
	}
	if legacyMapped.Steps[0].ResourceRef != (model.ResourceRef{}) || legacyMapped.Steps[0].NodeID != nodeA {
		t.Fatalf("Legacy mapping=%#v, want zero ResourceRef and node %q", legacyMapped, nodeA)
	}

	for _, nodeID := range []model.NodeID{nodeA, nodeB} {
		record, err := deps.Repository.GetNode(context.Background(), nodeID)
		if err != nil {
			t.Fatal(err)
		}
		if err := deps.Resources.WithdrawNodeResources(nodeID, record.Generation, record.RegistrationID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := deps.Mapper.Map(plan); !errors.Is(err, mapper.ErrResourceUnavailable) {
		t.Fatalf("Resource mode after all resources withdrawn=%v, want ErrResourceUnavailable", err)
	}
	if len(deps.Registry.Discover("temperature_sensor")) != 2 {
		t.Fatalf("Legacy capability registry unexpectedly lost candidates: %v", deps.Registry.Discover("temperature_sensor"))
	}
	legacyAfterWithdraw, err := legacyMapper.Map(plan)
	if err != nil || legacyAfterWithdraw.Steps[0].ResourceRef != (model.ResourceRef{}) {
		t.Fatalf("Legacy mapping after Resource withdrawal=%#v error=%v", legacyAfterWithdraw, err)
	}

	baseSource, err := mapper.NewResourceCandidateSource(deps.Resources, m4dReadiness(false))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := baseSource.Query(mapper.ResourceCandidateQuery{Requirement: mustM4DRequirement(t)}); !errors.Is(err, mapper.ErrResourceCandidateSourceNotReady) {
		t.Fatalf("not-ready Candidate Source error=%v, want ErrResourceCandidateSourceNotReady", err)
	}
	if _, err := model.LegacyCapabilityRequirement("unsupported_m4d_capability"); !errors.Is(err, model.ErrUnsupportedLegacyCapability) {
		t.Fatalf("unsupported capability error=%v, want ErrUnsupportedLegacyCapability", err)
	}
}

func mustM4DRequirement(t *testing.T) model.ResourceRequirement {
	t.Helper()
	requirement, err := model.LegacyCapabilityRequirement("temperature_sensor")
	if err != nil {
		t.Fatal(err)
	}
	return requirement
}
