package main

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/application"
	"dtm/internal/config"
	"dtm/internal/execution"
	"dtm/internal/invocation"
	"dtm/internal/lifecycle"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/platform/sqlite"
	storageport "dtm/internal/storage"
	"dtm/internal/task"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
)

type recoveryAgentServer struct {
	dtmv1.UnimplementedAgentExecutionServiceServer
	dtmv1.UnimplementedResourceInvocationServiceServer
}

func (recoveryAgentServer) ExecuteStep(
	_ context.Context,
	request *dtmv1.ExecuteStepRequest,
) (*dtmv1.ExecuteStepResponse, error) {
	return &dtmv1.ExecuteStepResponse{Result: &dtmv1.StepResult{
		StepId: request.GetStep().GetStepId(),
		NodeId: request.GetStep().GetNodeId(),
		Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED,
	}}, nil
}

func (recoveryAgentServer) InvokeResource(_ context.Context, request *dtmv1.InvocationRequest) (*dtmv1.InvocationResponse, error) {
	return &dtmv1.InvocationResponse{Outcome: &dtmv1.InvocationResponse_Result{Result: &dtmv1.InvocationResult{
		InvocationId: append([]byte(nil), request.GetInvocationId()...), Payload: []byte(`{}`),
	}}}, nil
}

type scriptedAgentServer struct {
	dtmv1.UnimplementedAgentExecutionServiceServer
	dtmv1.UnimplementedResourceInvocationServiceServer
	mu      sync.Mutex
	handler func(*dtmv1.ExecuteStepRequest, int) (*dtmv1.ExecuteStepResponse, error)
	calls   map[string]int
}

func (server *scriptedAgentServer) ExecuteStep(_ context.Context, request *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error) {
	return server.dispatch(request)
}

func (server *scriptedAgentServer) dispatch(request *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error) {
	server.mu.Lock()
	if server.calls == nil {
		server.calls = make(map[string]int)
	}
	server.calls[request.GetStep().GetStepId()]++
	call := server.calls[request.GetStep().GetStepId()]
	server.mu.Unlock()
	return server.handler(request, call)
}

func (server *scriptedAgentServer) InvokeResource(_ context.Context, request *dtmv1.InvocationRequest) (*dtmv1.InvocationResponse, error) {
	if request == nil || request.GetMetadata() == nil || request.GetResourceRef() == nil {
		return nil, status.Error(codes.InvalidArgument, "native invocation request is incomplete")
	}
	adapter := invocation.NewLegacyCapabilityAdapter()
	capability, inputs, err := adapter.DecodeRequest(model.OperationID(request.GetOperationId()), request.GetPayload())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "native payload: %v", err)
	}
	legacyRequest := &dtmv1.ExecuteStepRequest{
		TaskId: request.GetMetadata().GetTaskId(), IdempotencyKey: request.GetMetadata().GetIdempotencyKey(),
		Attempt: request.GetMetadata().GetAttempt(),
		Step: &dtmv1.MappedStep{
			StepId: request.GetMetadata().GetStepId(), Capability: capability.String(),
			NodeId: request.GetResourceRef().GetOwnerNodeId(), Inputs: inputs,
		},
	}
	response, err := server.dispatch(legacyRequest)
	if err != nil {
		return nil, err
	}
	if response == nil || response.GetResult() == nil {
		return nil, status.Error(codes.Internal, "scripted native response is incomplete")
	}
	result := response.GetResult()
	if result.GetStatus() == dtmv1.ExecutionStatus_EXECUTION_STATUS_FAILED {
		return &dtmv1.InvocationResponse{Outcome: &dtmv1.InvocationResponse_Error{Error: &dtmv1.InvocationError{
			InvocationId: append([]byte(nil), request.GetInvocationId()...), Code: "EXECUTION_FAILURE", Message: result.GetError(), Source: "scripted-agent",
			Classification: "handler_result", DispatchState: "remote_outcome_received",
		}}}, nil
	}
	output := map[string]any{}
	if result.GetOutput() != nil {
		output = result.GetOutput().AsMap()
	}
	payload, err := adapter.EncodeOutput(output)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode scripted native output: %v", err)
	}
	return &dtmv1.InvocationResponse{Outcome: &dtmv1.InvocationResponse_Result{Result: &dtmv1.InvocationResult{
		InvocationId: append([]byte(nil), request.GetInvocationId()...), Payload: payload,
	}}}, nil
}

func startScriptedAgent(t *testing.T, handler func(*dtmv1.ExecuteStepRequest, int) (*dtmv1.ExecuteStepResponse, error)) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	scripted := &scriptedAgentServer{handler: handler}
	dtmv1.RegisterAgentExecutionServiceServer(server, scripted)
	dtmv1.RegisterResourceInvocationServiceServer(server, scripted)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return listener.Addr().String()
}

func successfulAgentResponse(request *dtmv1.ExecuteStepRequest) *dtmv1.ExecuteStepResponse {
	output, _ := structpb.NewStruct(map[string]any{"step_id": request.GetStep().GetStepId(), "succeeded": true})
	return &dtmv1.ExecuteStepResponse{Result: &dtmv1.StepResult{
		StepId: request.GetStep().GetStepId(), NodeId: request.GetStep().GetNodeId(),
		Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED, Output: output,
	}}
}

func TestComposeCoreBuildsDependencies(t *testing.T) {
	deps, err := compose()

	if err != nil {
		t.Fatalf("compose() error = %v", err)
	}
	t.Cleanup(func() {
		deps.AsyncTasks.Close()
		_ = deps.Repository.Close()
	})
	if deps.NewLifecycle == nil || deps.Registry == nil || deps.Endpoints == nil || deps.Executor == nil || deps.Mapper == nil || deps.Runtime == nil || deps.TaskService == nil || deps.AsyncTasks == nil || deps.Recovery == nil || deps.RegistryAPI == nil || deps.Repository == nil || deps.Server == nil {
		t.Fatalf("compose() dependencies = %+v, want all dependencies", deps)
	}
	life, err := deps.NewLifecycle("task-1")
	if err != nil {
		t.Fatalf("NewLifecycle() error = %v", err)
	}
	if life.State() != lifecycle.StateCreated {
		t.Fatalf("lifecycle state = %q, want %q", life.State(), lifecycle.StateCreated)
	}
}

func TestComposeRegistersCoreAndRegistryServices(t *testing.T) {
	deps, err := compose()
	if err != nil {
		t.Fatalf("compose() error = %v", err)
	}
	t.Cleanup(func() {
		deps.AsyncTasks.Close()
		_ = deps.Repository.Close()
	})

	listener := bufconn.Listen(1024 * 1024)
	serveDone := make(chan error, 1)
	go func() { serveDone <- deps.Server.Serve(listener) }()

	connection, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}
	defer connection.Close()

	response, err := dtmv1.NewNodeRegistryServiceClient(connection).RegisterNode(context.Background(), &dtmv1.RegisterNodeRequest{
		Node:           &dtmv1.Node{Id: "node-1", Capabilities: []string{"temperature_sensor"}, Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, ExecutionAddress: "127.0.0.1:50061"},
		RegistrationId: "registration-1",
	})
	if err != nil {
		t.Fatalf("RegisterNode() error = %v", err)
	}
	if !response.GetAccepted() {
		t.Fatal("RegisterNode() accepted = false, want true")
	}
	if address, err := deps.Endpoints.Resolve("node-1"); err != nil || address != "127.0.0.1:50061" {
		t.Fatalf("endpoint = %q, %v", address, err)
	}
	if _, err := dtmv1.NewCoreServiceClient(connection).SubmitTask(context.Background(), nil); err == nil {
		t.Fatal("SubmitTask(nil) error = nil, want validation error")
	}
	coreClient := dtmv1.NewCoreServiceClient(connection)
	accepted, err := coreClient.SubmitTask(context.Background(), &dtmv1.SubmitTaskRequest{
		Task:  &dtmv1.Task{Id: "task-async", Intent: "unsupported_intent"},
		Async: true,
	})
	if err != nil {
		t.Fatalf("SubmitTask(async) error = %v", err)
	}
	if accepted.GetTaskId() != "task-async" ||
		accepted.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_CREATED {
		t.Fatalf("SubmitTask(async) response = %#v", accepted)
	}
	deadline := time.After(time.Second)
	for {
		taskStatus, err := coreClient.GetTaskStatus(
			context.Background(),
			&dtmv1.GetTaskStatusRequest{TaskId: "task-async"},
		)
		if err != nil {
			t.Fatalf("GetTaskStatus() error = %v", err)
		}
		if taskStatus.GetStatus() == dtmv1.TaskStatus_TASK_STATUS_FAILED {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("async task remained in state %v", taskStatus.GetStatus())
		case <-time.After(time.Millisecond):
		}
	}
	detail, err := coreClient.GetTask(context.Background(), &dtmv1.GetTaskRequest{TaskId: "task-async"})
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if detail.GetTask().GetTaskId() != "task-async" || detail.GetTask().GetStatus() != dtmv1.TaskStatus_TASK_STATUS_FAILED {
		t.Fatalf("GetTask() response = %#v", detail)
	}
	listed, err := coreClient.ListTasks(context.Background(), &dtmv1.ListTasksRequest{Status: dtmv1.TaskStatus_TASK_STATUS_FAILED})
	if err != nil {
		t.Fatalf("ListTasks() error = %v", err)
	}
	if len(listed.GetTasks()) != 1 || listed.GetTasks()[0].GetTaskId() != "task-async" {
		t.Fatalf("ListTasks() response = %#v", listed)
	}
	executions, err := coreClient.GetTaskExecutions(context.Background(), &dtmv1.GetTaskExecutionsRequest{TaskId: "task-async"})
	if err != nil {
		t.Fatalf("GetTaskExecutions() error = %v", err)
	}
	if len(executions.GetExecutions()) != 0 {
		t.Fatalf("GetTaskExecutions() response = %#v", executions)
	}
	deps.Server.Stop()
	select {
	case err := <-serveDone:
		if err != nil && err != grpc.ErrServerStopped {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not return after stopping server")
	}
	_ = listener.Close()
}

func TestPersistenceFailureRetryAndRemapLifecycles(t *testing.T) {
	tests := []struct {
		name             string
		maxAttempts      int
		twoNodes         bool
		nodeBFailed      bool
		retryThenSuccess bool
		nonRetryable     bool
		wantTask         lifecycle.State
		wantAttempts     int
		wantLastNode     model.NodeID
		wantRetryEvents  int
		wantRemapEvents  int
	}{
		{name: "non_retryable_failure", maxAttempts: 1, nonRetryable: true, wantTask: lifecycle.StateFailed, wantAttempts: 1, wantLastNode: "agent-a"},
		{name: "retry_then_success", maxAttempts: 2, retryThenSuccess: true, wantTask: lifecycle.StateSuccess, wantAttempts: 2, wantLastNode: "agent-a", wantRetryEvents: 1},
		{name: "retry_exhausted", maxAttempts: 2, wantTask: lifecycle.StateFailed, wantAttempts: 2, wantLastNode: "agent-a", wantRetryEvents: 1},
		{name: "remap_then_success", maxAttempts: 1, twoNodes: true, wantTask: lifecycle.StateSuccess, wantAttempts: 2, wantLastNode: "agent-b", wantRetryEvents: 1, wantRemapEvents: 1},
		{name: "remap_then_final_failure", maxAttempts: 1, twoNodes: true, nodeBFailed: true, wantTask: lifecycle.StateFailed, wantAttempts: 2, wantLastNode: "agent-b", wantRetryEvents: 1, wantRemapEvents: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			addressA := startScriptedAgent(t, func(request *dtmv1.ExecuteStepRequest, call int) (*dtmv1.ExecuteStepResponse, error) {
				if request.GetStep().GetStepId() != "step-1" {
					return successfulAgentResponse(request), nil
				}
				if test.nonRetryable {
					return &dtmv1.ExecuteStepResponse{Result: &dtmv1.StepResult{StepId: "step-1", NodeId: request.GetStep().GetNodeId(), Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_FAILED, Error: "target rejected"}}, nil
				}
				if test.retryThenSuccess && call > 1 {
					return successfulAgentResponse(request), nil
				}
				return nil, status.Error(codes.Unavailable, "agent-a unavailable")
			})
			var addressB string
			if test.twoNodes {
				addressB = startScriptedAgent(t, func(request *dtmv1.ExecuteStepRequest, _ int) (*dtmv1.ExecuteStepResponse, error) {
					if request.GetStep().GetStepId() == "step-1" && test.nodeBFailed {
						return nil, status.Error(codes.Unavailable, "agent-b unavailable")
					}
					return successfulAgentResponse(request), nil
				})
			}
			path := filepath.Join(t.TempDir(), "core.db")
			deps, err := composeWithConfig(ctx, config.Core{
				Lease:   config.Lease{TTL: config.Duration{Duration: time.Second}},
				Storage: config.Storage{Path: path},
				Retry:   config.Retry{MaxAttempts: test.maxAttempts, Backoff: config.Duration{}},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer deps.AsyncTasks.Close()
			defer deps.Repository.Close()
			register := func(id, address, registration string) {
				_, err := deps.RegistryAPI.RegisterNode(ctx, &dtmv1.RegisterNodeRequest{
					Node:           &dtmv1.Node{Id: id, Capabilities: []string{"temperature_sensor", "cooling_control"}, Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, ExecutionAddress: address},
					RegistrationId: registration,
				})
				if err != nil {
					t.Fatal(err)
				}
			}
			register("agent-a", addressA, "registration-a")
			if test.twoNodes {
				register("agent-b", addressB, "registration-b")
			}
			input, err := task.New(model.TaskID("task-"+test.name), "cool_environment", []model.Capability{"temperature_sensor", "cooling_control"}, task.Constraints{"target_temperature": "26"})
			if err != nil {
				t.Fatal(err)
			}
			outcome, submitErr := deps.TaskService.Submit(ctx, input)
			if outcome.State != test.wantTask {
				t.Fatalf("outcome = %#v error=%v", outcome, submitErr)
			}
			if test.wantTask == lifecycle.StateSuccess && submitErr != nil {
				t.Fatal(submitErr)
			}
			if test.wantTask == lifecycle.StateFailed && submitErr == nil {
				t.Fatal("failed task error = nil")
			}
			record, err := deps.Repository.GetTask(ctx, input.ID)
			if err != nil {
				t.Fatal(err)
			}
			firstStep := record.Steps[0]
			attempts, err := deps.Repository.GetExecutions(ctx, input.ID, firstStep.ID)
			if err != nil {
				t.Fatal(err)
			}
			if record.State != test.wantTask || firstStep.AssignedNodeID == nil || *firstStep.AssignedNodeID != test.wantLastNode || len(attempts) != test.wantAttempts || firstStep.AttemptCount != test.wantAttempts {
				t.Fatalf("persisted lifecycle: task=%#v step=%#v attempts=%#v", record, firstStep, attempts)
			}
			for index, attempt := range attempts {
				if attempt.AttemptNo != index+1 || attempt.State == storageport.ExecutionStateStarted || attempt.CompletedAt == nil {
					t.Fatalf("attempt %d = %#v", index, attempt)
				}
			}
			if test.wantTask == lifecycle.StateFailed {
				if firstStep.State != lifecycle.StepStateFailed || firstStep.CompletedAt == nil || firstStep.Result == nil || firstStep.Result.Status != execution.StatusFailed || record.CompletedAt == nil || record.FailureMessage == "" {
					t.Fatalf("failed terminal snapshot: task=%#v step=%#v", record, firstStep)
				}
			} else if firstStep.State != lifecycle.StepStateSuccess || firstStep.Result == nil || firstStep.Result.Status != execution.StatusSucceeded {
				t.Fatalf("successful terminal step = %#v", firstStep)
			}
			events, err := deps.Repository.ListTaskEvents(ctx, input.ID)
			if err != nil {
				t.Fatal(err)
			}
			retrying, remapped := 0, 0
			for _, event := range events {
				if event.Type == "task_retrying" {
					retrying++
				}
				if event.Type == "task_remapped" {
					remapped++
				}
			}
			wantLastEvent := "task_succeeded"
			if test.wantTask == lifecycle.StateFailed {
				wantLastEvent = "task_failed"
			}
			if retrying != test.wantRetryEvents || remapped != test.wantRemapEvents || len(events) == 0 || events[len(events)-1].Type != wantLastEvent {
				t.Fatalf("events retrying=%d remapped=%d last=%v all=%#v", retrying, remapped, events[len(events)-1], events)
			}
			if test.retryThenSuccess {
				listener := bufconn.Listen(1024 * 1024)
				go func() { _ = deps.Server.Serve(listener) }()
				connection, err := grpc.DialContext(ctx, "bufnet",
					grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
				if err != nil {
					t.Fatal(err)
				}
				client := dtmv1.NewCoreServiceClient(connection)
				history, err := client.GetTaskExecutions(ctx, &dtmv1.GetTaskExecutionsRequest{TaskId: string(input.ID)})
				_ = connection.Close()
				deps.Server.Stop()
				_ = listener.Close()
				if err != nil {
					t.Fatal(err)
				}
				queried := history.GetExecutions()
				requestIDs := make(map[string]struct{}, len(queried))
				for _, attempt := range queried {
					if attempt.GetRequestId() == "" {
						t.Fatalf("retry execution has empty request_id: %#v", attempt)
					}
					requestIDs[attempt.GetRequestId()] = struct{}{}
				}
				if len(queried) != 3 || len(requestIDs) != 3 ||
					queried[0].GetStepId() != queried[1].GetStepId() || queried[0].GetAttemptNumber() != 1 || queried[1].GetAttemptNumber() != 2 ||
					queried[2].GetStepId() == queried[0].GetStepId() || queried[2].GetAttemptNumber() != 1 ||
					queried[0].GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_FAILED || queried[0].GetFailureMessage() == "" ||
					queried[1].GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED || queried[1].GetResponse() == nil ||
					queried[2].GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED || queried[2].GetResponse() == nil ||
					queried[0].GetCompletedAt() == nil || queried[1].GetCompletedAt() == nil || queried[2].GetCompletedAt() == nil {
					t.Fatalf("retry execution query history = %#v", queried)
				}
			}
			t.Logf("lifecycle evidence: task=%s state=%s step=%s node=%s attempts=%d attempt_states=%v events=%d retrying=%d remapped=%d", input.ID, record.State, firstStep.State, *firstStep.AssignedNodeID, len(attempts), executionStates(attempts), len(events), retrying, remapped)
		})
	}
}

func TestCoreQueryRPCTracksRunningAndSuccessfulExecutionHistory(t *testing.T) {
	ctx := context.Background()
	firstExecutionStarted := make(chan struct{})
	releaseFirstExecution := make(chan struct{})
	var startedOnce sync.Once
	agentAddress := startScriptedAgent(t, func(request *dtmv1.ExecuteStepRequest, _ int) (*dtmv1.ExecuteStepResponse, error) {
		if request.GetStep().GetStepId() == "step-1" {
			startedOnce.Do(func() { close(firstExecutionStarted) })
			<-releaseFirstExecution
		}
		output, err := structpb.NewStruct(map[string]any{"step": request.GetStep().GetStepId(), "ok": true})
		if err != nil {
			return nil, err
		}
		return &dtmv1.ExecuteStepResponse{Result: &dtmv1.StepResult{
			StepId: request.GetStep().GetStepId(), NodeId: request.GetStep().GetNodeId(),
			Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED, Output: output,
		}}, nil
	})
	deps, err := composeWithConfig(ctx, config.Core{
		Lease:   config.Lease{TTL: config.Duration{Duration: time.Second}},
		Storage: config.Storage{Path: filepath.Join(t.TempDir(), "core-query.db")},
		Retry:   config.Retry{MaxAttempts: 1, Backoff: config.Duration{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deps.Server.Stop()
		deps.AsyncTasks.Close()
		_ = deps.Repository.Close()
	})
	if _, err := deps.RegistryAPI.RegisterNode(ctx, &dtmv1.RegisterNodeRequest{
		Node: &dtmv1.Node{
			Id: "query-agent", Capabilities: []string{"temperature_sensor", "cooling_control"},
			Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, ExecutionAddress: agentAddress,
		},
		RegistrationId: "query-agent-registration",
	}); err != nil {
		t.Fatal(err)
	}

	listener := bufconn.Listen(1024 * 1024)
	go func() { _ = deps.Server.Serve(listener) }()
	connection, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close(); _ = listener.Close() })
	client := dtmv1.NewCoreServiceClient(connection)
	if _, err := client.SubmitTask(ctx, &dtmv1.SubmitTaskRequest{
		Task: &dtmv1.Task{
			Id: "task-query-success", Intent: "cool_environment",
			Requirements: []string{"temperature_sensor", "cooling_control"},
			Constraints:  map[string]string{"target_temperature": "26"},
		},
		Async: true,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstExecutionStarted:
	case <-time.After(time.Second):
		t.Fatal("agent execution did not reach the test barrier")
	}

	running, err := client.GetTask(ctx, &dtmv1.GetTaskRequest{TaskId: "task-query-success"})
	if err != nil {
		t.Fatal(err)
	}
	if running.GetTask().GetStatus() != dtmv1.TaskStatus_TASK_STATUS_RUNNING || len(running.GetTask().GetSteps()) != 2 ||
		running.GetTask().GetSteps()[0].GetStatus() != dtmv1.StepStatus_STEP_STATUS_RUNNING {
		t.Fatalf("running GetTask() = %#v", running)
	}
	inFlight, err := client.GetTaskExecutions(ctx, &dtmv1.GetTaskExecutionsRequest{TaskId: "task-query-success"})
	if err != nil {
		t.Fatal(err)
	}
	if len(inFlight.GetExecutions()) != 1 || inFlight.GetExecutions()[0].GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_STARTED {
		t.Fatalf("in-flight executions = %#v", inFlight)
	}
	close(releaseFirstExecution)

	deadline := time.Now().Add(2 * time.Second)
	var completed *dtmv1.GetTaskResponse
	for time.Now().Before(deadline) {
		completed, err = client.GetTask(ctx, &dtmv1.GetTaskRequest{TaskId: "task-query-success"})
		if err == nil && completed.GetTask().GetStatus() == dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err != nil || completed == nil || completed.GetTask().GetStatus() != dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED {
		t.Fatalf("completed GetTask() = %#v, error = %v", completed, err)
	}
	if len(completed.GetTask().GetSteps()) != 2 {
		t.Fatalf("completed steps = %#v", completed.GetTask().GetSteps())
	}
	for _, step := range completed.GetTask().GetSteps() {
		if step.GetStatus() != dtmv1.StepStatus_STEP_STATUS_SUCCEEDED || step.GetCompletedAt() == nil {
			t.Fatalf("completed step = %#v", step)
		}
	}

	history, err := client.GetTaskExecutions(ctx, &dtmv1.GetTaskExecutionsRequest{TaskId: "task-query-success"})
	if err != nil {
		t.Fatal(err)
	}
	if len(history.GetExecutions()) != 2 {
		t.Fatalf("execution history = %#v", history)
	}
	for index, attempt := range history.GetExecutions() {
		if attempt.GetExecutionId() == "" || attempt.GetRequestId() != attempt.GetExecutionId() ||
			attempt.GetTaskId() != "task-query-success" || attempt.GetStepId() != completed.GetTask().GetSteps()[index].GetStepId() ||
			attempt.GetNodeId() != "query-agent" || attempt.GetAttemptNumber() != 1 ||
			attempt.GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED || attempt.GetFailureMessage() != "" ||
			attempt.GetResponse() == nil || !attempt.GetResponse().GetFields()["ok"].GetBoolValue() ||
			attempt.GetStartedAt() == nil || attempt.GetCompletedAt() == nil || attempt.GetUpdatedAt() == nil {
			t.Fatalf("execution %d = %#v", index, attempt)
		}
	}
}

func executionStates(attempts []storageport.Execution) []storageport.ExecutionState {
	states := make([]storageport.ExecutionState, len(attempts))
	for index := range attempts {
		states[index] = attempts[index].State
	}
	return states
}

func TestNormalCoolEnvironmentPersistsUnifiedLifecycle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "core.db")
	agentListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	agentServer := grpc.NewServer()
	dtmv1.RegisterAgentExecutionServiceServer(agentServer, recoveryAgentServer{})
	dtmv1.RegisterResourceInvocationServiceServer(agentServer, recoveryAgentServer{})
	go agentServer.Serve(agentListener)
	defer func() { agentServer.Stop(); _ = agentListener.Close() }()

	deps, err := composeWithConfig(ctx, config.Core{
		Lease:   config.Lease{TTL: config.Duration{Duration: time.Second}},
		Storage: config.Storage{Path: path},
		Retry:   config.Retry{MaxAttempts: 1, Backoff: config.Duration{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer deps.AsyncTasks.Close()
	defer deps.Repository.Close()
	if _, err := deps.RegistryAPI.RegisterNode(ctx, &dtmv1.RegisterNodeRequest{
		Node:           &dtmv1.Node{Id: "mesh-agent", Capabilities: []string{"temperature_sensor", "cooling_control"}, Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, ExecutionAddress: agentListener.Addr().String()},
		RegistrationId: "normal-lifecycle-registration",
	}); err != nil {
		t.Fatal(err)
	}
	input, err := task.New("task-normal-persistence", "cool_environment", []model.Capability{"temperature_sensor", "cooling_control"}, task.Constraints{"target_temperature": "26"})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := deps.TaskService.Submit(ctx, input)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.State != lifecycle.StateSuccess {
		t.Fatalf("outcome = %#v", outcome)
	}

	record, err := deps.Repository.GetTask(ctx, input.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != lifecycle.StateSuccess || len(record.Steps) != 2 {
		t.Fatalf("task = %#v", record)
	}
	totalAttempts := 0
	for _, step := range record.Steps {
		if step.State != lifecycle.StepStateSuccess || step.AssignedNodeID == nil || *step.AssignedNodeID != "mesh-agent" || step.Result == nil || step.Result.Status != execution.StatusSucceeded {
			t.Fatalf("step = %#v", step)
		}
		attempts, err := deps.Repository.GetExecutions(ctx, input.ID, step.ID)
		if err != nil {
			t.Fatal(err)
		}
		totalAttempts += len(attempts)
		if len(attempts) != 1 || attempts[0].AttemptNo != 1 || attempts[0].State != storageport.ExecutionStateSucceeded || attempts[0].Result == nil {
			t.Fatalf("attempts for %s = %#v", step.ID, attempts)
		}
	}
	events, err := deps.Repository.ListTaskEvents(ctx, input.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < 3 || events[0].Type != "task_accepted" || events[1].Type != "task_planned" || events[len(events)-1].Type != "task_succeeded" {
		t.Fatalf("events = %#v", events)
	}

	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var oldTable, orphanExecutions int
	if err := db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='execution_steps'").Scan(&oldTable); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT COUNT(*) FROM executions e LEFT JOIN task_steps s ON s.task_id=e.task_id AND s.step_id=e.step_id WHERE s.step_id IS NULL").Scan(&orphanExecutions); err != nil {
		t.Fatal(err)
	}
	if oldTable != 0 || orphanExecutions != 0 {
		t.Fatalf("old table=%d orphan executions=%d", oldTable, orphanExecutions)
	}
	t.Logf("sqlite evidence: task=%s state=%s steps=%d executions=%d events=%d old_execution_steps=%d orphan_executions=%d", record.ID, record.State, len(record.Steps), totalAttempts, len(events), oldTable, orphanExecutions)
}

func TestComposeWithConfigPreservesInterruptedTaskState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dtm.db")
	repository, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	input, err := task.New("task-running", "cool_environment", nil, task.Constraints{
		"target_temperature": "26",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	if err := repository.CreateTask(context.Background(), storageport.Task{
		ID: input.ID, Intent: input.Intent, Requirements: input.Requirements,
		Constraints: input.Constraints, State: lifecycle.StateRunning,
		CreatedAt: now, UpdatedAt: now, StartedAt: &now, Version: 1,
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	deps, err := composeWithConfig(context.Background(), config.Core{
		Lease: config.Lease{TTL: config.Duration{Duration: time.Second}},
		Storage: config.Storage{
			Path: path,
		},
		Retry: config.Retry{
			MaxAttempts: config.DefaultRetryMaxAttempts,
			Backoff:     config.Duration{Duration: config.DefaultRetryBackoff},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer deps.Repository.Close()
	defer deps.AsyncTasks.Close()
	got, err := deps.Repository.GetTask(context.Background(), input.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != lifecycle.StateRunning || got.FailureMessage != "" {
		t.Fatalf("restored task = %#v", got)
	}
}

func TestRecoverWithPersistedUnknownStartedExecutionFailsClosed(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dtm.db")
	repository, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	taskID := model.TaskID("task-started-interruption")
	stepID := model.StepID("step-1")
	nodeID := model.NodeID("sensor-1")
	if err := repository.CreateTask(ctx, storageport.Task{
		ID: taskID, Intent: "read_temperature", Requirements: []model.Capability{"temperature_sensor"},
		State: lifecycle.StateRunning, CreatedAt: now, UpdatedAt: now, StartedAt: &now, Version: 1,
		Steps: []storageport.TaskStep{{
			ID: stepID, TaskID: taskID, Sequence: 0, Capability: "temperature_sensor",
			Input: map[string]string{"operation": "read_temperature"}, State: lifecycle.StepStateDispatched,
			AssignedNodeID: &nodeID, MaxAttempts: 2, CreatedAt: now, UpdatedAt: now, Version: 1,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	executionID := model.ExecutionID("task-started-interruption-step-1-attempt-1")
	if _, _, err := repository.StartExecution(ctx, storageport.StartExecutionRequest{
		ExecutionID: executionID, TaskID: taskID, StepID: stepID, AttemptNo: 1, NodeID: nodeID,
		Request: map[string]string{"operation": "read_temperature"}, ExpectedStepState: lifecycle.StepStateDispatched, ExpectedStepVersion: 1, StartedAt: now.Add(time.Second),
		Event: storageport.TaskEvent{TaskID: taskID, StepID: &stepID, Type: "execution_started", FromState: string(lifecycle.StepStateDispatched), ToState: string(lifecycle.StepStateRunning), CreatedAt: now.Add(time.Second)},
	}); err != nil {
		t.Fatal(err)
	}
	eventsBefore, err := repository.ListTaskEvents(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	deps, err := composeWithConfig(ctx, config.Core{
		Lease:   config.Lease{TTL: config.Duration{Duration: time.Second}},
		Storage: config.Storage{Path: path},
		Retry:   config.Retry{MaxAttempts: 2, Backoff: config.Duration{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer deps.AsyncTasks.Close()
	defer deps.Repository.Close()
	for call := 1; call <= 2; call++ {
		if err := deps.TaskService.Recover(ctx, taskID); err != nil {
			t.Fatalf("Recover() call %d error = %v", call, err)
		}
	}
	attempts, err := deps.Repository.GetExecutions(ctx, taskID, stepID)
	if err != nil {
		t.Fatal(err)
	}
	eventsAfter, err := deps.Repository.ListTaskEvents(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	record, err := deps.Repository.GetTask(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].ID != executionID || attempts[0].AttemptNo != 1 || attempts[0].State != storageport.ExecutionStateFailed || attempts[0].FailureCode != application.CoreRestartInterruptedCode {
		t.Fatalf("started execution history = %#v", attempts)
	}
	if len(eventsAfter) != len(eventsBefore)+2 || record.State != lifecycle.StateFailed || record.FailureCode != application.RecoveryIdempotencyUnknownCode || record.Steps[0].State != lifecycle.StepStateFailed || record.Steps[0].AttemptCount != 1 {
		t.Fatalf("recovery changed persistence: events %d->%d task=%#v", len(eventsBefore), len(eventsAfter), record)
	}
	t.Logf("interruption evidence: attempts=%d state=%s attempt_no=%d events=%d step=%s", len(attempts), attempts[0].State, attempts[0].AttemptNo, len(eventsAfter), record.Steps[0].State)
}

func TestRecoverIdempotentStartedExecutionWaitsForReregisterThenCreatesNewAttempt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "dtm.db")
	repository, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	taskID, stepID, nodeID := model.TaskID("task-m4c-idempotent"), model.StepID("step-1"), model.NodeID("sensor-restart")
	if err := repository.CreateTask(ctx, storageport.Task{
		ID: taskID, Intent: "read_temperature", Requirements: []model.Capability{"temperature_sensor"},
		State: lifecycle.StateRunning, CreatedAt: now, UpdatedAt: now, StartedAt: &now, Version: 1,
		Steps: []storageport.TaskStep{{
			ID: stepID, TaskID: taskID, Sequence: 0, Capability: "temperature_sensor",
			IdempotencyMode: model.IdempotencyIdempotent, Input: map[string]string{"operation": "read_temperature"},
			State: lifecycle.StepStateDispatched, AssignedNodeID: &nodeID, MaxAttempts: 2,
			CreatedAt: now, UpdatedAt: now, Version: 1,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	oldExecutionID := model.ExecutionID("task-m4c-idempotent-step-1-attempt-1")
	if _, _, err := repository.StartExecution(ctx, storageport.StartExecutionRequest{
		ExecutionID: oldExecutionID, TaskID: taskID, StepID: stepID, AttemptNo: 1, NodeID: nodeID,
		Request: map[string]string{"operation": "read_temperature"}, ExpectedStepState: lifecycle.StepStateDispatched,
		ExpectedStepVersion: 1, StartedAt: now,
		Event: storageport.TaskEvent{TaskID: taskID, StepID: &stepID, Type: "execution_started", FromState: string(lifecycle.StepStateDispatched), ToState: string(lifecycle.StepStateRunning), CreatedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	deps, err := composeWithConfig(ctx, config.Core{Lease: config.Lease{TTL: config.Duration{Duration: time.Second}}, Storage: config.Storage{Path: path}, Retry: config.Retry{MaxAttempts: 2, Backoff: config.Duration{}}})
	if err != nil {
		t.Fatal(err)
	}
	defer deps.AsyncTasks.Close()
	defer deps.Repository.Close()
	for scan := 1; scan <= 2; scan++ {
		err := deps.TaskService.Recover(ctx, taskID)
		if !errors.Is(err, application.ErrRecoveryPending) || !strings.Contains(err.Error(), application.RecoveryWaitingForNodeCode) {
			t.Fatalf("pre-registration recovery %d error = %v", scan, err)
		}
		executions, queryErr := deps.TaskQueries.GetTaskExecutions(ctx, taskID)
		if queryErr != nil || len(executions) != 1 || executions[0].State != storageport.ExecutionStateFailed || executions[0].FailureCode != application.CoreRestartInterruptedCode {
			t.Fatalf("query during recovery = %#v, %v", executions, queryErr)
		}
	}

	agentListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	agentServer := grpc.NewServer()
	dtmv1.RegisterAgentExecutionServiceServer(agentServer, recoveryAgentServer{})
	dtmv1.RegisterResourceInvocationServiceServer(agentServer, recoveryAgentServer{})
	go agentServer.Serve(agentListener)
	defer func() { agentServer.Stop(); _ = agentListener.Close() }()
	if _, err := deps.RegistryAPI.RegisterNode(ctx, &dtmv1.RegisterNodeRequest{
		Node:           &dtmv1.Node{Id: string(nodeID), Capabilities: []string{"temperature_sensor"}, Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, ExecutionAddress: agentListener.Addr().String()},
		RegistrationId: "registration-after-restart",
	}); err != nil {
		t.Fatal(err)
	}
	if err := deps.TaskService.Recover(ctx, taskID); err != nil {
		t.Fatal(err)
	}
	record, err := deps.TaskQueries.GetTask(ctx, taskID)
	if err != nil || record.State != lifecycle.StateSuccess || record.Steps[0].State != lifecycle.StepStateSuccess {
		t.Fatalf("recovered query record = %#v, %v", record, err)
	}
	executions, err := deps.TaskQueries.GetTaskExecutions(ctx, taskID)
	if err != nil || len(executions) != 2 {
		t.Fatalf("execution history = %#v, %v", executions, err)
	}
	if executions[0].ID != oldExecutionID || executions[0].AttemptNo != 1 || executions[0].State != storageport.ExecutionStateFailed ||
		executions[1].ID == oldExecutionID || executions[1].RequestID == string(oldExecutionID) || executions[1].AttemptNo != 2 || executions[1].State != storageport.ExecutionStateSucceeded {
		t.Fatalf("old/new execution comparison = %#v", executions)
	}
	t.Logf("M4-C recovery old=%s/%d/%s new=%s/%d/%s", executions[0].ID, executions[0].AttemptNo, executions[0].State, executions[1].ID, executions[1].AttemptNo, executions[1].State)
}

func TestRecoveryControllerRemapsPersistedTaskToReplacementAgent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "dtm.db")
	repository, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	input, err := task.New(
		"task-running",
		"read_temperature",
		[]model.Capability{"temperature_sensor"},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	nodeID := model.NodeID("lost-agent")
	if err := repository.CreateTask(ctx, storageport.Task{
		ID: input.ID, Intent: input.Intent, Requirements: input.Requirements,
		Constraints: input.Constraints, State: lifecycle.StateRunning,
		CreatedAt: now, UpdatedAt: now, StartedAt: &now, Version: 1,
		Steps: []storageport.TaskStep{{
			ID: "step-1", TaskID: input.ID, Sequence: 0,
			Capability: "temperature_sensor", AssignedNodeID: &nodeID,
			Input: map[string]string{"operation": "read_temperature"},
			State: lifecycle.StepStateRunning, AttemptCount: 0, MaxAttempts: 2,
			CreatedAt: now, UpdatedAt: now, StartedAt: &now, Version: 1,
		}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	agentListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	agentServer := grpc.NewServer()
	dtmv1.RegisterAgentExecutionServiceServer(agentServer, recoveryAgentServer{})
	dtmv1.RegisterResourceInvocationServiceServer(agentServer, recoveryAgentServer{})
	go agentServer.Serve(agentListener)
	defer func() {
		agentServer.Stop()
		_ = agentListener.Close()
	}()

	deps, err := composeWithConfig(ctx, config.Core{
		Lease: config.Lease{
			TTL:           config.Duration{Duration: time.Second},
			SweepInterval: config.Duration{Duration: 100 * time.Millisecond},
		},
		Storage: config.Storage{Path: path},
		Retry: config.Retry{
			MaxAttempts: 1,
			Backoff:     config.Duration{},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer deps.AsyncTasks.Close()
	defer deps.Repository.Close()
	if _, err := deps.RegistryAPI.RegisterNode(ctx, &dtmv1.RegisterNodeRequest{
		Node: &dtmv1.Node{
			Id:               "replacement-agent",
			Capabilities:     []string{"temperature_sensor"},
			Status:           dtmv1.NodeStatus_NODE_STATUS_ONLINE,
			ExecutionAddress: agentListener.Addr().String(),
		},
		RegistrationId: "replacement-registration",
	}); err != nil {
		t.Fatal(err)
	}

	recoveryErrors := deps.Recovery.Run(ctx)
	deadline := time.After(3 * time.Second)
	for {
		record, err := deps.Repository.GetTask(context.Background(), input.ID)
		if err != nil {
			t.Fatal(err)
		}
		if record.State == lifecycle.StateSuccess {
			if record.Steps[0].Result == nil || record.Steps[0].Result.Status != execution.StatusSucceeded ||
				record.Steps[0].AssignedNodeID == nil || *record.Steps[0].AssignedNodeID != "replacement-agent" ||
				record.Steps[0].State != lifecycle.StepStateSuccess {
				t.Fatalf("recovered record = %#v", record)
			}
			break
		}
		select {
		case err := <-recoveryErrors:
			if err != nil {
				t.Fatalf("recovery error: %v", err)
			}
		case <-deadline:
			t.Fatalf("task remained in state %q", record.State)
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
}

func TestCoreRestartBlocksMappingUntilNodeReregistration(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "core.db")
	coreConfig := config.Core{
		Lease:   config.Lease{TTL: config.Duration{Duration: 10 * time.Second}},
		Storage: config.Storage{Path: path},
		Retry:   config.Retry{MaxAttempts: 1, Backoff: config.Duration{}},
	}
	first, err := composeWithConfig(ctx, coreConfig)
	if err != nil {
		t.Fatal(err)
	}
	registration := &dtmv1.RegisterNodeRequest{
		Node:           &dtmv1.Node{Id: "node-restart", Capabilities: []string{"temperature_sensor"}, Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, ExecutionAddress: "127.0.0.1:5001"},
		RegistrationId: "registration-1",
	}
	if _, err := first.RegistryAPI.RegisterNode(ctx, registration); err != nil {
		t.Fatal(err)
	}
	before, err := first.Repository.GetNode(ctx, "node-restart")
	if err != nil || before.Status != node.StatusActive || before.Generation != 1 {
		t.Fatalf("pre-restart node = %#v, %v", before, err)
	}
	input, err := task.New("task-restart-mapping", "cool_environment", []model.Capability{"temperature_sensor", "cooling_control"}, task.Constraints{"target_temperature": "26"})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := first.Planner.Plan(input)
	if err != nil {
		t.Fatal(err)
	}
	// Register a complete capability snapshot so the real Mapper succeeds before restart.
	registration.Node.Capabilities = []string{"temperature_sensor", "cooling_control"}
	if _, err := first.RegistryAPI.RegisterNode(ctx, registration); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Mapper.Map(plan); err != nil {
		t.Fatalf("Map() before restart error = %v", err)
	}
	first.AsyncTasks.Close()
	if err := first.Repository.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := composeWithConfig(ctx, coreConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer second.AsyncTasks.Close()
	defer second.Repository.Close()
	historical, err := second.Repository.GetNode(ctx, "node-restart")
	if err != nil || historical.Status != node.StatusStale {
		t.Fatalf("historical node = %#v, %v", historical, err)
	}
	if second.RegistryAPI.Eligible("node-restart") || len(second.Registry.Discover("temperature_sensor")) != 0 {
		t.Fatalf("historical node became schedulable: eligible=%v discovered=%v", second.RegistryAPI.Eligible("node-restart"), second.Registry.Discover("temperature_sensor"))
	}
	if len(second.Resources.ListEligible()) != 0 || len(second.Resources.ListIneligible()) != 2 {
		t.Fatalf("historical resources eligibility: eligible=%d ineligible=%d", len(second.Resources.ListEligible()), len(second.Resources.ListIneligible()))
	}
	if _, err := second.Mapper.Map(plan); !errors.Is(err, mapper.ErrCapabilityUnavailable) {
		t.Fatalf("Map() after restart error = %v", err)
	}
	registration.Node.ExecutionAddress = "127.0.0.1:5002"
	// The Agent may retain its registration ID across a Core restart; the stale
	// persisted status still defines a new Core registration generation.
	if _, err := second.RegistryAPI.RegisterNode(ctx, registration); err != nil {
		t.Fatal(err)
	}
	restored, err := second.Repository.GetNode(ctx, "node-restart")
	if err != nil || restored.Status != node.StatusActive || restored.Generation != 2 || restored.Endpoint != "127.0.0.1:5002" {
		t.Fatalf("reregistered node = %#v, %v", restored, err)
	}
	if !second.RegistryAPI.Eligible("node-restart") || len(second.Registry.Discover("temperature_sensor")) != 1 {
		t.Fatalf("reregistered node not schedulable: eligible=%v discovered=%v", second.RegistryAPI.Eligible("node-restart"), second.Registry.Discover("temperature_sensor"))
	}
	if len(second.Resources.ListEligible()) != 2 || len(second.Resources.ListIneligible()) != 0 {
		t.Fatalf("reregistered resources eligibility: eligible=%d ineligible=%d", len(second.Resources.ListEligible()), len(second.Resources.ListIneligible()))
	}
	if _, err := second.Mapper.Map(plan); err != nil {
		t.Fatalf("Map() after reregistration error = %v", err)
	}
}
