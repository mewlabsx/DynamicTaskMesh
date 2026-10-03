package grpcapi

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/application"
	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/planner"
	"dtm/internal/runtime"
	"dtm/internal/task"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestCoreSubmitTaskLogsExecutionChain(t *testing.T) {
	executionResult := successfulCoreExecution(t)
	submitter := &recordingTaskSubmitter{submit: func(
		context.Context,
		task.Task,
	) (application.Outcome, error) {
		return application.Outcome{
			TaskID:    "task-1",
			State:     lifecycle.StateSuccess,
			Execution: &executionResult,
		}, nil
	}}
	var output bytes.Buffer
	server, err := NewCoreServer(
		submitter,
		WithCoreLogger(log.New(&output, "", 0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = server.SubmitTask(context.Background(), &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{
		Id:     "task-1",
		Intent: "cool_environment",
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"task execution task_id=task-1 step_id=step-1 node_id=sensor-node result=succeeded",
		"task execution task_id=task-1 step_id=step-2 node_id=cooling-node result=succeeded",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("core log = %q, want %q", output.String(), want)
		}
	}
}

func TestNewCoreServerRejectsNilAndTypedNilSubmitter(t *testing.T) {
	var typedNil *recordingTaskSubmitter
	for _, submitter := range []TaskSubmitter{nil, typedNil} {
		server, err := NewCoreServer(submitter)
		if server != nil {
			t.Fatalf("NewCoreServer() server = %#v, want nil", server)
		}
		if !errors.Is(err, ErrInvalidTaskSubmitter) {
			t.Fatalf("NewCoreServer() error = %v, want ErrInvalidTaskSubmitter", err)
		}
	}
}

func TestCoreSubmitTaskRejectsInvalidTaskRequests(t *testing.T) {
	submitter := &recordingTaskSubmitter{}
	server, err := NewCoreServer(submitter)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		request *dtmv1.SubmitTaskRequest
	}{
		{name: "nil request"},
		{name: "nil task", request: &dtmv1.SubmitTaskRequest{}},
		{name: "blank id", request: submitRequest(" ", "cool_environment")},
		{name: "blank intent", request: submitRequest("task-1", "\t")},
		{name: "blank requirement", request: &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{
			Id: "task-1", Intent: "cool_environment", Requirements: []string{" "},
		}}},
		{name: "duplicate normalized requirement", request: &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{
			Id: "task-1", Intent: "cool_environment", Requirements: []string{"sensor", " sensor "},
		}}},
		{name: "blank constraint key", request: &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{
			Id: "task-1", Intent: "cool_environment", Constraints: map[string]string{" ": "26"},
		}}},
		{name: "duplicate normalized constraint", request: &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{
			Id: "task-1", Intent: "cool_environment", Constraints: map[string]string{"target": "26", " target ": "27"},
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := server.SubmitTask(context.Background(), tt.request)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("SubmitTask() code = %v, error = %v", status.Code(err), err)
			}
		})
	}
	if submitter.calls != 0 {
		t.Fatalf("Submit() calls = %d, want 0", submitter.calls)
	}
}

func TestCoreSubmitTaskSucceedsThroughGRPCWithTrimmedTaskAndTwoResults(t *testing.T) {
	wantExecution := successfulCoreExecution(t)
	wantPlan, wantMapped := validCorePlan()
	submitter := newApplicationTaskSubmitter(
		t,
		corePlanPortFunc(func(input task.Task) (planner.Plan, error) {
			want := task.Task{
				ID:           "task-1",
				Intent:       "cool_environment",
				Requirements: []model.Capability{"temperature_sensor", "cooling_control"},
				Constraints:  task.Constraints{"target_temperature": "26"},
			}
			if !reflect.DeepEqual(input, want) {
				return planner.Plan{}, errors.New("planner received an untrimmed task")
			}
			return wantPlan, nil
		}),
		coreMapPortFunc(func(input planner.Plan) (mapper.MappedPlan, error) {
			if !reflect.DeepEqual(input, wantPlan) {
				return mapper.MappedPlan{}, errors.New("mapper received the wrong plan")
			}
			return wantMapped, nil
		}),
		coreRunPortFunc(func(ctx context.Context, input mapper.MappedPlan) (execution.Result, error) {
			if ctx == nil || !reflect.DeepEqual(input, wantMapped) {
				return execution.Result{}, errors.New("runner received the wrong execution request")
			}
			return wantExecution, nil
		}),
	)
	client := newCoreClient(t, submitter)

	response, err := client.SubmitTask(context.Background(), &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{
		Id:           " task-1 ",
		Intent:       " cool_environment ",
		Requirements: []string{" temperature_sensor ", " cooling_control "},
		Constraints:  map[string]string{" target_temperature ": " 26 "},
	}})
	if err != nil {
		t.Fatalf("SubmitTask() error = %v", err)
	}
	if response.GetTaskId() != "task-1" ||
		response.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED ||
		response.GetError() != "" ||
		len(response.GetResults()) != 2 {
		t.Fatalf("SubmitTask() response = %#v", response)
	}
	if got := response.GetResults()[0]; got.GetStepId() != "step-1" ||
		got.GetNodeId() != "sensor-node" ||
		got.GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED ||
		got.GetOutput().AsMap()["temperature"] != float64(30) {
		t.Fatalf("first result = %#v", got)
	}
}

func TestCoreSubmitTaskReturnsBusinessFailuresAsNormalResponses(t *testing.T) {
	t.Run("real planner unknown intent", func(t *testing.T) {
		submitter := newApplicationTaskSubmitter(
			t,
			planner.New(),
			coreMapPortFunc(func(planner.Plan) (mapper.MappedPlan, error) {
				return mapper.MappedPlan{}, errors.New("mapper must not run")
			}),
			coreRunPortFunc(func(context.Context, mapper.MappedPlan) (execution.Result, error) {
				return execution.Result{}, errors.New("runner must not run")
			}),
		)
		response := submitCoreDirect(t, submitter, submitRequest("task-1", "unsupported_intent"))
		assertCoreFailedResponse(t, response, planner.ErrUnknownIntent, 0)
	})

	t.Run("mapper capability unavailable", func(t *testing.T) {
		plan, _ := validCorePlan()
		submitter := newApplicationTaskSubmitter(
			t,
			corePlanPortFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
			coreMapPortFunc(func(planner.Plan) (mapper.MappedPlan, error) {
				return mapper.MappedPlan{}, mapper.ErrCapabilityUnavailable
			}),
			coreRunPortFunc(func(context.Context, mapper.MappedPlan) (execution.Result, error) {
				return execution.Result{}, errors.New("runner must not run")
			}),
		)
		response := submitCoreDirect(t, submitter, submitRequest("task-1", "cool_environment"))
		assertCoreFailedResponse(t, response, mapper.ErrCapabilityUnavailable, 0)
	})

	partial := failedCoreExecution(t)
	runnerFailures := []struct {
		name       string
		result     execution.Result
		failure    error
		wantResult int
	}{
		{name: "endpoint missing", failure: ErrEndpointNotFound},
		{
			name:    "agent unavailable",
			failure: errors.Join(ErrRemoteExecution, status.Error(codes.Unavailable, "agent unavailable")),
		},
		{
			name:       "handler runtime failure preserves partial results",
			result:     partial,
			failure:    errors.Join(runtime.ErrExecutionFailed, errors.New("handler failed")),
			wantResult: 2,
		},
	}
	for _, tt := range runnerFailures {
		t.Run(tt.name, func(t *testing.T) {
			plan, mapped := validCorePlan()
			submitter := newApplicationTaskSubmitter(
				t,
				corePlanPortFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
				coreMapPortFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapped, nil }),
				coreRunPortFunc(func(context.Context, mapper.MappedPlan) (execution.Result, error) {
					return tt.result, tt.failure
				}),
			)
			response := submitCoreDirect(t, submitter, submitRequest("task-1", "cool_environment"))
			assertCoreFailedResponse(t, response, tt.failure, tt.wantResult)
			if tt.wantResult != 0 &&
				(response.GetResults()[0].GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED ||
					response.GetResults()[1].GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_FAILED) {
				t.Fatalf("SubmitTask() partial results = %#v", response.GetResults())
			}
		})
	}
}

func TestCoreSubmitTaskMapsContextErrorsToGRPCStatus(t *testing.T) {
	t.Run("runner cancels context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		submitter := coreSubmitterWithRunner(t, func(got context.Context, _ mapper.MappedPlan) (execution.Result, error) {
			if got != ctx {
				return execution.Result{}, errors.New("runner received a different context")
			}
			cancel()
			return execution.Result{}, got.Err()
		})
		server, err := NewCoreServer(submitter)
		if err != nil {
			t.Fatal(err)
		}
		_, err = server.SubmitTask(ctx, submitRequest("task-1", "cool_environment"))
		if status.Code(err) != codes.Canceled {
			t.Fatalf("SubmitTask() code = %v, error = %v", status.Code(err), err)
		}
	})

	t.Run("runner observes deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		submitter := coreSubmitterWithRunner(t, func(got context.Context, _ mapper.MappedPlan) (execution.Result, error) {
			if got != ctx {
				return execution.Result{}, errors.New("runner received a different context")
			}
			<-got.Done()
			return execution.Result{}, got.Err()
		})
		server, err := NewCoreServer(submitter)
		if err != nil {
			t.Fatal(err)
		}
		_, err = server.SubmitTask(ctx, submitRequest("task-1", "cool_environment"))
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("SubmitTask() code = %v, error = %v", status.Code(err), err)
		}
	})
}

func TestCoreSubmitTaskRejectsUnencodableOutputWithoutPanicking(t *testing.T) {
	step, err := execution.NewStepResult(
		"step-1",
		"sensor-node",
		execution.StatusSucceeded,
		map[string]any{"invalid": make(chan int)},
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := execution.NewResult("task-1", execution.StatusSucceeded, []execution.StepResult{step}, "")
	if err != nil {
		t.Fatal(err)
	}
	submitter := &recordingTaskSubmitter{submit: func(context.Context, task.Task) (application.Outcome, error) {
		return application.Outcome{TaskID: "task-1", State: lifecycle.StateSucceeded, Execution: &result}, nil
	}}
	server, err := NewCoreServer(submitter)
	if err != nil {
		t.Fatal(err)
	}

	_, err = server.SubmitTask(context.Background(), submitRequest("task-1", "cool_environment"))
	if status.Code(err) != codes.Internal {
		t.Fatalf("SubmitTask() code = %v, error = %v", status.Code(err), err)
	}
}

func TestCoreSubmitTaskRejectsInconsistentOutcomes(t *testing.T) {
	tests := []struct {
		name    string
		outcome application.Outcome
		err     error
	}{
		{
			name:    "succeeded with application error",
			outcome: application.Outcome{TaskID: "task-1", State: lifecycle.StateSucceeded},
			err:     errors.New("unexpected"),
		},
		{
			name:    "failed without application error",
			outcome: application.Outcome{TaskID: "task-1", State: lifecycle.StateFailed},
		},
		{
			name: "succeeded with invalid execution aggregate",
			outcome: application.Outcome{
				TaskID: "task-1",
				State:  lifecycle.StateSucceeded,
				Execution: &execution.Result{
					TaskID: "task-1",
					Status: execution.StatusSucceeded,
				},
			},
		},
		{
			name: "task identity mismatch",
			outcome: application.Outcome{
				TaskID: "another-task",
				State:  lifecycle.StateFailed,
			},
			err: errors.New("failed"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			submitter := &recordingTaskSubmitter{submit: func(context.Context, task.Task) (application.Outcome, error) {
				return tt.outcome, tt.err
			}}
			server, err := NewCoreServer(submitter)
			if err != nil {
				t.Fatal(err)
			}
			_, err = server.SubmitTask(context.Background(), submitRequest("task-1", "cool_environment"))
			if status.Code(err) != codes.Internal {
				t.Fatalf("SubmitTask() code = %v, error = %v", status.Code(err), err)
			}
		})
	}
}

func TestLifecycleStateToProtoMapsKnownStatesAndRejectsUnknown(t *testing.T) {
	tests := []struct {
		state lifecycle.State
		want  dtmv1.TaskStatus
	}{
		{lifecycle.StateCreated, dtmv1.TaskStatus_TASK_STATUS_CREATED},
		{lifecycle.StatePlanning, dtmv1.TaskStatus_TASK_STATUS_PLANNED},
		{lifecycle.StateMapped, dtmv1.TaskStatus_TASK_STATUS_MAPPED},
		{lifecycle.StateDispatched, dtmv1.TaskStatus_TASK_STATUS_MAPPED},
		{lifecycle.StateRunning, dtmv1.TaskStatus_TASK_STATUS_RUNNING},
		{lifecycle.StateRetrying, dtmv1.TaskStatus_TASK_STATUS_RUNNING},
		{lifecycle.StateRemapped, dtmv1.TaskStatus_TASK_STATUS_RUNNING},
		{lifecycle.StateSuccess, dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED},
		{lifecycle.StateFailed, dtmv1.TaskStatus_TASK_STATUS_FAILED},
		{lifecycle.StateCancelled, dtmv1.TaskStatus_TASK_STATUS_FAILED},
	}
	for _, tt := range tests {
		got, err := lifecycleStateToProto(tt.state)
		if err != nil || got != tt.want {
			t.Fatalf("lifecycleStateToProto(%q) = %v, %v; want %v, nil", tt.state, got, err, tt.want)
		}
	}
	if _, err := lifecycleStateToProto(lifecycle.State("mystery")); err == nil {
		t.Fatal("lifecycleStateToProto(unknown) error = nil")
	}
}

type recordingTaskSubmitter struct {
	calls  int
	submit func(context.Context, task.Task) (application.Outcome, error)
}

func (submitter *recordingTaskSubmitter) Submit(ctx context.Context, input task.Task) (application.Outcome, error) {
	submitter.calls++
	if submitter.submit == nil {
		return application.Outcome{}, nil
	}
	return submitter.submit(ctx, input)
}

type corePlanPortFunc func(task.Task) (planner.Plan, error)

func (function corePlanPortFunc) Plan(input task.Task) (planner.Plan, error) {
	return function(input)
}

type coreMapPortFunc func(planner.Plan) (mapper.MappedPlan, error)

func (function coreMapPortFunc) Map(input planner.Plan) (mapper.MappedPlan, error) {
	return function(input)
}

type coreRunPortFunc func(context.Context, mapper.MappedPlan) (execution.Result, error)

func (function coreRunPortFunc) Execute(ctx context.Context, input mapper.MappedPlan) (execution.Result, error) {
	return function(ctx, input)
}

func newApplicationTaskSubmitter(
	t *testing.T,
	planPort application.PlanPort,
	mapPort application.MapPort,
	runPort application.RunPort,
) *application.TaskService {
	t.Helper()
	service, err := application.NewTaskService(lifecycle.New, planPort, mapPort, runPort)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func coreSubmitterWithRunner(t *testing.T, runner coreRunPortFunc) *application.TaskService {
	t.Helper()
	plan, mapped := validCorePlan()
	return newApplicationTaskSubmitter(
		t,
		corePlanPortFunc(func(task.Task) (planner.Plan, error) { return plan, nil }),
		coreMapPortFunc(func(planner.Plan) (mapper.MappedPlan, error) { return mapped, nil }),
		runner,
	)
}

func validCorePlan() (planner.Plan, mapper.MappedPlan) {
	plan := planner.Plan{
		TaskID: "task-1",
		Steps: []planner.Step{
			{
				ID:         "step-1",
				Capability: "temperature_sensor",
				Inputs:     map[string]string{"operation": "read_temperature"},
			},
			{
				ID:         "step-2",
				Capability: "cooling_control",
				Inputs:     map[string]string{"target_temperature": "26"},
			},
		},
	}
	mapped := mapper.MappedPlan{
		TaskID: "task-1",
		Steps: []mapper.MappedStep{
			{
				ID:         "step-1",
				Capability: "temperature_sensor",
				NodeID:     "sensor-node",
				Inputs:     map[string]string{"operation": "read_temperature"},
			},
			{
				ID:         "step-2",
				Capability: "cooling_control",
				NodeID:     "cooling-node",
				Inputs:     map[string]string{"target_temperature": "26"},
			},
		},
	}
	return plan, mapped
}

func submitCoreDirect(
	t *testing.T,
	submitter TaskSubmitter,
	request *dtmv1.SubmitTaskRequest,
) *dtmv1.SubmitTaskResponse {
	t.Helper()
	server, err := NewCoreServer(submitter)
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.SubmitTask(context.Background(), request)
	if err != nil {
		t.Fatalf("SubmitTask() error = %v, want normal response", err)
	}
	return response
}

func assertCoreFailedResponse(
	t *testing.T,
	response *dtmv1.SubmitTaskResponse,
	failure error,
	wantResults int,
) {
	t.Helper()
	if response.GetTaskId() != "task-1" ||
		response.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_FAILED ||
		!strings.Contains(response.GetError(), failure.Error()) ||
		len(response.GetResults()) != wantResults {
		t.Fatalf("SubmitTask() response = %#v, want FAILED with %q and %d results", response, failure, wantResults)
	}
}

func submitRequest(id, intent string) *dtmv1.SubmitTaskRequest {
	return &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{Id: id, Intent: intent}}
}

func successfulCoreExecution(t *testing.T) execution.Result {
	t.Helper()
	first, err := execution.NewStepResult(
		"step-1", "sensor-node", execution.StatusSucceeded,
		map[string]any{"temperature": float64(30)}, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := execution.NewStepResult(
		"step-2", "cooling-node", execution.StatusSucceeded,
		map[string]any{"cooling_started": true}, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := execution.NewResult("task-1", execution.StatusSucceeded, []execution.StepResult{first, second}, "")
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func failedCoreExecution(t *testing.T) execution.Result {
	t.Helper()
	first, err := execution.NewStepResult(
		"step-1", "sensor-node", execution.StatusSucceeded,
		map[string]any{"temperature": float64(30)}, "",
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := execution.NewStepResult(
		"step-2", "cooling-node", execution.StatusFailed,
		nil, "handler failed",
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := execution.NewResult(
		"task-1", execution.StatusFailed,
		[]execution.StepResult{first, second}, "handler failed",
	)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func newCoreClient(t *testing.T, submitter TaskSubmitter) dtmv1.CoreServiceClient {
	t.Helper()
	server, err := NewCoreServer(submitter)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	dtmv1.RegisterCoreServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	connection, err := grpc.DialContext(
		ctx,
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return dtmv1.NewCoreServiceClient(connection)
}
