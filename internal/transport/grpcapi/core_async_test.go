package grpcapi

import (
	"context"
	"errors"
	"testing"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/application"
	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/task"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCoreSubmitTaskAsyncReturnsCreatedWithoutCallingSyncSubmitter(t *testing.T) {
	syncSubmitter := &recordingTaskSubmitter{}
	async := &recordingAsyncTaskService{
		submit: func(_ context.Context, input task.Task) (application.TaskRecord, error) {
			return application.TaskRecord{Task: input, State: lifecycle.StateCreated}, nil
		},
	}
	server, err := NewCoreServer(syncSubmitter, WithAsyncTaskService(async))
	if err != nil {
		t.Fatal(err)
	}

	response, err := server.SubmitTask(context.Background(), &dtmv1.SubmitTaskRequest{
		Task:  &dtmv1.Task{Id: " task-1 ", Intent: " cool_environment "},
		Async: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetTaskId() != "task-1" ||
		response.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_CREATED ||
		response.GetError() != "" ||
		len(response.GetResults()) != 0 {
		t.Fatalf("SubmitTask(async) response = %#v", response)
	}
	if syncSubmitter.calls != 0 {
		t.Fatalf("sync Submit() calls = %d, want 0", syncSubmitter.calls)
	}
}

func TestCoreSubmitTaskAsyncMapsAcceptanceErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{"duplicate", application.ErrTaskAlreadyExists, codes.AlreadyExists},
		{"closed", application.ErrAsyncTaskServiceClosed, codes.Unavailable},
		{"canceled", context.Canceled, codes.Canceled},
		{"storage", errors.New("storage failed"), codes.Internal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server, err := NewCoreServer(
				&recordingTaskSubmitter{},
				WithAsyncTaskService(&recordingAsyncTaskService{
					submit: func(context.Context, task.Task) (application.TaskRecord, error) {
						return application.TaskRecord{}, test.err
					},
				}),
			)
			if err != nil {
				t.Fatal(err)
			}
			_, err = server.SubmitTask(context.Background(), &dtmv1.SubmitTaskRequest{
				Task:  &dtmv1.Task{Id: "task-1", Intent: "cool_environment"},
				Async: true,
			})
			if status.Code(err) != test.code {
				t.Fatalf("SubmitTask(async) code = %v, error = %v", status.Code(err), err)
			}
		})
	}
}

func TestCoreGetTaskStatusReturnsPersistedTaskAndResults(t *testing.T) {
	input, err := task.New("task-1", "cool_environment", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	async := &recordingAsyncTaskService{
		find: func(_ context.Context, taskID model.TaskID) (application.TaskRecord, error) {
			if taskID != input.ID {
				t.Fatalf("Find() task ID = %q", taskID)
			}
			return application.TaskRecord{
				Task:  input,
				State: lifecycle.StateSucceeded,
				Steps: []application.ExecutionStepRecord{
					{
						ID:         "step-1",
						Position:   0,
						Capability: "temperature_sensor",
						NodeID:     "sensor-1",
						Status:     execution.StatusSucceeded,
						Output:     map[string]any{"temperature": 30.0},
					},
					{
						ID:         "step-2",
						Position:   1,
						Capability: "cooling_control",
						NodeID:     "cooling-1",
					},
				},
			}, nil
		},
	}
	server, err := NewCoreServer(&recordingTaskSubmitter{}, WithAsyncTaskService(async))
	if err != nil {
		t.Fatal(err)
	}

	response, err := server.GetTaskStatus(
		context.Background(),
		&dtmv1.GetTaskStatusRequest{TaskId: " task-1 "},
	)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetTaskId() != "task-1" ||
		response.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED ||
		response.GetError() != "" ||
		len(response.GetResults()) != 1 {
		t.Fatalf("GetTaskStatus() response = %#v", response)
	}
	if got := response.GetResults()[0]; got.GetStepId() != "step-1" ||
		got.GetNodeId() != "sensor-1" ||
		got.GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED ||
		got.GetOutput().AsMap()["temperature"] != 30.0 {
		t.Fatalf("GetTaskStatus() result = %#v", got)
	}
}

func TestCoreGetTaskStatusValidatesAndMapsLookupErrors(t *testing.T) {
	serverWithoutAsync, err := NewCoreServer(&recordingTaskSubmitter{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := serverWithoutAsync.GetTaskStatus(context.Background(), nil); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("GetTaskStatus(nil) code = %v", status.Code(err))
	}
	if _, err := serverWithoutAsync.GetTaskStatus(
		context.Background(),
		&dtmv1.GetTaskStatusRequest{TaskId: "task-1"},
	); status.Code(err) != codes.Unimplemented {
		t.Fatalf("GetTaskStatus() without async code = %v", status.Code(err))
	}

	for _, test := range []struct {
		name string
		err  error
		code codes.Code
	}{
		{"missing", application.ErrTaskNotFound, codes.NotFound},
		{"deadline", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"storage", errors.New("storage failed"), codes.Internal},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, err := NewCoreServer(
				&recordingTaskSubmitter{},
				WithAsyncTaskService(&recordingAsyncTaskService{
					find: func(context.Context, model.TaskID) (application.TaskRecord, error) {
						return application.TaskRecord{}, test.err
					},
				}),
			)
			if err != nil {
				t.Fatal(err)
			}
			_, err = server.GetTaskStatus(
				context.Background(),
				&dtmv1.GetTaskStatusRequest{TaskId: "task-1"},
			)
			if status.Code(err) != test.code {
				t.Fatalf("GetTaskStatus() code = %v, error = %v", status.Code(err), err)
			}
		})
	}
}

func TestNewCoreServerRejectsNilAsyncTaskService(t *testing.T) {
	var async *recordingAsyncTaskService
	server, err := NewCoreServer(&recordingTaskSubmitter{}, WithAsyncTaskService(async))
	if server != nil || !errors.Is(err, ErrInvalidAsyncTaskService) {
		t.Fatalf("NewCoreServer() = %#v, %v", server, err)
	}
}

type recordingAsyncTaskService struct {
	submit func(context.Context, task.Task) (application.TaskRecord, error)
	find   func(context.Context, model.TaskID) (application.TaskRecord, error)
}

func (service *recordingAsyncTaskService) Submit(
	ctx context.Context,
	input task.Task,
) (application.TaskRecord, error) {
	if service.submit == nil {
		return application.TaskRecord{}, nil
	}
	return service.submit(ctx, input)
}

func (service *recordingAsyncTaskService) Find(
	ctx context.Context,
	taskID model.TaskID,
) (application.TaskRecord, error) {
	if service.find == nil {
		return application.TaskRecord{}, application.ErrTaskNotFound
	}
	return service.find(ctx, taskID)
}
