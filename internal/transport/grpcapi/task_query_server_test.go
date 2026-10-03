package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/application"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCoreGetTaskQuerySuccessValidationAndNotFound(t *testing.T) {
	now := time.Date(2026, 8, 2, 1, 2, 3, 4, time.UTC)
	nodeID := model.NodeID("node-a")
	queries := &recordingTaskQueries{task: storage.Task{
		ID: "task-1", Intent: "cool_environment", State: lifecycle.StateRunning,
		CreatedAt: now, UpdatedAt: now, StartedAt: &now, Version: 3,
		Steps: []storage.TaskStep{{
			ID: "step-1", TaskID: "task-1", Sequence: 0, Capability: "temperature_sensor",
			State: lifecycle.StepStateRunning, AssignedNodeID: &nodeID, AttemptCount: 1, MaxAttempts: 3,
			CreatedAt: now, UpdatedAt: now, StartedAt: &now, Version: 2,
		}},
	}}
	server := newQueryTestServer(t, queries)
	response, err := server.GetTask(context.Background(), &dtmv1.GetTaskRequest{TaskId: " task-1 "})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetTask().GetTaskId() != "task-1" || response.GetTask().GetStatus() != dtmv1.TaskStatus_TASK_STATUS_RUNNING ||
		len(response.GetTask().GetSteps()) != 1 || response.GetTask().GetSteps()[0].GetStatus() != dtmv1.StepStatus_STEP_STATUS_RUNNING {
		t.Fatalf("GetTask() response = %#v", response)
	}
	if _, err := server.GetTask(context.Background(), &dtmv1.GetTaskRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty GetTask() code = %v", status.Code(err))
	}
	queries.err = application.ErrTaskNotFound
	if _, err := server.GetTask(context.Background(), &dtmv1.GetTaskRequest{TaskId: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing GetTask() code = %v", status.Code(err))
	}
}

func TestCoreListTasksQuerySuccessAndValidation(t *testing.T) {
	now := time.Date(2026, 8, 2, 1, 2, 3, 0, time.UTC)
	queries := &recordingTaskQueries{page: storage.TaskPage{
		Tasks:         []storage.Task{{ID: "task-1", Intent: "cool_environment", State: lifecycle.StateCreated, CreatedAt: now, UpdatedAt: now, Version: 1}},
		NextPageToken: "next",
	}}
	server := newQueryTestServer(t, queries)
	response, err := server.ListTasks(context.Background(), &dtmv1.ListTasksRequest{Status: dtmv1.TaskStatus_TASK_STATUS_CREATED, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.GetTasks()) != 1 || response.GetTasks()[0].GetTaskId() != "task-1" || response.GetNextPageToken() != "next" ||
		queries.filter.Status == nil || *queries.filter.Status != lifecycle.StateCreated {
		t.Fatalf("ListTasks() response = %#v, filter = %#v", response, queries.filter)
	}
	if _, err := server.ListTasks(context.Background(), &dtmv1.ListTasksRequest{Status: dtmv1.TaskStatus(999)}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid status code = %v", status.Code(err))
	}
	after, before := timestamppb.New(now.Add(time.Second)), timestamppb.New(now)
	if _, err := server.ListTasks(context.Background(), &dtmv1.ListTasksRequest{CreatedAfter: after, CreatedBefore: before}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid time range code = %v", status.Code(err))
	}
	queries.err = application.ErrInvalidTaskQuery
	if _, err := server.ListTasks(context.Background(), &dtmv1.ListTasksRequest{PageToken: "invalid"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid token code = %v", status.Code(err))
	}
}

func TestOptionalTimeFromProtoRejectsUnixNanoOverflow(t *testing.T) {
	minimum := time.Unix(0, math.MinInt64).UTC()
	maximum := time.Unix(0, math.MaxInt64).UTC()
	tests := []struct {
		name    string
		value   time.Time
		wantErr bool
	}{
		{name: "minimum", value: minimum},
		{name: "maximum", value: maximum},
		{name: "before minimum", value: minimum.Add(-time.Nanosecond), wantErr: true},
		{name: "after maximum", value: maximum.Add(time.Nanosecond), wantErr: true},
		{name: "protobuf year 1", value: time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC), wantErr: true},
		{name: "protobuf year 9999", value: time.Date(9999, time.December, 31, 23, 59, 59, 999999999, time.UTC), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := timestamppb.New(test.value)
			if err := value.CheckValid(); err != nil {
				t.Fatalf("test timestamp is not protobuf-valid: %v", err)
			}
			got, err := optionalTimeFromProto("created_after", value)
			if test.wantErr {
				if err == nil || err.Error() != "created_after is outside supported timestamp range" {
					t.Fatalf("optionalTimeFromProto() error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got == nil || !got.Equal(test.value) {
				t.Fatalf("optionalTimeFromProto() = %v, want %v", got, test.value)
			}
		})
	}
}

func TestCoreListTasksRejectsEachOverflowingTimestampAndAcceptsBoundaries(t *testing.T) {
	minimum := time.Unix(0, math.MinInt64).UTC()
	maximum := time.Unix(0, math.MaxInt64).UTC()
	queries := &recordingTaskQueries{}
	server := newQueryTestServer(t, queries)

	tests := []struct {
		name        string
		request     *dtmv1.ListTasksRequest
		wantMessage string
	}{
		{
			name: "created before overflows",
			request: &dtmv1.ListTasksRequest{
				CreatedAfter:  timestamppb.New(minimum),
				CreatedBefore: timestamppb.New(maximum.Add(time.Nanosecond)),
			},
			wantMessage: "created_before is outside supported timestamp range",
		},
		{
			name: "created after overflows",
			request: &dtmv1.ListTasksRequest{
				CreatedAfter:  timestamppb.New(minimum.Add(-time.Nanosecond)),
				CreatedBefore: timestamppb.New(maximum),
			},
			wantMessage: "created_after is outside supported timestamp range",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := server.ListTasks(context.Background(), test.request)
			if status.Code(err) != codes.InvalidArgument || status.Convert(err).Message() != test.wantMessage {
				t.Fatalf("ListTasks() error = %v", err)
			}
		})
	}

	_, err := server.ListTasks(context.Background(), &dtmv1.ListTasksRequest{
		CreatedAfter:  timestamppb.New(minimum),
		CreatedBefore: timestamppb.New(maximum),
	})
	if err != nil {
		t.Fatal(err)
	}
	if queries.filter.CreatedAfter == nil || !queries.filter.CreatedAfter.Equal(minimum) ||
		queries.filter.CreatedBefore == nil || !queries.filter.CreatedBefore.Equal(maximum) {
		t.Fatalf("boundary filter = %#v", queries.filter)
	}
}

func TestCoreGetTaskExecutionsSuccessAndNotFound(t *testing.T) {
	now := time.Date(2026, 8, 2, 1, 2, 3, 0, time.UTC)
	queries := &recordingTaskQueries{executions: []storage.Execution{{
		ID: "execution-1", RequestID: "execution-1", TaskID: "task-1", StepID: "step-1", AttemptNo: 1, NodeID: "node-a",
		State: storage.ExecutionStateStarted, Request: map[string]string{"operation": "read"},
		StartedAt: &now, CreatedAt: now, UpdatedAt: now, Version: 1,
	}}}
	server := newQueryTestServer(t, queries)
	response, err := server.GetTaskExecutions(context.Background(), &dtmv1.GetTaskExecutionsRequest{TaskId: "task-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.GetExecutions()) != 1 || response.GetExecutions()[0].GetExecutionId() != "execution-1" ||
		response.GetExecutions()[0].GetRequestId() != "execution-1" || response.GetExecutions()[0].GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_STARTED {
		t.Fatalf("GetTaskExecutions() response = %#v", response)
	}
	queries.err = application.ErrTaskNotFound
	if _, err := server.GetTaskExecutions(context.Background(), &dtmv1.GetTaskExecutionsRequest{TaskId: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing executions code = %v", status.Code(err))
	}
}

func TestTaskQueryRejectsPersistedIntegerOverflowWithoutWrapping(t *testing.T) {
	if ^uint(0)>>63 == 0 {
		t.Skip("int overflow fixture requires a 64-bit target")
	}
	overflow := int(int64(1) << 32)
	if _, err := safeInt32("attempt", overflow); err == nil {
		t.Fatal("safeInt32() overflow error = nil")
	}
	now := time.Date(2026, 8, 2, 1, 2, 3, 0, time.UTC)
	queries := &recordingTaskQueries{task: storage.Task{
		ID: "task-overflow", Intent: "cool_environment", State: lifecycle.StateCreated,
		CreatedAt: now, UpdatedAt: now, Version: 1,
		Steps: []storage.TaskStep{{
			ID: "step-overflow", TaskID: "task-overflow", Capability: "temperature_sensor",
			State: lifecycle.StepStateCreated, Sequence: overflow, MaxAttempts: 1,
			CreatedAt: now, UpdatedAt: now, Version: 1,
		}},
	}}
	server := newQueryTestServer(t, queries)
	_, err := server.GetTask(context.Background(), &dtmv1.GetTaskRequest{TaskId: "task-overflow"})
	if status.Code(err) != codes.Internal || status.Convert(err).Message() != "encode task query response" {
		t.Fatalf("overflow GetTask() error = %v", err)
	}
	if strings.Contains(status.Convert(err).Message(), "4294967296") {
		t.Fatalf("overflow details leaked to client: %v", err)
	}
}

func TestCoreTaskQueryErrorsAreSafeAndCancellationIsMapped(t *testing.T) {
	queries := &recordingTaskQueries{err: errors.New("SELECT * FROM tasks at C:\\secret\\state.db: file is not a database")}
	server := newQueryTestServer(t, queries)
	_, err := server.GetTask(context.Background(), &dtmv1.GetTaskRequest{TaskId: "task-1"})
	if status.Code(err) != codes.Internal {
		t.Fatalf("internal error code = %v", status.Code(err))
	}
	for _, sensitive := range []string{"SELECT", "tasks", "secret", "state.db"} {
		if strings.Contains(status.Convert(err).Message(), sensitive) {
			t.Fatalf("client error leaked %q: %v", sensitive, err)
		}
	}
	queries.err = fmt.Errorf("%w: SELECT task_id FROM tasks at C:\\secret\\state.db", application.ErrTaskQueryUnavailable)
	_, err = server.ListTasks(context.Background(), &dtmv1.ListTasksRequest{})
	if status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "task query unavailable" {
		t.Fatalf("unavailable error = %v", err)
	}
	for _, sensitive := range []string{"SELECT", "tasks", "secret", "state.db"} {
		if strings.Contains(status.Convert(err).Message(), sensitive) {
			t.Fatalf("unavailable client error leaked %q: %v", sensitive, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	queries.err = ctx.Err()
	if _, err := server.GetTask(ctx, &dtmv1.GetTaskRequest{TaskId: "task-1"}); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled query code = %v", status.Code(err))
	}
}

func newQueryTestServer(t *testing.T, queries TaskQueryService) *CoreServer {
	t.Helper()
	server, err := NewCoreServer(&recordingTaskSubmitter{}, WithTaskQueryService(queries))
	if err != nil {
		t.Fatal(err)
	}
	return server
}

type recordingTaskQueries struct {
	task       storage.Task
	page       storage.TaskPage
	executions []storage.Execution
	filter     storage.TaskFilter
	err        error
}

func (queries *recordingTaskQueries) GetTask(context.Context, model.TaskID) (storage.Task, error) {
	return queries.task, queries.err
}

func (queries *recordingTaskQueries) ListTasks(_ context.Context, filter storage.TaskFilter) (storage.TaskPage, error) {
	queries.filter = filter
	return queries.page, queries.err
}

func (queries *recordingTaskQueries) GetTaskExecutions(context.Context, model.TaskID) ([]storage.Execution, error) {
	return queries.executions, queries.err
}
