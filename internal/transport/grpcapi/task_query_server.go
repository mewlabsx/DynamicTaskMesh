package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/application"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (server *CoreServer) GetTask(ctx context.Context, request *dtmv1.GetTaskRequest) (*dtmv1.GetTaskResponse, error) {
	if err := server.requireReady(); err != nil {
		return nil, err
	}
	if request == nil || strings.TrimSpace(request.GetTaskId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id is required")
	}
	if server.queries == nil {
		return nil, status.Error(codes.Unimplemented, "task query service is not configured")
	}
	record, err := server.queries.GetTask(ctx, model.TaskID(strings.TrimSpace(request.GetTaskId())))
	if err != nil {
		return nil, server.mapTaskQueryError("get_task", err)
	}
	converted, err := taskDetailsToProto(record)
	if err != nil {
		server.logger.Printf("task query encode failed operation=get_task cause=%q", err)
		return nil, status.Error(codes.Internal, "encode task query response")
	}
	return &dtmv1.GetTaskResponse{Task: converted}, nil
}

func (server *CoreServer) ListTasks(ctx context.Context, request *dtmv1.ListTasksRequest) (*dtmv1.ListTasksResponse, error) {
	if err := server.requireReady(); err != nil {
		return nil, err
	}
	if request == nil {
		request = &dtmv1.ListTasksRequest{}
	}
	if server.queries == nil {
		return nil, status.Error(codes.Unimplemented, "task query service is not configured")
	}
	filter, err := taskFilterFromProto(request)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	page, err := server.queries.ListTasks(ctx, filter)
	if err != nil {
		return nil, server.mapTaskQueryError("list_tasks", err)
	}
	items := make([]*dtmv1.TaskSummary, 0, len(page.Tasks))
	for _, record := range page.Tasks {
		converted, err := taskSummaryToProto(record)
		if err != nil {
			server.logger.Printf("task query encode failed operation=list_tasks cause=%q", err)
			return nil, status.Error(codes.Internal, "encode task query response")
		}
		items = append(items, converted)
	}
	return &dtmv1.ListTasksResponse{Tasks: items, NextPageToken: page.NextPageToken}, nil
}

func (server *CoreServer) GetTaskExecutions(ctx context.Context, request *dtmv1.GetTaskExecutionsRequest) (*dtmv1.GetTaskExecutionsResponse, error) {
	if err := server.requireReady(); err != nil {
		return nil, err
	}
	if request == nil || strings.TrimSpace(request.GetTaskId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id is required")
	}
	if server.queries == nil {
		return nil, status.Error(codes.Unimplemented, "task query service is not configured")
	}
	records, err := server.queries.GetTaskExecutions(ctx, model.TaskID(strings.TrimSpace(request.GetTaskId())))
	if err != nil {
		return nil, server.mapTaskQueryError("get_task_executions", err)
	}
	items := make([]*dtmv1.TaskExecution, 0, len(records))
	for _, record := range records {
		converted, err := taskExecutionToProto(record)
		if err != nil {
			server.logger.Printf("task query encode failed operation=get_task_executions cause=%q", err)
			return nil, status.Error(codes.Internal, "encode task query response")
		}
		items = append(items, converted)
	}
	return &dtmv1.GetTaskExecutionsResponse{Executions: items}, nil
}

func (server *CoreServer) mapTaskQueryError(operation string, err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "task query canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "task query deadline exceeded")
	case errors.Is(err, application.ErrInvalidTaskQuery):
		return status.Error(codes.InvalidArgument, "invalid task query")
	case errors.Is(err, application.ErrTaskNotFound):
		return status.Error(codes.NotFound, "task not found")
	case errors.Is(err, application.ErrTaskQueryUnavailable):
		server.logger.Printf("task query failed operation=%s cause=%q", operation, err)
		return status.Error(codes.Unavailable, "task query unavailable")
	default:
		server.logger.Printf("task query failed operation=%s cause=%q", operation, err)
		return status.Error(codes.Internal, "task query failed")
	}
}

func taskFilterFromProto(request *dtmv1.ListTasksRequest) (storage.TaskFilter, error) {
	filter := storage.TaskFilter{Limit: int(request.GetLimit()), PageToken: request.GetPageToken()}
	if request.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_UNSPECIFIED {
		state, err := taskStatusFromProto(request.GetStatus())
		if err != nil {
			return storage.TaskFilter{}, err
		}
		filter.Status = &state
	}
	var err error
	filter.CreatedAfter, err = optionalTimeFromProto("created_after", request.GetCreatedAfter())
	if err != nil {
		return storage.TaskFilter{}, err
	}
	filter.CreatedBefore, err = optionalTimeFromProto("created_before", request.GetCreatedBefore())
	if err != nil {
		return storage.TaskFilter{}, err
	}
	if filter.CreatedAfter != nil && filter.CreatedBefore != nil && filter.CreatedAfter.After(*filter.CreatedBefore) {
		return storage.TaskFilter{}, errors.New("created_after must not be later than created_before")
	}
	if filter.Limit < 0 || filter.Limit > storage.MaxTaskPageLimit {
		return storage.TaskFilter{}, fmt.Errorf("limit must be between 0 and %d", storage.MaxTaskPageLimit)
	}
	return filter, nil
}

func optionalTimeFromProto(name string, value *timestamppb.Timestamp) (*time.Time, error) {
	if value == nil {
		return nil, nil
	}
	if err := value.CheckValid(); err != nil {
		return nil, fmt.Errorf("%s is invalid", name)
	}
	converted := value.AsTime().UTC()
	nanoseconds := converted.UnixNano()
	if !time.Unix(0, nanoseconds).UTC().Equal(converted) {
		return nil, fmt.Errorf("%s is outside supported timestamp range", name)
	}
	return &converted, nil
}

func taskDetailsToProto(record storage.Task) (*dtmv1.TaskDetails, error) {
	state, err := taskStatusToProto(record.State)
	if err != nil {
		return nil, err
	}
	steps := make([]*dtmv1.TaskStepDetails, 0, len(record.Steps))
	for _, step := range record.Steps {
		converted, err := taskStepToProto(step)
		if err != nil {
			return nil, err
		}
		steps = append(steps, converted)
	}
	return &dtmv1.TaskDetails{
		TaskId: string(record.ID), Intent: record.Intent, Status: state,
		CreatedAt: timestampToProto(record.CreatedAt), UpdatedAt: timestampToProto(record.UpdatedAt),
		StartedAt: optionalTimestampToProto(record.StartedAt), CompletedAt: optionalTimestampToProto(record.CompletedAt),
		FailureCode: record.FailureCode, FailureMessage: record.FailureMessage, Version: record.Version, Steps: steps,
	}, nil
}

func taskSummaryToProto(record storage.Task) (*dtmv1.TaskSummary, error) {
	state, err := taskStatusToProto(record.State)
	if err != nil {
		return nil, err
	}
	return &dtmv1.TaskSummary{
		TaskId: string(record.ID), Intent: record.Intent, Status: state,
		CreatedAt: timestampToProto(record.CreatedAt), UpdatedAt: timestampToProto(record.UpdatedAt),
		StartedAt: optionalTimestampToProto(record.StartedAt), CompletedAt: optionalTimestampToProto(record.CompletedAt),
		FailureCode: record.FailureCode, FailureMessage: record.FailureMessage, Version: record.Version,
	}, nil
}

func taskStepToProto(step storage.TaskStep) (*dtmv1.TaskStepDetails, error) {
	state, err := stepStatusToProto(step.State)
	if err != nil {
		return nil, err
	}
	sequence, err := safeInt32("step sequence", step.Sequence)
	if err != nil {
		return nil, err
	}
	attemptCount, err := safeInt32("step attempt_count", step.AttemptCount)
	if err != nil {
		return nil, err
	}
	maxAttempts, err := safeInt32("step max_attempts", step.MaxAttempts)
	if err != nil {
		return nil, err
	}
	assignedNodeID := ""
	if step.AssignedNodeID != nil {
		assignedNodeID = string(*step.AssignedNodeID)
	}
	return &dtmv1.TaskStepDetails{
		StepId: string(step.ID), Sequence: sequence, Capability: string(step.Capability),
		IdempotencyMode: idempotencyModeToProto(step.IdempotencyMode),
		Status:          state, AssignedNodeId: assignedNodeID, AttemptCount: attemptCount, MaxAttempts: maxAttempts,
		FailureCode: step.FailureCode, FailureMessage: step.FailureMessage,
		CreatedAt: timestampToProto(step.CreatedAt), UpdatedAt: timestampToProto(step.UpdatedAt),
		StartedAt: optionalTimestampToProto(step.StartedAt), CompletedAt: optionalTimestampToProto(step.CompletedAt), Version: step.Version,
	}, nil
}

func idempotencyModeToProto(mode model.IdempotencyMode) dtmv1.IdempotencyMode {
	switch mode {
	case model.IdempotencyIdempotent:
		return dtmv1.IdempotencyMode_IDEMPOTENCY_MODE_IDEMPOTENT
	case model.IdempotencyNonIdempotent:
		return dtmv1.IdempotencyMode_IDEMPOTENCY_MODE_NON_IDEMPOTENT
	default:
		return dtmv1.IdempotencyMode_IDEMPOTENCY_MODE_UNSPECIFIED
	}
}

func taskExecutionToProto(record storage.Execution) (*dtmv1.TaskExecution, error) {
	state, err := executionStateToProto(record.State)
	if err != nil {
		return nil, err
	}
	attemptNumber, err := safeInt32("execution attempt_number", record.AttemptNo)
	if err != nil {
		return nil, err
	}
	requestValues := make(map[string]any, len(record.Request))
	for key, value := range record.Request {
		requestValues[key] = value
	}
	request, err := structpb.NewStruct(requestValues)
	if err != nil {
		return nil, fmt.Errorf("encode execution %q request: %w", record.ID, err)
	}
	var response *structpb.Struct
	if record.Result != nil && record.Result.Output != nil {
		response, err = structpb.NewStruct(record.Result.Output)
		if err != nil {
			return nil, fmt.Errorf("encode execution %q response: %w", record.ID, err)
		}
	}
	return &dtmv1.TaskExecution{
		ExecutionId: string(record.ID), TaskId: string(record.TaskID), StepId: string(record.StepID),
		AttemptNumber: attemptNumber, NodeId: string(record.NodeID), RequestId: record.RequestID, Status: state,
		FailureCode: record.FailureCode, FailureMessage: record.FailureMessage,
		StartedAt: optionalTimestampToProto(record.StartedAt), CompletedAt: optionalTimestampToProto(record.CompletedAt),
		UpdatedAt: timestampToProto(record.UpdatedAt), Request: request, Response: response,
	}, nil
}

func safeInt32(name string, value int) (int32, error) {
	const maxInt32 = int(^uint32(0) >> 1)
	const minInt32 = -maxInt32 - 1
	if value < minInt32 || value > maxInt32 {
		return 0, fmt.Errorf("%s is outside int32 range", name)
	}
	return int32(value), nil
}

func taskStatusToProto(state lifecycle.State) (dtmv1.TaskStatus, error) {
	switch state {
	case lifecycle.StateCreated:
		return dtmv1.TaskStatus_TASK_STATUS_CREATED, nil
	case lifecycle.StatePlanning:
		return dtmv1.TaskStatus_TASK_STATUS_PLANNED, nil
	case lifecycle.StateMapped:
		return dtmv1.TaskStatus_TASK_STATUS_MAPPED, nil
	case lifecycle.StateDispatched:
		return dtmv1.TaskStatus_TASK_STATUS_DISPATCHED, nil
	case lifecycle.StateRunning:
		return dtmv1.TaskStatus_TASK_STATUS_RUNNING, nil
	case lifecycle.StateRetrying:
		return dtmv1.TaskStatus_TASK_STATUS_RETRYING, nil
	case lifecycle.StateRemapped:
		return dtmv1.TaskStatus_TASK_STATUS_REMAPPED, nil
	case lifecycle.StateSuccess:
		return dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED, nil
	case lifecycle.StateFailed:
		return dtmv1.TaskStatus_TASK_STATUS_FAILED, nil
	case lifecycle.StateCancelled:
		return dtmv1.TaskStatus_TASK_STATUS_CANCELLED, nil
	default:
		return 0, fmt.Errorf("unknown task status %q", state)
	}
}

func taskStatusFromProto(state dtmv1.TaskStatus) (lifecycle.State, error) {
	switch state {
	case dtmv1.TaskStatus_TASK_STATUS_CREATED:
		return lifecycle.StateCreated, nil
	case dtmv1.TaskStatus_TASK_STATUS_PLANNED:
		return lifecycle.StatePlanning, nil
	case dtmv1.TaskStatus_TASK_STATUS_MAPPED:
		return lifecycle.StateMapped, nil
	case dtmv1.TaskStatus_TASK_STATUS_DISPATCHED:
		return lifecycle.StateDispatched, nil
	case dtmv1.TaskStatus_TASK_STATUS_RUNNING:
		return lifecycle.StateRunning, nil
	case dtmv1.TaskStatus_TASK_STATUS_RETRYING:
		return lifecycle.StateRetrying, nil
	case dtmv1.TaskStatus_TASK_STATUS_REMAPPED:
		return lifecycle.StateRemapped, nil
	case dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED:
		return lifecycle.StateSuccess, nil
	case dtmv1.TaskStatus_TASK_STATUS_FAILED:
		return lifecycle.StateFailed, nil
	case dtmv1.TaskStatus_TASK_STATUS_CANCELLED:
		return lifecycle.StateCancelled, nil
	default:
		return "", errors.New("status is invalid")
	}
}

func stepStatusToProto(state lifecycle.StepState) (dtmv1.StepStatus, error) {
	switch state {
	case lifecycle.StepStateCreated:
		return dtmv1.StepStatus_STEP_STATUS_CREATED, nil
	case lifecycle.StepStateMapped:
		return dtmv1.StepStatus_STEP_STATUS_MAPPED, nil
	case lifecycle.StepStateDispatched:
		return dtmv1.StepStatus_STEP_STATUS_DISPATCHED, nil
	case lifecycle.StepStateRunning:
		return dtmv1.StepStatus_STEP_STATUS_RUNNING, nil
	case lifecycle.StepStateRetrying:
		return dtmv1.StepStatus_STEP_STATUS_RETRYING, nil
	case lifecycle.StepStateRemapped:
		return dtmv1.StepStatus_STEP_STATUS_REMAPPED, nil
	case lifecycle.StepStateSuccess:
		return dtmv1.StepStatus_STEP_STATUS_SUCCEEDED, nil
	case lifecycle.StepStateFailed:
		return dtmv1.StepStatus_STEP_STATUS_FAILED, nil
	case lifecycle.StepStateCancelled:
		return dtmv1.StepStatus_STEP_STATUS_CANCELLED, nil
	default:
		return 0, fmt.Errorf("unknown step status %q", state)
	}
}

func executionStateToProto(state storage.ExecutionState) (dtmv1.ExecutionStatus, error) {
	switch state {
	case storage.ExecutionStateCreated:
		return dtmv1.ExecutionStatus_EXECUTION_STATUS_CREATED, nil
	case storage.ExecutionStateStarted:
		return dtmv1.ExecutionStatus_EXECUTION_STATUS_STARTED, nil
	case storage.ExecutionStateSucceeded:
		return dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED, nil
	case storage.ExecutionStateFailed:
		return dtmv1.ExecutionStatus_EXECUTION_STATUS_FAILED, nil
	case storage.ExecutionStateCanceled:
		return dtmv1.ExecutionStatus_EXECUTION_STATUS_CANCELED, nil
	default:
		return 0, fmt.Errorf("unknown execution status %q", state)
	}
}

func timestampToProto(value time.Time) *timestamppb.Timestamp {
	return timestamppb.New(value.UTC())
}

func optionalTimestampToProto(value *time.Time) *timestamppb.Timestamp {
	if value == nil {
		return nil
	}
	return timestampToProto(*value)
}
