package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/application"
	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/storage"
	"dtm/internal/task"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidTaskSubmitter          = errors.New("invalid task submitter")
	ErrInvalidAsyncTaskService       = errors.New("invalid async task service")
	ErrInvalidTaskQueryService       = errors.New("invalid task query service")
	ErrInvalidCoreReadiness          = errors.New("invalid core readiness provider")
	ErrAsyncTaskServiceNotConfigured = errors.New("async task service is not configured")
	ErrInvalidCoreLogger             = errors.New("invalid core logger")
)

const (
	submissionUnavailableMessage = "task submission is temporarily unavailable"
	submissionPersistenceMessage = "task submission persistence failed"
)

type TaskSubmitter interface {
	Submit(context.Context, task.Task) (application.Outcome, error)
}

type idempotentTaskSubmitter interface {
	SubmitSubmission(context.Context, task.Task, string, string) (application.Outcome, bool, error)
}

type AsyncTaskService interface {
	Submit(context.Context, task.Task) (application.TaskRecord, error)
	Find(context.Context, model.TaskID) (application.TaskRecord, error)
}

type idempotentAsyncTaskService interface {
	SubmitSubmission(context.Context, task.Task, string, string) (application.TaskRecord, bool, error)
}

type TaskQueryService interface {
	GetTask(context.Context, model.TaskID) (storage.Task, error)
	ListTasks(context.Context, storage.TaskFilter) (storage.TaskPage, error)
	GetTaskExecutions(context.Context, model.TaskID) ([]storage.Execution, error)
}

type CoreServer struct {
	dtmv1.UnimplementedCoreServiceServer

	submitter TaskSubmitter
	async     AsyncTaskService
	queries   TaskQueryService
	logger    *log.Logger
	readiness func() bool
}

type CoreServerOption func(*CoreServer) error

func WithAsyncTaskService(service AsyncTaskService) CoreServerOption {
	return func(server *CoreServer) error {
		if isNilDependency(service) {
			return ErrInvalidAsyncTaskService
		}
		server.async = service
		return nil
	}
}

func WithTaskQueryService(service TaskQueryService) CoreServerOption {
	return func(server *CoreServer) error {
		if isNilDependency(service) {
			return ErrInvalidTaskQueryService
		}
		server.queries = service
		return nil
	}
}

// WithReadiness installs an optional runtime-mode ingress gate. Static
// dtm-core does not provide this option and therefore keeps its v0.4 behavior.
// NodeRegistryService is registered separately and is intentionally not gated.
func WithReadiness(ready func() bool) CoreServerOption {
	return func(server *CoreServer) error {
		if ready == nil {
			return ErrInvalidCoreReadiness
		}
		server.readiness = ready
		return nil
	}
}

func WithCoreLogger(logger *log.Logger) CoreServerOption {
	return func(server *CoreServer) error {
		if logger == nil {
			return ErrInvalidCoreLogger
		}
		server.logger = logger
		return nil
	}
}

func NewCoreServer(submitter TaskSubmitter, options ...CoreServerOption) (*CoreServer, error) {
	if isNilDependency(submitter) {
		return nil, ErrInvalidTaskSubmitter
	}
	server := &CoreServer{
		submitter: submitter,
		logger:    log.New(io.Discard, "", 0),
	}
	for _, option := range options {
		if option == nil {
			return nil, ErrInvalidAsyncTaskService
		}
		if err := option(server); err != nil {
			return nil, err
		}
	}
	return server, nil
}

func (server *CoreServer) SubmitTask(
	ctx context.Context,
	request *dtmv1.SubmitTaskRequest,
) (*dtmv1.SubmitTaskResponse, error) {
	if err := server.requireReady(); err != nil {
		return nil, err
	}
	key := request.GetIdempotencyKey()
	if err := validateIdempotencyKey(key); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	input, err := taskFromProto(request)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid task: %v", err)
	}
	fingerprint := ""
	if key != "" {
		fingerprint, err = submissionFingerprint(input)
		if err != nil {
			server.logSubmissionError(key, input.ID, "fingerprint", "internal")
			return nil, status.Error(codes.Internal, "task submission persistence failed")
		}
	}
	if request.GetAsync() {
		return server.submitTaskAsync(ctx, input, key, fingerprint)
	}

	var outcome application.Outcome
	var deduplicated bool
	var submitErr error
	if key != "" {
		submitter, ok := server.submitter.(idempotentTaskSubmitter)
		if !ok {
			return nil, status.Error(codes.Internal, "idempotent submission is not configured")
		}
		outcome, deduplicated, submitErr = submitter.SubmitSubmission(ctx, input, key, fingerprint)
	} else {
		outcome, submitErr = server.submitter.Submit(ctx, input)
	}
	if mapped, handled := server.mapSubmissionError(key, input.ID, submitErr); handled {
		return nil, mapped
	}

	response, err := submissionOutcomeToProto(input, outcome, submitErr, deduplicated)
	if err != nil {
		server.logSubmissionError(key, input.ID, "encode_response", "internal")
		return nil, status.Error(codes.Internal, submissionPersistenceMessage)
	}
	server.logOutcome(outcome, submitErr)
	if deduplicated {
		server.logger.Printf("event=submit_task_deduplicated task_id=%s key_hash_prefix=%s task_status=%s", outcome.TaskID, idempotencyKeyHashPrefix(key), outcome.State)
	}
	return response, nil
}

func (server *CoreServer) requireReady() error {
	if server.readiness != nil && !server.readiness() {
		return status.Error(codes.Unavailable, "runtime ingress is not ready")
	}
	return nil
}

func (server *CoreServer) submitTaskAsync(
	ctx context.Context,
	input task.Task,
	idempotencyKey string,
	requestFingerprint string,
) (*dtmv1.SubmitTaskResponse, error) {
	if server.async == nil {
		return nil, status.Error(codes.Unimplemented, ErrAsyncTaskServiceNotConfigured.Error())
	}
	var record application.TaskRecord
	var deduplicated bool
	var err error
	if idempotencyKey != "" {
		service, ok := server.async.(idempotentAsyncTaskService)
		if !ok {
			return nil, status.Error(codes.Internal, "idempotent async submission is not configured")
		}
		record, deduplicated, err = service.SubmitSubmission(ctx, input, idempotencyKey, requestFingerprint)
	} else {
		record, err = server.async.Submit(ctx, input)
	}
	if mapped, handled := server.mapSubmissionError(idempotencyKey, input.ID, err); handled {
		return nil, mapped
	}
	switch {
	case errors.Is(err, application.ErrTaskAlreadyExists):
		return nil, status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, application.ErrAsyncTaskServiceClosed):
		server.logSubmissionError(idempotencyKey, input.ID, "accept_async", "unavailable")
		return nil, status.Error(codes.Unavailable, submissionUnavailableMessage)
	case err != nil:
		server.logSubmissionError(idempotencyKey, input.ID, "accept_async", "internal")
		return nil, status.Error(codes.Internal, submissionPersistenceMessage)
	}
	if (!deduplicated && record.Task.ID != input.ID) || record.Task.ID == "" {
		server.logSubmissionError(idempotencyKey, input.ID, "validate_async_response", "internal")
		return nil, status.Error(codes.Internal, submissionPersistenceMessage)
	}
	statusValue, statusErr := lifecycleStateToProto(record.State)
	if statusErr != nil {
		server.logSubmissionError(idempotencyKey, input.ID, "encode_async_response", "internal")
		return nil, status.Error(codes.Internal, submissionPersistenceMessage)
	}
	server.logger.Printf("task accepted task_id=%s result=accepted", record.Task.ID)
	if deduplicated {
		server.logger.Printf("event=submit_task_deduplicated task_id=%s key_hash_prefix=%s task_status=%s", record.Task.ID, idempotencyKeyHashPrefix(idempotencyKey), record.State)
	}
	return &dtmv1.SubmitTaskResponse{
		TaskId:       string(record.Task.ID),
		Status:       statusValue,
		Error:        record.Error,
		Deduplicated: deduplicated,
	}, nil
}

func (server *CoreServer) mapSubmissionError(
	key string,
	taskID model.TaskID,
	err error,
) (error, bool) {
	switch {
	case err == nil:
		return nil, false
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, context.Canceled.Error()), true
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, context.DeadlineExceeded.Error()), true
	case errors.Is(err, application.ErrIdempotencyConflict):
		server.logSubmissionError(key, taskID, "bind_submission", "conflict")
		return status.Error(codes.AlreadyExists, application.ErrIdempotencyConflict.Error()), true
	case errors.Is(err, application.ErrSubmissionUnavailable),
		errors.Is(err, storage.ErrUnavailable), errors.Is(err, storage.ErrClosed):
		server.logSubmissionError(key, taskID, "persist_submission", "unavailable")
		return status.Error(codes.Unavailable, submissionUnavailableMessage), true
	case errors.Is(err, application.ErrSubmissionBindingCorrupt),
		errors.Is(err, application.ErrSubmissionPersistence),
		errors.Is(err, application.ErrIdempotentSubmissionUnsupported),
		errors.Is(err, application.ErrPersistence):
		server.logSubmissionError(key, taskID, "persist_submission", "internal")
		return status.Error(codes.Internal, submissionPersistenceMessage), true
	default:
		return nil, false
	}
}

func (server *CoreServer) logSubmissionError(key string, taskID model.TaskID, operation, errorClass string) {
	if key == "" {
		server.logger.Printf("event=submit_task_error operation=%s error_class=%s task_id=%s", operation, errorClass, taskID)
		return
	}
	server.logger.Printf(
		"event=submit_task_error operation=%s error_class=%s task_id=%s key_hash_prefix=%s",
		operation, errorClass, taskID, idempotencyKeyHashPrefix(key),
	)
}

func (server *CoreServer) logOutcome(outcome application.Outcome, submitErr error) {
	if outcome.Execution == nil || len(outcome.Execution.StepResults) == 0 {
		server.logger.Printf(
			"task execution task_id=%s result=%s error=%q",
			outcome.TaskID,
			outcome.State,
			errorText(submitErr),
		)
		return
	}
	for _, result := range outcome.Execution.StepResults {
		server.logger.Printf(
			"task execution task_id=%s step_id=%s node_id=%s result=%s error=%q",
			outcome.TaskID,
			result.StepID,
			result.NodeID,
			result.Status,
			result.Error,
		)
	}
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func (server *CoreServer) GetTaskStatus(
	ctx context.Context,
	request *dtmv1.GetTaskStatusRequest,
) (*dtmv1.GetTaskStatusResponse, error) {
	if err := server.requireReady(); err != nil {
		return nil, err
	}
	if request == nil || strings.TrimSpace(request.GetTaskId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "task_id is required")
	}
	if server.async == nil {
		return nil, status.Error(codes.Unimplemented, ErrAsyncTaskServiceNotConfigured.Error())
	}
	taskID := model.TaskID(strings.TrimSpace(request.GetTaskId()))
	record, err := server.async.Find(ctx, taskID)
	switch {
	case errors.Is(err, context.Canceled):
		return nil, status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return nil, status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, application.ErrTaskNotFound):
		return nil, status.Error(codes.NotFound, err.Error())
	case err != nil:
		return nil, status.Errorf(codes.Internal, "query task: %v", err)
	}
	if record.Task.ID != taskID {
		return nil, status.Error(codes.Internal, "query returned an inconsistent task record")
	}
	return taskRecordToProto(record)
}

func taskRecordToProto(record application.TaskRecord) (*dtmv1.GetTaskStatusResponse, error) {
	taskStatus, err := lifecycleStateToProto(record.State)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode task status: %v", err)
	}
	results := make([]*dtmv1.StepResult, 0, len(record.Steps))
	for _, step := range record.Steps {
		if step.Status == "" {
			continue
		}
		result, err := execution.NewStepResult(
			step.ID,
			step.NodeID,
			step.Status,
			step.Output,
			step.Error,
		)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode step %q: %v", step.ID, err)
		}
		converted, err := stepResultToProto(result)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode step %q: %v", step.ID, err)
		}
		results = append(results, converted)
	}
	return &dtmv1.GetTaskStatusResponse{
		TaskId:  string(record.Task.ID),
		Status:  taskStatus,
		Error:   record.Error,
		Results: results,
	}, nil
}

func outcomeToProto(
	input task.Task,
	outcome application.Outcome,
	submitErr error,
) (*dtmv1.SubmitTaskResponse, error) {
	return submissionOutcomeToProto(input, outcome, submitErr, false)
}

func submissionOutcomeToProto(
	input task.Task,
	outcome application.Outcome,
	submitErr error,
	deduplicated bool,
) (*dtmv1.SubmitTaskResponse, error) {
	if !deduplicated && outcome.TaskID != input.ID {
		return nil, fmt.Errorf("outcome task ID %q does not match submitted task %q", outcome.TaskID, input.ID)
	}

	taskStatus, err := lifecycleStateToProto(outcome.State)
	if err != nil {
		return nil, err
	}
	if submitErr == nil && !deduplicated {
		if outcome.State != lifecycle.StateSucceeded ||
			outcome.Execution == nil ||
			outcome.Execution.Status != execution.StatusSucceeded {
			return nil, errors.New("successful submission returned an inconsistent outcome")
		}
	} else if submitErr != nil && outcome.State != lifecycle.StateFailed {
		return nil, errors.New("failed submission returned an inconsistent lifecycle state")
	}
	if outcome.Execution != nil && outcome.Execution.TaskID != outcome.TaskID {
		return nil, fmt.Errorf(
			"execution task ID %q does not match submitted task %q",
			outcome.Execution.TaskID,
			input.ID,
		)
	}
	if submitErr != nil && outcome.Execution != nil && outcome.Execution.Status != execution.StatusFailed {
		return nil, errors.New("failed submission returned a non-failed execution result")
	}

	var validatedExecution *execution.Result
	if outcome.Execution != nil {
		validated, err := execution.NewResult(
			outcome.Execution.TaskID,
			outcome.Execution.Status,
			outcome.Execution.StepResults,
			outcome.Execution.Error,
		)
		if err != nil {
			return nil, fmt.Errorf("invalid execution result: %w", err)
		}
		validatedExecution = &validated
	}

	results := make([]*dtmv1.StepResult, 0)
	if validatedExecution != nil {
		results = make([]*dtmv1.StepResult, 0, len(validatedExecution.StepResults))
		for _, result := range validatedExecution.StepResults {
			converted, err := stepResultToProto(result)
			if err != nil {
				return nil, err
			}
			results = append(results, converted)
		}
	}

	var message string
	if submitErr != nil {
		message = submitErr.Error()
	} else if deduplicated {
		message = outcome.Error
	}
	return &dtmv1.SubmitTaskResponse{
		TaskId:       string(outcome.TaskID),
		Status:       taskStatus,
		Error:        message,
		Results:      results,
		Deduplicated: deduplicated,
	}, nil
}

func lifecycleStateToProto(state lifecycle.State) (dtmv1.TaskStatus, error) {
	switch state {
	case lifecycle.StateCreated:
		return dtmv1.TaskStatus_TASK_STATUS_CREATED, nil
	case lifecycle.StatePlanning:
		return dtmv1.TaskStatus_TASK_STATUS_PLANNED, nil
	case lifecycle.StateMapped:
		return dtmv1.TaskStatus_TASK_STATUS_MAPPED, nil
	case lifecycle.StateDispatched:
		// V0.2 clients do not have a dispatched enum. MAPPED is the closest
		// backward-compatible transport projection.
		return dtmv1.TaskStatus_TASK_STATUS_MAPPED, nil
	case lifecycle.StateRunning, lifecycle.StateRetrying, lifecycle.StateRemapped:
		return dtmv1.TaskStatus_TASK_STATUS_RUNNING, nil
	case lifecycle.StateSuccess:
		return dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED, nil
	case lifecycle.StateFailed, lifecycle.StateCancelled:
		return dtmv1.TaskStatus_TASK_STATUS_FAILED, nil
	default:
		return dtmv1.TaskStatus_TASK_STATUS_UNSPECIFIED, fmt.Errorf("unknown lifecycle state %q", state)
	}
}

func taskFromProto(request *dtmv1.SubmitTaskRequest) (task.Task, error) {
	if request == nil || request.GetTask() == nil {
		return task.Task{}, fmt.Errorf("%w: task is required", task.ErrInvalidTask)
	}
	input := request.GetTask()

	requirements := make([]model.Capability, 0, len(input.GetRequirements()))
	for _, raw := range input.GetRequirements() {
		capability, err := model.NewCapability(strings.TrimSpace(raw))
		if err != nil {
			return task.Task{}, fmt.Errorf("%w: invalid requirement", task.ErrInvalidTask)
		}
		requirements = append(requirements, capability)
	}

	constraints := make(task.Constraints, len(input.GetConstraints()))
	for rawKey, rawValue := range input.GetConstraints() {
		key := strings.TrimSpace(rawKey)
		if key == "" {
			return task.Task{}, fmt.Errorf("%w: constraint key is required", task.ErrInvalidTask)
		}
		if _, duplicate := constraints[key]; duplicate {
			return task.Task{}, fmt.Errorf("%w: duplicate constraint %q", task.ErrInvalidTask, key)
		}
		constraints[key] = strings.TrimSpace(rawValue)
	}

	converted, err := task.New(
		model.TaskID(strings.TrimSpace(input.GetId())),
		strings.TrimSpace(input.GetIntent()),
		requirements,
		constraints,
	)
	if err != nil {
		return task.Task{}, err
	}
	return converted, nil
}

var _ TaskSubmitter = (*application.TaskService)(nil)
var _ AsyncTaskService = (*application.AsyncTaskService)(nil)
