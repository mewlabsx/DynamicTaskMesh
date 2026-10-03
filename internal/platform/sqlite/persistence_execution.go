package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	storageport "dtm/internal/storage"
)

func (repository *Repository) StartExecution(
	ctx context.Context,
	request storageport.StartExecutionRequest,
) (record storageport.Execution, newStepVersion int64, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if strings.TrimSpace(string(request.ExecutionID)) == "" || request.AttemptNo < 1 ||
		strings.TrimSpace(string(request.NodeID)) == "" || request.Request == nil ||
		request.ExpectedStepVersion < 1 {
		return storageport.Execution{}, 0, invalidData("start execution %q fields", request.ExecutionID)
	}
	if err := validateStepTransition(request.StepID, request.ExpectedStepState, lifecycle.StepStateRunning); err != nil {
		return storageport.Execution{}, 0, err
	}
	if err := validateStepStateEvent(request.TaskID, request.StepID, request.ExpectedStepState, lifecycle.StepStateRunning, request.Event); err != nil {
		return storageport.Execution{}, 0, err
	}
	startedAt, err := encodeTime("execution started_at", request.StartedAt)
	if err != nil {
		return storageport.Execution{}, 0, err
	}
	requestJSON, err := encodeJSON("execution request", request.Request)
	if err != nil {
		return storageport.Execution{}, 0, err
	}
	started := request.StartedAt.UTC()
	record = storageport.Execution{
		ID: request.ExecutionID, RequestID: string(request.ExecutionID), TaskID: request.TaskID, StepID: request.StepID,
		AttemptNo: request.AttemptNo, NodeID: request.NodeID,
		State: storageport.ExecutionStateStarted, Request: request.Request,
		StartedAt: &started, CreatedAt: started, UpdatedAt: started, Version: 1,
	}
	if err := validateExecutionRecord(record); err != nil {
		return storageport.Execution{}, 0, err
	}

	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return storageport.Execution{}, 0, fmt.Errorf("begin start execution %q: %w", request.ExecutionID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("start execution %q", request.ExecutionID))
	currentStep, err := getTaskStepWithQueryer(ctx, transaction, request.TaskID, request.StepID)
	if err != nil {
		return storageport.Execution{}, 0, err
	}
	if currentStep.State != request.ExpectedStepState || currentStep.Version != request.ExpectedStepVersion {
		return storageport.Execution{}, 0, fmt.Errorf("%w: task step %q/%q expected state or version mismatch", storageport.ErrConflict, request.TaskID, request.StepID)
	}
	var active int
	err = transaction.QueryRowContext(ctx, `SELECT 1 FROM executions WHERE task_id = ? AND step_id = ? AND status = ? LIMIT 1`, request.TaskID, request.StepID, storageport.ExecutionStateStarted).Scan(&active)
	if err == nil {
		return storageport.Execution{}, 0, fmt.Errorf("%w: task step %q/%q already has a started execution", storageport.ErrConflict, request.TaskID, request.StepID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return storageport.Execution{}, 0, fmt.Errorf("inspect task step %q/%q active execution: %w", request.TaskID, request.StepID, err)
	}
	var duplicate int
	err = transaction.QueryRowContext(ctx, `SELECT 1 FROM executions WHERE execution_id = ? OR (task_id = ? AND step_id = ? AND attempt_no = ?) LIMIT 1`, request.ExecutionID, request.TaskID, request.StepID, request.AttemptNo).Scan(&duplicate)
	if err == nil {
		return storageport.Execution{}, 0, fmt.Errorf("%w: execution %q or attempt %d for task step %q/%q", storageport.ErrAlreadyExists, request.ExecutionID, request.AttemptNo, request.TaskID, request.StepID)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return storageport.Execution{}, 0, fmt.Errorf("inspect execution %q duplicate: %w", request.ExecutionID, err)
	}
	nextAttempt := currentStep.AttemptCount + 1
	if request.AttemptNo != nextAttempt {
		return storageport.Execution{}, 0, invalidData("execution %q attempt_no %d, want %d for task step %q/%q", request.ExecutionID, request.AttemptNo, nextAttempt, request.TaskID, request.StepID)
	}
	if nextAttempt > currentStep.MaxAttempts {
		return storageport.Execution{}, 0, invalidData("execution %q attempt_no %d exceeds max_attempts %d for task step %q/%q", request.ExecutionID, nextAttempt, currentStep.MaxAttempts, request.TaskID, request.StepID)
	}
	if currentStep.AssignedNodeID == nil || *currentStep.AssignedNodeID != request.NodeID {
		return storageport.Execution{}, 0, invalidData("execution %q node %q does not match task step %q/%q assignment", request.ExecutionID, request.NodeID, request.TaskID, request.StepID)
	}
	stepCandidate := currentStep
	stepCandidate.State, stepCandidate.AssignedNodeID = lifecycle.StepStateRunning, &request.NodeID
	stepCandidate.AttemptCount, stepCandidate.Result = nextAttempt, nil
	stepCandidate.FailureCode, stepCandidate.FailureMessage, stepCandidate.CompletedAt = "", "", nil
	stepCandidate.UpdatedAt, stepCandidate.Version = started, currentStep.Version+1
	if stepCandidate.StartedAt == nil {
		stepCandidate.StartedAt = &started
	}
	if err := validateTaskStep(request.TaskID, stepCandidate); err != nil {
		return storageport.Execution{}, 0, err
	}

	_, err = transaction.ExecContext(ctx, `
INSERT INTO executions (
	execution_id, task_id, step_id, attempt_no, node_id, status,
	request_json, result_json, failure_code, failure_message,
	started_at, completed_at, created_at, updated_at, version
) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL, ?, NULL, ?, ?, 1)`,
		record.ID, record.TaskID, record.StepID, record.AttemptNo, record.NodeID,
		record.State, requestJSON, startedAt, startedAt, startedAt)
	if err != nil {
		return storageport.Execution{}, 0, classifyCreateError("execution", string(record.ID), err)
	}
	result, err := transaction.ExecContext(ctx, `
UPDATE task_steps
SET state = ?, assigned_node_id = ?, attempt_count = ?, result_json = NULL,
    failure_code = NULL, failure_message = NULL, completed_at = NULL,
    started_at = COALESCE(started_at, ?), updated_at = ?, version = version + 1
WHERE task_id = ? AND step_id = ? AND state = ? AND version = ?`,
		lifecycle.StepStateRunning, request.NodeID, nextAttempt, startedAt, startedAt,
		request.TaskID, request.StepID, request.ExpectedStepState, request.ExpectedStepVersion)
	if err != nil {
		return storageport.Execution{}, 0, fmt.Errorf("start execution %q step update: %w", request.ExecutionID, err)
	}
	if err := classifyConditionalUpdate(ctx, transaction, result, "task step", fmt.Sprintf("%s/%s", request.TaskID, request.StepID), "SELECT 1 FROM task_steps WHERE task_id = ? AND step_id = ?", request.TaskID, request.StepID); err != nil {
		return storageport.Execution{}, 0, err
	}
	if err := insertTaskEvent(ctx, transaction, request.Event); err != nil {
		return storageport.Execution{}, 0, err
	}
	if err := transaction.Commit(); err != nil {
		return storageport.Execution{}, 0, fmt.Errorf("commit start execution %q: %w", request.ExecutionID, err)
	}
	return record, request.ExpectedStepVersion + 1, nil
}

func (repository *Repository) CompleteExecution(
	ctx context.Context,
	request storageport.CompleteExecutionRequest,
) (newExecutionVersion int64, newStepVersion int64, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if strings.TrimSpace(string(request.ExecutionID)) == "" || request.ExpectedExecutionVersion < 1 || request.ExpectedStepVersion < 1 {
		return 0, 0, invalidData("complete execution %q fields or versions", request.ExecutionID)
	}
	if err := validateExecutionTransition(request.ExpectedExecutionState, request.NewExecutionState); err != nil {
		return 0, 0, err
	}
	if err := validateCompletionPair(request.NewExecutionState, request.NewStepState, request.Result, request.FailureCode, request.FailureMessage, request.CompletedAt); err != nil {
		return 0, 0, err
	}
	completedAt, err := encodeTime("execution completed_at", request.CompletedAt)
	if err != nil {
		return 0, 0, err
	}
	var resultJSON any
	if request.Result != nil {
		encoded, err := encodeJSON("execution result", request.Result)
		if err != nil {
			return 0, 0, err
		}
		resultJSON = encoded
	}

	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("begin complete execution %q: %w", request.ExecutionID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("complete execution %q", request.ExecutionID))
	currentExecution, err := getExecutionWithQueryer(ctx, transaction, request.ExecutionID)
	if err != nil {
		return 0, 0, err
	}
	if currentExecution.State != request.ExpectedExecutionState || currentExecution.Version != request.ExpectedExecutionVersion {
		return 0, 0, fmt.Errorf("%w: execution %q expected state or version mismatch", storageport.ErrConflict, request.ExecutionID)
	}
	currentStep, err := getTaskStepWithQueryer(ctx, transaction, currentExecution.TaskID, currentExecution.StepID)
	if err != nil {
		return 0, 0, err
	}
	if currentStep.State != request.ExpectedStepState || currentStep.Version != request.ExpectedStepVersion {
		return 0, 0, fmt.Errorf("%w: task step %q/%q expected state or version mismatch", storageport.ErrConflict, currentExecution.TaskID, currentExecution.StepID)
	}
	if err := validateStepTransition(currentExecution.StepID, request.ExpectedStepState, request.NewStepState); err != nil {
		return 0, 0, err
	}
	if err := validateStepStateEvent(currentExecution.TaskID, currentExecution.StepID, request.ExpectedStepState, request.NewStepState, request.Event); err != nil {
		return 0, 0, err
	}
	if currentStep.AssignedNodeID == nil || *currentStep.AssignedNodeID != currentExecution.NodeID {
		return 0, 0, invalidData("execution %q node %q does not match task step %q/%q assignment", request.ExecutionID, currentExecution.NodeID, currentExecution.TaskID, currentExecution.StepID)
	}
	completed := request.CompletedAt.UTC()
	executionCandidate := currentExecution
	executionCandidate.State, executionCandidate.Result = request.NewExecutionState, request.Result
	executionCandidate.FailureCode, executionCandidate.FailureMessage = request.FailureCode, request.FailureMessage
	executionCandidate.CompletedAt, executionCandidate.UpdatedAt = &completed, completed
	executionCandidate.Version = request.ExpectedExecutionVersion + 1
	if err := validateExecutionRecord(executionCandidate); err != nil {
		return 0, 0, err
	}
	stepCandidate := currentStep
	stepCandidate.State, stepCandidate.Result = request.NewStepState, request.Result
	stepCandidate.FailureCode, stepCandidate.FailureMessage = request.FailureCode, request.FailureMessage
	stepCandidate.CompletedAt, stepCandidate.UpdatedAt = &completed, completed
	stepCandidate.Version = request.ExpectedStepVersion + 1
	if err := validateTaskStep(currentExecution.TaskID, stepCandidate); err != nil {
		return 0, 0, err
	}
	if request.Result != nil && (request.Result.StepID != currentExecution.StepID || request.Result.NodeID != currentExecution.NodeID) {
		return 0, 0, invalidData("execution %q result identity does not match execution and step", request.ExecutionID)
	}

	executionUpdate, err := transaction.ExecContext(ctx, `
UPDATE executions
SET status = ?, result_json = ?, failure_code = ?, failure_message = ?,
    completed_at = ?, updated_at = ?, version = version + 1
WHERE execution_id = ? AND status = ? AND version = ?`,
		request.NewExecutionState, resultJSON, nullableString(request.FailureCode), nullableString(request.FailureMessage),
		completedAt, completedAt, request.ExecutionID, request.ExpectedExecutionState, request.ExpectedExecutionVersion)
	if err != nil {
		return 0, 0, fmt.Errorf("complete execution %q: %w", request.ExecutionID, err)
	}
	if err := classifyConditionalUpdate(ctx, transaction, executionUpdate, "execution", string(request.ExecutionID), "SELECT 1 FROM executions WHERE execution_id = ?", request.ExecutionID); err != nil {
		return 0, 0, err
	}
	stepUpdate, err := transaction.ExecContext(ctx, `
UPDATE task_steps
SET state = ?, result_json = ?, failure_code = ?, failure_message = ?,
    completed_at = ?, updated_at = ?, version = version + 1
WHERE task_id = ? AND step_id = ? AND state = ? AND version = ?`,
		request.NewStepState, resultJSON, nullableString(request.FailureCode), nullableString(request.FailureMessage),
		completedAt, completedAt, currentExecution.TaskID, currentExecution.StepID, request.ExpectedStepState, request.ExpectedStepVersion)
	if err != nil {
		return 0, 0, fmt.Errorf("complete execution %q step update: %w", request.ExecutionID, err)
	}
	if err := classifyConditionalUpdate(ctx, transaction, stepUpdate, "task step", fmt.Sprintf("%s/%s", currentExecution.TaskID, currentExecution.StepID), "SELECT 1 FROM task_steps WHERE task_id = ? AND step_id = ?", currentExecution.TaskID, currentExecution.StepID); err != nil {
		return 0, 0, err
	}
	if err := insertTaskEvent(ctx, transaction, request.Event); err != nil {
		return 0, 0, err
	}
	if err := transaction.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit complete execution %q: %w", request.ExecutionID, err)
	}
	return request.ExpectedExecutionVersion + 1, request.ExpectedStepVersion + 1, nil
}

func (repository *Repository) GetExecution(
	ctx context.Context,
	executionID model.ExecutionID,
) (record storageport.Execution, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	return getExecutionWithQueryer(ctx, repository.db, executionID)
}

type rowQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getExecutionWithQueryer(
	ctx context.Context,
	queryer rowQueryer,
	executionID model.ExecutionID,
) (storageport.Execution, error) {
	if strings.TrimSpace(string(executionID)) == "" {
		return storageport.Execution{}, invalidData("execution ID is required")
	}
	row := queryer.QueryRowContext(ctx, `
SELECT execution_id, task_id, step_id, attempt_no, node_id, status,
       request_json, result_json, failure_code, failure_message,
       started_at, completed_at, created_at, updated_at, version
FROM executions
WHERE execution_id = ?`, executionID)
	record, err := scanExecution(row)
	if errors.Is(err, sql.ErrNoRows) {
		return storageport.Execution{}, fmt.Errorf("%w: execution %q", storageport.ErrNotFound, executionID)
	}
	if err != nil {
		return storageport.Execution{}, fmt.Errorf("get execution %q: %w", executionID, err)
	}
	return record, nil
}

func scanExecution(scanner rowScanner) (storageport.Execution, error) {
	var record storageport.Execution
	var state, requestJSON string
	var resultJSON, failureCode, failureMessage sql.NullString
	var startedAt, completedAt, createdAt, updatedAt any
	err := scanner.Scan(
		&record.ID, &record.TaskID, &record.StepID, &record.AttemptNo, &record.NodeID,
		&state, &requestJSON, &resultJSON, &failureCode, &failureMessage,
		&startedAt, &completedAt, &createdAt, &updatedAt, &record.Version,
	)
	if err != nil {
		return storageport.Execution{}, err
	}
	record.State = storageport.ExecutionState(state)
	record.RequestID = string(record.ID)
	record.FailureCode = failureCode.String
	record.FailureMessage = failureMessage.String
	if err := decodeJSON("execution request", requestJSON, &record.Request); err != nil {
		return storageport.Execution{}, err
	}
	if resultJSON.Valid {
		var result execution.StepResult
		if err := decodeJSON("execution result", resultJSON.String, &result); err != nil {
			return storageport.Execution{}, err
		}
		record.Result = &result
	}
	record.StartedAt, err = decodeOptionalTime("execution started_at", startedAt)
	if err != nil {
		return storageport.Execution{}, err
	}
	record.CompletedAt, err = decodeOptionalTime("execution completed_at", completedAt)
	if err != nil {
		return storageport.Execution{}, err
	}
	record.CreatedAt, err = decodeTime("execution created_at", createdAt)
	if err != nil {
		return storageport.Execution{}, err
	}
	record.UpdatedAt, err = decodeTime("execution updated_at", updatedAt)
	if err != nil {
		return storageport.Execution{}, err
	}
	if err := validateExecutionRecord(record); err != nil {
		return storageport.Execution{}, err
	}
	return record, nil
}

func (repository *Repository) GetExecutions(
	ctx context.Context,
	taskID model.TaskID,
	stepID model.StepID,
) (records []storageport.Execution, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if strings.TrimSpace(string(taskID)) == "" || strings.TrimSpace(string(stepID)) == "" {
		return nil, invalidData("execution task and step IDs are required")
	}
	rows, err := repository.db.QueryContext(ctx, `
SELECT execution_id
FROM executions
WHERE task_id = ? AND step_id = ?
ORDER BY attempt_no, execution_id`, taskID, stepID)
	if err != nil {
		return nil, fmt.Errorf("list task %q step %q executions: %w", taskID, stepID, err)
	}
	defer rows.Close()
	var executionIDs []model.ExecutionID
	for rows.Next() {
		var executionID model.ExecutionID
		if err := rows.Scan(&executionID); err != nil {
			return nil, fmt.Errorf("scan task %q step %q execution: %w", taskID, stepID, err)
		}
		executionIDs = append(executionIDs, executionID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read task %q step %q executions: %w", taskID, stepID, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close task %q step %q executions: %w", taskID, stepID, err)
	}
	records = make([]storageport.Execution, 0, len(executionIDs))
	for _, executionID := range executionIDs {
		record, err := repository.GetExecution(ctx, executionID)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}
