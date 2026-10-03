package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	storageport "dtm/internal/storage"
)

const (
	restartInterruptedCode         = "CORE_RESTART_INTERRUPTED"
	recoveryNonIdempotentCode      = "RECOVERY_NON_IDEMPOTENT"
	recoveryIdempotencyUnknownCode = "RECOVERY_IDEMPOTENCY_UNKNOWN"
	recoveryRetryExhaustedCode     = "RECOVERY_RETRY_EXHAUSTED"
)

func (repository *Repository) ResolveInterruptedExecution(
	ctx context.Context,
	request storageport.ResolveInterruptedExecutionRequest,
) (resolution storageport.InterruptedExecutionResolution, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if strings.TrimSpace(string(request.ExecutionID)) == "" || request.ExpectedExecutionVersion < 1 ||
		request.ExpectedTaskVersion < 1 || request.ExpectedStepVersion < 1 {
		return resolution, invalidData("resolve interrupted execution %q fields or versions", request.ExecutionID)
	}
	resolvedAt, err := encodeTime("interrupted execution resolved_at", request.ResolvedAt)
	if err != nil {
		return resolution, err
	}
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return resolution, fmt.Errorf("begin resolve interrupted execution %q: %w", request.ExecutionID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("resolve interrupted execution %q", request.ExecutionID))

	currentExecution, err := getExecutionWithQueryer(ctx, transaction, request.ExecutionID)
	if err != nil {
		return resolution, err
	}
	if currentExecution.State != storageport.ExecutionStateStarted || currentExecution.Version != request.ExpectedExecutionVersion {
		return resolution, fmt.Errorf("%w: execution %q is no longer the expected started execution", storageport.ErrConflict, request.ExecutionID)
	}
	currentStep, err := getTaskStepWithQueryer(ctx, transaction, currentExecution.TaskID, currentExecution.StepID)
	if err != nil {
		return resolution, err
	}
	if currentStep.Version != request.ExpectedStepVersion || currentStep.State != lifecycle.StepStateRunning {
		return resolution, fmt.Errorf("%w: task step %q/%q is no longer the expected running step", storageport.ErrConflict, currentExecution.TaskID, currentExecution.StepID)
	}
	resolvedTime := request.ResolvedAt.UTC()
	if currentExecution.StartedAt != nil && resolvedTime.Before(*currentExecution.StartedAt) {
		resolvedTime = currentExecution.StartedAt.UTC()
	}
	if currentStep.StartedAt != nil && resolvedTime.Before(*currentStep.StartedAt) {
		resolvedTime = currentStep.StartedAt.UTC()
	}
	resolvedAt, err = encodeTime("interrupted execution resolved_at", resolvedTime)
	if err != nil {
		return resolution, err
	}
	var taskState lifecycle.State
	var taskVersion int64
	err = transaction.QueryRowContext(ctx, `SELECT status, version FROM tasks WHERE task_id = ?`, currentExecution.TaskID).Scan(&taskState, &taskVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return resolution, fmt.Errorf("%w: task %q", storageport.ErrNotFound, currentExecution.TaskID)
	}
	if err != nil {
		return resolution, fmt.Errorf("load interrupted task %q: %w", currentExecution.TaskID, err)
	}
	if taskVersion != request.ExpectedTaskVersion {
		return resolution, fmt.Errorf("%w: task %q is no longer the expected version", storageport.ErrConflict, currentExecution.TaskID)
	}
	mode := currentStep.IdempotencyMode
	if mode == "" {
		mode = model.IdempotencyUnspecified
	}
	retryAllowed := mode == model.IdempotencyIdempotent && currentExecution.AttemptNo < currentStep.MaxAttempts
	failureCode := recoveryIdempotencyUnknownCode
	if mode == model.IdempotencyNonIdempotent {
		failureCode = recoveryNonIdempotentCode
	} else if mode == model.IdempotencyIdempotent {
		failureCode = recoveryRetryExhaustedCode
	}
	message := fmt.Sprintf("core restart interrupted execution %q attempt %d", currentExecution.ID, currentExecution.AttemptNo)
	failedResult, err := execution.NewStepResult(currentExecution.StepID, currentExecution.NodeID, execution.StatusFailed, nil, message)
	if err != nil {
		return resolution, err
	}
	resultJSON, err := encodeJSON("interrupted execution result", failedResult)
	if err != nil {
		return resolution, err
	}

	executionUpdate, err := transaction.ExecContext(ctx, `
UPDATE executions
SET status = ?, result_json = ?, failure_code = ?, failure_message = ?,
    completed_at = ?, updated_at = ?, version = version + 1
WHERE execution_id = ? AND status = ? AND version = ?`,
		storageport.ExecutionStateFailed, resultJSON, restartInterruptedCode, message,
		resolvedAt, resolvedAt, currentExecution.ID, storageport.ExecutionStateStarted, currentExecution.Version)
	if err != nil {
		return resolution, fmt.Errorf("mark interrupted execution %q failed: %w", currentExecution.ID, err)
	}
	if err := classifyConditionalUpdate(ctx, transaction, executionUpdate, "execution", string(currentExecution.ID), "SELECT 1 FROM executions WHERE execution_id = ?", currentExecution.ID); err != nil {
		return resolution, err
	}

	newTaskState, newStepState := lifecycle.StateFailed, lifecycle.StepStateFailed
	stepResult := any(resultJSON)
	completedAt := any(resolvedAt)
	stepFailureCode, stepFailureMessage := failureCode, message
	if retryAllowed {
		newTaskState, newStepState = lifecycle.StateRetrying, lifecycle.StepStateRetrying
		stepResult, completedAt = nil, nil
		stepFailureCode, stepFailureMessage = "", ""
	}
	stepUpdate, err := transaction.ExecContext(ctx, `
UPDATE task_steps
SET state = ?, result_json = ?, failure_code = ?, failure_message = ?, completed_at = ?,
    updated_at = ?, version = version + 1
WHERE task_id = ? AND step_id = ? AND state = ? AND version = ?`,
		newStepState, stepResult, nullableString(stepFailureCode), nullableString(stepFailureMessage), completedAt,
		resolvedAt, currentExecution.TaskID, currentExecution.StepID, currentStep.State, currentStep.Version)
	if err != nil {
		return resolution, fmt.Errorf("classify interrupted step %q/%q: %w", currentExecution.TaskID, currentExecution.StepID, err)
	}
	if err := classifyConditionalUpdate(ctx, transaction, stepUpdate, "task step", fmt.Sprintf("%s/%s", currentExecution.TaskID, currentExecution.StepID), "SELECT 1 FROM task_steps WHERE task_id = ? AND step_id = ?", currentExecution.TaskID, currentExecution.StepID); err != nil {
		return resolution, err
	}
	taskFailureCode, taskFailureMessage := failureCode, message
	taskCompletedAt := any(resolvedAt)
	if retryAllowed {
		taskFailureCode, taskFailureMessage, taskCompletedAt = "", "", nil
	}
	taskUpdate, err := transaction.ExecContext(ctx, `
UPDATE tasks
SET status = ?, failure_code = ?, failure_message = ?, completed_at = ?, updated_at = ?, version = version + 1
WHERE task_id = ? AND status = ? AND version = ?`,
		newTaskState, nullableString(taskFailureCode), nullableString(taskFailureMessage), taskCompletedAt, resolvedAt,
		currentExecution.TaskID, taskState, taskVersion)
	if err != nil {
		return resolution, fmt.Errorf("classify interrupted task %q: %w", currentExecution.TaskID, err)
	}
	if err := classifyConditionalUpdate(ctx, transaction, taskUpdate, "task", string(currentExecution.TaskID), "SELECT 1 FROM tasks WHERE task_id = ?", currentExecution.TaskID); err != nil {
		return resolution, err
	}
	detail := map[string]any{"execution_id": currentExecution.ID, "attempt_no": currentExecution.AttemptNo, "idempotency_mode": mode, "retry_allowed": retryAllowed}
	stepID := currentExecution.StepID
	if err := insertTaskEvent(ctx, transaction, storageport.TaskEvent{TaskID: currentExecution.TaskID, StepID: &stepID, Type: "core_restart_execution_classified", FromState: string(currentStep.State), ToState: string(newStepState), Detail: detail, CreatedAt: resolvedTime}); err != nil {
		return resolution, err
	}
	if err := insertTaskEvent(ctx, transaction, storageport.TaskEvent{TaskID: currentExecution.TaskID, Type: "core_restart_task_classified", FromState: string(taskState), ToState: string(newTaskState), Detail: detail, CreatedAt: resolvedTime}); err != nil {
		return resolution, err
	}
	if err := transaction.Commit(); err != nil {
		return resolution, fmt.Errorf("commit interrupted execution %q classification: %w", currentExecution.ID, err)
	}
	return storageport.InterruptedExecutionResolution{
		RetryAllowed: retryAllowed, IdempotencyMode: mode, FailureCode: failureCode,
		ExecutionVersion: currentExecution.Version + 1, StepVersion: currentStep.Version + 1, TaskVersion: taskVersion + 1,
	}, nil
}

func (repository *Repository) ReplaceTaskForRecovery(
	ctx context.Context,
	record storageport.Task,
) (returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if err := validateTaskRecord(record); err != nil {
		return err
	}
	updatedAt, _ := encodeTime("task recovery updated_at", record.UpdatedAt)
	startedAt, err := encodeOptionalTime("task recovery started_at", record.StartedAt)
	if err != nil {
		return err
	}
	completedAt, err := encodeOptionalTime("task recovery completed_at", record.CompletedAt)
	if err != nil {
		return err
	}
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin replace recovered task %q: %w", record.ID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("replace recovered task %q", record.ID))
	result, err := transaction.ExecContext(ctx, `
UPDATE tasks
SET status = ?, failure_code = ?, failure_message = ?, updated_at = ?,
    started_at = ?, completed_at = ?, version = version + 1
WHERE task_id = ? AND version = ?`,
		record.State, nullableString(record.FailureCode), nullableString(record.FailureMessage),
		updatedAt, startedAt, completedAt, record.ID, record.Version,
	)
	if err != nil {
		return fmt.Errorf("replace recovered task %q: %w", record.ID, err)
	}
	if err := classifyConditionalUpdate(
		ctx, transaction, result, "task", string(record.ID),
		"SELECT 1 FROM tasks WHERE task_id = ?", record.ID,
	); err != nil {
		return err
	}
	for _, step := range record.Steps {
		if err := replaceRecoveredStep(ctx, transaction, step); err != nil {
			return err
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit replace recovered task %q: %w", record.ID, err)
	}
	return nil
}

func replaceRecoveredStep(ctx context.Context, transaction sqlExecutor, step storageport.TaskStep) error {
	var assignedNodeID any
	if step.AssignedNodeID != nil {
		assignedNodeID = *step.AssignedNodeID
	}
	var resultJSON any
	if step.Result != nil {
		encoded, err := encodeJSON("recovered step result", step.Result)
		if err != nil {
			return err
		}
		resultJSON = encoded
	}
	updatedAt, _ := encodeTime("recovered step updated_at", step.UpdatedAt)
	startedAt, err := encodeOptionalTime("recovered step started_at", step.StartedAt)
	if err != nil {
		return err
	}
	completedAt, err := encodeOptionalTime("recovered step completed_at", step.CompletedAt)
	if err != nil {
		return err
	}
	result, err := transaction.ExecContext(ctx, `
UPDATE task_steps
SET state = ?, assigned_node_id = ?, attempt_count = ?,
    failure_code = ?, failure_message = ?, result_json = ?,
    updated_at = ?, started_at = ?, completed_at = ?, version = version + 1
WHERE task_id = ? AND step_id = ? AND version = ?`,
		step.State, assignedNodeID, step.AttemptCount,
		nullableString(step.FailureCode), nullableString(step.FailureMessage), resultJSON,
		updatedAt, startedAt, completedAt, step.TaskID, step.ID, step.Version,
	)
	if err != nil {
		return fmt.Errorf("replace recovered task %q step %q: %w", step.TaskID, step.ID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect recovered task %q step %q: %w", step.TaskID, step.ID, err)
	}
	if affected == 0 {
		return fmt.Errorf("%w: recovered task step %q/%q", storageport.ErrConflict, step.TaskID, step.ID)
	}
	return nil
}

func (repository *Repository) FindRecoverableTasks(
	ctx context.Context,
	limit int,
) ([]storageport.Task, error) {
	return repository.FindRecoverableTasksAfter(ctx, limit, nil, time.Time{})
}

func (repository *Repository) FindRecoverableTasksAfter(
	ctx context.Context,
	limit int,
	after *storageport.RecoveryCursor,
	createdBefore time.Time,
) (records []storageport.Task, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if limit < 1 {
		return nil, invalidData("find recoverable tasks limit")
	}
	query := `
SELECT task_id
FROM tasks
WHERE status IN (?, ?, ?, ?, ?, ?, ?)`
	arguments := []any{
		lifecycle.StateCreated,
		lifecycle.StatePlanning,
		lifecycle.StateMapped,
		lifecycle.StateDispatched,
		lifecycle.StateRunning,
		lifecycle.StateRetrying,
		lifecycle.StateRemapped,
	}
	if !createdBefore.IsZero() {
		encoded, err := encodeTime("recoverable task creation upper bound", createdBefore)
		if err != nil {
			return nil, err
		}
		query += ` AND created_at <= ?`
		arguments = append(arguments, encoded)
	}
	if after != nil {
		if after.UpdatedAt.IsZero() || strings.TrimSpace(string(after.TaskID)) == "" {
			return nil, invalidData("find recoverable tasks cursor")
		}
		updatedAt, err := encodeTime("recoverable task cursor updated_at", after.UpdatedAt)
		if err != nil {
			return nil, err
		}
		query += ` AND (updated_at > ? OR (updated_at = ? AND task_id > ?))`
		arguments = append(arguments, updatedAt, updatedAt, after.TaskID)
	}
	query += `
ORDER BY updated_at, task_id
LIMIT ?`
	arguments = append(arguments, limit)
	rows, err := repository.db.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("find recoverable task IDs: %w", err)
	}
	defer rows.Close()
	var taskIDs []model.TaskID
	for rows.Next() {
		var taskID model.TaskID
		if err := rows.Scan(&taskID); err != nil {
			return nil, fmt.Errorf("scan recoverable task ID: %w", err)
		}
		taskIDs = append(taskIDs, taskID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read recoverable task IDs: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close recoverable task IDs: %w", err)
	}
	records = make([]storageport.Task, 0, len(taskIDs))
	for _, taskID := range taskIDs {
		record, err := repository.GetTask(ctx, taskID)
		if err != nil {
			return nil, fmt.Errorf("load recoverable task %q: %w", taskID, err)
		}
		records = append(records, record)
	}
	return records, nil
}

var _ storageport.Repository = (*Repository)(nil)
