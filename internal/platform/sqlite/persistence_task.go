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

type Repository struct {
	database                  *Database
	db                        *sql.DB
	resourceRegistrationFault resourceRegistrationFaultInjector
	resourceCommit            func(*sql.Tx) error
}

func Open(path string) (*Repository, error) {
	return OpenWithOptions(Options{Path: path, AutoMigrate: true})
}

func OpenWithOptions(options Options) (*Repository, error) {
	options.MigrationSet = "core"
	database, err := openDatabase(context.Background(), options)
	if err != nil {
		return nil, err
	}
	db, err := database.SQLDB()
	if err != nil {
		_ = database.Close()
		return nil, err
	}
	return &Repository{database: database, db: db}, nil
}

func (repository *Repository) Close() error {
	if repository == nil || repository.database == nil {
		return nil
	}
	return repository.database.Close()
}

func (repository *Repository) Health(ctx context.Context) error {
	if repository == nil || repository.database == nil {
		return ErrDatabaseClosed
	}
	return repository.database.Health(ctx)
}

func (repository *Repository) StorageStatus() Status {
	if repository == nil || repository.database == nil {
		return Status{}
	}
	return repository.database.Status()
}

func (repository *Repository) CreateTask(ctx context.Context, record storageport.Task) (returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if err := validateTaskRecord(record); err != nil {
		return err
	}
	requirementsJSON, err := encodeJSON("task requirements", record.Requirements)
	if err != nil {
		return err
	}
	constraintsJSON, err := encodeJSON("task constraints", record.Constraints)
	if err != nil {
		return err
	}
	createdAt, _ := encodeTime("task created_at", record.CreatedAt)
	updatedAt, _ := encodeTime("task updated_at", record.UpdatedAt)
	startedAt, err := encodeOptionalTime("task started_at", record.StartedAt)
	if err != nil {
		return err
	}
	completedAt, err := encodeOptionalTime("task completed_at", record.CompletedAt)
	if err != nil {
		return err
	}

	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin create task %q: %w", record.ID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("create task %q", record.ID))
	_, err = transaction.ExecContext(ctx, `
INSERT INTO tasks (
	task_id, intent, requirements_json, constraints_json, status,
	failure_code, failure_message, created_at, updated_at,
	started_at, completed_at, version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID, record.Intent, requirementsJSON, constraintsJSON, record.State,
		nullableString(record.FailureCode), nullableString(record.FailureMessage),
		createdAt, updatedAt, startedAt, completedAt, record.Version,
	)
	if err != nil {
		return classifyCreateError("task", string(record.ID), err)
	}
	for index := range record.Steps {
		if err := insertTaskStep(ctx, transaction, record.Steps[index]); err != nil {
			return err
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit create task %q: %w", record.ID, err)
	}
	return nil
}

func (repository *Repository) CreateTaskSubmission(
	ctx context.Context,
	request storageport.CreateTaskSubmissionRequest,
) (result storageport.CreateTaskSubmissionResult, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if repository == nil || repository.database == nil {
		return result, storageport.ErrClosed
	}
	if _, err := repository.database.SQLDB(); err != nil {
		return result, err
	}
	record := request.Task
	if err := validateTaskRecord(record); err != nil {
		return result, err
	}
	if err := validateEventForAppend(request.Event); err != nil {
		return result, err
	}
	if request.Event.TaskID != record.ID || request.Event.ToState != string(lifecycle.StateCreated) {
		return result, invalidData("task %q acceptance event does not match created task", record.ID)
	}
	if (request.IdempotencyKey == "") != (request.RequestFingerprint == "") {
		return result, fmt.Errorf("%w: idempotency key and request fingerprint must be provided together", storageport.ErrInvalidArgument)
	}
	requirementsJSON, err := encodeJSON("task requirements", record.Requirements)
	if err != nil {
		return result, err
	}
	constraintsJSON, err := encodeJSON("task constraints", record.Constraints)
	if err != nil {
		return result, err
	}
	createdAt, _ := encodeTime("task created_at", record.CreatedAt)
	updatedAt, _ := encodeTime("task updated_at", record.UpdatedAt)
	startedAt, err := encodeOptionalTime("task started_at", record.StartedAt)
	if err != nil {
		return result, err
	}
	completedAt, err := encodeOptionalTime("task completed_at", record.CompletedAt)
	if err != nil {
		return result, err
	}

	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return result, classifySubmissionStorageError(ctx, fmt.Errorf("begin create task submission: %w", err))
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("create task submission %q", record.ID))
	_, err = transaction.ExecContext(ctx, `
INSERT INTO tasks (
	task_id, intent, requirements_json, constraints_json, status,
	failure_code, failure_message, created_at, updated_at,
	started_at, completed_at, version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID, record.Intent, requirementsJSON, constraintsJSON, record.State,
		nullableString(record.FailureCode), nullableString(record.FailureMessage),
		createdAt, updatedAt, startedAt, completedAt, record.Version,
	)
	if err != nil {
		classified := classifyCreateError("task", string(record.ID), err)
		_ = transaction.Rollback()
		if request.IdempotencyKey != "" && errors.Is(classified, storageport.ErrAlreadyExists) {
			return repository.resolveTaskSubmission(ctx, request, classified)
		}
		return result, classified
	}
	for index := range record.Steps {
		if err := insertTaskStep(ctx, transaction, record.Steps[index]); err != nil {
			return result, err
		}
	}
	if err := insertTaskEvent(ctx, transaction, request.Event); err != nil {
		return result, err
	}
	if request.IdempotencyKey != "" {
		_, err = transaction.ExecContext(ctx, `
INSERT INTO task_submission_keys (
	idempotency_key, request_fingerprint, task_id, created_at
) VALUES (?, ?, ?, ?)`, request.IdempotencyKey, request.RequestFingerprint, record.ID, createdAt)
		if err != nil {
			classified := classifyCreateError("task submission key", "binding", err)
			_ = transaction.Rollback()
			if errors.Is(classified, storageport.ErrAlreadyExists) {
				return repository.resolveTaskSubmission(ctx, request, classified)
			}
			return result, classified
		}
	}
	if err := transaction.Commit(); err != nil {
		return result, classifySubmissionStorageError(ctx, fmt.Errorf("commit create task submission: %w", err))
	}
	return storageport.CreateTaskSubmissionResult{Task: record, Created: true}, nil
}

func (repository *Repository) resolveTaskSubmission(
	ctx context.Context,
	request storageport.CreateTaskSubmissionRequest,
	cause error,
) (storageport.CreateTaskSubmissionResult, error) {
	var fingerprint string
	var taskID model.TaskID
	err := repository.db.QueryRowContext(ctx, `
SELECT request_fingerprint, task_id
FROM task_submission_keys
WHERE idempotency_key = ?`, request.IdempotencyKey).Scan(&fingerprint, &taskID)
	if errors.Is(err, sql.ErrNoRows) {
		return storageport.CreateTaskSubmissionResult{}, cause
	}
	if err != nil {
		return storageport.CreateTaskSubmissionResult{}, classifySubmissionStorageError(ctx, fmt.Errorf("resolve task submission binding: %w", err))
	}
	if fingerprint != request.RequestFingerprint {
		return storageport.CreateTaskSubmissionResult{}, storageport.ErrIdempotencyConflict
	}
	record, err := repository.GetTask(ctx, taskID)
	if err != nil {
		if errors.Is(err, storageport.ErrNotFound) || errors.Is(err, storageport.ErrInvalidData) {
			return storageport.CreateTaskSubmissionResult{}, storageport.ErrSubmissionBindingCorrupt
		}
		return storageport.CreateTaskSubmissionResult{}, classifySubmissionStorageError(ctx, fmt.Errorf("load idempotent task binding: %w", err))
	}
	return storageport.CreateTaskSubmissionResult{Task: record, Deduplicated: true}, nil
}

func classifySubmissionStorageError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, storageport.ErrClosed) || errors.Is(err, storageport.ErrUnavailable) {
		return err
	}
	code, ok := sqliteErrorCode(err)
	if ok && isSQLiteTransientQueryCode(code) {
		return fmt.Errorf("%w: submission storage contention", storageport.ErrUnavailable)
	}
	return err
}

func (repository *Repository) RecordTaskPlan(
	ctx context.Context,
	taskID model.TaskID,
	expectedState lifecycle.State,
	expectedVersion int64,
	steps []storageport.TaskStep,
	event storageport.TaskEvent,
) (newVersion int64, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if expectedVersion < 1 || len(steps) == 0 {
		return 0, invalidData("task %q plan version or steps", taskID)
	}
	if err := validateTaskTransition(taskID, expectedState, lifecycle.StatePlanning); err != nil {
		return 0, err
	}
	if err := validateTaskStateEvent(taskID, expectedState, lifecycle.StatePlanning, event); err != nil {
		return 0, err
	}
	for index := range steps {
		if err := validateTaskStep(taskID, steps[index]); err != nil {
			return 0, err
		}
	}
	updatedAt, _ := encodeTime("task plan created_at", event.CreatedAt)
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin record task %q plan: %w", taskID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("record task %q plan", taskID))
	result, err := transaction.ExecContext(ctx, `
UPDATE tasks
SET status = ?, updated_at = ?, version = version + 1
WHERE task_id = ? AND status = ? AND version = ?`,
		lifecycle.StatePlanning, updatedAt, taskID, expectedState, expectedVersion,
	)
	if err != nil {
		return 0, fmt.Errorf("record task %q plan: %w", taskID, err)
	}
	if err := classifyConditionalUpdate(
		ctx, transaction, result, "task", string(taskID),
		"SELECT 1 FROM tasks WHERE task_id = ?", taskID,
	); err != nil {
		return 0, err
	}
	for index := range steps {
		if err := insertTaskStep(ctx, transaction, steps[index]); err != nil {
			return 0, err
		}
	}
	if err := insertTaskEvent(ctx, transaction, event); err != nil {
		return 0, err
	}
	if err := transaction.Commit(); err != nil {
		return 0, fmt.Errorf("commit task %q plan: %w", taskID, err)
	}
	return expectedVersion + 1, nil
}

func insertTaskStep(ctx context.Context, transaction *sql.Tx, step storageport.TaskStep) error {
	inputJSON, err := encodeJSON("step input", step.Input)
	if err != nil {
		return err
	}
	var resultJSON any
	if step.Result != nil {
		encoded, err := encodeJSON("step result", step.Result)
		if err != nil {
			return err
		}
		resultJSON = encoded
	}
	createdAt, _ := encodeTime("step created_at", step.CreatedAt)
	updatedAt, _ := encodeTime("step updated_at", step.UpdatedAt)
	startedAt, err := encodeOptionalTime("step started_at", step.StartedAt)
	if err != nil {
		return err
	}
	completedAt, err := encodeOptionalTime("step completed_at", step.CompletedAt)
	if err != nil {
		return err
	}
	var assignedNodeID any
	if step.AssignedNodeID != nil {
		assignedNodeID = *step.AssignedNodeID
	}
	idempotencyMode := step.IdempotencyMode
	if idempotencyMode == "" {
		idempotencyMode = model.IdempotencyUnspecified
	}
	_, err = transaction.ExecContext(ctx, `
INSERT INTO task_steps (
	task_id, step_id, sequence_no, capability, idempotency_mode, input_json, state,
	assigned_node_id, attempt_count, max_attempts, failure_code,
	failure_message, result_json, created_at, updated_at, started_at,
	completed_at, version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		step.TaskID, step.ID, step.Sequence, step.Capability, idempotencyMode, inputJSON, step.State,
		assignedNodeID, step.AttemptCount, step.MaxAttempts,
		nullableString(step.FailureCode), nullableString(step.FailureMessage), resultJSON,
		createdAt, updatedAt, startedAt, completedAt, step.Version,
	)
	if err != nil {
		return classifyCreateError("task step", fmt.Sprintf("%s/%s", step.TaskID, step.ID), err)
	}
	return nil
}

func (repository *Repository) GetTask(ctx context.Context, taskID model.TaskID) (record storageport.Task, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	defer func() { returnErr = classifyTaskQueryError(ctx, returnErr) }()
	if strings.TrimSpace(string(taskID)) == "" {
		return storageport.Task{}, invalidArgument("task ID is required")
	}
	if _, err := repository.database.SQLDB(); err != nil {
		return storageport.Task{}, err
	}
	transaction, err := repository.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return storageport.Task{}, fmt.Errorf("begin task %q query: %w", taskID, err)
	}
	defer transaction.Rollback()
	record, err = getTaskWithQueryer(ctx, transaction, taskID)
	if err != nil {
		return storageport.Task{}, err
	}
	if err := transaction.Commit(); err != nil {
		return storageport.Task{}, fmt.Errorf("commit task %q query: %w", taskID, err)
	}
	return record, nil
}

type taskQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func getTaskWithQueryer(ctx context.Context, queryer taskQueryer, taskID model.TaskID) (storageport.Task, error) {
	var record storageport.Task
	var requirementsJSON, constraintsJSON, state string
	var failureCode, failureMessage sql.NullString
	var createdAt, updatedAt, startedAt, completedAt any
	err := queryer.QueryRowContext(ctx, `
SELECT task_id, intent, requirements_json, constraints_json, status,
       failure_code, failure_message, created_at, updated_at,
       started_at, completed_at, version
FROM tasks
WHERE task_id = ?`, taskID).Scan(
		&record.ID, &record.Intent, &requirementsJSON, &constraintsJSON, &state,
		&failureCode, &failureMessage, &createdAt, &updatedAt,
		&startedAt, &completedAt, &record.Version,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storageport.Task{}, fmt.Errorf("%w: task %q", storageport.ErrNotFound, taskID)
	}
	if err != nil {
		return storageport.Task{}, fmt.Errorf("get task %q: %w", taskID, err)
	}
	record.State = lifecycle.State(state)
	record.FailureCode = failureCode.String
	record.FailureMessage = failureMessage.String
	if err := decodeJSON("task requirements", requirementsJSON, &record.Requirements); err != nil {
		return storageport.Task{}, fmt.Errorf("get task %q: %w", taskID, err)
	}
	if err := decodeJSON("task constraints", constraintsJSON, &record.Constraints); err != nil {
		return storageport.Task{}, fmt.Errorf("get task %q: %w", taskID, err)
	}
	record.CreatedAt, err = decodeTime("task created_at", createdAt)
	if err != nil {
		return storageport.Task{}, fmt.Errorf("get task %q: %w", taskID, err)
	}
	record.UpdatedAt, err = decodeTime("task updated_at", updatedAt)
	if err != nil {
		return storageport.Task{}, fmt.Errorf("get task %q: %w", taskID, err)
	}
	record.StartedAt, err = decodeOptionalTime("task started_at", startedAt)
	if err != nil {
		return storageport.Task{}, fmt.Errorf("get task %q: %w", taskID, err)
	}
	record.CompletedAt, err = decodeOptionalTime("task completed_at", completedAt)
	if err != nil {
		return storageport.Task{}, fmt.Errorf("get task %q: %w", taskID, err)
	}
	record.Steps, err = listTaskStepsWithQueryer(ctx, queryer, taskID)
	if err != nil {
		return storageport.Task{}, err
	}
	if err := validateTaskRecord(record); err != nil {
		return storageport.Task{}, fmt.Errorf("get task %q: %w", taskID, err)
	}
	return record, nil
}

func (repository *Repository) listTaskSteps(ctx context.Context, taskID model.TaskID) ([]storageport.TaskStep, error) {
	return listTaskStepsWithQueryer(ctx, repository.db, taskID)
}

func listTaskStepsWithQueryer(ctx context.Context, queryer taskQueryer, taskID model.TaskID) ([]storageport.TaskStep, error) {
	rows, err := queryer.QueryContext(ctx, `
SELECT step_id, task_id, sequence_no, capability, idempotency_mode, input_json, state,
       assigned_node_id, attempt_count, max_attempts, failure_code,
       failure_message, result_json, created_at, updated_at, started_at,
       completed_at, version
FROM task_steps
WHERE task_id = ?
ORDER BY sequence_no, step_id`, taskID)
	if err != nil {
		return nil, fmt.Errorf("get task %q steps: %w", taskID, err)
	}
	defer rows.Close()
	steps := make([]storageport.TaskStep, 0)
	for rows.Next() {
		var step storageport.TaskStep
		var inputJSON, state, idempotencyMode string
		var assignedNodeID, failureCode, failureMessage, resultJSON sql.NullString
		var createdAt, updatedAt, startedAt, completedAt any
		if err := rows.Scan(
			&step.ID, &step.TaskID, &step.Sequence, &step.Capability, &idempotencyMode, &inputJSON, &state,
			&assignedNodeID, &step.AttemptCount, &step.MaxAttempts,
			&failureCode, &failureMessage, &resultJSON, &createdAt, &updatedAt,
			&startedAt, &completedAt, &step.Version,
		); err != nil {
			return nil, fmt.Errorf("scan task %q step: %w", taskID, err)
		}
		step.State = lifecycle.StepState(state)
		step.IdempotencyMode = model.IdempotencyMode(idempotencyMode)
		step.FailureCode = failureCode.String
		step.FailureMessage = failureMessage.String
		if assignedNodeID.Valid {
			value := model.NodeID(assignedNodeID.String)
			step.AssignedNodeID = &value
		}
		if err := decodeJSON("step input", inputJSON, &step.Input); err != nil {
			return nil, fmt.Errorf("get task %q step %q: %w", taskID, step.ID, err)
		}
		if resultJSON.Valid {
			var result execution.StepResult
			if err := decodeJSON("step result", resultJSON.String, &result); err != nil {
				return nil, fmt.Errorf("get task %q step %q: %w", taskID, step.ID, err)
			}
			step.Result = &result
		}
		step.CreatedAt, err = decodeTime("step created_at", createdAt)
		if err != nil {
			return nil, fmt.Errorf("get task %q step %q: %w", taskID, step.ID, err)
		}
		step.UpdatedAt, err = decodeTime("step updated_at", updatedAt)
		if err != nil {
			return nil, fmt.Errorf("get task %q step %q: %w", taskID, step.ID, err)
		}
		step.StartedAt, err = decodeOptionalTime("step started_at", startedAt)
		if err != nil {
			return nil, fmt.Errorf("get task %q step %q: %w", taskID, step.ID, err)
		}
		step.CompletedAt, err = decodeOptionalTime("step completed_at", completedAt)
		if err != nil {
			return nil, fmt.Errorf("get task %q step %q: %w", taskID, step.ID, err)
		}
		if err := validateTaskStep(taskID, step); err != nil {
			return nil, fmt.Errorf("get task %q step %q: %w", taskID, step.ID, err)
		}
		steps = append(steps, step)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read task %q steps: %w", taskID, err)
	}
	return steps, nil
}

func getTaskStepWithQueryer(
	ctx context.Context,
	queryer rowQueryer,
	taskID model.TaskID,
	stepID model.StepID,
) (storageport.TaskStep, error) {
	var step storageport.TaskStep
	var inputJSON, state, idempotencyMode string
	var assignedNodeID, failureCode, failureMessage, resultJSON sql.NullString
	var createdAt, updatedAt, startedAt, completedAt any
	err := queryer.QueryRowContext(ctx, `
SELECT step_id, task_id, sequence_no, capability, idempotency_mode, input_json, state,
       assigned_node_id, attempt_count, max_attempts, failure_code,
       failure_message, result_json, created_at, updated_at, started_at,
       completed_at, version
FROM task_steps
WHERE task_id = ? AND step_id = ?`, taskID, stepID).Scan(
		&step.ID, &step.TaskID, &step.Sequence, &step.Capability, &idempotencyMode, &inputJSON, &state,
		&assignedNodeID, &step.AttemptCount, &step.MaxAttempts, &failureCode,
		&failureMessage, &resultJSON, &createdAt, &updatedAt, &startedAt,
		&completedAt, &step.Version,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storageport.TaskStep{}, fmt.Errorf("%w: task step %q/%q", storageport.ErrNotFound, taskID, stepID)
	}
	if err != nil {
		return storageport.TaskStep{}, fmt.Errorf("get task step %q/%q: %w", taskID, stepID, err)
	}
	step.State, step.IdempotencyMode, step.FailureCode, step.FailureMessage = lifecycle.StepState(state), model.IdempotencyMode(idempotencyMode), failureCode.String, failureMessage.String
	if assignedNodeID.Valid {
		value := model.NodeID(assignedNodeID.String)
		step.AssignedNodeID = &value
	}
	if err := decodeJSON("step input", inputJSON, &step.Input); err != nil {
		return storageport.TaskStep{}, fmt.Errorf("get task step %q/%q: %w", taskID, stepID, err)
	}
	if resultJSON.Valid {
		var result execution.StepResult
		if err := decodeJSON("step result", resultJSON.String, &result); err != nil {
			return storageport.TaskStep{}, fmt.Errorf("get task step %q/%q: %w", taskID, stepID, err)
		}
		step.Result = &result
	}
	step.CreatedAt, err = decodeTime("step created_at", createdAt)
	if err != nil {
		return storageport.TaskStep{}, err
	}
	step.UpdatedAt, err = decodeTime("step updated_at", updatedAt)
	if err != nil {
		return storageport.TaskStep{}, err
	}
	step.StartedAt, err = decodeOptionalTime("step started_at", startedAt)
	if err != nil {
		return storageport.TaskStep{}, err
	}
	step.CompletedAt, err = decodeOptionalTime("step completed_at", completedAt)
	if err != nil {
		return storageport.TaskStep{}, err
	}
	if err := validateTaskStep(taskID, step); err != nil {
		return storageport.TaskStep{}, fmt.Errorf("get task step %q/%q: %w", taskID, stepID, err)
	}
	return step, nil
}

func (repository *Repository) RecordStepAssignment(
	ctx context.Context,
	taskID model.TaskID,
	stepID model.StepID,
	expectedState lifecycle.StepState,
	expectedVersion int64,
	nodeID model.NodeID,
	event storageport.TaskEvent,
) (newVersion int64, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if strings.TrimSpace(string(nodeID)) == "" || expectedVersion < 1 {
		return 0, invalidData("task %q step %q assignment", taskID, stepID)
	}
	if err := validateStepTransition(stepID, expectedState, lifecycle.StepStateMapped); err != nil {
		return 0, err
	}
	if err := validateStepStateEvent(taskID, stepID, expectedState, lifecycle.StepStateMapped, event); err != nil {
		return 0, err
	}
	updatedAt, _ := encodeTime("step assignment created_at", event.CreatedAt)
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin assign task %q step %q: %w", taskID, stepID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("assign task %q step %q", taskID, stepID))
	result, err := transaction.ExecContext(ctx, `
UPDATE task_steps
SET assigned_node_id = ?, state = ?, updated_at = ?, version = version + 1
WHERE task_id = ? AND step_id = ? AND state = ? AND version = ?`,
		nodeID, lifecycle.StepStateMapped, updatedAt,
		taskID, stepID, expectedState, expectedVersion,
	)
	if err != nil {
		return 0, fmt.Errorf("assign task %q step %q: %w", taskID, stepID, err)
	}
	if err := classifyConditionalUpdate(
		ctx, transaction, result, "task step", fmt.Sprintf("%s/%s", taskID, stepID),
		"SELECT 1 FROM task_steps WHERE task_id = ? AND step_id = ?", taskID, stepID,
	); err != nil {
		return 0, err
	}
	if err := insertTaskEvent(ctx, transaction, event); err != nil {
		return 0, err
	}
	if err := transaction.Commit(); err != nil {
		return 0, fmt.Errorf("commit task %q step %q assignment: %w", taskID, stepID, err)
	}
	return expectedVersion + 1, nil
}

func (repository *Repository) RecordTaskStepProgress(
	ctx context.Context,
	request storageport.RecordTaskStepProgressRequest,
) (newTaskVersion int64, newStepVersion int64, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if err := validateTaskStepProgressRequest(request); err != nil {
		return 0, 0, err
	}
	updatedAt, err := encodeTime("task step progress updated_at", request.UpdatedAt)
	if err != nil {
		return 0, 0, err
	}
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("begin %s progress for task %q step %q: %w", request.Kind, request.TaskID, request.StepID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("%s progress for task %q step %q", request.Kind, request.TaskID, request.StepID))

	var currentTaskState lifecycle.State
	var currentTaskVersion int64
	var taskUpdatedAt, taskStartedAt, taskCompletedAt any
	err = transaction.QueryRowContext(ctx, `
SELECT status, version, updated_at, started_at, completed_at
FROM tasks WHERE task_id = ?`, request.TaskID).Scan(
		&currentTaskState, &currentTaskVersion, &taskUpdatedAt, &taskStartedAt, &taskCompletedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, fmt.Errorf("%w: %s progress task %q step %q", storageport.ErrNotFound, request.Kind, request.TaskID, request.StepID)
	}
	if err != nil {
		return 0, 0, fmt.Errorf("inspect %s progress task %q step %q: %w", request.Kind, request.TaskID, request.StepID, err)
	}
	if currentTaskState != request.ExpectedTaskState || currentTaskVersion != request.ExpectedTaskVersion {
		return 0, 0, fmt.Errorf("%w: %s progress task %q step %q expected task state %q version %d, current state %q version %d", storageport.ErrConflict, request.Kind, request.TaskID, request.StepID, request.ExpectedTaskState, request.ExpectedTaskVersion, currentTaskState, currentTaskVersion)
	}
	currentStep, err := getTaskStepWithQueryer(ctx, transaction, request.TaskID, request.StepID)
	if err != nil {
		return 0, 0, fmt.Errorf("inspect %s progress task %q step %q: %w", request.Kind, request.TaskID, request.StepID, err)
	}
	if currentStep.State != request.ExpectedStepState || currentStep.Version != request.ExpectedStepVersion {
		return 0, 0, fmt.Errorf("%w: %s progress task %q step %q expected step state %q version %d, current state %q version %d", storageport.ErrConflict, request.Kind, request.TaskID, request.StepID, request.ExpectedStepState, request.ExpectedStepVersion, currentStep.State, currentStep.Version)
	}
	stepCandidate := currentStep
	stepCandidate.State = request.NewStepState
	stepCandidate.Result = nil
	stepCandidate.FailureCode = ""
	stepCandidate.FailureMessage = ""
	stepCandidate.CompletedAt = nil
	stepCandidate.UpdatedAt = request.UpdatedAt.UTC()
	stepCandidate.Version = request.ExpectedStepVersion + 1
	if request.Kind == storageport.TaskStepProgressRemap || request.Kind == storageport.TaskStepProgressRecoveryRemap {
		if currentStep.AssignedNodeID == nil {
			return 0, 0, invalidData("%s progress task %q step %q requires current node", request.Kind, request.TaskID, request.StepID)
		}
		// Resource fence staleness can remap to a newer Generation or
		// Registration on the same owner Node. The persisted v0.3-compatible
		// shape stores only NodeID, so an unchanged NodeID is still a real
		// Resource remap and must advance the lifecycle/progress versions.
		stepCandidate.AssignedNodeID = request.NewNodeID
	}
	if err := validateTaskStep(request.TaskID, stepCandidate); err != nil {
		return 0, 0, err
	}

	taskUpdate, err := transaction.ExecContext(ctx, `
UPDATE tasks
SET status = ?, updated_at = ?, completed_at = NULL, version = version + 1
WHERE task_id = ? AND status = ? AND version = ?`,
		request.NewTaskState, updatedAt, request.TaskID, request.ExpectedTaskState, request.ExpectedTaskVersion)
	if err != nil {
		return 0, 0, fmt.Errorf("update %s progress task %q step %q task: %w", request.Kind, request.TaskID, request.StepID, err)
	}
	if err := classifyConditionalUpdate(ctx, transaction, taskUpdate, "task", string(request.TaskID), "SELECT 1 FROM tasks WHERE task_id = ?", request.TaskID); err != nil {
		return 0, 0, err
	}

	var assignedNodeID any
	if request.Kind == storageport.TaskStepProgressRemap || request.Kind == storageport.TaskStepProgressRecoveryRemap {
		assignedNodeID = *request.NewNodeID
	}
	stepUpdate, err := transaction.ExecContext(ctx, `
UPDATE task_steps
SET state = ?,
    assigned_node_id = CASE WHEN ? THEN ? ELSE assigned_node_id END,
    result_json = NULL, failure_code = NULL, failure_message = NULL,
    completed_at = NULL, updated_at = ?, version = version + 1
WHERE task_id = ? AND step_id = ? AND state = ? AND version = ?`,
		request.NewStepState, request.Kind == storageport.TaskStepProgressRemap || request.Kind == storageport.TaskStepProgressRecoveryRemap, assignedNodeID, updatedAt,
		request.TaskID, request.StepID, request.ExpectedStepState, request.ExpectedStepVersion)
	if err != nil {
		return 0, 0, fmt.Errorf("update %s progress task %q step %q step: %w", request.Kind, request.TaskID, request.StepID, err)
	}
	if err := classifyConditionalUpdate(ctx, transaction, stepUpdate, "task step", fmt.Sprintf("%s/%s", request.TaskID, request.StepID), "SELECT 1 FROM task_steps WHERE task_id = ? AND step_id = ?", request.TaskID, request.StepID); err != nil {
		return 0, 0, err
	}
	if err := insertTaskEvent(ctx, transaction, request.TaskEvent); err != nil {
		return 0, 0, fmt.Errorf("append %s progress task %q step %q task event: %w", request.Kind, request.TaskID, request.StepID, err)
	}
	if err := insertTaskEvent(ctx, transaction, request.StepEvent); err != nil {
		return 0, 0, fmt.Errorf("append %s progress task %q step %q step event: %w", request.Kind, request.TaskID, request.StepID, err)
	}
	if err := transaction.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit %s progress for task %q step %q: %w", request.Kind, request.TaskID, request.StepID, err)
	}
	return request.ExpectedTaskVersion + 1, request.ExpectedStepVersion + 1, nil
}

func validateTaskStepProgressRequest(request storageport.RecordTaskStepProgressRequest) error {
	if strings.TrimSpace(string(request.TaskID)) == "" || strings.TrimSpace(string(request.StepID)) == "" ||
		request.ExpectedTaskVersion < 1 || request.ExpectedStepVersion < 1 || request.UpdatedAt.IsZero() {
		return invalidData("%s progress task %q step %q fields", request.Kind, request.TaskID, request.StepID)
	}
	wantTaskState, wantStepState := lifecycle.State(""), lifecycle.StepState("")
	switch request.Kind {
	case storageport.TaskStepProgressRetry:
		wantTaskState, wantStepState = lifecycle.StateRetrying, lifecycle.StepStateRetrying
		if request.ExpectedTaskState != lifecycle.StateRunning || request.ExpectedStepState != lifecycle.StepStateFailed {
			return invalidData("retry progress task %q step %q source states %q/%q", request.TaskID, request.StepID, request.ExpectedTaskState, request.ExpectedStepState)
		}
	case storageport.TaskStepProgressDispatch:
		wantTaskState, wantStepState = lifecycle.StateDispatched, lifecycle.StepStateDispatched
		if request.ExpectedTaskState != lifecycle.StateRetrying || request.ExpectedStepState != lifecycle.StepStateRetrying {
			return invalidData("dispatch progress task %q step %q source states %q/%q", request.TaskID, request.StepID, request.ExpectedTaskState, request.ExpectedStepState)
		}
	case storageport.TaskStepProgressRemap:
		wantTaskState, wantStepState = lifecycle.StateRemapped, lifecycle.StepStateRemapped
		validSource := (request.ExpectedTaskState == lifecycle.StateRetrying && request.ExpectedStepState == lifecycle.StepStateRetrying) ||
			(request.ExpectedTaskState == lifecycle.StateRunning && request.ExpectedStepState == lifecycle.StepStateRunning) ||
			(request.ExpectedTaskState == lifecycle.StateRunning && request.ExpectedStepState == lifecycle.StepStateDispatched) ||
			(request.ExpectedTaskState == lifecycle.StateDispatched && request.ExpectedStepState == lifecycle.StepStateDispatched)
		if !validSource || request.NewNodeID == nil || strings.TrimSpace(string(*request.NewNodeID)) == "" {
			return invalidData("remap progress task %q step %q source states or new node", request.TaskID, request.StepID)
		}
	case storageport.TaskStepProgressRecoveryRemap:
		wantTaskState, wantStepState = lifecycle.StateMapped, lifecycle.StepStateMapped
		if request.ExpectedTaskState != lifecycle.StateMapped || request.ExpectedStepState != lifecycle.StepStateMapped ||
			request.NewNodeID == nil || strings.TrimSpace(string(*request.NewNodeID)) == "" {
			return invalidData("recovery mapped remap task %q step %q source states or new node", request.TaskID, request.StepID)
		}
	default:
		return invalidData("task %q step %q progress kind %q", request.TaskID, request.StepID, request.Kind)
	}
	if request.NewTaskState != wantTaskState || request.NewStepState != wantStepState {
		return invalidData("%s progress task %q step %q target states %q/%q", request.Kind, request.TaskID, request.StepID, request.NewTaskState, request.NewStepState)
	}
	if request.Kind != storageport.TaskStepProgressRemap && request.Kind != storageport.TaskStepProgressRecoveryRemap && request.NewNodeID != nil {
		return invalidData("%s progress task %q step %q cannot change node", request.Kind, request.TaskID, request.StepID)
	}
	if request.Kind != storageport.TaskStepProgressRecoveryRemap {
		if err := validateTaskTransition(request.TaskID, request.ExpectedTaskState, request.NewTaskState); err != nil {
			return err
		}
		if err := validateStepTransition(request.StepID, request.ExpectedStepState, request.NewStepState); err != nil {
			return err
		}
	}
	if err := validateTaskStateEvent(request.TaskID, request.ExpectedTaskState, request.NewTaskState, request.TaskEvent); err != nil {
		return err
	}
	if err := validateStepStateEvent(request.TaskID, request.StepID, request.ExpectedStepState, request.NewStepState, request.StepEvent); err != nil {
		return err
	}
	return nil
}
func (repository *Repository) UpdateTaskState(
	ctx context.Context,
	taskID model.TaskID,
	expected lifecycle.State,
	expectedVersion int64,
	next lifecycle.State,
	failureCode string,
	failureMessage string,
	event storageport.TaskEvent,
) (newVersion int64, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if expectedVersion < 1 {
		return 0, invalidData("task %q expected version", taskID)
	}
	if err := validateTaskTransition(taskID, expected, next); err != nil {
		return 0, err
	}
	if err := validateTaskStateEvent(taskID, expected, next, event); err != nil {
		return 0, err
	}
	updatedAt, _ := encodeTime("task event created_at", event.CreatedAt)
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin update task %q state: %w", taskID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("update task %q state", taskID))
	var currentState lifecycle.State
	var currentVersion int64
	if err := transaction.QueryRowContext(ctx, "SELECT status, version FROM tasks WHERE task_id = ?", taskID).Scan(&currentState, &currentVersion); errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("%w: task %q", storageport.ErrNotFound, taskID)
	} else if err != nil {
		return 0, fmt.Errorf("inspect task %q state: %w", taskID, err)
	}
	if currentState != expected || currentVersion != expectedVersion {
		return 0, fmt.Errorf("%w: task %q expected state or version mismatch", storageport.ErrConflict, taskID)
	}
	if next == lifecycle.StateSuccess || next == lifecycle.StateFailed {
		var totalSteps, successfulSteps int
		if err := transaction.QueryRowContext(ctx, `
SELECT COUNT(*),
       COALESCE(SUM(CASE WHEN state = ? THEN 1 ELSE 0 END), 0)
FROM task_steps WHERE task_id = ?`, lifecycle.StepStateSuccess, taskID).Scan(&totalSteps, &successfulSteps); err != nil {
			return 0, fmt.Errorf("inspect task %q step summary: %w", taskID, err)
		}
		if next == lifecycle.StateSuccess && (totalSteps == 0 || successfulSteps != totalSteps || strings.TrimSpace(failureCode) != "" || strings.TrimSpace(failureMessage) != "") {
			return 0, invalidData("task %q success requires all steps successful and no failure", taskID)
		}
		if next == lifecycle.StateFailed && strings.TrimSpace(failureCode) == "" && strings.TrimSpace(failureMessage) == "" {
			return 0, invalidData("task %q failure requires task failure reason", taskID)
		}
	}
	result, err := transaction.ExecContext(ctx, `
UPDATE tasks
SET status = ?, failure_code = ?, failure_message = ?, updated_at = ?,
    started_at = CASE WHEN ? = 'running' THEN COALESCE(started_at, ?) ELSE started_at END,
    completed_at = CASE WHEN ? IN ('success', 'failed', 'cancelled') THEN ? ELSE NULL END,
    version = version + 1
WHERE task_id = ? AND status = ? AND version = ?`,
		next, nullableString(failureCode), nullableString(failureMessage), updatedAt,
		next, updatedAt, next, updatedAt, taskID, expected, expectedVersion,
	)
	if err != nil {
		return 0, fmt.Errorf("update task %q state %q -> %q: %w", taskID, expected, next, err)
	}
	if err := classifyConditionalUpdate(ctx, transaction, result, "task", string(taskID),
		"SELECT 1 FROM tasks WHERE task_id = ?", taskID); err != nil {
		return 0, err
	}
	if err := insertTaskEvent(ctx, transaction, event); err != nil {
		return 0, err
	}
	if err := transaction.Commit(); err != nil {
		return 0, fmt.Errorf("commit task %q state update: %w", taskID, err)
	}
	return expectedVersion + 1, nil
}

func (repository *Repository) UpdateStepState(
	ctx context.Context,
	taskID model.TaskID,
	stepID model.StepID,
	expected lifecycle.StepState,
	expectedVersion int64,
	next lifecycle.StepState,
	executionResult *execution.StepResult,
	event storageport.TaskEvent,
) (newVersion int64, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if expectedVersion < 1 {
		return 0, invalidData("task %q step %q expected version", taskID, stepID)
	}
	if err := validateStepTransition(stepID, expected, next); err != nil {
		return 0, err
	}
	if err := validateExecutionResult(stepID, executionResult); err != nil {
		return 0, err
	}
	if err := validateStepStateEvent(taskID, stepID, expected, next, event); err != nil {
		return 0, err
	}
	switch next {
	case lifecycle.StepStateSuccess:
		if executionResult == nil || executionResult.Status != execution.StatusSucceeded {
			return 0, invalidData("task %q step %q success result", taskID, stepID)
		}
	case lifecycle.StepStateFailed:
		if executionResult == nil || executionResult.Status != execution.StatusFailed {
			return 0, invalidData("task %q step %q failure result", taskID, stepID)
		}
	default:
		if executionResult != nil {
			return 0, invalidData("task %q step %q non-terminal result", taskID, stepID)
		}
	}
	updatedAt, _ := encodeTime("step event created_at", event.CreatedAt)
	var resultJSON any
	var failureMessage any
	if executionResult != nil {
		encoded, err := encodeJSON("step result", executionResult)
		if err != nil {
			return 0, err
		}
		resultJSON = encoded
		if executionResult.Status == execution.StatusFailed {
			failureMessage = nullableString(executionResult.Error)
		}
	}
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin update task %q step %q: %w", taskID, stepID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("update task %q step %q", taskID, stepID))
	result, err := transaction.ExecContext(ctx, `
UPDATE task_steps
SET state = ?, result_json = ?,
    failure_code = CASE WHEN ? IN ('retrying', 'remapped') THEN NULL ELSE failure_code END,
    failure_message = CASE WHEN ? IN ('retrying', 'remapped') THEN NULL ELSE ? END,
    updated_at = ?,
    started_at = CASE WHEN ? = 'running' THEN COALESCE(started_at, ?) ELSE started_at END,
    completed_at = CASE WHEN ? IN ('success', 'failed', 'cancelled') THEN ? ELSE NULL END,
    version = version + 1
WHERE task_id = ? AND step_id = ? AND state = ? AND version = ?`,
		next, resultJSON, next, next, failureMessage, updatedAt,
		next, updatedAt, next, updatedAt,
		taskID, stepID, expected, expectedVersion,
	)
	if err != nil {
		return 0, fmt.Errorf("update task %q step %q state %q -> %q: %w", taskID, stepID, expected, next, err)
	}
	if err := classifyConditionalUpdate(
		ctx, transaction, result, "task step", fmt.Sprintf("%s/%s", taskID, stepID),
		"SELECT 1 FROM task_steps WHERE task_id = ? AND step_id = ?", taskID, stepID,
	); err != nil {
		return 0, err
	}
	if err := insertTaskEvent(ctx, transaction, event); err != nil {
		return 0, err
	}
	if err := transaction.Commit(); err != nil {
		return 0, fmt.Errorf("commit task %q step %q update: %w", taskID, stepID, err)
	}
	return expectedVersion + 1, nil
}

func rollbackOnError(transaction *sql.Tx, returnErr *error, operation string) {
	if *returnErr == nil {
		return
	}
	if err := transaction.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
		*returnErr = errors.Join(*returnErr, fmt.Errorf("rollback %s: %w", operation, err))
	}
}
