package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"dtm/internal/agentexecution"
	"dtm/internal/execution"
	storageport "dtm/internal/storage"
)

type ExecutionRepository struct {
	database *Database
	db       *sql.DB
	now      func() time.Time
}

func OpenExecutionRepository(path string) (*ExecutionRepository, error) {
	return OpenExecutionRepositoryWithOptions(Options{
		Path:        path,
		AutoMigrate: true,
	})
}

func OpenExecutionRepositoryWithOptions(options Options) (*ExecutionRepository, error) {
	options.MigrationSet = "agent"
	database, err := openDatabase(context.Background(), options)
	if err != nil {
		return nil, err
	}
	db, err := database.SQLDB()
	if err != nil {
		_ = database.Close()
		return nil, err
	}
	repository := &ExecutionRepository{database: database, db: db, now: time.Now}
	if err := repository.initialize(context.Background()); err != nil {
		_ = database.Close()
		return nil, err
	}
	return repository, nil
}

func (repository *ExecutionRepository) initialize(ctx context.Context) (returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if _, err := repository.db.ExecContext(ctx, `
UPDATE executions
SET status = ?, updated_at = ?
WHERE status = ?`,
		agentexecution.StatusInterrupted,
		repository.now().UTC().Format(time.RFC3339Nano),
		agentexecution.StatusRunning,
	); err != nil {
		return fmt.Errorf("recover interrupted agent executions: %w", err)
	}
	return nil
}

func (repository *ExecutionRepository) Close() error {
	if repository == nil || repository.database == nil {
		return nil
	}
	return repository.database.Close()
}

func (repository *ExecutionRepository) Health(ctx context.Context) error {
	if repository == nil || repository.database == nil {
		return ErrDatabaseClosed
	}
	return repository.database.Health(ctx)
}

func (repository *ExecutionRepository) StorageStatus() Status {
	if repository == nil || repository.database == nil {
		return Status{}
	}
	return repository.database.Status()
}

func (repository *ExecutionRepository) Find(
	ctx context.Context,
	key string,
) (record agentexecution.Record, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	key = strings.TrimSpace(key)
	if key == "" {
		return agentexecution.Record{}, agentexecution.ErrNotFound
	}
	var (
		fingerprintJSON, resultJSON string
		createdAt, updatedAt        string
		taskID, stepID              string
	)
	err := repository.db.QueryRowContext(ctx, `
SELECT execution_id, task_id, step_id, idempotency_key,
       fingerprint_json, status, result_json, execution_error,
       created_at, updated_at
FROM executions
WHERE idempotency_key = ?`, key).Scan(
		&record.ExecutionID,
		&taskID,
		&stepID,
		&record.IdempotencyKey,
		&fingerprintJSON,
		&record.Status,
		&resultJSON,
		&record.ExecutionError,
		&createdAt,
		&updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return agentexecution.Record{}, agentexecution.ErrNotFound
	}
	if err != nil {
		return agentexecution.Record{}, fmt.Errorf("find agent execution: %w", err)
	}
	if err := json.Unmarshal([]byte(fingerprintJSON), &record.Fingerprint); err != nil {
		return agentexecution.Record{}, invalidAgentExecutionRecord()
	}
	if err := validatePersistedAgentIdentity(record, taskID, stepID); err != nil {
		return agentexecution.Record{}, err
	}
	var storedResult execution.StepResult
	if err := json.Unmarshal([]byte(resultJSON), &storedResult); err != nil {
		return agentexecution.Record{}, invalidAgentExecutionRecord()
	}
	if record.Status == agentexecution.StatusCompleted {
		validated, validationErr := execution.NewStepResult(storedResult.StepID, storedResult.NodeID, storedResult.Status, storedResult.Output, storedResult.Error)
		if validationErr != nil || validated.StepID != record.Fingerprint.StepID || validated.NodeID != record.Fingerprint.NodeID {
			return agentexecution.Record{}, invalidAgentExecutionRecord()
		}
		record.Result = validated
	}
	record.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
	if err != nil {
		return agentexecution.Record{}, invalidAgentExecutionRecord()
	}
	record.UpdatedAt, err = time.Parse(time.RFC3339Nano, updatedAt)
	if err != nil {
		return agentexecution.Record{}, invalidAgentExecutionRecord()
	}
	if record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() || record.UpdatedAt.Before(record.CreatedAt) {
		return agentexecution.Record{}, invalidAgentExecutionRecord()
	}
	return record, nil
}

func (repository *ExecutionRepository) Start(
	ctx context.Context,
	key string,
	fingerprint agentexecution.Fingerprint,
) (returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	key = strings.TrimSpace(key)
	fingerprintJSON, err := encodeFingerprint(key, fingerprint)
	if err != nil {
		return err
	}
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin agent execution: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()

	var currentFingerprint, currentTaskID, currentStepID string
	var currentStatus agentexecution.Status
	err = transaction.QueryRowContext(ctx, `
SELECT task_id, step_id, fingerprint_json, status
FROM executions
WHERE idempotency_key = ?`, key).Scan(&currentTaskID, &currentStepID, &currentFingerprint, &currentStatus)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		now := repository.now().UTC().Format(time.RFC3339Nano)
		_, err = transaction.ExecContext(ctx, `
INSERT INTO executions (
	execution_id, task_id, step_id, idempotency_key,
	fingerprint_json, status, result_json, execution_error,
	created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, '{}', '', ?, ?)`,
			executionID(key),
			fingerprint.TaskID,
			fingerprint.StepID,
			key,
			fingerprintJSON,
			agentexecution.StatusRunning,
			now,
			now,
		)
		if err != nil {
			if isUniqueConstraint(err) {
				return agentexecution.ErrConflict
			}
			return fmt.Errorf("insert agent execution: %w", err)
		}
	case err != nil:
		return fmt.Errorf("inspect agent execution: %w", err)
	default:
		var persisted agentexecution.Fingerprint
		if err := json.Unmarshal([]byte(currentFingerprint), &persisted); err != nil {
			return invalidAgentExecutionRecord()
		}
		if err := validatePersistedAgentIdentity(agentexecution.Record{ExecutionID: executionID(key), IdempotencyKey: key, Fingerprint: persisted, Status: currentStatus}, currentTaskID, currentStepID); err != nil {
			return err
		}
		if currentFingerprint != fingerprintJSON {
			return agentexecution.ErrConflict
		}
		if currentStatus == agentexecution.StatusCompleted {
			return agentexecution.ErrAlreadyComplete
		}
		_, err = transaction.ExecContext(ctx, `
UPDATE executions
SET status = ?, result_json = '{}', execution_error = '', updated_at = ?
WHERE idempotency_key = ?`,
			agentexecution.StatusRunning,
			repository.now().UTC().Format(time.RFC3339Nano),
			key,
		)
		if err != nil {
			return fmt.Errorf("restart agent execution: %w", err)
		}
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit agent execution start: %w", err)
	}
	return nil
}

func (repository *ExecutionRepository) Complete(
	ctx context.Context,
	key string,
	fingerprint agentexecution.Fingerprint,
	result execution.StepResult,
	executionError string,
) (returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	key = strings.TrimSpace(key)
	fingerprintJSON, err := encodeFingerprint(key, fingerprint)
	if err != nil {
		return err
	}
	validated, err := execution.NewStepResult(
		result.StepID,
		result.NodeID,
		result.Status,
		result.Output,
		result.Error,
	)
	if err != nil ||
		validated.StepID != fingerprint.StepID ||
		validated.NodeID != fingerprint.NodeID ||
		(validated.Status == execution.StatusSucceeded && executionError != "") {
		return fmt.Errorf("validate agent execution result")
	}
	resultJSON, err := json.Marshal(validated)
	if err != nil {
		return fmt.Errorf("encode agent execution result: %w", err)
	}
	update, err := repository.db.ExecContext(ctx, `
UPDATE executions
SET status = ?, result_json = ?, execution_error = ?, updated_at = ?
WHERE idempotency_key = ? AND fingerprint_json = ?`,
		agentexecution.StatusCompleted,
		string(resultJSON),
		executionError,
		repository.now().UTC().Format(time.RFC3339Nano),
		key,
		fingerprintJSON,
	)
	if err != nil {
		return fmt.Errorf("complete agent execution: %w", err)
	}
	affected, err := update.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect agent execution completion: %w", err)
	}
	if affected == 0 {
		record, findErr := repository.Find(ctx, key)
		if errors.Is(findErr, agentexecution.ErrNotFound) {
			return agentexecution.ErrNotFound
		}
		if findErr != nil {
			return findErr
		}
		if !record.Fingerprint.Equal(fingerprint) {
			return agentexecution.ErrConflict
		}
		return fmt.Errorf("complete agent execution: record was not updated")
	}
	return nil
}

func encodeFingerprint(
	key string,
	fingerprint agentexecution.Fingerprint,
) (string, error) {
	if key == "" ||
		strings.TrimSpace(string(fingerprint.TaskID)) == "" ||
		strings.TrimSpace(string(fingerprint.StepID)) == "" ||
		strings.TrimSpace(string(fingerprint.Capability)) == "" ||
		strings.TrimSpace(string(fingerprint.NodeID)) == "" {
		return "", fmt.Errorf("validate agent execution fingerprint")
	}
	encoded, err := json.Marshal(fingerprint)
	if err != nil {
		return "", fmt.Errorf("encode agent execution fingerprint: %w", err)
	}
	return string(encoded), nil
}

func validatePersistedAgentIdentity(record agentexecution.Record, taskID, stepID string) error {
	if strings.TrimSpace(record.ExecutionID) == "" || strings.TrimSpace(record.IdempotencyKey) == "" ||
		strings.TrimSpace(taskID) == "" || strings.TrimSpace(stepID) == "" ||
		strings.TrimSpace(string(record.Fingerprint.TaskID)) == "" || strings.TrimSpace(string(record.Fingerprint.StepID)) == "" ||
		strings.TrimSpace(string(record.Fingerprint.Capability)) == "" || strings.TrimSpace(string(record.Fingerprint.NodeID)) == "" ||
		string(record.Fingerprint.TaskID) != taskID || string(record.Fingerprint.StepID) != stepID ||
		record.ExecutionID != executionID(record.IdempotencyKey) {
		return invalidAgentExecutionRecord()
	}
	switch record.Status {
	case agentexecution.StatusRunning, agentexecution.StatusCompleted, agentexecution.StatusInterrupted:
		return nil
	default:
		return invalidAgentExecutionRecord()
	}
}

func invalidAgentExecutionRecord() error {
	return fmt.Errorf("%w: persisted agent execution record is invalid", storageport.ErrStorageIntegrity)
}

func executionID(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

func isUniqueConstraint(err error) bool {
	var sqliteError interface{ Code() int }
	return errors.As(err, &sqliteError) && sqliteError.Code()&0xff == 19
}

var _ agentexecution.Repository = (*ExecutionRepository)(nil)
