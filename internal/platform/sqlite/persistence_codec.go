package sqlite

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/node"
	storageport "dtm/internal/storage"
	"dtm/internal/task"
)

func invalidData(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", storageport.ErrInvalidData, fmt.Sprintf(format, arguments...))
}

func invalidArgument(format string, arguments ...any) error {
	return fmt.Errorf("%w: %s", storageport.ErrInvalidArgument, fmt.Sprintf(format, arguments...))
}

func encodeJSON(field string, value any) (string, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", invalidData("encode %s: %v", field, err)
	}
	return string(encoded), nil
}

func decodeJSON(field, encoded string, target any) error {
	decoder := json.NewDecoder(strings.NewReader(encoded))
	if err := decoder.Decode(target); err != nil {
		return invalidData("decode %s: %v", field, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return invalidData("decode %s: trailing JSON value", field)
		}
		return invalidData("decode %s: %v", field, err)
	}
	return nil
}

func encodeTime(field string, value time.Time) (int64, error) {
	if value.IsZero() {
		return 0, invalidData("%s is required", field)
	}
	return value.UTC().UnixNano(), nil
}

func encodeOptionalTime(field string, value *time.Time) (any, error) {
	if value == nil {
		return nil, nil
	}
	return encodeTime(field, *value)
}

func decodeTime(field string, stored any) (time.Time, error) {
	value, ok := stored.(int64)
	if !ok {
		return time.Time{}, invalidData("decode %s: want INTEGER unix nanoseconds, got %T", field, stored)
	}
	return time.Unix(0, value).UTC(), nil
}

func decodeOptionalTime(field string, stored any) (*time.Time, error) {
	if stored == nil {
		return nil, nil
	}
	parsed, err := decodeTime(field, stored)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func validateTaskState(taskID model.TaskID, state lifecycle.State) error {
	if _, err := lifecycle.Restore(taskID, state); err != nil {
		return invalidData("task %q state %q", taskID, state)
	}
	return nil
}

func validateTaskTransition(taskID model.TaskID, expected, next lifecycle.State) error {
	machine, err := lifecycle.Restore(taskID, expected)
	if err != nil || machine.Transition(next) != nil {
		return invalidData("task %q transition %q -> %q", taskID, expected, next)
	}
	return nil
}

func validateStepState(stepID model.StepID, state lifecycle.StepState) error {
	if _, err := lifecycle.RestoreStep(stepID, state); err != nil {
		return invalidData("step %q state %q", stepID, state)
	}
	return nil
}

func validateStepTransition(stepID model.StepID, expected, next lifecycle.StepState) error {
	machine, err := lifecycle.RestoreStep(stepID, expected)
	if err != nil || machine.Transition(next) != nil {
		return invalidData("step %q transition %q -> %q", stepID, expected, next)
	}
	return nil
}

func validateTaskRecord(record storageport.Task) error {
	if _, err := task.New(record.ID, record.Intent, record.Requirements, record.Constraints); err != nil {
		return invalidData("task %q identity or payload", record.ID)
	}
	if err := validateTaskState(record.ID, record.State); err != nil {
		return err
	}
	if record.Version < 1 || record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return invalidData("task %q timestamps or version", record.ID)
	}
	if record.StartedAt != nil && record.CompletedAt != nil && record.CompletedAt.Before(*record.StartedAt) {
		return invalidData("task %q completed_at precedes started_at", record.ID)
	}
	successfulSteps := 0
	for index := range record.Steps {
		if err := validateTaskStep(record.ID, record.Steps[index]); err != nil {
			return err
		}
		switch record.Steps[index].State {
		case lifecycle.StepStateSuccess:
			successfulSteps++
		}
	}
	hasFailure := strings.TrimSpace(record.FailureCode) != "" || strings.TrimSpace(record.FailureMessage) != ""
	switch record.State {
	case lifecycle.StateSuccess:
		if record.CompletedAt == nil || hasFailure || len(record.Steps) == 0 || successfulSteps != len(record.Steps) {
			return invalidData("task %q success requires completed_at, no failure, and all steps successful", record.ID)
		}
	case lifecycle.StateFailed:
		if record.CompletedAt == nil || !hasFailure {
			return invalidData("task %q failure requires completed_at and task failure reason", record.ID)
		}
	case lifecycle.StateCancelled:
		if record.CompletedAt == nil {
			return invalidData("task %q cancellation requires completed_at", record.ID)
		}
	case lifecycle.StateRunning:
		if record.StartedAt == nil || record.CompletedAt != nil {
			return invalidData("task %q running timestamps", record.ID)
		}
	default:
		if record.CompletedAt != nil {
			return invalidData("task %q non-terminal state %q has completed_at", record.ID, record.State)
		}
	}
	return nil
}

func validateTaskStep(taskID model.TaskID, step storageport.TaskStep) error {
	if step.TaskID != taskID || strings.TrimSpace(string(step.ID)) == "" ||
		strings.TrimSpace(string(step.Capability)) == "" || step.Sequence < 0 ||
		!step.IdempotencyMode.Valid() ||
		step.AttemptCount < 0 || step.MaxAttempts < 1 || step.AttemptCount > step.MaxAttempts ||
		step.Version < 1 || step.CreatedAt.IsZero() || step.UpdatedAt.IsZero() ||
		step.UpdatedAt.Before(step.CreatedAt) {
		return invalidData("task %q step %q fields", taskID, step.ID)
	}
	if err := validateStepState(step.ID, step.State); err != nil {
		return err
	}
	if step.AssignedNodeID != nil && strings.TrimSpace(string(*step.AssignedNodeID)) == "" {
		return invalidData("task %q step %q assigned node", taskID, step.ID)
	}
	if err := validateTimeRange("step", fmt.Sprintf("%s/%s", taskID, step.ID), step.StartedAt, step.CompletedAt); err != nil {
		return err
	}
	return validateStepStateResult(step)
}

func validateStepStateResult(step storageport.TaskStep) error {
	switch step.State {
	case lifecycle.StepStateSuccess:
		if step.Result == nil || step.Result.Status != execution.StatusSucceeded || step.CompletedAt == nil {
			return invalidData("task %q step %q success requires succeeded result and completed_at", step.TaskID, step.ID)
		}
	case lifecycle.StepStateFailed:
		if step.Result == nil || step.Result.Status != execution.StatusFailed || step.CompletedAt == nil ||
			(strings.TrimSpace(step.FailureCode) == "" && strings.TrimSpace(step.FailureMessage) == "" && strings.TrimSpace(step.Result.Error) == "") {
			return invalidData("task %q step %q failure requires failed result, reason, and completed_at", step.TaskID, step.ID)
		}
	case lifecycle.StepStateCancelled:
		if step.CompletedAt == nil || (step.Result != nil && step.Result.Status == execution.StatusSucceeded) {
			return invalidData("task %q step %q cancellation fields", step.TaskID, step.ID)
		}
	default:
		if step.Result != nil || step.CompletedAt != nil {
			return invalidData("task %q step %q non-terminal state %q has terminal data", step.TaskID, step.ID, step.State)
		}
	}
	if step.Result != nil {
		validated, err := execution.NewStepResult(
			step.Result.StepID, step.Result.NodeID, step.Result.Status,
			step.Result.Output, step.Result.Error,
		)
		if err != nil || validated.StepID != step.ID {
			return invalidData("task %q step %q result", step.TaskID, step.ID)
		}
	}
	return nil
}

func validateExecutionRecord(record storageport.Execution) error {
	if strings.TrimSpace(string(record.ID)) == "" || strings.TrimSpace(record.RequestID) == "" || strings.TrimSpace(string(record.TaskID)) == "" ||
		strings.TrimSpace(string(record.StepID)) == "" || record.AttemptNo < 1 ||
		strings.TrimSpace(string(record.NodeID)) == "" || record.Request == nil ||
		record.Version < 1 || record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() ||
		record.UpdatedAt.Before(record.CreatedAt) {
		return invalidData("execution %q fields", record.ID)
	}
	if err := validateTimeRange("execution", string(record.ID), record.StartedAt, record.CompletedAt); err != nil {
		return err
	}
	switch record.State {
	case storageport.ExecutionStateCreated:
		if record.StartedAt != nil || record.CompletedAt != nil || record.Result != nil {
			return invalidData("execution %q created state has lifecycle data", record.ID)
		}
	case storageport.ExecutionStateStarted:
		if record.StartedAt == nil || record.CompletedAt != nil || record.Result != nil {
			return invalidData("execution %q started state fields", record.ID)
		}
	case storageport.ExecutionStateSucceeded:
		if record.StartedAt == nil || record.CompletedAt == nil || record.Result == nil ||
			record.Result.Status != execution.StatusSucceeded ||
			record.FailureCode != "" || record.FailureMessage != "" {
			return invalidData("execution %q succeeded state fields", record.ID)
		}
	case storageport.ExecutionStateFailed:
		if record.StartedAt == nil || record.CompletedAt == nil || record.Result == nil ||
			record.Result.Status != execution.StatusFailed ||
			(strings.TrimSpace(record.FailureCode) == "" && strings.TrimSpace(record.FailureMessage) == "" && strings.TrimSpace(record.Result.Error) == "") {
			return invalidData("execution %q failed state fields", record.ID)
		}
	case storageport.ExecutionStateCanceled:
		if record.StartedAt == nil || record.CompletedAt == nil || (record.Result != nil && record.Result.Status == execution.StatusSucceeded) {
			return invalidData("execution %q canceled state fields", record.ID)
		}
	default:
		return invalidData("execution %q state %q", record.ID, record.State)
	}
	if record.Result != nil {
		validated, err := execution.NewStepResult(
			record.Result.StepID, record.Result.NodeID, record.Result.Status,
			record.Result.Output, record.Result.Error,
		)
		if err != nil || validated.StepID != record.StepID || validated.NodeID != record.NodeID {
			return invalidData("execution %q result", record.ID)
		}
	}
	return nil
}

func validateCompletionPair(
	executionState storageport.ExecutionState,
	stepState lifecycle.StepState,
	result *execution.StepResult,
	failureCode string,
	failureMessage string,
	completedAt time.Time,
) error {
	if completedAt.IsZero() {
		return invalidData("execution completion requires completed_at")
	}
	hasFailure := strings.TrimSpace(failureCode) != "" || strings.TrimSpace(failureMessage) != ""
	switch executionState {
	case storageport.ExecutionStateSucceeded:
		if stepState != lifecycle.StepStateSuccess || result == nil || result.Status != execution.StatusSucceeded || hasFailure {
			return invalidData("succeeded execution requires successful step/result without failure")
		}
	case storageport.ExecutionStateFailed:
		if stepState != lifecycle.StepStateFailed || result == nil || result.Status != execution.StatusFailed || !hasFailure {
			return invalidData("failed execution requires failed step/result and failure reason")
		}
	case storageport.ExecutionStateCanceled:
		if stepState != lifecycle.StepStateCancelled || (result != nil && result.Status == execution.StatusSucceeded) {
			return invalidData("canceled execution requires cancelled step and non-success result")
		}
	default:
		return invalidData("execution completion state %q is not terminal", executionState)
	}
	return nil
}

func validateExecutionTransition(expected, next storageport.ExecutionState) error {
	switch expected {
	case storageport.ExecutionStateStarted:
		if next == storageport.ExecutionStateSucceeded ||
			next == storageport.ExecutionStateFailed ||
			next == storageport.ExecutionStateCanceled {
			return nil
		}
	}
	return invalidData("execution transition %q -> %q", expected, next)
}

func validateTimeRange(objectType, objectID string, startedAt, completedAt *time.Time) error {
	if completedAt != nil && startedAt == nil {
		return invalidData("%s %q completed_at requires started_at", objectType, objectID)
	}
	if startedAt != nil && completedAt != nil && completedAt.Before(*startedAt) {
		return invalidData("%s %q completed_at precedes started_at", objectType, objectID)
	}
	return nil
}

func validateNodeRecord(record storageport.NodeRecord) error {
	if strings.TrimSpace(record.Endpoint) == "" || record.Capabilities == nil || record.Generation < 1 ||
		strings.TrimSpace(record.RegistrationID) == "" || record.Metadata == nil ||
		record.RegisteredAt.IsZero() || record.LastHeartbeatAt.IsZero() || record.LeaseExpiresAt.IsZero() ||
		record.CreatedAt.IsZero() || record.UpdatedAt.IsZero() ||
		record.RegisteredAt.Before(record.CreatedAt) || record.LastHeartbeatAt.Before(record.RegisteredAt) ||
		!record.LeaseExpiresAt.After(record.LastHeartbeatAt) || record.UpdatedAt.Before(record.CreatedAt) ||
		record.UpdatedAt.Before(record.RegisteredAt) || record.UpdatedAt.Before(record.LastHeartbeatAt) {
		return invalidData("node %q fields", record.ID)
	}
	validated, err := node.New(record.ID, record.Capabilities, record.Status)
	if err != nil || len(validated.Capabilities()) != len(record.Capabilities) {
		return invalidData("node %q identity, capabilities, or status", record.ID)
	}
	return nil
}

func validateEvent(event storageport.TaskEvent) error {
	if event.ID < 0 || strings.TrimSpace(string(event.TaskID)) == "" ||
		strings.TrimSpace(event.Type) == "" || event.CreatedAt.IsZero() {
		return invalidData("task event fields")
	}
	if event.StepID != nil && strings.TrimSpace(string(*event.StepID)) == "" {
		return invalidData("task event step ID")
	}
	for _, state := range []string{event.FromState, event.ToState} {
		if state != "" && !knownEventState(state) {
			return invalidData("task event state %q", state)
		}
	}
	return nil
}

func knownEventState(value string) bool {
	if _, err := lifecycle.Restore("event-task", lifecycle.State(value)); err == nil {
		return true
	}
	if _, err := lifecycle.RestoreStep("event-step", lifecycle.StepState(value)); err == nil {
		return true
	}
	switch storageport.ExecutionState(value) {
	case storageport.ExecutionStateCreated,
		storageport.ExecutionStateStarted,
		storageport.ExecutionStateSucceeded,
		storageport.ExecutionStateFailed,
		storageport.ExecutionStateCanceled:
		return true
	default:
		return false
	}
}

func validateExecutionResult(stepID model.StepID, result *execution.StepResult) error {
	if result == nil {
		return nil
	}
	validated, err := execution.NewStepResult(
		result.StepID, result.NodeID, result.Status, result.Output, result.Error,
	)
	if err != nil || validated.StepID != stepID {
		return invalidData("step %q execution result", stepID)
	}
	return nil
}
