package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/node"
	storageport "dtm/internal/storage"
)

func TestUnifiedSchemaHasOnlyTaskStepsForCoreStepSummary(t *testing.T) {
	repository := newRepository(t)
	for table, want := range map[string]int{"tasks": 1, "task_steps": 1, "executions": 1, "task_events": 1, "execution_steps": 0} {
		var count int
		if err := repository.db.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("table %s count = %d, want %d", table, count, want)
		}
	}
}

func TestCreateTaskIsAtomicAndClassifiesDuplicate(t *testing.T) {
	repository := newRepository(t)
	now := testTime(0)
	record := baseTask("task-atomic", now)
	record.Steps = []storageport.TaskStep{
		baseStep(record.ID, "step-1", 0, now),
		baseStep(record.ID, "step-2", 0, now),
	}
	if err := repository.CreateTask(context.Background(), record); !errors.Is(err, storageport.ErrAlreadyExists) {
		t.Fatalf("CreateTask() error = %v, want ErrAlreadyExists", err)
	}
	assertCount(t, repository, "tasks", "task_id", string(record.ID), 0)
	assertCount(t, repository, "task_steps", "task_id", string(record.ID), 0)

	record.Steps = record.Steps[:1]
	if err := repository.CreateTask(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if err := repository.CreateTask(context.Background(), record); !errors.Is(err, storageport.ErrAlreadyExists) {
		t.Fatalf("duplicate CreateTask() error = %v", err)
	}
}

func TestCreateAndGetTaskRestoresAllStepsAndClassifiesMissing(t *testing.T) {
	repository := newRepository(t)
	now := testTime(0)
	record := baseTask("task-roundtrip", now)
	record.Steps = []storageport.TaskStep{baseStep(record.ID, "step-1", 0, now), baseStep(record.ID, "step-2", 1, now)}
	if err := repository.CreateTask(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	got, err := repository.GetTask(context.Background(), record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != record.ID || got.Intent != record.Intent || len(got.Steps) != 2 || got.Steps[0].ID != "step-1" || got.Steps[1].ID != "step-2" {
		t.Fatalf("task = %#v", got)
	}
	if _, err := repository.GetTask(context.Background(), "missing"); !errors.Is(err, storageport.ErrNotFound) {
		t.Fatalf("missing task error = %v", err)
	}
	if _, err := repository.GetExecution(context.Background(), "missing"); !errors.Is(err, storageport.ErrNotFound) {
		t.Fatalf("missing execution error = %v", err)
	}
}

func TestExecutionAttemptsPersistHistoryAndCurrentStepResult(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-attempts")
	first, stepVersion := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(10))
	failed := mustStepResult(t, stepID, "node-a", execution.StatusFailed, map[string]any{"partial": true}, "offline")
	event := stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateFailed, "execution_failed", testTime(20))
	executionVersion, stepVersion, err := repository.CompleteExecution(context.Background(), storageport.CompleteExecutionRequest{
		ExecutionID: first.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: first.Version,
		ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion,
		NewExecutionState: storageport.ExecutionStateFailed, NewStepState: lifecycle.StepStateFailed,
		Result: &failed, FailureCode: "agent_unavailable", FailureMessage: "offline", CompletedAt: testTime(20), Event: event,
	})
	if err != nil {
		t.Fatal(err)
	}
	if executionVersion != 2 {
		t.Fatalf("execution version = %d", executionVersion)
	}

	stepVersion = updateStep(t, repository, taskID, stepID, lifecycle.StepStateFailed, stepVersion, lifecycle.StepStateRetrying, nil, testTime(30))
	taskRecord, err := repository.GetTask(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	if taskRecord.Steps[0].Result != nil || taskRecord.Steps[0].CompletedAt != nil || taskRecord.Steps[0].FailureMessage != "" {
		t.Fatalf("retrying step retained terminal data: %#v", taskRecord.Steps[0])
	}
	stepVersion = updateStep(t, repository, taskID, stepID, lifecycle.StepStateRetrying, stepVersion, lifecycle.StepStateDispatched, nil, testTime(40))
	second, stepVersion := startAttempt(t, repository, taskID, stepID, 2, stepVersion, "node-a", testTime(50))
	succeeded := mustStepResult(t, stepID, "node-a", execution.StatusSucceeded, map[string]any{"temperature": 26.0}, "")
	_, _, err = repository.CompleteExecution(context.Background(), storageport.CompleteExecutionRequest{
		ExecutionID: second.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: second.Version,
		ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion,
		NewExecutionState: storageport.ExecutionStateSucceeded, NewStepState: lifecycle.StepStateSuccess,
		Result: &succeeded, CompletedAt: testTime(60),
		Event: stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateSuccess, "execution_succeeded", testTime(60)),
	})
	if err != nil {
		t.Fatal(err)
	}

	attempts, err := repository.GetExecutions(context.Background(), taskID, stepID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 2 || attempts[0].AttemptNo != 1 || attempts[0].State != storageport.ExecutionStateFailed || attempts[1].AttemptNo != 2 || attempts[1].State != storageport.ExecutionStateSucceeded {
		t.Fatalf("attempts = %#v", attempts)
	}
	if attempts[0].Result == nil || attempts[0].Result.Error != "offline" {
		t.Fatalf("historical failure lost: %#v", attempts[0])
	}
	taskRecord, err = repository.GetTask(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	step := taskRecord.Steps[0]
	if step.State != lifecycle.StepStateSuccess || step.Result == nil || step.Result.Status != execution.StatusSucceeded || step.AttemptCount != 2 || step.AssignedNodeID == nil || *step.AssignedNodeID != "node-a" {
		t.Fatalf("current step = %#v", step)
	}
}

func TestExecutionWriteValidationRejectsInconsistentResultsBeforeSQL(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-validation")
	attempt, stepVersion := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(10))
	failed := mustStepResult(t, stepID, "node-a", execution.StatusFailed, nil, "failed")
	_, _, err := repository.CompleteExecution(context.Background(), storageport.CompleteExecutionRequest{
		ExecutionID: attempt.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: attempt.Version,
		ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion,
		NewExecutionState: storageport.ExecutionStateSucceeded, NewStepState: lifecycle.StepStateSuccess,
		Result: &failed, CompletedAt: testTime(20), Event: stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateSuccess, "bad", testTime(20)),
	})
	if !errors.Is(err, storageport.ErrInvalidData) {
		t.Fatalf("mismatch error = %v", err)
	}
	got, getErr := repository.GetExecution(context.Background(), attempt.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if got.State != storageport.ExecutionStateStarted {
		t.Fatalf("state changed after rejected write: %q", got.State)
	}

	runningResult := mustStepResult(t, stepID, "node-a", execution.StatusSucceeded, nil, "")
	_, err = repository.UpdateStepState(context.Background(), taskID, stepID, lifecycle.StepStateRunning, stepVersion, lifecycle.StepStateRunning, &runningResult,
		stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateRunning, "bad_running_result", testTime(21)))
	if !errors.Is(err, storageport.ErrInvalidData) {
		t.Fatalf("running result error = %v", err)
	}
}

func TestExecutionRejectsCompletionBeforeStartAndDuplicateAttempt(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-time-validation")
	attempt, stepVersion := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(20))
	request := startRequest(taskID, stepID, 1, stepVersion, "node-a", testTime(21))
	request.ExpectedStepState = lifecycle.StepStateRunning
	request.Event = stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateRunning, "duplicate", testTime(21))
	if _, _, err := repository.StartExecution(context.Background(), request); !errors.Is(err, storageport.ErrConflict) {
		t.Fatalf("second active attempt error = %v", err)
	}
	succeeded := mustStepResult(t, stepID, "node-a", execution.StatusSucceeded, nil, "")
	_, _, err := repository.CompleteExecution(context.Background(), storageport.CompleteExecutionRequest{
		ExecutionID: attempt.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: 1,
		ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion,
		NewExecutionState: storageport.ExecutionStateSucceeded, NewStepState: lifecycle.StepStateSuccess,
		Result: &succeeded, CompletedAt: testTime(10), Event: stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateSuccess, "too_early", testTime(10)),
	})
	if !errors.Is(err, storageport.ErrInvalidData) {
		t.Fatalf("early completion error = %v", err)
	}
}

func TestM2CRejectsAttemptJumpAndSecondStartedExecution(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-attempt-invariants")
	jump := startRequest(taskID, stepID, 3, stepVersion, "node-a", testTime(10))
	if _, _, err := repository.StartExecution(context.Background(), jump); !errors.Is(err, storageport.ErrInvalidData) {
		t.Fatalf("attempt jump error = %v, want ErrInvalidData", err)
	}
	first, runningVersion := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(11))
	second := startRequest(taskID, stepID, 2, runningVersion, "node-a", testTime(12))
	second.ExpectedStepState = lifecycle.StepStateRunning
	second.Event = stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateRunning, "second_started", testTime(12))
	if _, _, err := repository.StartExecution(context.Background(), second); !errors.Is(err, storageport.ErrConflict) {
		t.Fatalf("second started error = %v, first=%s", err, first.ID)
	}
}

func TestCompletionConsistencyMatrixRejectsInvalidPairsWithoutWrites(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*storageport.CompleteExecutionRequest, execution.StepResult, execution.StepResult)
	}{
		{"execution_success_step_failed", func(r *storageport.CompleteExecutionRequest, succeeded, _ execution.StepResult) {
			r.NewStepState, r.Result = lifecycle.StepStateFailed, &succeeded
		}},
		{"execution_failed_step_success", func(r *storageport.CompleteExecutionRequest, _, failed execution.StepResult) {
			r.NewExecutionState, r.Result, r.FailureCode = storageport.ExecutionStateFailed, &failed, "agent_failed"
		}},
		{"execution_canceled_step_success", func(r *storageport.CompleteExecutionRequest, _, _ execution.StepResult) {
			r.NewExecutionState = storageport.ExecutionStateCanceled
			r.Result = nil
		}},
		{"execution_success_failed_result", func(r *storageport.CompleteExecutionRequest, _, failed execution.StepResult) { r.Result = &failed }},
		{"execution_failed_succeeded_result", func(r *storageport.CompleteExecutionRequest, succeeded, _ execution.StepResult) {
			r.NewExecutionState, r.NewStepState, r.Result, r.FailureCode = storageport.ExecutionStateFailed, lifecycle.StepStateFailed, &succeeded, "agent_failed"
		}},
		{"terminal_execution_without_completed_at", func(r *storageport.CompleteExecutionRequest, _, _ execution.StepResult) { r.CompletedAt = time.Time{} }},
		{"completed_before_started", func(r *storageport.CompleteExecutionRequest, _, _ execution.StepResult) { r.CompletedAt = testTime(5) }},
		{"succeeded_with_failure_code", func(r *storageport.CompleteExecutionRequest, _, _ execution.StepResult) { r.FailureCode = "unexpected" }},
		{"failed_without_reason", func(r *storageport.CompleteExecutionRequest, _, failed execution.StepResult) {
			failed.Error = ""
			r.NewExecutionState, r.NewStepState, r.Result = storageport.ExecutionStateFailed, lifecycle.StepStateFailed, &failed
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := newRepository(t)
			taskID, stepID, stepVersion := createDispatchedStep(t, repository, model.TaskID("task-pair-"+test.name))
			attempt, stepVersion := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(10))
			succeeded := mustStepResult(t, stepID, "node-a", execution.StatusSucceeded, nil, "")
			failed := mustStepResult(t, stepID, "node-a", execution.StatusFailed, nil, "agent failed")
			request := storageport.CompleteExecutionRequest{
				ExecutionID: attempt.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: attempt.Version,
				ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion,
				NewExecutionState: storageport.ExecutionStateSucceeded, NewStepState: lifecycle.StepStateSuccess,
				Result: &succeeded, CompletedAt: testTime(20),
			}
			test.mutate(&request, succeeded, failed)
			request.Event = stepEvent(taskID, stepID, lifecycle.StepStateRunning, request.NewStepState, "invalid_completion", testTime(20))
			_, _, err := repository.CompleteExecution(context.Background(), request)
			if !errors.Is(err, storageport.ErrInvalidData) {
				t.Fatalf("CompleteExecution() error = %v, want ErrInvalidData", err)
			}
			storedAttempt, getErr := repository.GetExecution(context.Background(), attempt.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			storedTask, getErr := repository.GetTask(context.Background(), taskID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if storedAttempt.State != storageport.ExecutionStateStarted || storedAttempt.Version != 1 || storedAttempt.CompletedAt != nil || storedTask.Steps[0].State != lifecycle.StepStateRunning || storedTask.Steps[0].Version != stepVersion {
				t.Fatalf("invalid completion changed database: attempt=%#v step=%#v", storedAttempt, storedTask.Steps[0])
			}
			events, getErr := repository.ListTaskEvents(context.Background(), taskID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			for _, event := range events {
				if event.Type == "invalid_completion" {
					t.Fatalf("invalid event persisted: %#v", event)
				}
			}
		})
	}
}

func TestM2CCompletionPairRejectsExecutionSuccessWithFailedStep(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-completion-pair")
	attempt, stepVersion := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(10))
	succeeded := mustStepResult(t, stepID, "node-a", execution.StatusSucceeded, nil, "")
	_, _, err := repository.CompleteExecution(context.Background(), storageport.CompleteExecutionRequest{
		ExecutionID: attempt.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: 1,
		ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion,
		NewExecutionState: storageport.ExecutionStateSucceeded, NewStepState: lifecycle.StepStateFailed,
		Result: &succeeded, CompletedAt: testTime(20), Event: stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateFailed, "invalid_pair", testTime(20)),
	})
	if !errors.Is(err, storageport.ErrInvalidData) {
		t.Fatalf("completion pair error = %v", err)
	}
}

func TestAttemptSequenceRejectsDuplicateHistoryAndMaximumExceeded(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-attempt-bounds")
	if _, err := repository.db.Exec("UPDATE task_steps SET max_attempts=1 WHERE task_id=? AND step_id=?", taskID, stepID); err != nil {
		t.Fatal(err)
	}
	first, stepVersion := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(10))
	failed := mustStepResult(t, stepID, "node-a", execution.StatusFailed, nil, "offline")
	_, stepVersion, err := repository.CompleteExecution(context.Background(), storageport.CompleteExecutionRequest{
		ExecutionID: first.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: first.Version,
		ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion,
		NewExecutionState: storageport.ExecutionStateFailed, NewStepState: lifecycle.StepStateFailed, Result: &failed, FailureCode: "offline", CompletedAt: testTime(20),
		Event: stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateFailed, "execution_failed", testTime(20)),
	})
	if err != nil {
		t.Fatal(err)
	}
	stepVersion = updateStep(t, repository, taskID, stepID, lifecycle.StepStateFailed, stepVersion, lifecycle.StepStateRetrying, nil, testTime(30))
	stepVersion = updateStep(t, repository, taskID, stepID, lifecycle.StepStateRetrying, stepVersion, lifecycle.StepStateDispatched, nil, testTime(31))
	duplicate := startRequest(taskID, stepID, 1, stepVersion, "node-a", testTime(40))
	if _, _, err := repository.StartExecution(context.Background(), duplicate); !errors.Is(err, storageport.ErrAlreadyExists) {
		t.Fatalf("duplicate historical attempt error = %v", err)
	}
	exceeded := startRequest(taskID, stepID, 2, stepVersion, "node-a", testTime(41))
	if _, _, err := repository.StartExecution(context.Background(), exceeded); !errors.Is(err, storageport.ErrInvalidData) {
		t.Fatalf("maximum exceeded error = %v", err)
	}
	attempts, err := repository.GetExecutions(context.Background(), taskID, stepID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 1 || attempts[0].AttemptNo != 1 || attempts[0].State != storageport.ExecutionStateFailed {
		t.Fatalf("attempt history = %#v", attempts)
	}
}

func TestRecordStepAssignmentInterruptionLeavesAtomicMappedSnapshot(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	now := testTime(0)
	taskRecord := baseTask("task-assignment-interruption", now)
	if err := repository.CreateTask(ctx, taskRecord); err != nil {
		t.Fatal(err)
	}
	stepID := model.StepID("step-1")
	step := baseStep(taskRecord.ID, stepID, 0, now)
	if _, err := repository.RecordTaskPlan(ctx, taskRecord.ID, lifecycle.StateCreated, 1, []storageport.TaskStep{step}, taskEvent(taskRecord.ID, lifecycle.StateCreated, lifecycle.StatePlanning, "planned", testTime(1))); err != nil {
		t.Fatal(err)
	}
	version, err := repository.RecordStepAssignment(ctx, taskRecord.ID, stepID, lifecycle.StepStateCreated, 1, "node-a", stepEvent(taskRecord.ID, stepID, lifecycle.StepStateCreated, lifecycle.StepStateMapped, "assigned", testTime(2)))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := repository.GetTask(ctx, taskRecord.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Steps) != 1 || snapshot.Steps[0].State != lifecycle.StepStateMapped || snapshot.Steps[0].AssignedNodeID == nil || *snapshot.Steps[0].AssignedNodeID != "node-a" || snapshot.Steps[0].Version != version {
		t.Fatalf("assignment interruption snapshot = %#v", snapshot)
	}
	attempts, err := repository.GetExecutions(ctx, taskRecord.ID, stepID)
	if err != nil {
		t.Fatal(err)
	}
	if len(attempts) != 0 {
		t.Fatalf("assignment interruption executions = %#v", attempts)
	}
	events, err := repository.ListTaskEvents(ctx, taskRecord.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[len(events)-1].Type != "assigned" {
		t.Fatalf("assignment interruption events = %#v", events)
	}
}

func TestM2CTaskSuccessRejectsIncompleteStep(t *testing.T) {
	repository := newRepository(t)
	taskID, _, _ := createDispatchedStep(t, repository, "task-terminal-invariant")
	version := int64(2)
	for _, transition := range []struct{ from, to lifecycle.State }{{lifecycle.StatePlanning, lifecycle.StateMapped}, {lifecycle.StateMapped, lifecycle.StateDispatched}, {lifecycle.StateDispatched, lifecycle.StateRunning}} {
		next, err := repository.UpdateTaskState(context.Background(), taskID, transition.from, version, transition.to, "", "", taskEvent(taskID, transition.from, transition.to, "advance", testTime(time.Duration(version))))
		if err != nil {
			t.Fatal(err)
		}
		version = next
	}
	_, err := repository.UpdateTaskState(context.Background(), taskID, lifecycle.StateRunning, version, lifecycle.StateSuccess, "", "", taskEvent(taskID, lifecycle.StateRunning, lifecycle.StateSuccess, "invalid_success", testTime(20)))
	if !errors.Is(err, storageport.ErrInvalidData) {
		t.Fatalf("task success error = %v", err)
	}
	record, getErr := repository.GetTask(context.Background(), taskID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if record.State != lifecycle.StateRunning || record.Version != version || record.CompletedAt != nil {
		t.Fatalf("rejected task finalization changed task: %#v", record)
	}
	events, getErr := repository.ListTaskEvents(context.Background(), taskID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	for _, event := range events {
		if event.Type == "invalid_success" {
			t.Fatalf("rejected finalization event persisted: %#v", event)
		}
	}
}

func TestGetTaskRejectsContradictoryPersistedTerminalState(t *testing.T) {
	tests := []struct{ name, update string }{
		{"success_with_incomplete_step", "UPDATE tasks SET status='success', completed_at=updated_at WHERE task_id=?"},
		{"failed_without_reason_or_failed_step", "UPDATE tasks SET status='failed', completed_at=updated_at, failure_code=NULL, failure_message=NULL WHERE task_id=?"},
		{"running_without_started_at", "UPDATE tasks SET status='running', started_at=NULL WHERE task_id=?"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := newRepository(t)
			taskID, _, _ := createDispatchedStep(t, repository, model.TaskID("task-invalid-"+test.name))
			if _, err := repository.db.Exec(test.update, taskID); err != nil {
				t.Fatal(err)
			}
			if _, err := repository.GetTask(context.Background(), taskID); !errors.Is(err, storageport.ErrInvalidData) {
				t.Fatalf("GetTask() error = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestStartExecutionRollsBackInsertWhenStepUpdateFails(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-start-rollback")
	if _, err := repository.db.Exec("CREATE TRIGGER fail_step_start BEFORE UPDATE ON task_steps BEGIN SELECT RAISE(ABORT, 'step update failed'); END"); err != nil {
		t.Fatal(err)
	}
	request := startRequest(taskID, stepID, 1, stepVersion, "node-a", testTime(10))
	_, _, err := repository.StartExecution(context.Background(), request)
	if err == nil || errors.Is(err, storageport.ErrNotFound) || errors.Is(err, storageport.ErrConflict) {
		t.Fatalf("StartExecution() error = %v", err)
	}
	assertCount(t, repository, "executions", "execution_id", string(request.ExecutionID), 0)
	taskRecord, getErr := repository.GetTask(context.Background(), taskID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if taskRecord.Steps[0].State != lifecycle.StepStateDispatched || taskRecord.Steps[0].Version != stepVersion {
		t.Fatalf("step changed: %#v", taskRecord.Steps[0])
	}
}

func TestCompleteExecutionRollsBackWhenStepOrEventWriteFails(t *testing.T) {
	tests := []struct{ name, trigger string }{
		{"step", "CREATE TRIGGER fail_step_complete BEFORE UPDATE ON task_steps WHEN NEW.state='success' BEGIN SELECT RAISE(ABORT, 'step complete failed'); END"},
		{"event", "CREATE TRIGGER fail_event_insert BEFORE INSERT ON task_events WHEN NEW.event_type='execution_succeeded' BEGIN SELECT RAISE(ABORT, 'event failed'); END"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := newRepository(t)
			taskID, stepID, stepVersion := createDispatchedStep(t, repository, model.TaskID("task-complete-"+test.name))
			attempt, stepVersion := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(10))
			if _, err := repository.db.Exec(test.trigger); err != nil {
				t.Fatal(err)
			}
			succeeded := mustStepResult(t, stepID, "node-a", execution.StatusSucceeded, map[string]any{"ok": true}, "")
			_, _, err := repository.CompleteExecution(context.Background(), storageport.CompleteExecutionRequest{
				ExecutionID: attempt.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: attempt.Version,
				ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion,
				NewExecutionState: storageport.ExecutionStateSucceeded, NewStepState: lifecycle.StepStateSuccess,
				Result: &succeeded, CompletedAt: testTime(20), Event: stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateSuccess, "execution_succeeded", testTime(20)),
			})
			if err == nil {
				t.Fatal("CompleteExecution() error = nil")
			}
			storedAttempt, getErr := repository.GetExecution(context.Background(), attempt.ID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if storedAttempt.State != storageport.ExecutionStateStarted || storedAttempt.Version != 1 || storedAttempt.CompletedAt != nil {
				t.Fatalf("execution not rolled back: %#v", storedAttempt)
			}
			taskRecord, getErr := repository.GetTask(context.Background(), taskID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if taskRecord.Steps[0].State != lifecycle.StepStateRunning || taskRecord.Steps[0].Version != stepVersion {
				t.Fatalf("step not rolled back: %#v", taskRecord.Steps[0])
			}
			events, getErr := repository.ListTaskEvents(context.Background(), taskID)
			if getErr != nil {
				t.Fatal(getErr)
			}
			for _, event := range events {
				if event.Type == "execution_succeeded" {
					t.Fatalf("completion event persisted: %#v", event)
				}
			}
		})
	}
}

func TestTaskStateEventRollsBackOnEventFailure(t *testing.T) {
	repository := newRepository(t)
	now := testTime(0)
	record := baseTask("task-event-rollback", now)
	if err := repository.CreateTask(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.db.Exec("CREATE TRIGGER fail_task_event BEFORE INSERT ON task_events BEGIN SELECT RAISE(ABORT, 'event failed'); END"); err != nil {
		t.Fatal(err)
	}
	_, err := repository.UpdateTaskState(context.Background(), record.ID, lifecycle.StateCreated, 1, lifecycle.StatePlanning, "", "",
		taskEvent(record.ID, lifecycle.StateCreated, lifecycle.StatePlanning, "planning", testTime(1)))
	if err == nil {
		t.Fatal("UpdateTaskState() error = nil")
	}
	got, getErr := repository.GetTask(context.Background(), record.ID)
	if getErr != nil {
		t.Fatal(getErr)
	}
	if got.State != lifecycle.StateCreated || got.Version != 1 {
		t.Fatalf("task update not rolled back: %#v", got)
	}
}

func TestVersionPreventsABAForTaskAndStep(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-aba")
	// Same-state updates demonstrate that state alone cannot distinguish the newer value.
	nextTaskVersion, err := repository.UpdateTaskState(context.Background(), taskID, lifecycle.StatePlanning, 2, lifecycle.StatePlanning, "", "",
		taskEvent(taskID, lifecycle.StatePlanning, lifecycle.StatePlanning, "task_heartbeat", testTime(10)))
	if err != nil || nextTaskVersion != 3 {
		t.Fatalf("task same-state update = %d, %v", nextTaskVersion, err)
	}
	_, err = repository.UpdateTaskState(context.Background(), taskID, lifecycle.StatePlanning, 2, lifecycle.StatePlanning, "", "",
		taskEvent(taskID, lifecycle.StatePlanning, lifecycle.StatePlanning, "stale_task", testTime(11)))
	if !errors.Is(err, storageport.ErrConflict) {
		t.Fatalf("stale task error = %v", err)
	}

	nextStepVersion := updateStep(t, repository, taskID, stepID, lifecycle.StepStateDispatched, stepVersion, lifecycle.StepStateDispatched, nil, testTime(12))
	_, err = repository.UpdateStepState(context.Background(), taskID, stepID, lifecycle.StepStateDispatched, stepVersion, lifecycle.StepStateDispatched, nil,
		stepEvent(taskID, stepID, lifecycle.StepStateDispatched, lifecycle.StepStateDispatched, "stale_step", testTime(13)))
	if !errors.Is(err, storageport.ErrConflict) {
		t.Fatalf("stale step error = %v (new version %d)", err, nextStepVersion)
	}
}

func TestConcurrentStartAndCompleteAllowOneWinnerWithoutLockErrors(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-concurrent")
	request := startRequest(taskID, stepID, 1, stepVersion, "node-a", testTime(10))
	startErrors := concurrently(2, func() error { _, _, err := repository.StartExecution(context.Background(), request); return err })
	assertOneWinner(t, startErrors)
	for _, err := range startErrors {
		if err != nil && !errors.Is(err, storageport.ErrAlreadyExists) && !errors.Is(err, storageport.ErrConflict) {
			t.Fatalf("unclassified concurrent start error = %v", err)
		}
	}
	attempt, err := repository.GetExecution(context.Background(), request.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	taskRecord, err := repository.GetTask(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	succeeded := mustStepResult(t, stepID, "node-a", execution.StatusSucceeded, map[string]any{"ok": true}, "")
	completeRequest := storageport.CompleteExecutionRequest{
		ExecutionID: attempt.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: attempt.Version,
		ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: taskRecord.Steps[0].Version,
		NewExecutionState: storageport.ExecutionStateSucceeded, NewStepState: lifecycle.StepStateSuccess,
		Result: &succeeded, CompletedAt: testTime(20), Event: stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateSuccess, "concurrent_complete", testTime(20)),
	}
	completeErrors := concurrently(2, func() error {
		_, _, err := repository.CompleteExecution(context.Background(), completeRequest)
		return err
	})
	assertOneWinner(t, completeErrors)
	for _, err := range completeErrors {
		if err != nil && !errors.Is(err, storageport.ErrConflict) {
			t.Fatalf("unclassified concurrent complete error = %v", err)
		}
	}
	events, err := repository.ListTaskEvents(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Type == "concurrent_complete" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("completion events = %d, want 1", count)
	}
}

func TestIntegerNanosecondTimesSortEventsAndProtectNodeUpdates(t *testing.T) {
	repository := newRepository(t)
	now := testTime(0)
	record := baseTask("task-times", now)
	if err := repository.CreateTask(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	times := []time.Time{now.Add(time.Second), now.Add(100 * time.Millisecond), now.Add(10 * time.Millisecond), now.Add(time.Nanosecond), now.Add(10 * time.Millisecond)}
	for index, at := range times {
		if err := repository.AppendTaskEvent(context.Background(), storageport.TaskEvent{TaskID: record.ID, Type: fmt.Sprintf("event-%d", index), CreatedAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	events, err := repository.ListTaskEvents(context.Background(), record.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"event-3", "event-2", "event-4", "event-1", "event-0"}
	for index := range want {
		if events[index].Type != want[index] {
			t.Fatalf("events[%d] = %s, want %s", index, events[index].Type, want[index])
		}
	}
	var storedType string
	if err := repository.db.QueryRow("SELECT typeof(created_at) FROM task_events LIMIT 1").Scan(&storedType); err != nil {
		t.Fatal(err)
	}
	if storedType != "integer" {
		t.Fatalf("created_at type = %q", storedType)
	}

	nodeRecord := storageport.NodeRecord{ID: "node-time", Endpoint: "127.0.0.1:1", Capabilities: []model.Capability{"temperature_sensor"}, Status: node.StatusOnline, Generation: 1, RegistrationID: "registration-1", Metadata: map[string]string{}, RegisteredAt: now, LastHeartbeatAt: now, LeaseExpiresAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now.Add(100 * time.Millisecond)}
	if err := repository.UpsertNode(context.Background(), nodeRecord); err != nil {
		t.Fatal(err)
	}
	stale := nodeRecord
	stale.Endpoint = "stale"
	stale.UpdatedAt = now.Add(10 * time.Millisecond)
	if err := repository.UpsertNode(context.Background(), stale); !errors.Is(err, storageport.ErrConflict) {
		t.Fatalf("stale node error = %v", err)
	}
	gotNode, err := repository.GetNode(context.Background(), nodeRecord.ID)
	if err != nil {
		t.Fatal(err)
	}
	if gotNode.Endpoint != nodeRecord.Endpoint || !gotNode.UpdatedAt.Equal(nodeRecord.UpdatedAt) {
		t.Fatalf("node = %#v", gotNode)
	}
}

func TestNodeRoundTripPreservesCreatedAtAndRejectsInvalidStoredData(t *testing.T) {
	repository := newRepository(t)
	now := testTime(0)
	record := storageport.NodeRecord{ID: "node-roundtrip", Endpoint: "127.0.0.1:1", Capabilities: []model.Capability{"temperature_sensor"}, Status: node.StatusOnline, Generation: 1, RegistrationID: "registration-1", Metadata: map[string]string{}, RegisteredAt: now, LastHeartbeatAt: now, LeaseExpiresAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now}
	if err := repository.UpsertNode(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	updated := record
	updated.Endpoint = "127.0.0.1:2"
	updated.Capabilities = []model.Capability{"cooling_control"}
	updated.Generation = 2
	updated.RegistrationID = "registration-2"
	updated.RegisteredAt = now.Add(time.Second)
	updated.LastHeartbeatAt = now.Add(time.Second)
	updated.LeaseExpiresAt = now.Add(2 * time.Minute)
	updated.UpdatedAt = now.Add(time.Second)
	if err := repository.UpsertNode(context.Background(), updated); err != nil {
		t.Fatal(err)
	}
	got, err := repository.GetNode(context.Background(), record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.CreatedAt.Equal(now) || got.Endpoint != updated.Endpoint || got.Generation != 2 || len(got.Capabilities) != 1 || got.Capabilities[0] != "cooling_control" {
		t.Fatalf("node = %#v", got)
	}
	if _, err := repository.db.Exec("UPDATE nodes SET capabilities_json='{' WHERE node_id=?", record.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.GetNode(context.Background(), record.ID); !errors.Is(err, storageport.ErrInvalidData) {
		t.Fatalf("invalid node error = %v", err)
	}
}

func TestStoragePreservesCanceledContext(t *testing.T) {
	repository := newRepository(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := repository.GetTask(ctx, "task-canceled")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("GetTask() error = %v", err)
	}
}

func TestReadsRejectInvalidPersistedStateJSONTimeAndResult(t *testing.T) {
	tests := []struct{ name, update string }{
		{"state", "UPDATE task_steps SET state='unknown'"},
		{"json", "UPDATE task_steps SET input_json='{'"},
		{"time", "UPDATE task_steps SET updated_at='2026-01-01T00:00:00Z'"},
		{"result", "UPDATE task_steps SET state='success', result_json=NULL, completed_at=updated_at"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := newRepository(t)
			taskID, _, _ := createDispatchedStep(t, repository, model.TaskID("task-invalid-"+test.name))
			if _, err := repository.db.Exec(test.update); err != nil {
				t.Fatal(err)
			}
			_, err := repository.GetTask(context.Background(), taskID)
			if !errors.Is(err, storageport.ErrInvalidData) {
				t.Fatalf("GetTask() error = %v", err)
			}
		})
	}

	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-invalid-execution")
	attempt, _ := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(10))
	if _, err := repository.db.Exec("UPDATE executions SET status='unknown' WHERE execution_id=?", attempt.ID); err != nil {
		t.Fatal(err)
	}
	_, err := repository.GetExecution(context.Background(), attempt.ID)
	if !errors.Is(err, storageport.ErrInvalidData) {
		t.Fatalf("GetExecution() error = %v", err)
	}
}

func newRepository(t *testing.T) *Repository {
	t.Helper()
	repository, err := Open(filepath.Join(t.TempDir(), "state.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := repository.Close(); err != nil {
			t.Error(err)
		}
	})
	return repository
}

func testTime(offset time.Duration) time.Time {
	return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Add(offset)
}

func baseTask(id model.TaskID, now time.Time) storageport.Task {
	return storageport.Task{ID: id, Intent: "cool_environment", Requirements: []model.Capability{"temperature_sensor"}, State: lifecycle.StateCreated, CreatedAt: now, UpdatedAt: now, Version: 1}
}

func baseStep(taskID model.TaskID, stepID model.StepID, sequence int, now time.Time) storageport.TaskStep {
	return storageport.TaskStep{ID: stepID, TaskID: taskID, Sequence: sequence, Capability: "temperature_sensor", Input: map[string]string{"operation": "read_temperature"}, State: lifecycle.StepStateCreated, MaxAttempts: 3, CreatedAt: now, UpdatedAt: now, Version: 1}
}

func createDispatchedStep(t *testing.T, repository *Repository, id model.TaskID) (model.TaskID, model.StepID, int64) {
	t.Helper()
	ctx := context.Background()
	now := testTime(0)
	record := baseTask(id, now)
	if err := repository.CreateTask(ctx, record); err != nil {
		t.Fatal(err)
	}
	stepID := model.StepID("step-1")
	step := baseStep(id, stepID, 0, now)
	if _, err := repository.RecordTaskPlan(ctx, id, lifecycle.StateCreated, 1, []storageport.TaskStep{step}, taskEvent(id, lifecycle.StateCreated, lifecycle.StatePlanning, "planned", testTime(1))); err != nil {
		t.Fatal(err)
	}
	version, err := repository.RecordStepAssignment(ctx, id, stepID, lifecycle.StepStateCreated, 1, "node-a", stepEvent(id, stepID, lifecycle.StepStateCreated, lifecycle.StepStateMapped, "assigned", testTime(2)))
	if err != nil {
		t.Fatal(err)
	}
	version = updateStep(t, repository, id, stepID, lifecycle.StepStateMapped, version, lifecycle.StepStateDispatched, nil, testTime(3))
	return id, stepID, version
}

func taskEvent(taskID model.TaskID, from, to lifecycle.State, kind string, at time.Time) storageport.TaskEvent {
	return storageport.TaskEvent{TaskID: taskID, Type: kind, FromState: string(from), ToState: string(to), CreatedAt: at}
}
func stepEvent(taskID model.TaskID, stepID model.StepID, from, to lifecycle.StepState, kind string, at time.Time) storageport.TaskEvent {
	id := stepID
	return storageport.TaskEvent{TaskID: taskID, StepID: &id, Type: kind, FromState: string(from), ToState: string(to), CreatedAt: at}
}
func startRequest(taskID model.TaskID, stepID model.StepID, attempt int, version int64, nodeID model.NodeID, at time.Time) storageport.StartExecutionRequest {
	return storageport.StartExecutionRequest{ExecutionID: model.ExecutionID(fmt.Sprintf("%s-%s-%d", taskID, stepID, attempt)), TaskID: taskID, StepID: stepID, AttemptNo: attempt, NodeID: nodeID, Request: map[string]string{"operation": "read_temperature"}, ExpectedStepState: lifecycle.StepStateDispatched, ExpectedStepVersion: version, StartedAt: at, Event: stepEvent(taskID, stepID, lifecycle.StepStateDispatched, lifecycle.StepStateRunning, "execution_started", at)}
}
func startAttempt(t *testing.T, repository *Repository, taskID model.TaskID, stepID model.StepID, attempt int, version int64, nodeID model.NodeID, at time.Time) (storageport.Execution, int64) {
	t.Helper()
	request := startRequest(taskID, stepID, attempt, version, nodeID, at)
	record, newVersion, err := repository.StartExecution(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return record, newVersion
}
func updateStep(t *testing.T, repository *Repository, taskID model.TaskID, stepID model.StepID, from lifecycle.StepState, version int64, to lifecycle.StepState, result *execution.StepResult, at time.Time) int64 {
	t.Helper()
	newVersion, err := repository.UpdateStepState(context.Background(), taskID, stepID, from, version, to, result, stepEvent(taskID, stepID, from, to, "step_"+string(to), at))
	if err != nil {
		t.Fatal(err)
	}
	return newVersion
}
func mustStepResult(t *testing.T, stepID model.StepID, nodeID model.NodeID, status execution.Status, output map[string]any, message string) execution.StepResult {
	t.Helper()
	result, err := execution.NewStepResult(stepID, nodeID, status, output, message)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func assertCount(t *testing.T, repository *Repository, table, column, value string, want int) {
	t.Helper()
	var got int
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s = ?", table, column)
	if err := repository.db.QueryRow(query, value).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", table, got, want)
	}
}
func concurrently(count int, fn func() error) []error {
	start := make(chan struct{})
	errorsOut := make([]error, count)
	var wait sync.WaitGroup
	wait.Add(count)
	for index := 0; index < count; index++ {
		go func(i int) { defer wait.Done(); <-start; errorsOut[i] = fn() }(index)
	}
	close(start)
	wait.Wait()
	return errorsOut
}
func assertOneWinner(t *testing.T, values []error) {
	t.Helper()
	successes := 0
	for _, err := range values {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("errors = %v, successes = %d", values, successes)
	}
}
