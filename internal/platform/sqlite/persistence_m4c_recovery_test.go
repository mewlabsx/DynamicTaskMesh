package sqlite

import (
	"context"
	"errors"
	"testing"

	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	storageport "dtm/internal/storage"
)

func TestResolveInterruptedExecutionUsesPersistedIdempotencyAndCAS(t *testing.T) {
	tests := []struct {
		name      string
		mode      model.IdempotencyMode
		wantRetry bool
		wantCode  string
	}{
		{name: "explicit idempotent retries", mode: model.IdempotencyIdempotent, wantRetry: true},
		{name: "explicit non-idempotent fails closed", mode: model.IdempotencyNonIdempotent, wantCode: recoveryNonIdempotentCode},
		{name: "legacy unspecified fails closed", mode: model.IdempotencyUnspecified, wantCode: recoveryIdempotencyUnknownCode},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := newRepository(t)
			taskID, stepID, taskVersion, stepVersion, attempt := createInterruptedM4CTask(t, repository, model.TaskID("task-"+string(test.mode)), test.mode)
			resolution, err := repository.ResolveInterruptedExecution(context.Background(), storageport.ResolveInterruptedExecutionRequest{
				ExecutionID: attempt.ID, ExpectedExecutionVersion: attempt.Version,
				ExpectedTaskVersion: taskVersion, ExpectedStepVersion: stepVersion,
				ResolvedAt: testTime(20),
			})
			if err != nil {
				t.Fatal(err)
			}
			if resolution.RetryAllowed != test.wantRetry || resolution.IdempotencyMode != test.mode {
				t.Fatalf("resolution = %#v", resolution)
			}
			persistedExecution, err := repository.GetExecution(context.Background(), attempt.ID)
			if err != nil {
				t.Fatal(err)
			}
			if persistedExecution.State != storageport.ExecutionStateFailed || persistedExecution.FailureCode != restartInterruptedCode {
				t.Fatalf("interrupted execution = %#v", persistedExecution)
			}
			record, err := repository.GetTask(context.Background(), taskID)
			if err != nil {
				t.Fatal(err)
			}
			wantTaskState, wantStepState := lifecycle.StateFailed, lifecycle.StepStateFailed
			if test.wantRetry {
				wantTaskState, wantStepState = lifecycle.StateRetrying, lifecycle.StepStateRetrying
			}
			if record.State != wantTaskState || record.Steps[0].State != wantStepState {
				t.Fatalf("classified task = %#v", record)
			}
			if !test.wantRetry && (record.FailureCode != test.wantCode || record.Steps[0].FailureCode != test.wantCode) {
				t.Fatalf("safe failure codes task=%q step=%q", record.FailureCode, record.Steps[0].FailureCode)
			}
			executions, err := repository.GetExecutions(context.Background(), taskID, stepID)
			if err != nil || len(executions) != 1 {
				t.Fatalf("execution history = %#v, %v", executions, err)
			}
			if _, err := repository.ResolveInterruptedExecution(context.Background(), storageport.ResolveInterruptedExecutionRequest{ExecutionID: attempt.ID, ExpectedExecutionVersion: attempt.Version, ExpectedTaskVersion: taskVersion, ExpectedStepVersion: stepVersion, ResolvedAt: testTime(30)}); !errors.Is(err, storageport.ErrConflict) {
				t.Fatalf("second classification error = %v", err)
			}
			late := mustStepResult(t, stepID, "node-a", execution.StatusSucceeded, map[string]any{"late": true}, "")
			if _, _, err := repository.CompleteExecution(context.Background(), storageport.CompleteExecutionRequest{ExecutionID: attempt.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: attempt.Version, ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion, NewExecutionState: storageport.ExecutionStateSucceeded, NewStepState: lifecycle.StepStateSuccess, Result: &late, CompletedAt: testTime(40), Event: stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateSuccess, "late_result", testTime(40))}); !errors.Is(err, storageport.ErrConflict) {
				t.Fatalf("late result error = %v", err)
			}
		})
	}
}

func createInterruptedM4CTask(t *testing.T, repository *Repository, taskID model.TaskID, mode model.IdempotencyMode) (model.TaskID, model.StepID, int64, int64, storageport.Execution) {
	t.Helper()
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, taskID)
	if _, err := repository.db.Exec(`UPDATE task_steps SET idempotency_mode = ? WHERE task_id = ? AND step_id = ?`, mode, taskID, stepID); err != nil {
		t.Fatal(err)
	}
	taskVersion := int64(2)
	for _, transition := range []struct{ from, to lifecycle.State }{{lifecycle.StatePlanning, lifecycle.StateMapped}, {lifecycle.StateMapped, lifecycle.StateDispatched}, {lifecycle.StateDispatched, lifecycle.StateRunning}} {
		var err error
		taskVersion, err = repository.UpdateTaskState(context.Background(), taskID, transition.from, taskVersion, transition.to, "", "", taskEvent(taskID, transition.from, transition.to, "advance", testTime(5)))
		if err != nil {
			t.Fatal(err)
		}
	}
	attempt, stepVersion := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(10))
	return taskID, stepID, taskVersion, stepVersion, attempt
}
