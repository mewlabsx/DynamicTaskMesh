package sqlite

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	storageport "dtm/internal/storage"
)

func TestFindRecoverableTasksAfterAdvancesPastFullPendingBatch(t *testing.T) {
	repository := newRepository(t)
	for index := 0; index <= 100; index++ {
		record := baseTask(model.TaskID(fmt.Sprintf("task-%03d", index)), testTime(1))
		record.CreatedAt = testTime(1)
		record.UpdatedAt = testTime(1)
		if err := repository.CreateTask(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
	first, err := repository.FindRecoverableTasksAfter(context.Background(), 100, nil, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 100 || first[0].ID != "task-000" || first[99].ID != "task-099" {
		t.Fatalf("first page boundaries = len:%d first:%q last:%q", len(first), first[0].ID, first[len(first)-1].ID)
	}
	cursor := &storageport.RecoveryCursor{UpdatedAt: first[99].UpdatedAt, TaskID: first[99].ID}
	second, err := repository.FindRecoverableTasksAfter(context.Background(), 100, cursor, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || second[0].ID != "task-100" {
		t.Fatalf("second page = %#v", second)
	}
}

func TestFindRecoverableTasksAfterExcludesTasksCreatedAfterCoreStartup(t *testing.T) {
	repository := newRepository(t)
	before := baseTask("task-before-start", testTime(1))
	after := baseTask("task-after-start", testTime(3))
	// A startup candidate may be updated during recovery and must remain in the
	// cohort for a later node re-registration scan.
	before.UpdatedAt = testTime(4)
	if err := repository.CreateTask(context.Background(), before); err != nil {
		t.Fatal(err)
	}
	if err := repository.CreateTask(context.Background(), after); err != nil {
		t.Fatal(err)
	}
	records, err := repository.FindRecoverableTasksAfter(context.Background(), 100, nil, testTime(2))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ID != before.ID {
		t.Fatalf("startup recovery cohort = %#v", records)
	}
}

func TestRecoveryMappedRemapKeepsAggregateIdentityAndNoExecution(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	taskID, stepID := model.TaskID("task-mapped-remap"), model.StepID("step-1")
	if err := repository.CreateTask(ctx, baseTask(taskID, testTime(1))); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.RecordTaskPlan(ctx, taskID, lifecycle.StateCreated, 1, []storageport.TaskStep{baseStep(taskID, stepID, 0, testTime(1))}, taskEvent(taskID, lifecycle.StateCreated, lifecycle.StatePlanning, "planned", testTime(2))); err != nil {
		t.Fatal(err)
	}
	stepVersion, err := repository.RecordStepAssignment(ctx, taskID, stepID, lifecycle.StepStateCreated, 1, "node-a", stepEvent(taskID, stepID, lifecycle.StepStateCreated, lifecycle.StepStateMapped, "assigned", testTime(3)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.UpdateTaskState(ctx, taskID, lifecycle.StatePlanning, 2, lifecycle.StateMapped, "", "", taskEvent(taskID, lifecycle.StatePlanning, lifecycle.StateMapped, "mapped", testTime(20))); err != nil {
		t.Fatal(err)
	}
	newNodeID := model.NodeID("node-b")
	request := storageport.RecordTaskStepProgressRequest{
		Kind:   storageport.TaskStepProgressRecoveryRemap,
		TaskID: taskID, ExpectedTaskState: lifecycle.StateMapped, ExpectedTaskVersion: 3, NewTaskState: lifecycle.StateMapped,
		StepID: stepID, ExpectedStepState: lifecycle.StepStateMapped, ExpectedStepVersion: stepVersion, NewStepState: lifecycle.StepStateMapped,
		NewNodeID: &newNodeID, UpdatedAt: testTime(30),
		TaskEvent: taskEvent(taskID, lifecycle.StateMapped, lifecycle.StateMapped, "recovery_mapped_task_revalidated", testTime(30)),
		StepEvent: stepEvent(taskID, stepID, lifecycle.StepStateMapped, lifecycle.StepStateMapped, "recovery_mapped_step_remapped", testTime(30)),
	}
	if _, _, err := repository.RecordTaskStepProgress(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	record, err := repository.GetTask(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != lifecycle.StateMapped || len(record.Steps) != 1 || record.Steps[0].AssignedNodeID == nil || *record.Steps[0].AssignedNodeID != newNodeID {
		t.Fatalf("mapped remap record = %#v", record)
	}
	executions, err := repository.GetExecutions(context.Background(), taskID, stepID)
	if err != nil || len(executions) != 0 {
		t.Fatalf("remap created execution: %#v, %v", executions, err)
	}
}

func TestRecordTaskStepProgressCommitsAggregateSnapshot(t *testing.T) {
	for _, kind := range []storageport.TaskStepProgressKind{storageport.TaskStepProgressRetry, storageport.TaskStepProgressDispatch, storageport.TaskStepProgressRemap} {
		t.Run(string(kind), func(t *testing.T) {
			repository := newRepository(t)
			request := prepareM2DProgressRequest(t, repository, model.TaskID("task-m2d-success-"+string(kind)), kind)
			before, err := repository.GetTask(context.Background(), request.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			taskVersion, stepVersion, err := repository.RecordTaskStepProgress(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			after, err := repository.GetTask(context.Background(), request.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			step := after.Steps[0]
			if after.State != request.NewTaskState || step.State != request.NewStepState || taskVersion != before.Version+1 || stepVersion != before.Steps[0].Version+1 || after.Version != taskVersion || step.Version != stepVersion {
				t.Fatalf("aggregate snapshot versions/states: before=%#v after=%#v", before, after)
			}
			if step.Result != nil || step.FailureCode != "" || step.FailureMessage != "" || step.CompletedAt != nil {
				t.Fatalf("aggregate progress retained terminal data: %#v", step)
			}
			wantNode := model.NodeID("node-a")
			if kind == storageport.TaskStepProgressRemap {
				wantNode = "node-b"
			}
			if step.AssignedNodeID == nil || *step.AssignedNodeID != wantNode {
				t.Fatalf("aggregate progress node = %v, want %s", step.AssignedNodeID, wantNode)
			}
			events, err := repository.ListTaskEvents(context.Background(), request.TaskID)
			if err != nil {
				t.Fatal(err)
			}
			if len(events) < 2 || events[len(events)-2].Type != request.TaskEvent.Type || events[len(events)-1].Type != request.StepEvent.Type || events[len(events)-2].StepID != nil || events[len(events)-1].StepID == nil {
				t.Fatalf("aggregate progress events = %#v", events)
			}
			t.Logf("aggregate %s snapshot: task=%s v%d step=%s v%d node=%s task_event=%s step_event=%s", kind, after.State, after.Version, step.State, step.Version, *step.AssignedNodeID, events[len(events)-2].Type, events[len(events)-1].Type)
		})
	}
}

func TestRecordTaskStepProgressRollsBackEveryFailurePoint(t *testing.T) {
	failures := []struct {
		name    string
		install func(*testing.T, *Repository, *storageport.RecordTaskStepProgressRequest)
		want    error
	}{
		{"task_update", func(t *testing.T, r *Repository, _ *storageport.RecordTaskStepProgressRequest) {
			mustExecM2D(t, r, "CREATE TRIGGER fail_progress_task BEFORE UPDATE ON tasks BEGIN SELECT RAISE(ABORT, \"task update failed\"); END")
		}, nil},
		{"step_update", func(t *testing.T, r *Repository, _ *storageport.RecordTaskStepProgressRequest) {
			mustExecM2D(t, r, "CREATE TRIGGER fail_progress_step BEFORE UPDATE ON task_steps BEGIN SELECT RAISE(ABORT, \"step update failed\"); END")
		}, nil},
		{"task_event", func(t *testing.T, r *Repository, _ *storageport.RecordTaskStepProgressRequest) {
			mustExecM2D(t, r, "CREATE TRIGGER fail_progress_task_event BEFORE INSERT ON task_events WHEN NEW.step_id IS NULL BEGIN SELECT RAISE(ABORT, \"task event failed\"); END")
		}, nil},
		{"step_event", func(t *testing.T, r *Repository, _ *storageport.RecordTaskStepProgressRequest) {
			mustExecM2D(t, r, "CREATE TRIGGER fail_progress_step_event BEFORE INSERT ON task_events WHEN NEW.step_id IS NOT NULL BEGIN SELECT RAISE(ABORT, \"step event failed\"); END")
		}, nil},
		{"task_version", func(_ *testing.T, _ *Repository, request *storageport.RecordTaskStepProgressRequest) {
			request.ExpectedTaskVersion--
		}, storageport.ErrConflict},
		{"step_version", func(_ *testing.T, _ *Repository, request *storageport.RecordTaskStepProgressRequest) {
			request.ExpectedStepVersion--
		}, storageport.ErrConflict},
	}
	for _, kind := range []storageport.TaskStepProgressKind{storageport.TaskStepProgressRetry, storageport.TaskStepProgressDispatch, storageport.TaskStepProgressRemap} {
		for _, failure := range failures {
			t.Run(string(kind)+"/"+failure.name, func(t *testing.T) {
				repository := newRepository(t)
				request := prepareM2DProgressRequest(t, repository, model.TaskID("task-m2d-"+string(kind)+"-"+failure.name), kind)
				before, err := repository.GetTask(context.Background(), request.TaskID)
				if err != nil {
					t.Fatal(err)
				}
				eventsBefore, err := repository.ListTaskEvents(context.Background(), request.TaskID)
				if err != nil {
					t.Fatal(err)
				}
				failure.install(t, repository, &request)
				_, _, err = repository.RecordTaskStepProgress(context.Background(), request)
				if err == nil {
					t.Fatal("RecordTaskStepProgress() error = nil")
				}
				if failure.want != nil && !errors.Is(err, failure.want) {
					t.Fatalf("error = %v, want %v", err, failure.want)
				}
				if !strings.Contains(err.Error(), string(request.TaskID)) || !strings.Contains(err.Error(), string(request.StepID)) || !strings.Contains(err.Error(), string(kind)) {
					t.Fatalf("error lacks progress context: %v", err)
				}
				after, getErr := repository.GetTask(context.Background(), request.TaskID)
				if getErr != nil {
					t.Fatal(getErr)
				}
				eventsAfter, getErr := repository.ListTaskEvents(context.Background(), request.TaskID)
				if getErr != nil {
					t.Fatal(getErr)
				}
				if !reflect.DeepEqual(after, before) || !reflect.DeepEqual(eventsAfter, eventsBefore) {
					t.Fatalf("%s %s left half-state: before=%#v after=%#v events %d->%d", kind, failure.name, before, after, len(eventsBefore), len(eventsAfter))
				}
			})
		}
	}
}

func TestRecordTaskStepProgressRejectsInvalidRemapBeforeSQL(t *testing.T) {
	repository := newRepository(t)
	request := prepareM2DProgressRequest(t, repository, "task-m2d-remap-no-node", storageport.TaskStepProgressRemap)
	before, err := repository.GetTask(context.Background(), request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	request.NewNodeID = nil
	if _, _, err := repository.RecordTaskStepProgress(context.Background(), request); !errors.Is(err, storageport.ErrInvalidData) {
		t.Fatalf("missing node error = %v", err)
	}
	after, err := repository.GetTask(context.Background(), request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("invalid remap changed database: before=%#v after=%#v", before, after)
	}
}

func TestRecordTaskStepProgressAllowsSameOwnerNodeResourceRemap(t *testing.T) {
	repository := newRepository(t)
	request := prepareM2DProgressRequest(t, repository, "task-m2d-remap-same-node", storageport.TaskStepProgressRemap)
	current, err := repository.GetTask(context.Background(), request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Steps[0].AssignedNodeID == nil {
		t.Fatal("prepared remap has no current owner node")
	}
	sameNode := *current.Steps[0].AssignedNodeID
	request.NewNodeID = &sameNode
	if _, _, err := repository.RecordTaskStepProgress(context.Background(), request); err != nil {
		t.Fatalf("same-owner Resource remap error = %v", err)
	}
	updated, err := repository.GetTask(context.Background(), request.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if updated.Steps[0].AssignedNodeID == nil || *updated.Steps[0].AssignedNodeID != sameNode || updated.Version != current.Version+1 || updated.Steps[0].Version != current.Steps[0].Version+1 {
		t.Fatalf("same-owner remap snapshot = %#v, want advanced versions and node %q", updated, sameNode)
	}
}

func TestRecordTaskStepProgressAllowsPreAttemptStaleResourceRemap(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, _ := createDispatchedStep(t, repository, "task-m2d-remap-before-attempt")
	taskVersion := int64(2)
	for _, transition := range []struct {
		from lifecycle.State
		to   lifecycle.State
	}{{lifecycle.StatePlanning, lifecycle.StateMapped}, {lifecycle.StateMapped, lifecycle.StateDispatched}, {lifecycle.StateDispatched, lifecycle.StateRunning}} {
		var err error
		taskVersion, err = repository.UpdateTaskState(context.Background(), taskID, transition.from, taskVersion, transition.to, "", "", taskEvent(taskID, transition.from, transition.to, "advance", testTime(5)))
		if err != nil {
			t.Fatal(err)
		}
	}
	record, err := repository.GetTask(context.Background(), taskID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != lifecycle.StateRunning || record.Steps[0].State != lifecycle.StepStateDispatched || record.Steps[0].AssignedNodeID == nil {
		t.Fatalf("pre-attempt setup = %#v", record)
	}
	sameNode := *record.Steps[0].AssignedNodeID
	request := storageport.RecordTaskStepProgressRequest{
		Kind: storageport.TaskStepProgressRemap, TaskID: taskID,
		ExpectedTaskState: lifecycle.StateRunning, ExpectedTaskVersion: record.Version, NewTaskState: lifecycle.StateRemapped,
		StepID: stepID, ExpectedStepState: lifecycle.StepStateDispatched, ExpectedStepVersion: record.Steps[0].Version, NewStepState: lifecycle.StepStateRemapped,
		NewNodeID: &sameNode, UpdatedAt: testTime(30),
		TaskEvent: taskEvent(taskID, lifecycle.StateRunning, lifecycle.StateRemapped, "task_remapped", testTime(30)),
		StepEvent: stepEvent(taskID, stepID, lifecycle.StepStateDispatched, lifecycle.StepStateRemapped, "step_remapped", testTime(30)),
	}
	if _, _, err := repository.RecordTaskStepProgress(context.Background(), request); err != nil {
		t.Fatalf("pre-attempt stale Resource remap error = %v", err)
	}
}

func TestRecordTaskStepProgressClassifiesMissingObjectsAndContext(t *testing.T) {
	t.Run("task", func(t *testing.T) {
		repository := newRepository(t)
		request := m2DProgressRequest("missing-task", "step-1", storageport.TaskStepProgressRetry, 1, 1, nil)
		if _, _, err := repository.RecordTaskStepProgress(context.Background(), request); !errors.Is(err, storageport.ErrNotFound) {
			t.Fatalf("missing task error = %v", err)
		}
	})
	t.Run("step", func(t *testing.T) {
		repository := newRepository(t)
		request := prepareM2DProgressRequest(t, repository, "task-m2d-missing-step", storageport.TaskStepProgressRetry)
		request.StepID = "missing-step"
		stepID := request.StepID
		request.StepEvent.StepID = &stepID
		if _, _, err := repository.RecordTaskStepProgress(context.Background(), request); !errors.Is(err, storageport.ErrNotFound) {
			t.Fatalf("missing step error = %v", err)
		}
	})
	t.Run("context", func(t *testing.T) {
		repository := newRepository(t)
		request := prepareM2DProgressRequest(t, repository, "task-m2d-canceled", storageport.TaskStepProgressRetry)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, _, err := repository.RecordTaskStepProgress(ctx, request); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled error = %v", err)
		}
	})
}

func TestFindRecoverableTasksReturnsStartedExecutionAcrossRepeatedScans(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-m2d-started-filter")
	taskVersion := int64(2)
	for _, transition := range []struct{ from, to lifecycle.State }{{lifecycle.StatePlanning, lifecycle.StateMapped}, {lifecycle.StateMapped, lifecycle.StateDispatched}, {lifecycle.StateDispatched, lifecycle.StateRunning}} {
		var err error
		taskVersion, err = repository.UpdateTaskState(context.Background(), taskID, transition.from, taskVersion, transition.to, "", "", taskEvent(taskID, transition.from, transition.to, "advance", testTime(5)))
		if err != nil {
			t.Fatal(err)
		}
	}
	attempt, stepVersion := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(10))
	for scan := 1; scan <= 20; scan++ {
		records, err := repository.FindRecoverableTasks(context.Background(), 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 1 || records[0].ID != taskID {
			t.Fatalf("scan %d did not return task for deterministic STARTED classification: %#v", scan, records)
		}
	}
	failed := mustStepResult(t, stepID, "node-a", execution.StatusFailed, nil, "offline")
	_, _, err := repository.CompleteExecution(context.Background(), storageport.CompleteExecutionRequest{ExecutionID: attempt.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: attempt.Version, ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion, NewExecutionState: storageport.ExecutionStateFailed, NewStepState: lifecycle.StepStateFailed, Result: &failed, FailureCode: "offline", FailureMessage: "offline", CompletedAt: testTime(20), Event: stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateFailed, "execution_completed", testTime(20))})
	if err != nil {
		t.Fatal(err)
	}
	for scan := 1; scan <= 20; scan++ {
		records, err := repository.FindRecoverableTasks(context.Background(), 100)
		if err != nil {
			t.Fatal(err)
		}
		if len(records) != 1 || records[0].ID != taskID {
			t.Fatalf("post-completion scan %d records = %#v", scan, records)
		}
	}
	t.Logf("recovery scan evidence: started scans=20 and completed scans=20 returned task=%s", taskID)
}

func TestFindRecoverableTasksIncludesStartedExecutionForM4CClassification(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-m4c-started-classification")
	taskVersion := int64(2)
	for _, transition := range []struct{ from, to lifecycle.State }{{lifecycle.StatePlanning, lifecycle.StateMapped}, {lifecycle.StateMapped, lifecycle.StateDispatched}, {lifecycle.StateDispatched, lifecycle.StateRunning}} {
		var err error
		taskVersion, err = repository.UpdateTaskState(context.Background(), taskID, transition.from, taskVersion, transition.to, "", "", taskEvent(taskID, transition.from, transition.to, "advance", testTime(5)))
		if err != nil {
			t.Fatal(err)
		}
	}
	startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(10))
	records, err := repository.FindRecoverableTasks(context.Background(), 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].ID != taskID {
		t.Fatalf("started execution must be returned for conservative M4-C classification: %#v", records)
	}
}

func prepareM2DProgressRequest(t *testing.T, repository *Repository, taskID model.TaskID, kind storageport.TaskStepProgressKind) storageport.RecordTaskStepProgressRequest {
	t.Helper()
	taskID, stepID, taskVersion, stepVersion := prepareM2DFailedAttempt(t, repository, taskID)
	if kind == storageport.TaskStepProgressRetry {
		return m2DProgressRequest(taskID, stepID, kind, taskVersion, stepVersion, nil)
	}
	retry := m2DProgressRequest(taskID, stepID, storageport.TaskStepProgressRetry, taskVersion, stepVersion, nil)
	taskVersion, stepVersion, err := repository.RecordTaskStepProgress(context.Background(), retry)
	if err != nil {
		t.Fatal(err)
	}
	var newNodeID *model.NodeID
	if kind == storageport.TaskStepProgressRemap {
		nodeID := model.NodeID("node-b")
		newNodeID = &nodeID
	}
	return m2DProgressRequest(taskID, stepID, kind, taskVersion, stepVersion, newNodeID)
}

func m2DProgressRequest(taskID model.TaskID, stepID model.StepID, kind storageport.TaskStepProgressKind, taskVersion, stepVersion int64, nodeID *model.NodeID) storageport.RecordTaskStepProgressRequest {
	request := storageport.RecordTaskStepProgressRequest{Kind: kind, TaskID: taskID, ExpectedTaskVersion: taskVersion, StepID: stepID, ExpectedStepVersion: stepVersion, NewNodeID: nodeID, UpdatedAt: testTime(30)}
	switch kind {
	case storageport.TaskStepProgressRetry:
		request.ExpectedTaskState, request.NewTaskState = lifecycle.StateRunning, lifecycle.StateRetrying
		request.ExpectedStepState, request.NewStepState = lifecycle.StepStateFailed, lifecycle.StepStateRetrying
	case storageport.TaskStepProgressDispatch:
		request.ExpectedTaskState, request.NewTaskState = lifecycle.StateRetrying, lifecycle.StateDispatched
		request.ExpectedStepState, request.NewStepState = lifecycle.StepStateRetrying, lifecycle.StepStateDispatched
	case storageport.TaskStepProgressRemap:
		request.ExpectedTaskState, request.NewTaskState = lifecycle.StateRetrying, lifecycle.StateRemapped
		request.ExpectedStepState, request.NewStepState = lifecycle.StepStateRetrying, lifecycle.StepStateRemapped
	}
	request.TaskEvent = taskEvent(taskID, request.ExpectedTaskState, request.NewTaskState, "task_"+string(kind), request.UpdatedAt)
	request.StepEvent = stepEvent(taskID, stepID, request.ExpectedStepState, request.NewStepState, "step_"+string(kind), request.UpdatedAt)
	return request
}

func prepareM2DFailedAttempt(t *testing.T, repository *Repository, taskID model.TaskID) (model.TaskID, model.StepID, int64, int64) {
	t.Helper()
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, taskID)
	taskVersion := int64(2)
	for _, transition := range []struct{ from, to lifecycle.State }{{lifecycle.StatePlanning, lifecycle.StateMapped}, {lifecycle.StateMapped, lifecycle.StateDispatched}, {lifecycle.StateDispatched, lifecycle.StateRunning}} {
		var err error
		taskVersion, err = repository.UpdateTaskState(context.Background(), taskID, transition.from, taskVersion, transition.to, "", "", taskEvent(taskID, transition.from, transition.to, "advance", testTime(5)))
		if err != nil {
			t.Fatal(err)
		}
	}
	attempt, stepVersion := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(10))
	failed := mustStepResult(t, stepID, "node-a", execution.StatusFailed, nil, "offline")
	_, stepVersion, err := repository.CompleteExecution(context.Background(), storageport.CompleteExecutionRequest{ExecutionID: attempt.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: attempt.Version, ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion, NewExecutionState: storageport.ExecutionStateFailed, NewStepState: lifecycle.StepStateFailed, Result: &failed, FailureCode: "offline", FailureMessage: "offline", CompletedAt: testTime(20), Event: stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateFailed, "execution_completed", testTime(20))})
	if err != nil {
		t.Fatal(err)
	}
	return taskID, stepID, taskVersion, stepVersion
}

func mustExecM2D(t *testing.T, repository *Repository, statement string) {
	t.Helper()
	if _, err := repository.db.Exec(statement); err != nil {
		t.Fatal(err)
	}
}
