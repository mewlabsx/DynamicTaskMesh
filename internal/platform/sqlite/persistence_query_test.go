package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"dtm/internal/lifecycle"
	"dtm/internal/model"
	storageport "dtm/internal/storage"
)

func TestGetTaskQueryReturnsStableStepOrderAndMissingClassification(t *testing.T) {
	repository := newRepository(t)
	record := baseTask("task-query", testTime(0))
	record.Steps = []storageport.TaskStep{
		baseStep(record.ID, "step-b", 1, testTime(0)),
		baseStep(record.ID, "step-a", 0, testTime(0)),
	}
	if err := repository.CreateTask(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	got, err := repository.GetTask(context.Background(), record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Steps) != 2 || got.Steps[0].ID != "step-a" || got.Steps[1].ID != "step-b" {
		t.Fatalf("steps = %#v", got.Steps)
	}
	if _, err := repository.GetTask(context.Background(), "missing"); !errors.Is(err, storageport.ErrNotFound) {
		t.Fatalf("missing GetTask() error = %v", err)
	}
	if _, err := repository.GetTask(context.Background(), " "); !errors.Is(err, storageport.ErrInvalidArgument) {
		t.Fatalf("empty GetTask() error = %v", err)
	}
}

func TestListTasksFiltersOrdersAndUsesSummaryRecords(t *testing.T) {
	repository := newRepository(t)
	created := testTime(0)
	fixtures := []storageport.Task{
		baseTask("task-a", created),
		baseTask("task-c", created),
		baseTask("task-b", created.Add(time.Second)),
	}
	failed := baseTask("task-failed", created.Add(2*time.Second))
	failed.State = lifecycle.StateFailed
	failed.FailureCode = "failed"
	failed.FailureMessage = "fixture failure"
	failed.CompletedAt = pointerTime(failed.CreatedAt)
	fixtures = append(fixtures, failed)
	for _, record := range fixtures {
		if err := repository.CreateTask(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}

	page, err := repository.ListTasks(context.Background(), storageport.TaskFilter{})
	if err != nil {
		t.Fatal(err)
	}
	want := []model.TaskID{"task-failed", "task-b", "task-c", "task-a"}
	if len(page.Tasks) != len(want) || page.NextPageToken != "" {
		t.Fatalf("page = %#v", page)
	}
	for index := range want {
		if page.Tasks[index].ID != want[index] || page.Tasks[index].Steps != nil {
			t.Fatalf("page task %d = %#v, want %s summary", index, page.Tasks[index], want[index])
		}
	}

	status := lifecycle.StateFailed
	filtered, err := repository.ListTasks(context.Background(), storageport.TaskFilter{Status: &status})
	if err != nil || len(filtered.Tasks) != 1 || filtered.Tasks[0].ID != "task-failed" {
		t.Fatalf("status page = %#v, error = %v", filtered, err)
	}
	after := created
	before := created.Add(2 * time.Second)
	filtered, err = repository.ListTasks(context.Background(), storageport.TaskFilter{CreatedAfter: &after, CreatedBefore: &before})
	if err != nil || len(filtered.Tasks) != 1 || filtered.Tasks[0].ID != "task-b" {
		t.Fatalf("time page = %#v, error = %v", filtered, err)
	}
	after = created.Add(10 * time.Second)
	empty, err := repository.ListTasks(context.Background(), storageport.TaskFilter{CreatedAfter: &after})
	if err != nil || empty.Tasks == nil || len(empty.Tasks) != 0 {
		t.Fatalf("empty page = %#v, error = %v", empty, err)
	}
}

func TestListTasksCursorPaginationHasNoDuplicatesOrOmissionsAtEqualTimestamp(t *testing.T) {
	repository := newRepository(t)
	created := testTime(0)
	for _, id := range []model.TaskID{"task-a", "task-b", "task-c", "task-d", "task-e"} {
		if err := repository.CreateTask(context.Background(), baseTask(id, created)); err != nil {
			t.Fatal(err)
		}
	}
	seen := make(map[model.TaskID]bool)
	token := ""
	for {
		page, err := repository.ListTasks(context.Background(), storageport.TaskFilter{Limit: 2, PageToken: token})
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range page.Tasks {
			if seen[record.ID] {
				t.Fatalf("duplicate task %q", record.ID)
			}
			seen[record.ID] = true
		}
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}
	if len(seen) != 5 {
		t.Fatalf("seen = %v", seen)
	}
}

func TestListTasksPageBoundariesDefaultsAndFilterChange(t *testing.T) {
	repository := newRepository(t)
	created := testTime(0)
	for _, id := range []model.TaskID{"task-a", "task-b", "task-c"} {
		record := baseTask(id, created)
		if id == "task-b" {
			record.State = lifecycle.StateFailed
			record.FailureCode = "fixture"
			record.FailureMessage = "fixture failure"
			record.CompletedAt = pointerTime(created)
		}
		if err := repository.CreateTask(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}

	first, err := repository.ListTasks(context.Background(), storageport.TaskFilter{Limit: 1})
	if err != nil || len(first.Tasks) != 1 || first.Tasks[0].ID != "task-c" || first.NextPageToken == "" {
		t.Fatalf("first page = %#v, error = %v", first, err)
	}
	second, err := repository.ListTasks(context.Background(), storageport.TaskFilter{Limit: 1, PageToken: first.NextPageToken})
	if err != nil || len(second.Tasks) != 1 || second.Tasks[0].ID != "task-b" || second.NextPageToken == "" {
		t.Fatalf("middle page = %#v, error = %v", second, err)
	}
	last, err := repository.ListTasks(context.Background(), storageport.TaskFilter{Limit: 1, PageToken: second.NextPageToken})
	if err != nil || len(last.Tasks) != 1 || last.Tasks[0].ID != "task-a" || last.NextPageToken != "" {
		t.Fatalf("last page = %#v, error = %v", last, err)
	}

	defaultPage, err := repository.ListTasks(context.Background(), storageport.TaskFilter{})
	if err != nil || len(defaultPage.Tasks) != 3 || defaultPage.NextPageToken != "" {
		t.Fatalf("default page = %#v, error = %v", defaultPage, err)
	}
	maximumPage, err := repository.ListTasks(context.Background(), storageport.TaskFilter{Limit: storageport.MaxTaskPageLimit})
	if err != nil || len(maximumPage.Tasks) != 3 {
		t.Fatalf("maximum page = %#v, error = %v", maximumPage, err)
	}

	failed := lifecycle.StateFailed
	changedFilter, err := repository.ListTasks(context.Background(), storageport.TaskFilter{
		Status: &failed, Limit: 1, PageToken: first.NextPageToken,
	})
	if err != nil || len(changedFilter.Tasks) != 1 || changedFilter.Tasks[0].ID != "task-b" {
		t.Fatalf("changed-filter page = %#v, error = %v", changedFilter, err)
	}
	createdAfter := created
	empty, err := repository.ListTasks(context.Background(), storageport.TaskFilter{CreatedAfter: &createdAfter})
	if err != nil || empty.Tasks == nil || len(empty.Tasks) != 0 || empty.NextPageToken != "" {
		t.Fatalf("empty page = %#v, error = %v", empty, err)
	}
}

func TestListTasksValidatesTokenLimitTimeAndContext(t *testing.T) {
	repository := newRepository(t)
	unsupported, err := encodeTaskPageToken(taskPageCursor{Version: 99, CreatedAtNano: testTime(0).UnixNano(), TaskID: "task-a"})
	if err != nil {
		t.Fatal(err)
	}
	after, before := testTime(time.Second), testTime(0)
	overflow := time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)
	tests := []storageport.TaskFilter{
		{PageToken: "not-a-token"},
		{PageToken: unsupported},
		{Limit: -1},
		{Limit: storageport.MaxTaskPageLimit + 1},
		{CreatedAfter: &after, CreatedBefore: &before},
		{CreatedAfter: &overflow},
	}
	for _, filter := range tests {
		if _, err := repository.ListTasks(context.Background(), filter); !errors.Is(err, storageport.ErrInvalidArgument) {
			t.Fatalf("ListTasks(%#v) error = %v", filter, err)
		}
	}
	if _, err := repository.ListTasks(context.Background(), storageport.TaskFilter{Limit: storageport.MaxTaskPageLimit}); err != nil {
		t.Fatalf("maximum limit error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := repository.ListTasks(ctx, storageport.TaskFilter{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ListTasks() error = %v", err)
	}
}

func TestListExecutionsByTaskOrdersAttemptsAndDistinguishesMissingTask(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-executions-query")
	first, stepVersion := startAttempt(t, repository, taskID, stepID, 1, stepVersion, "node-a", testTime(10))
	failed := mustStepResult(t, stepID, "node-a", "failed", nil, "retry")
	_, stepVersion, err := repository.CompleteExecution(context.Background(), storageport.CompleteExecutionRequest{
		ExecutionID: first.ID, ExpectedExecutionState: storageport.ExecutionStateStarted, ExpectedExecutionVersion: first.Version,
		ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion,
		NewExecutionState: storageport.ExecutionStateFailed, NewStepState: lifecycle.StepStateFailed,
		Result: &failed, FailureCode: "retry", FailureMessage: "retry", CompletedAt: testTime(20),
		Event: stepEvent(taskID, stepID, lifecycle.StepStateRunning, lifecycle.StepStateFailed, "failed", testTime(20)),
	})
	if err != nil {
		t.Fatal(err)
	}
	stepVersion = updateStep(t, repository, taskID, stepID, lifecycle.StepStateFailed, stepVersion, lifecycle.StepStateRetrying, nil, testTime(30))
	stepVersion = updateStep(t, repository, taskID, stepID, lifecycle.StepStateRetrying, stepVersion, lifecycle.StepStateDispatched, nil, testTime(40))
	second, _ := startAttempt(t, repository, taskID, stepID, 2, stepVersion, "node-a", testTime(50))

	records, err := repository.ListExecutionsByTask(context.Background(), taskID)
	if err != nil || len(records) != 2 || records[0].ID != first.ID || records[1].ID != second.ID {
		t.Fatalf("executions = %#v, error = %v", records, err)
	}
	emptyTask := baseTask("task-no-executions", testTime(0))
	if err := repository.CreateTask(context.Background(), emptyTask); err != nil {
		t.Fatal(err)
	}
	empty, err := repository.ListExecutionsByTask(context.Background(), emptyTask.ID)
	if err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("empty executions = %#v, error = %v", empty, err)
	}
	if _, err := repository.ListExecutionsByTask(context.Background(), "missing"); !errors.Is(err, storageport.ErrNotFound) {
		t.Fatalf("missing executions error = %v", err)
	}
}

func TestTaskQueriesRemainValidDuringConcurrentWritesAndPagination(t *testing.T) {
	repository := newRepository(t)
	for index := 0; index < 12; index++ {
		id := model.TaskID("task-page-" + string(rune('a'+index)))
		if err := repository.CreateTask(context.Background(), baseTask(id, testTime(0))); err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	errorsFound := make(chan error, 9)
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			token := ""
			for {
				page, err := repository.ListTasks(context.Background(), storageport.TaskFilter{Limit: 3, PageToken: token})
				if err != nil {
					errorsFound <- err
					return
				}
				if page.NextPageToken == "" {
					return
				}
				token = page.NextPageToken
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		errorsFound <- repository.CreateTask(context.Background(), baseTask("task-concurrent-write", testTime(time.Second)))
	}()
	close(start)
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent task queries did not finish")
	}
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestExecutionWriteWaitDuringControlledReadIsQuantitativelyBounded(t *testing.T) {
	repository := newRepository(t)
	taskID, stepID, stepVersion := createDispatchedStep(t, repository, "task-during-read")

	queryStarted := time.Now()
	transaction, err := repository.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := transaction.QueryContext(context.Background(), "SELECT task_id FROM tasks ORDER BY task_id")
	if err != nil {
		_ = transaction.Rollback()
		t.Fatal(err)
	}
	if !rows.Next() {
		_ = rows.Close()
		_ = transaction.Rollback()
		t.Fatalf("controlled read returned no row: %v", rows.Err())
	}
	var heldTaskID string
	if err := rows.Scan(&heldTaskID); err != nil {
		_ = rows.Close()
		_ = transaction.Rollback()
		t.Fatal(err)
	}
	queryAcquired := time.Now()
	initialStats := repository.db.Stats()

	type interval struct {
		started  time.Time
		finished time.Time
		err      error
	}
	writeCalled := make(chan time.Time, 1)
	writeDone := make(chan interval, 1)
	go func() {
		measurement := interval{started: time.Now()}
		writeCalled <- measurement.started
		_, _, measurement.err = repository.StartExecution(context.Background(), storageport.StartExecutionRequest{
			ExecutionID: "execution-during-read", TaskID: taskID, StepID: stepID, AttemptNo: 1, NodeID: "node-a",
			Request: map[string]string{"operation": "measure"}, ExpectedStepState: lifecycle.StepStateDispatched,
			ExpectedStepVersion: stepVersion, StartedAt: testTime(time.Second),
			Event: stepEvent(taskID, stepID, lifecycle.StepStateDispatched, lifecycle.StepStateRunning, "execution_started", testTime(time.Second)),
		})
		measurement.finished = time.Now()
		writeDone <- measurement
	}()
	writeStarted := <-writeCalled
	waitDeadline := time.NewTimer(time.Second)
	defer waitDeadline.Stop()
	waitPoll := time.NewTicker(time.Millisecond)
	defer waitPoll.Stop()
	var waitingStats sql.DBStats
	var waitObserved time.Time
	waiting := false
	for !waiting {
		select {
		case measurement := <-writeDone:
			_ = rows.Close()
			_ = transaction.Rollback()
			t.Fatalf("execution write completed before entering the connection wait queue: %#v", measurement)
		case <-waitPoll.C:
			waitingStats = repository.db.Stats()
			if waitingStats.WaitCount > initialStats.WaitCount {
				waitObserved = time.Now()
				waiting = true
			}
		case <-waitDeadline.C:
			_ = rows.Close()
			_ = transaction.Rollback()
			t.Fatalf("execution write did not enter database/sql wait queue: initial=%+v current=%+v", initialStats, repository.db.Stats())
		}
	}
	select {
	case measurement := <-writeDone:
		_ = rows.Close()
		_ = transaction.Rollback()
		t.Fatalf("execution write completed while the read transaction still held the only connection: %#v", measurement)
	default:
	}
	if err := rows.Close(); err != nil {
		_ = transaction.Rollback()
		t.Fatal(err)
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}
	queryReleased := time.Now()

	var writeMeasurement interval
	select {
	case writeMeasurement = <-writeDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("execution write did not finish within 500ms after releasing the read transaction")
	}
	if writeMeasurement.err != nil {
		t.Fatalf("execution write error = %v", writeMeasurement.err)
	}
	if waitingStats.WaitCount <= initialStats.WaitCount {
		t.Fatalf("WaitCount did not increase: initial=%d observed=%d", initialStats.WaitCount, waitingStats.WaitCount)
	}
	releaseToCompletion := writeMeasurement.finished.Sub(queryReleased)
	if releaseToCompletion < 0 || releaseToCompletion > 500*time.Millisecond {
		t.Fatalf("execution write completion after release = %v", releaseToCompletion)
	}
	final, err := repository.GetTask(context.Background(), taskID)
	if err != nil || len(final.Steps) != 1 || final.Steps[0].State != lifecycle.StepStateRunning || final.Steps[0].AttemptCount != 1 {
		t.Fatalf("final written task = %#v, error = %v", final, err)
	}
	executionRecord, err := repository.GetExecution(context.Background(), "execution-during-read")
	if err != nil || executionRecord.State != storageport.ExecutionStateStarted || executionRecord.AttemptNo != 1 {
		t.Fatalf("final execution = %#v, error = %v", executionRecord, err)
	}
	t.Logf("contention evidence: initial_wait_count=%d observed_wait_count=%d initial_wait_duration=%s observed_wait_duration=%s query_start=%s query_acquired=%s write_call=%s wait_observed=%s query_release=%s write_finish=%s total_write_time=%s release_to_completion=%s deadline=false",
		initialStats.WaitCount, waitingStats.WaitCount, initialStats.WaitDuration, waitingStats.WaitDuration,
		queryStarted.UTC().Format(time.RFC3339Nano), queryAcquired.UTC().Format(time.RFC3339Nano),
		writeStarted.UTC().Format(time.RFC3339Nano), waitObserved.UTC().Format(time.RFC3339Nano),
		queryReleased.UTC().Format(time.RFC3339Nano), writeMeasurement.finished.UTC().Format(time.RFC3339Nano),
		writeMeasurement.finished.Sub(writeMeasurement.started), releaseToCompletion)
}

func TestNormalTaskQueriesAndExecutionWriteHaveNoSecondScaleBlocking(t *testing.T) {
	repository := newRepository(t)
	created := testTime(0)
	for index := 0; index < 200; index++ {
		taskID := model.TaskID(fmt.Sprintf("task-load-%03d", index))
		record := baseTask(taskID, created.Add(time.Duration(index)*time.Nanosecond))
		record.Steps = []storageport.TaskStep{
			baseStep(taskID, "step-1", 0, record.CreatedAt),
			baseStep(taskID, "step-2", 1, record.CreatedAt),
			baseStep(taskID, "step-3", 2, record.CreatedAt),
		}
		if err := repository.CreateTask(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
	writeTaskID, writeStepID, writeStepVersion := createDispatchedStep(t, repository, "task-load-write")

	type measurement struct {
		name     string
		duration time.Duration
		err      error
	}
	start := make(chan struct{})
	results := make(chan measurement, 4)
	measure := func(name string, operation func() error) {
		go func() {
			<-start
			started := time.Now()
			err := operation()
			results <- measurement{name: name, duration: time.Since(started), err: err}
		}()
	}
	measure("ListTasks", func() error {
		page, err := repository.ListTasks(context.Background(), storageport.TaskFilter{Limit: storageport.MaxTaskPageLimit})
		if err == nil && len(page.Tasks) != storageport.MaxTaskPageLimit {
			return fmt.Errorf("ListTasks returned %d tasks", len(page.Tasks))
		}
		return err
	})
	measure("GetTask", func() error {
		record, err := repository.GetTask(context.Background(), "task-load-100")
		if err == nil && len(record.Steps) != 3 {
			return fmt.Errorf("GetTask returned %d steps", len(record.Steps))
		}
		return err
	})
	measure("GetTaskExecutions", func() error {
		records, err := repository.ListExecutionsByTask(context.Background(), writeTaskID)
		if err == nil && len(records) > 1 {
			return fmt.Errorf("GetTaskExecutions returned %d records", len(records))
		}
		return err
	})
	measure("StartExecution", func() error {
		_, _, err := repository.StartExecution(context.Background(), storageport.StartExecutionRequest{
			ExecutionID: "execution-load-write", TaskID: writeTaskID, StepID: writeStepID, AttemptNo: 1, NodeID: "node-a",
			Request: map[string]string{"operation": "load"}, ExpectedStepState: lifecycle.StepStateDispatched,
			ExpectedStepVersion: writeStepVersion, StartedAt: testTime(time.Second),
			Event: stepEvent(writeTaskID, writeStepID, lifecycle.StepStateDispatched, lifecycle.StepStateRunning, "execution_started", testTime(time.Second)),
		})
		return err
	})
	close(start)

	measurements := make(map[string]time.Duration, 4)
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for len(measurements) < 4 {
		select {
		case result := <-results:
			if result.err != nil {
				t.Fatalf("%s error = %v", result.name, result.err)
			}
			if result.duration > time.Second {
				t.Fatalf("%s duration = %v, want below one second", result.name, result.duration)
			}
			measurements[result.name] = result.duration
		case <-deadline.C:
			t.Fatalf("normal query load did not finish: %v", measurements)
		}
	}
	executionRecord, err := repository.GetExecution(context.Background(), "execution-load-write")
	if err != nil || executionRecord.State != storageport.ExecutionStateStarted {
		t.Fatalf("load execution = %#v, error = %v", executionRecord, err)
	}
	t.Logf("normal query load evidence: tasks=201 steps=601 list_tasks=%s get_task=%s get_task_executions=%s start_execution=%s",
		measurements["ListTasks"], measurements["GetTask"], measurements["GetTaskExecutions"], measurements["StartExecution"])
}

func TestGetTaskQueryReturnsCommittedSnapshotDuringStateTransition(t *testing.T) {
	repository := newRepository(t)
	taskID := model.TaskID("task-snapshot")
	if err := repository.CreateTask(context.Background(), baseTask(taskID, testTime(0))); err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	errorsFound := make(chan error, 7)
	var workers sync.WaitGroup
	for worker := 0; worker < 6; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for attempt := 0; attempt < 20; attempt++ {
				record, err := repository.GetTask(context.Background(), taskID)
				if err != nil {
					errorsFound <- err
					return
				}
				if record.State != lifecycle.StateCreated && record.State != lifecycle.StateFailed {
					errorsFound <- errors.New("GetTask returned a non-committed state")
					return
				}
			}
		}()
	}
	workers.Add(1)
	go func() {
		defer workers.Done()
		<-start
		_, err := repository.UpdateTaskState(
			context.Background(), taskID, lifecycle.StateCreated, 1, lifecycle.StateFailed,
			"fixture_failure", "fixture failure",
			storageport.TaskEvent{TaskID: taskID, Type: "task_failed", FromState: string(lifecycle.StateCreated), ToState: string(lifecycle.StateFailed), CreatedAt: testTime(time.Second)},
		)
		errorsFound <- err
	}()
	close(start)
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent GetTask queries did not finish")
	}
	close(errorsFound)
	for err := range errorsFound {
		if err != nil {
			t.Fatal(err)
		}
	}
	final, err := repository.GetTask(context.Background(), taskID)
	if err != nil || final.State != lifecycle.StateFailed {
		t.Fatalf("final task = %#v, error = %v", final, err)
	}
}

func TestSQLiteQueryDatabaseCanCloseAndDeleteOnWindows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query-close.db")
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.ListTasks(context.Background(), storageport.TaskFilter{}); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.ListTasks(context.Background(), storageport.TaskFilter{}); !errors.Is(err, storageport.ErrClosed) {
		t.Fatalf("closed ListTasks() error = %v", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatalf("remove closed sqlite database: %v", err)
	}
}

func TestTaskQueryBusyLockIsUnavailableAndRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "query-busy.db")
	repository, err := OpenWithOptions(Options{
		Path: path, JournalMode: "DELETE", BusyTimeout: 25 * time.Millisecond, AutoMigrate: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	if err := repository.CreateTask(context.Background(), baseTask("task-busy", testTime(0))); err != nil {
		t.Fatal(err)
	}
	dsn, err := databaseSourceName(path)
	if err != nil {
		t.Fatal(err)
	}
	blocker, err := sql.Open(defaultSQLDriverName, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = blocker.Close() })
	blocker.SetMaxOpenConns(1)
	connection, err := blocker.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if _, err := connection.ExecContext(context.Background(), "PRAGMA busy_timeout = 0"); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.ExecContext(context.Background(), "BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	locked := true
	t.Cleanup(func() {
		if locked {
			_, _ = connection.ExecContext(context.Background(), "ROLLBACK")
		}
	})

	started := time.Now()
	_, err = repository.ListTasks(context.Background(), storageport.TaskFilter{})
	waited := time.Since(started)
	if !errors.Is(err, storageport.ErrUnavailable) {
		t.Fatalf("locked ListTasks() error = %v", err)
	}
	if waited < 15*time.Millisecond || waited > 500*time.Millisecond {
		t.Fatalf("locked ListTasks() waited %v, want a bounded busy-timeout wait", waited)
	}

	deadlineContext, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-deadlineContext.Done()
	if _, err := repository.ListTasks(deadlineContext, storageport.TaskFilter{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired-context ListTasks() error = %v", err)
	}

	if _, err := connection.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
	locked = false
	page, err := repository.ListTasks(context.Background(), storageport.TaskFilter{})
	if err != nil || len(page.Tasks) != 1 || page.Tasks[0].ID != "task-busy" {
		t.Fatalf("recovered ListTasks() page = %#v, error = %v", page, err)
	}
}

func TestTaskQueryLockedCodeIsUnavailable(t *testing.T) {
	for _, code := range []int{sqlitePrimaryBusy, sqlitePrimaryLocked, sqlitePrimaryBusy | 3<<8, sqlitePrimaryLocked | 1<<8} {
		if !isSQLiteTransientQueryCode(code) {
			t.Fatalf("SQLite transient code %d was not classified", code)
		}
	}
	for _, code := range []int{0, 1, 11, 26} {
		if isSQLiteTransientQueryCode(code) {
			t.Fatalf("SQLite internal code %d was classified as transient", code)
		}
	}
}

func pointerTime(value time.Time) *time.Time { return &value }
