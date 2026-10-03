package application

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/task"
)

func TestAsyncTaskServiceAcceptsBeforeReturningAndDetachesFromRequestContext(t *testing.T) {
	runner := newBlockingAcceptedRunner()
	service, err := NewAsyncTaskService(context.Background(), runner)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	input := asyncTask(t, "task-1")
	requestCtx, cancelRequest := context.WithCancel(context.Background())

	record, err := service.Submit(requestCtx, input)
	if err != nil {
		t.Fatal(err)
	}
	cancelRequest()
	if record.Task.ID != input.ID || record.State != lifecycle.StateCreated {
		t.Fatalf("Submit() record = %#v", record)
	}
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("accepted task did not start")
	}
	runner.mu.Lock()
	runCtx := runner.runCtx
	runner.mu.Unlock()
	if err := runCtx.Err(); err != nil {
		t.Fatalf("background context was canceled with request: %v", err)
	}
	close(runner.release)
}

func TestAsyncTaskServiceCloseCancelsAndWaitsForActiveTasks(t *testing.T) {
	runner := newBlockingAcceptedRunner()
	service, err := NewAsyncTaskService(context.Background(), runner)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(context.Background(), asyncTask(t, "task-1")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("accepted task did not start")
	}

	closed := make(chan struct{})
	go func() {
		service.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close() did not cancel and wait for active task")
	}
	if _, err := service.Submit(context.Background(), asyncTask(t, "task-2")); !errors.Is(err, ErrAsyncTaskServiceClosed) {
		t.Fatalf("Submit() after Close error = %v", err)
	}
}

func TestAsyncTaskServiceRejectsDuplicateAndQueriesAcceptedTask(t *testing.T) {
	runner := newBlockingAcceptedRunner()
	service, err := NewAsyncTaskService(context.Background(), runner)
	if err != nil {
		t.Fatal(err)
	}
	input := asyncTask(t, "task-1")
	if _, err := service.Submit(context.Background(), input); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Submit(context.Background(), input); !errors.Is(err, ErrTaskAlreadyExists) {
		t.Fatalf("duplicate Submit() error = %v", err)
	}
	got, err := service.Find(context.Background(), input.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Task.ID != input.ID || got.State != lifecycle.StateCreated {
		t.Fatalf("Find() = %#v", got)
	}
	service.Close()
}

func TestAsyncTaskServiceIdempotentRetryDoesNotStartSecondWorker(t *testing.T) {
	runner := newBlockingAcceptedRunner()
	service, err := NewAsyncTaskService(context.Background(), runner)
	if err != nil {
		t.Fatal(err)
	}
	first := asyncTask(t, "task-first")
	retry := asyncTask(t, "task-retry")
	created, deduplicated, err := service.SubmitSubmission(context.Background(), first, "key-1", "v1:same")
	if err != nil || deduplicated || created.Task.ID != first.ID {
		t.Fatalf("first = (%#v, %t, %v)", created, deduplicated, err)
	}
	returned, deduplicated, err := service.SubmitSubmission(context.Background(), retry, "key-1", "v1:same")
	if err != nil || !deduplicated || returned.Task.ID != first.ID {
		t.Fatalf("retry = (%#v, %t, %v)", returned, deduplicated, err)
	}
	close(runner.release)
	service.Close()
	runner.mu.Lock()
	runs := runner.runs
	runner.mu.Unlock()
	if runs != 1 {
		t.Fatalf("worker runs = %d, want 1", runs)
	}
}

func TestNewAsyncTaskServiceRejectsNilRunner(t *testing.T) {
	var runner *blockingAcceptedRunner
	service, err := NewAsyncTaskService(context.Background(), runner)
	if service != nil || !errors.Is(err, ErrInvalidAcceptedTaskRunner) {
		t.Fatalf("NewAsyncTaskService() = %#v, %v", service, err)
	}
}

func TestAsyncTaskServiceAllowsEmptyKeyWithNonIdempotentRunner(t *testing.T) {
	runner := &nonIdempotentAcceptedRunner{}
	service, err := NewAsyncTaskService(context.Background(), runner)
	if err != nil {
		t.Fatal(err)
	}
	input := asyncTask(t, "task-compatible")
	if _, deduplicated, err := service.SubmitSubmission(context.Background(), input, "", ""); err != nil || deduplicated {
		t.Fatalf("empty-key submission = (%t, %v)", deduplicated, err)
	}
	service.Close()
	if runner.acceptCalls != 1 {
		t.Fatalf("Accept calls = %d, want 1", runner.acceptCalls)
	}
}

func TestAsyncTaskServiceRejectsKeyWhenRunnerDoesNotSupportIdempotency(t *testing.T) {
	runner := &nonIdempotentAcceptedRunner{}
	service, err := NewAsyncTaskService(context.Background(), runner)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	_, _, err = service.SubmitSubmission(context.Background(), asyncTask(t, "task-unsupported"), "key-1", "v1:same")
	if !errors.Is(err, ErrIdempotentSubmissionUnsupported) {
		t.Fatalf("unsupported runner error = %v", err)
	}
	if runner.acceptCalls != 0 {
		t.Fatalf("Accept calls = %d, want 0", runner.acceptCalls)
	}
}

type blockingAcceptedRunner struct {
	mu       sync.Mutex
	records  map[model.TaskID]TaskRecord
	started  chan struct{}
	release  chan struct{}
	runCtx   context.Context
	once     sync.Once
	runs     int
	bindings map[string]struct {
		fingerprint string
		taskID      model.TaskID
	}
}

type nonIdempotentAcceptedRunner struct {
	acceptCalls int
}

func (runner *nonIdempotentAcceptedRunner) Accept(_ context.Context, _ task.Task) error {
	runner.acceptCalls++
	return nil
}

func (*nonIdempotentAcceptedRunner) RunAccepted(_ context.Context, input task.Task) (Outcome, error) {
	return Outcome{TaskID: input.ID, State: lifecycle.StateSucceeded}, nil
}

func (*nonIdempotentAcceptedRunner) Find(_ context.Context, _ model.TaskID) (TaskRecord, error) {
	return TaskRecord{}, ErrTaskNotFound
}

func newBlockingAcceptedRunner() *blockingAcceptedRunner {
	return &blockingAcceptedRunner{
		records: make(map[model.TaskID]TaskRecord),
		started: make(chan struct{}),
		release: make(chan struct{}),
		bindings: make(map[string]struct {
			fingerprint string
			taskID      model.TaskID
		}),
	}
}

func (runner *blockingAcceptedRunner) AcceptSubmission(
	_ context.Context,
	input task.Task,
	key string,
	fingerprint string,
) (TaskRecord, bool, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if key == "" {
		if _, exists := runner.records[input.ID]; exists {
			return TaskRecord{}, false, ErrTaskAlreadyExists
		}
		record := TaskRecord{Task: input, State: lifecycle.StateCreated}
		runner.records[input.ID] = record
		return record, false, nil
	}
	if binding, exists := runner.bindings[key]; exists {
		if binding.fingerprint != fingerprint {
			return TaskRecord{}, false, ErrIdempotencyConflict
		}
		return runner.records[binding.taskID], true, nil
	}
	record := TaskRecord{Task: input, State: lifecycle.StateCreated}
	runner.records[input.ID] = record
	runner.bindings[key] = struct {
		fingerprint string
		taskID      model.TaskID
	}{fingerprint, input.ID}
	return record, false, nil
}

func (runner *blockingAcceptedRunner) Accept(_ context.Context, input task.Task) error {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if _, exists := runner.records[input.ID]; exists {
		return ErrTaskAlreadyExists
	}
	runner.records[input.ID] = TaskRecord{Task: input, State: lifecycle.StateCreated}
	return nil
}

func (runner *blockingAcceptedRunner) RunAccepted(ctx context.Context, input task.Task) (Outcome, error) {
	runner.mu.Lock()
	runner.runCtx = ctx
	runner.runs++
	runner.mu.Unlock()
	runner.once.Do(func() { close(runner.started) })
	select {
	case <-runner.release:
		return Outcome{TaskID: input.ID, State: lifecycle.StateSucceeded}, nil
	case <-ctx.Done():
		return Outcome{TaskID: input.ID, State: lifecycle.StateFailed}, ctx.Err()
	}
}

func (runner *blockingAcceptedRunner) Find(_ context.Context, taskID model.TaskID) (TaskRecord, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	record, exists := runner.records[taskID]
	if !exists {
		return TaskRecord{}, ErrTaskNotFound
	}
	return record, nil
}

func asyncTask(t *testing.T, id model.TaskID) task.Task {
	t.Helper()
	input, err := task.New(id, "cool_environment", nil, task.Constraints{"target_temperature": "26"})
	if err != nil {
		t.Fatal(err)
	}
	return input
}
