package application

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/planner"
	"dtm/internal/storage"
	"dtm/internal/task"
)

func TestSubmissionIdempotencyCrossesAsyncAndSyncEntrypoints(t *testing.T) {
	for _, asyncFirst := range []bool{true, false} {
		name := "sync_then_async"
		if asyncFirst {
			name = "async_then_sync"
		}
		t.Run(name, func(t *testing.T) {
			testSubmissionCrossMode(t, asyncFirst)
		})
	}
}

func testSubmissionCrossMode(t *testing.T, asyncFirst bool) {
	t.Helper()
	repository := &crossModeRepository{}
	first, basePlan, baseMapped, baseResult := validPipeline(t)
	retry, err := task.New("task-retry", first.Intent, first.Requirements, first.Constraints)
	if err != nil {
		t.Fatal(err)
	}
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var runs atomic.Int32
	service, err := NewTaskService(
		lifecycle.New,
		planFunc(func(input task.Task) (planner.Plan, error) {
			value := basePlan
			value.TaskID = input.ID
			return value, nil
		}),
		mapFunc(func(input planner.Plan) (mapper.MappedPlan, error) {
			value := baseMapped
			value.TaskID = input.TaskID
			return value, nil
		}),
		runFunc(func(_ context.Context, input mapper.MappedPlan) (execution.Result, error) {
			runs.Add(1)
			once.Do(func() { close(started) })
			<-release
			value := baseResult
			value.TaskID = input.TaskID
			return value, nil
		}),
		WithTaskRepository(repository),
	)
	if err != nil {
		t.Fatal(err)
	}
	async, err := NewAsyncTaskService(context.Background(), service)
	if err != nil {
		t.Fatal(err)
	}

	if asyncFirst {
		if _, deduplicated, err := async.SubmitSubmission(context.Background(), first, "cross-key", "v1:same"); err != nil || deduplicated {
			t.Fatalf("async first = (%t, %v)", deduplicated, err)
		}
		<-started
		outcome, deduplicated, err := service.SubmitSubmission(context.Background(), retry, "cross-key", "v1:same")
		if err != nil || !deduplicated || outcome.TaskID != first.ID {
			t.Fatalf("sync retry = (%#v, %t, %v)", outcome, deduplicated, err)
		}
		close(release)
		async.Close()
	} else {
		type syncResult struct {
			outcome      Outcome
			deduplicated bool
			err          error
		}
		completed := make(chan syncResult, 1)
		go func() {
			outcome, deduplicated, err := service.SubmitSubmission(context.Background(), first, "cross-key", "v1:same")
			completed <- syncResult{outcome, deduplicated, err}
		}()
		<-started
		record, deduplicated, err := async.SubmitSubmission(context.Background(), retry, "cross-key", "v1:same")
		if err != nil || !deduplicated || record.Task.ID != first.ID {
			t.Fatalf("async retry = (%#v, %t, %v)", record, deduplicated, err)
		}
		close(release)
		result := <-completed
		if result.err != nil || result.deduplicated || result.outcome.TaskID != model.TaskID("task-1") {
			t.Fatalf("sync first = %#v, error=%v", result, result.err)
		}
		async.Close()
	}
	if runs.Load() != 1 {
		t.Fatalf("runtime executions = %d, want 1", runs.Load())
	}
}

type crossModeRepository struct {
	noopTaskRepository
	mu          sync.Mutex
	task        storage.Task
	key         string
	fingerprint string
}

func (repository *crossModeRepository) CreateTaskSubmission(
	_ context.Context,
	request storage.CreateTaskSubmissionRequest,
) (storage.CreateTaskSubmissionResult, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.key != "" {
		if repository.key != request.IdempotencyKey || repository.fingerprint != request.RequestFingerprint {
			return storage.CreateTaskSubmissionResult{}, storage.ErrIdempotencyConflict
		}
		return storage.CreateTaskSubmissionResult{Task: repository.task, Deduplicated: true}, nil
	}
	repository.task, repository.key, repository.fingerprint = request.Task, request.IdempotencyKey, request.RequestFingerprint
	return storage.CreateTaskSubmissionResult{Task: request.Task, Created: true}, nil
}

func (repository *crossModeRepository) GetTask(_ context.Context, taskID model.TaskID) (storage.Task, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if repository.task.ID != taskID {
		return storage.Task{}, storage.ErrNotFound
	}
	return repository.task, nil
}
