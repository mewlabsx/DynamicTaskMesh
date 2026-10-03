package application

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/storage"
	"dtm/internal/task"
)

type recoverableRepositoryFunc func(context.Context, int) ([]TaskRecord, error)

func (function recoverableRepositoryFunc) FindRecoverable(
	ctx context.Context,
	limit int,
) ([]TaskRecord, error) {
	return function(ctx, limit)
}

type taskRecovererFunc func(context.Context, model.TaskID) error

func (function taskRecovererFunc) Recover(ctx context.Context, taskID model.TaskID) error {
	return function(ctx, taskID)
}

type pagedRecoverableRepository struct {
	records []TaskRecord
}

func (repository *pagedRecoverableRepository) FindRecoverable(context.Context, int) ([]TaskRecord, error) {
	return append([]TaskRecord(nil), repository.records...), nil
}

func (repository *pagedRecoverableRepository) FindRecoverablePage(
	_ context.Context,
	limit int,
	after *storage.RecoveryCursor,
	_ time.Time,
) ([]TaskRecord, *storage.RecoveryCursor, error) {
	start := 0
	if after != nil {
		for index := range repository.records {
			if repository.records[index].Task.ID == after.TaskID {
				start = index + 1
				break
			}
		}
	}
	end := min(start+limit, len(repository.records))
	page := append([]TaskRecord(nil), repository.records[start:end]...)
	if len(page) < limit {
		return page, nil, nil
	}
	last := page[len(page)-1]
	return page, &storage.RecoveryCursor{UpdatedAt: last.UpdatedAt, TaskID: last.Task.ID}, nil
}

func TestRecoveryControllerSuppressesRepeatedPendingWithoutHidingErrors(t *testing.T) {
	input, err := task.New("task-pending", "recover", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("pending", func(t *testing.T) {
		var calls atomic.Int64
		controller, err := NewRecoveryController(
			recoverableRepositoryFunc(func(context.Context, int) ([]TaskRecord, error) {
				return []TaskRecord{{Task: input, State: lifecycle.StateRunning}}, nil
			}),
			taskRecovererFunc(func(context.Context, model.TaskID) error { calls.Add(1); return ErrRecoveryPending }),
			time.Millisecond,
		)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		errorChannel := controller.Run(ctx)
		deadline := time.After(time.Second)
		for calls.Load() < 3 {
			select {
			case reported := <-errorChannel:
				t.Fatalf("pending recovery reported error: %v", reported)
			case <-deadline:
				t.Fatalf("pending recovery calls = %d, want at least 3", calls.Load())
			case <-time.After(time.Millisecond):
			}
		}
		cancel()
		for reported := range errorChannel {
			t.Fatalf("pending recovery reported during shutdown: %v", reported)
		}
	})
	t.Run("real_error", func(t *testing.T) {
		recoveryFailure := errors.New("recovery failed")
		controller, err := NewRecoveryController(
			recoverableRepositoryFunc(func(context.Context, int) ([]TaskRecord, error) {
				return []TaskRecord{{Task: input, State: lifecycle.StateRunning}}, nil
			}),
			taskRecovererFunc(func(context.Context, model.TaskID) error { return recoveryFailure }),
			time.Hour,
		)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		errorChannel := controller.Run(ctx)
		select {
		case reported := <-errorChannel:
			if !errors.Is(reported, recoveryFailure) {
				t.Fatalf("reported error = %v", reported)
			}
		case <-time.After(time.Second):
			t.Fatal("real recovery error was not reported")
		}
		cancel()
		for range errorChannel {
		}
	})
}

func TestRecoveryControllerScansAndRecoversTask(t *testing.T) {
	input, err := task.New("task-recovery", "recover", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	called := make(chan model.TaskID, 1)
	controller, err := NewRecoveryController(
		recoverableRepositoryFunc(func(_ context.Context, limit int) ([]TaskRecord, error) {
			if limit != defaultRecoveryBatchSize {
				t.Fatalf("scan limit = %d", limit)
			}
			return []TaskRecord{{Task: input, State: lifecycle.StateRunning}}, nil
		}),
		taskRecovererFunc(func(_ context.Context, taskID model.TaskID) error {
			called <- taskID
			return nil
		}),
		time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errors := controller.Run(ctx)
	select {
	case got := <-called:
		if got != input.ID {
			t.Fatalf("recovered task = %q", got)
		}
	case <-time.After(time.Second):
		t.Fatal("controller did not scan immediately")
	}
	cancel()
	select {
	case _, open := <-errors:
		if open {
			t.Fatal("recovery error channel remained open")
		}
	case <-time.After(time.Second):
		t.Fatal("controller did not stop")
	}
}

func TestRecoveryControllerKeysetCursorPreventsPendingBatchStarvation(t *testing.T) {
	now := time.Date(2026, 8, 3, 1, 2, 3, 0, time.UTC)
	records := make([]TaskRecord, 0, defaultRecoveryBatchSize+1)
	for index := 0; index < defaultRecoveryBatchSize; index++ {
		input, err := task.New(model.TaskID(fmt.Sprintf("task-pending-%03d", index)), "recover", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		records = append(records, TaskRecord{Task: input, State: lifecycle.StateMapped, UpdatedAt: now})
	}
	eligible, err := task.New("task-z-eligible", "recover", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	records = append(records, TaskRecord{Task: eligible, State: lifecycle.StateRunning, UpdatedAt: now})
	recovered := make(chan struct{}, 1)
	var eligibleCalls atomic.Int64
	controller, err := NewRecoveryController(
		&pagedRecoverableRepository{records: records},
		taskRecovererFunc(func(_ context.Context, taskID model.TaskID) error {
			if taskID != eligible.ID {
				return ErrRecoveryPending
			}
			eligibleCalls.Add(1)
			select {
			case recovered <- struct{}{}:
			default:
			}
			return nil
		}),
		time.Millisecond,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errorChannel := controller.Run(ctx)
	select {
	case <-recovered:
	case reported := <-errorChannel:
		t.Fatalf("unexpected recovery error: %v", reported)
	case <-time.After(time.Second):
		t.Fatal("eligible task starved behind the first 100 pending tasks")
	}
	cancel()
	for reported := range errorChannel {
		t.Fatalf("unexpected shutdown error: %v", reported)
	}
	if eligibleCalls.Load() < 1 {
		t.Fatal("eligible task was not recovered")
	}
}
