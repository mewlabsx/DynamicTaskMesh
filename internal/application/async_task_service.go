package application

import (
	"context"
	"errors"
	"sync"

	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/task"
)

var (
	ErrInvalidAcceptedTaskRunner = errors.New("invalid accepted task runner")
	ErrAsyncTaskServiceClosed    = errors.New("async task service is closed")
)

type AcceptedTaskRunner interface {
	Accept(context.Context, task.Task) error
	RunAccepted(context.Context, task.Task) (Outcome, error)
	Find(context.Context, model.TaskID) (TaskRecord, error)
}

type idempotentAcceptedTaskRunner interface {
	AcceptSubmission(context.Context, task.Task, string, string) (TaskRecord, bool, error)
}

type AsyncTaskService struct {
	runner AcceptedTaskRunner
	ctx    context.Context
	cancel context.CancelFunc

	mu     sync.Mutex
	closed bool
	active sync.WaitGroup
}

func NewAsyncTaskService(
	parent context.Context,
	runner AcceptedTaskRunner,
) (*AsyncTaskService, error) {
	if parent == nil {
		parent = context.Background()
	}
	if isNilPort(runner) {
		return nil, ErrInvalidAcceptedTaskRunner
	}
	ctx, cancel := context.WithCancel(parent)
	return &AsyncTaskService{
		runner: runner,
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

func (service *AsyncTaskService) Submit(
	ctx context.Context,
	input task.Task,
) (TaskRecord, error) {
	record, _, err := service.SubmitSubmission(ctx, input, "", "")
	return record, err
}

func (service *AsyncTaskService) SubmitSubmission(
	ctx context.Context,
	input task.Task,
	idempotencyKey string,
	requestFingerprint string,
) (TaskRecord, bool, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.closed || service.ctx.Err() != nil {
		return TaskRecord{}, false, ErrAsyncTaskServiceClosed
	}
	var record TaskRecord
	var deduplicated bool
	var err error
	if idempotencyKey != "" {
		runner, ok := service.runner.(idempotentAcceptedTaskRunner)
		if !ok {
			return TaskRecord{}, false, ErrIdempotentSubmissionUnsupported
		}
		record, deduplicated, err = runner.AcceptSubmission(ctx, input, idempotencyKey, requestFingerprint)
	} else {
		err = service.runner.Accept(ctx, input)
		record = TaskRecord{Task: input, State: lifecycle.StateCreated}
	}
	if err != nil {
		return TaskRecord{}, false, err
	}
	if deduplicated {
		return record, true, nil
	}

	service.active.Add(1)
	go func() {
		defer service.active.Done()
		_, _ = service.runner.RunAccepted(service.ctx, input)
	}()
	return record, false, nil
}

func (service *AsyncTaskService) Find(
	ctx context.Context,
	taskID model.TaskID,
) (TaskRecord, error) {
	return service.runner.Find(ctx, taskID)
}

func (service *AsyncTaskService) Close() {
	service.mu.Lock()
	if service.closed {
		service.mu.Unlock()
		return
	}
	service.closed = true
	service.cancel()
	service.mu.Unlock()
	service.active.Wait()
}

var _ AcceptedTaskRunner = (*TaskService)(nil)
var _ idempotentAcceptedTaskRunner = (*TaskService)(nil)
