package application

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"dtm/internal/model"
	"dtm/internal/storage"
)

var (
	ErrInvalidTaskQueryRepository = errors.New("invalid task query repository")
	ErrInvalidTaskQuery           = errors.New("invalid task query")
	ErrTaskQueryUnavailable       = errors.New("task query unavailable")
	ErrTaskQueryFailed            = errors.New("task query failed")
)

type TaskQueryRepository interface {
	GetTask(context.Context, model.TaskID) (storage.Task, error)
	ListTasks(context.Context, storage.TaskFilter) (storage.TaskPage, error)
	ListExecutionsByTask(context.Context, model.TaskID) ([]storage.Execution, error)
}

type TaskQueryService struct {
	repository TaskQueryRepository
}

func NewTaskQueryService(repository TaskQueryRepository) (*TaskQueryService, error) {
	if isNilPort(repository) {
		return nil, ErrInvalidTaskQueryRepository
	}
	return &TaskQueryService{repository: repository}, nil
}

func (service *TaskQueryService) GetTask(ctx context.Context, taskID model.TaskID) (storage.Task, error) {
	if strings.TrimSpace(string(taskID)) == "" {
		return storage.Task{}, ErrInvalidTaskQuery
	}
	record, err := service.repository.GetTask(ctx, taskID)
	if err != nil {
		return storage.Task{}, mapTaskQueryRepositoryError(err)
	}
	return record, nil
}

func (service *TaskQueryService) ListTasks(ctx context.Context, filter storage.TaskFilter) (storage.TaskPage, error) {
	page, err := service.repository.ListTasks(ctx, filter)
	if err != nil {
		return storage.TaskPage{}, mapTaskQueryRepositoryError(err)
	}
	if page.Tasks == nil {
		page.Tasks = []storage.Task{}
	}
	return page, nil
}

func (service *TaskQueryService) GetTaskExecutions(ctx context.Context, taskID model.TaskID) ([]storage.Execution, error) {
	if strings.TrimSpace(string(taskID)) == "" {
		return nil, ErrInvalidTaskQuery
	}
	records, err := service.repository.ListExecutionsByTask(ctx, taskID)
	if err != nil {
		return nil, mapTaskQueryRepositoryError(err)
	}
	if records == nil {
		records = []storage.Execution{}
	}
	return records, nil
}

func mapTaskQueryRepositoryError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return err
	case errors.Is(err, storage.ErrNotFound):
		return fmt.Errorf("%w: task", ErrTaskNotFound)
	case errors.Is(err, storage.ErrInvalidArgument):
		return fmt.Errorf("%w: query parameters", ErrInvalidTaskQuery)
	case errors.Is(err, storage.ErrUnavailable), errors.Is(err, storage.ErrClosed):
		return fmt.Errorf("%w: %v", ErrTaskQueryUnavailable, err)
	default:
		return fmt.Errorf("%w: %v", ErrTaskQueryFailed, err)
	}
}
