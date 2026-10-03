package application

import (
	"context"
	"errors"
	"testing"

	"dtm/internal/model"
	"dtm/internal/storage"
)

func TestTaskQueryServiceMapsRepositoryErrorsAndNormalizesEmptySlices(t *testing.T) {
	repository := &queryRepositoryStub{}
	service, err := NewTaskQueryService(repository)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.GetTask(context.Background(), " "); !errors.Is(err, ErrInvalidTaskQuery) {
		t.Fatalf("empty GetTask() error = %v", err)
	}
	repository.err = storage.ErrNotFound
	if _, err := service.GetTask(context.Background(), "missing"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("missing GetTask() error = %v", err)
	}
	repository.err = storage.ErrInvalidArgument
	if _, err := service.ListTasks(context.Background(), storage.TaskFilter{}); !errors.Is(err, ErrInvalidTaskQuery) {
		t.Fatalf("invalid ListTasks() error = %v", err)
	}
	repository.err = storage.ErrClosed
	if _, err := service.GetTaskExecutions(context.Background(), "task-1"); !errors.Is(err, ErrTaskQueryUnavailable) {
		t.Fatalf("closed executions error = %v", err)
	}
	repository.err = nil
	page, err := service.ListTasks(context.Background(), storage.TaskFilter{})
	if err != nil || page.Tasks == nil {
		t.Fatalf("empty ListTasks() = %#v, %v", page, err)
	}
	executions, err := service.GetTaskExecutions(context.Background(), "task-1")
	if err != nil || executions == nil {
		t.Fatalf("empty executions = %#v, %v", executions, err)
	}
}

type queryRepositoryStub struct{ err error }

func (repository *queryRepositoryStub) GetTask(context.Context, model.TaskID) (storage.Task, error) {
	return storage.Task{}, repository.err
}

func (repository *queryRepositoryStub) ListTasks(context.Context, storage.TaskFilter) (storage.TaskPage, error) {
	return storage.TaskPage{}, repository.err
}

func (repository *queryRepositoryStub) ListExecutionsByTask(context.Context, model.TaskID) ([]storage.Execution, error) {
	return nil, repository.err
}
