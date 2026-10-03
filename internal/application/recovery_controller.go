package application

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	"dtm/internal/model"
	"dtm/internal/storage"
)

var (
	ErrInvalidRecoveryRepository = errors.New("invalid recovery repository")
	ErrInvalidTaskRecoverer      = errors.New("invalid task recoverer")
	ErrInvalidRecoveryInterval   = errors.New("invalid recovery interval")
)

const defaultRecoveryBatchSize = 100

type RecoverableTaskRepository interface {
	FindRecoverable(context.Context, int) ([]TaskRecord, error)
}

type RecoverableTaskPageRepository interface {
	FindRecoverablePage(context.Context, int, *storage.RecoveryCursor, time.Time) ([]TaskRecord, *storage.RecoveryCursor, error)
}

type TaskRecoverer interface {
	Recover(context.Context, model.TaskID) error
}

type RecoveryController struct {
	repository RecoverableTaskRepository
	recoverer  TaskRecoverer
	interval   time.Duration

	mu         sync.Mutex
	inFlight   map[model.TaskID]struct{}
	active     sync.WaitGroup
	cursor     *storage.RecoveryCursor
	scanBefore time.Time
}

func NewRecoveryController(
	repository RecoverableTaskRepository,
	recoverer TaskRecoverer,
	interval time.Duration,
) (*RecoveryController, error) {
	if isNilRecoveryDependency(repository) {
		return nil, ErrInvalidRecoveryRepository
	}
	if isNilRecoveryDependency(recoverer) {
		return nil, ErrInvalidTaskRecoverer
	}
	if interval <= 0 {
		return nil, ErrInvalidRecoveryInterval
	}
	return &RecoveryController{
		repository: repository,
		recoverer:  recoverer,
		interval:   interval,
		inFlight:   make(map[model.TaskID]struct{}),
	}, nil
}

func (controller *RecoveryController) Run(ctx context.Context) <-chan error {
	errorChannel := make(chan error, 1)
	go func() {
		controller.scanBefore = time.Now().UTC()
		ticker := time.NewTicker(controller.interval)
		defer ticker.Stop()
		controller.scan(ctx, errorChannel)
		for {
			select {
			case <-ctx.Done():
				controller.active.Wait()
				close(errorChannel)
				return
			case <-ticker.C:
				controller.scan(ctx, errorChannel)
			}
		}
	}()
	return errorChannel
}

func (controller *RecoveryController) scan(ctx context.Context, errorChannel chan<- error) {
	records, err := controller.nextBatch(ctx)
	if err != nil {
		reportRecoveryError(errorChannel, err)
		return
	}
	for _, record := range records {
		taskID := record.Task.ID
		controller.mu.Lock()
		if _, exists := controller.inFlight[taskID]; exists {
			controller.mu.Unlock()
			continue
		}
		controller.inFlight[taskID] = struct{}{}
		controller.active.Add(1)
		controller.mu.Unlock()

		go func() {
			defer controller.active.Done()
			defer func() {
				controller.mu.Lock()
				delete(controller.inFlight, taskID)
				controller.mu.Unlock()
			}()
			if err := controller.recoverer.Recover(ctx, taskID); err != nil &&
				!errors.Is(err, context.Canceled) &&
				!errors.Is(err, ErrRecoveryPending) {
				reportRecoveryError(errorChannel, err)
			}
		}()
	}
}

func (controller *RecoveryController) nextBatch(ctx context.Context) ([]TaskRecord, error) {
	paged, ok := controller.repository.(RecoverableTaskPageRepository)
	if !ok {
		return controller.repository.FindRecoverable(ctx, defaultRecoveryBatchSize)
	}
	records, next, err := paged.FindRecoverablePage(ctx, defaultRecoveryBatchSize, controller.cursor, controller.scanBefore)
	if err != nil {
		return nil, err
	}
	if len(records) == 0 && controller.cursor != nil {
		controller.cursor = nil
		records, next, err = paged.FindRecoverablePage(ctx, defaultRecoveryBatchSize, nil, controller.scanBefore)
		if err != nil {
			return nil, err
		}
	}
	controller.cursor = next
	return records, nil
}

func reportRecoveryError(channel chan<- error, err error) {
	select {
	case channel <- err:
	default:
	}
}

func isNilRecoveryDependency(dependency any) bool {
	if dependency == nil {
		return true
	}
	value := reflect.ValueOf(dependency)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
