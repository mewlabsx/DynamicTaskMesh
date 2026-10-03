package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"dtm/internal/agentexecution"
	"dtm/internal/execution"
)

var ErrIdempotencyConflict = agentexecution.ErrConflict

type stepExecutionEntry struct {
	fingerprint agentexecution.Fingerprint
	done        chan struct{}
	result      execution.StepResult
	err         error
}

type stepExecutionStore struct {
	mu         sync.Mutex
	entries    map[string]*stepExecutionEntry
	repository agentexecution.Repository
}

func newStepExecutionStore(repository agentexecution.Repository) *stepExecutionStore {
	return &stepExecutionStore{
		entries:    make(map[string]*stepExecutionEntry),
		repository: repository,
	}
}

func (store *stepExecutionStore) execute(
	ctx context.Context,
	key string,
	fingerprint agentexecution.Fingerprint,
	run func() (execution.StepResult, error),
) (execution.StepResult, error, bool) {
	store.mu.Lock()
	entry, exists := store.entries[key]
	if exists {
		if !entry.fingerprint.Equal(fingerprint) {
			store.mu.Unlock()
			return execution.StepResult{}, ErrIdempotencyConflict, true
		}
		done := entry.done
		store.mu.Unlock()
		select {
		case <-ctx.Done():
			return execution.StepResult{}, ctx.Err(), true
		case <-done:
			return entry.result, entry.err, true
		}
	}
	persistCtx := context.WithoutCancel(ctx)
	record, findErr := store.repository.Find(persistCtx, key)
	switch {
	case findErr == nil:
		if !record.Fingerprint.Equal(fingerprint) {
			store.mu.Unlock()
			return execution.StepResult{}, ErrIdempotencyConflict, true
		}
		if record.Status == agentexecution.StatusCompleted {
			store.mu.Unlock()
			var replayErr error
			if record.ExecutionError != "" {
				replayErr = errors.New(record.ExecutionError)
			}
			return record.Result, replayErr, true
		}
	case !errors.Is(findErr, agentexecution.ErrNotFound):
		store.mu.Unlock()
		return execution.StepResult{}, fmt.Errorf("load idempotent execution: %w", findErr), true
	}
	entry = &stepExecutionEntry{
		fingerprint: fingerprint,
		done:        make(chan struct{}),
	}
	store.entries[key] = entry
	if err := store.repository.Start(persistCtx, key, fingerprint); err != nil {
		if errors.Is(err, agentexecution.ErrAlreadyComplete) {
			record, findErr = store.repository.Find(persistCtx, key)
			if findErr == nil && record.Status == agentexecution.StatusCompleted &&
				record.Fingerprint.Equal(fingerprint) {
				delete(store.entries, key)
				store.mu.Unlock()
				var replayErr error
				if record.ExecutionError != "" {
					replayErr = errors.New(record.ExecutionError)
				}
				return record.Result, replayErr, true
			}
		}
		delete(store.entries, key)
		store.mu.Unlock()
		if errors.Is(err, agentexecution.ErrConflict) {
			return execution.StepResult{}, ErrIdempotencyConflict, true
		}
		return execution.StepResult{}, fmt.Errorf("start idempotent execution: %w", err), false
	}
	store.mu.Unlock()

	result, err := run()
	persistResult := shouldPersistExecution(result, err)
	if persistResult {
		executionError := ""
		if err != nil {
			executionError = err.Error()
		}
		if persistErr := store.repository.Complete(
			persistCtx,
			key,
			fingerprint,
			result,
			executionError,
		); persistErr != nil {
			err = errors.Join(err, fmt.Errorf("persist idempotent execution: %w", persistErr))
		}
	}
	store.mu.Lock()
	entry.result = result
	entry.err = err
	close(entry.done)
	if !persistResult {
		delete(store.entries, key)
	}
	store.mu.Unlock()
	return result, err, false
}

func shouldPersistExecution(result execution.StepResult, err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	switch result.Status {
	case execution.StatusSucceeded:
		return err == nil
	case execution.StatusFailed:
		return true
	default:
		return false
	}
}
