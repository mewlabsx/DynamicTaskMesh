package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"dtm/internal/agentexecution"
	"dtm/internal/execution"
	"dtm/internal/mapper"
)

func TestExecutionRepositoryPersistsCompletedResultAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.db")
	repository, err := OpenExecutionRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	fingerprint := executionFingerprint()
	result, err := execution.NewStepResult(
		"step-1",
		"node-1",
		execution.StatusSucceeded,
		map[string]any{"temperature": float64(30)},
		"",
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Start(context.Background(), "key-1", fingerprint); err != nil {
		t.Fatal(err)
	}
	if err := repository.Complete(context.Background(), "key-1", fingerprint, result, ""); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenExecutionRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	record, err := reopened.Find(context.Background(), "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != agentexecution.StatusCompleted ||
		!record.Fingerprint.Equal(fingerprint) ||
		!reflect.DeepEqual(record.Result, result) ||
		record.ExecutionID == "" {
		t.Fatalf("restored record = %#v", record)
	}
	if err := reopened.Start(context.Background(), "key-1", fingerprint); !errors.Is(err, agentexecution.ErrAlreadyComplete) {
		t.Fatalf("Start(completed) error = %v", err)
	}
}

func TestExecutionRepositoryMarksRunningRecordInterruptedOnRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.db")
	repository, err := OpenExecutionRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Start(context.Background(), "key-1", executionFingerprint()); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := OpenExecutionRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	record, err := reopened.Find(context.Background(), "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != agentexecution.StatusInterrupted {
		t.Fatalf("status = %q, want %q", record.Status, agentexecution.StatusInterrupted)
	}
	if err := reopened.Start(context.Background(), "key-1", executionFingerprint()); err != nil {
		t.Fatalf("restart interrupted execution error = %v", err)
	}
}

func TestExecutionRepositoryRejectsPersistentKeyConflict(t *testing.T) {
	repository, err := OpenExecutionRepository(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	first := executionFingerprint()
	if err := repository.Start(context.Background(), "shared-key", first); err != nil {
		t.Fatal(err)
	}
	different := first
	different.NodeID = "node-2"
	if err := repository.Start(context.Background(), "shared-key", different); !errors.Is(err, agentexecution.ErrConflict) {
		t.Fatalf("Start(conflict) error = %v", err)
	}
	result, _ := execution.NewStepResult(
		"step-1",
		"node-2",
		execution.StatusSucceeded,
		nil,
		"",
	)
	if err := repository.Complete(context.Background(), "shared-key", different, result, ""); !errors.Is(err, agentexecution.ErrConflict) {
		t.Fatalf("Complete(conflict) error = %v", err)
	}
}

func executionFingerprint() agentexecution.Fingerprint {
	return agentexecution.NewFingerprint("task-1", mapper.MappedStep{
		ID:         "step-1",
		Capability: "temperature_sensor",
		NodeID:     "node-1",
		Inputs:     map[string]string{"operation": "read_temperature"},
	})
}
