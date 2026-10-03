package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"dtm/internal/agentexecution"
	storageport "dtm/internal/storage"
)

func TestM7RAgentExecutionRecordCorruptionIsIntegrityAndRedacted(t *testing.T) {
	tests := []struct {
		name   string
		update string
	}{
		{name: "fingerprint JSON", update: `fingerprint_json='{invalid'`},
		{name: "result JSON", update: `result_json='{invalid'`},
		{name: "status", update: `status='UNKNOWN_DATABASE_VALUE'`},
		{name: "timestamp", update: `created_at='not-a-time'`},
		{name: "required field", update: `task_id=''`},
		{name: "identity mismatch", update: `step_id='different-step'`},
		{name: "execution identity", update: `execution_id='different-execution'`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "agent-secret.db")
			const key = "m7r-secret-full-key"
			repository, err := OpenExecutionRepository(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := repository.Start(context.Background(), key, executionFingerprint()); err != nil {
				t.Fatal(err)
			}
			if _, err := repository.db.Exec(`UPDATE executions SET `+test.update+` WHERE idempotency_key=?`, key); err != nil {
				t.Fatal(err)
			}
			_, err = repository.Find(context.Background(), key)
			if !errors.Is(err, storageport.ErrStorageIntegrity) {
				t.Fatalf("Find() error = %v, want storage integrity", err)
			}
			assertM7RAgentIntegrityRedacted(t, err)
			if test.name == "fingerprint JSON" || test.name == "status" {
				err = repository.Start(context.Background(), key, executionFingerprint())
				if !errors.Is(err, storageport.ErrStorageIntegrity) {
					t.Fatalf("Start() error = %v, want storage integrity", err)
				}
				assertM7RAgentIntegrityRedacted(t, err)
			}
			_ = repository.Close()
		})
	}
}

func TestM7RAgentCompletedResultSemanticCorruptionIsIntegrity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.db")
	repository, err := OpenExecutionRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	const key = "m7r-result-key"
	if err := repository.Start(context.Background(), key, executionFingerprint()); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.db.Exec(`UPDATE executions SET status=?, result_json=? WHERE idempotency_key=?`, agentexecution.StatusCompleted, `{"step_id":"other","node_id":"node-1","status":"succeeded"}`, key); err != nil {
		t.Fatal(err)
	}
	_, err = repository.Find(context.Background(), key)
	if !errors.Is(err, storageport.ErrStorageIntegrity) {
		t.Fatalf("Find() error = %v, want storage integrity", err)
	}
	assertM7RAgentIntegrityRedacted(t, err)
	_ = repository.Close()
}

func assertM7RAgentIntegrityRedacted(t *testing.T, err error) {
	t.Helper()
	lower := strings.ToLower(err.Error())
	for _, forbidden := range []string{"m7r-secret", "{invalid", "fingerprint", "result_json", "created_at", "unknown_database_value", "agent-secret.db", "sqlite", "select", "update"} {
		if strings.Contains(lower, strings.ToLower(forbidden)) {
			t.Fatalf("integrity error leaked %q: %v", forbidden, err)
		}
	}
}
