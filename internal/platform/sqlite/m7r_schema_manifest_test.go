package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	storageport "dtm/internal/storage"
)

func TestM7RSchemaManifestRejectsStructuralDrift(t *testing.T) {
	tests := []struct {
		name        string
		definition  string
		createIndex string
	}{
		{name: "missing column", definition: agentExecutionsDDL(``, `status TEXT NOT NULL`)},
		{name: "missing index", definition: agentExecutionsDDL(`execution_error TEXT NOT NULL DEFAULT '',`, `status TEXT NOT NULL`)},
		{name: "not null", definition: agentExecutionsDDL(`execution_error TEXT NOT NULL DEFAULT '',`, `status TEXT`), createIndex: agentExecutionsIndex},
		{name: "column type", definition: agentExecutionsDDL(`execution_error TEXT NOT NULL DEFAULT '',`, `status TEXT NOT NULL`, `created_at INTEGER NOT NULL`), createIndex: agentExecutionsIndex},
		{name: "primary key", definition: agentExecutionsDDL(`execution_error TEXT NOT NULL DEFAULT '',`, `status TEXT NOT NULL`, `execution_id TEXT NOT NULL`), createIndex: agentExecutionsIndex},
		{name: "unique degraded", definition: agentExecutionsDDL(`execution_error TEXT NOT NULL DEFAULT '',`, `status TEXT NOT NULL`, `idempotency_key TEXT NOT NULL`), createIndex: agentExecutionsIndex + `; CREATE INDEX executions_idempotency_nonunique ON executions(idempotency_key) `},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "agent.db")
			repository, err := OpenExecutionRepository(path)
			if err != nil {
				t.Fatal(err)
			}
			_ = repository.Close()
			rebuildM7RTable(t, path, "executions", test.definition, test.createIndex)
			before := fileDigest(t, path)
			repository, err = OpenExecutionRepository(path)
			if repository != nil {
				_ = repository.Close()
			}
			if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
				t.Fatalf("OpenExecutionRepository() error = %v, want schema mismatch", err)
			}
			if after := fileDigest(t, path); after != before {
				t.Fatalf("structurally drifted database changed: before=%x after=%x", before, after)
			}
		})
	}
}

func TestM7RSchemaManifestRejectsForeignKeyDrift(t *testing.T) {
	for _, test := range []struct {
		name       string
		foreignKey string
	}{
		{name: "missing", foreignKey: ""},
		{name: "wrong target", foreignKey: `, FOREIGN KEY (task_id) REFERENCES wrong_targets(id)`},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "core.db")
			repository, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			_ = repository.Close()
			db := openM7RawDatabase(t, path)
			if test.foreignKey != "" {
				if _, err := db.Exec(`CREATE TABLE wrong_targets(id TEXT PRIMARY KEY)`); err != nil {
					t.Fatal(err)
				}
			}
			_ = db.Close()
			definition := `CREATE TABLE task_submission_keys (
idempotency_key TEXT PRIMARY KEY,
request_fingerprint TEXT NOT NULL,
task_id TEXT NOT NULL UNIQUE,
created_at INTEGER NOT NULL` + test.foreignKey + `)`
			rebuildM7RTable(t, path, "task_submission_keys", definition, "")
			repository, err = Open(path)
			if repository != nil {
				_ = repository.Close()
			}
			if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
				t.Fatalf("Open() error = %v, want schema mismatch", err)
			}
		})
	}
}

func TestM7RSchemaManifestValidatesDisabledMigrationAndSetIsolation(t *testing.T) {
	agentPath := filepath.Join(t.TempDir(), "agent.db")
	agent, err := OpenExecutionRepository(agentPath)
	if err != nil {
		t.Fatal(err)
	}
	_ = agent.Close()
	database, err := openDatabase(context.Background(), Options{Path: agentPath, MigrationSet: "core", AutoMigrate: false})
	if database != nil {
		_ = database.Close()
	}
	if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
		t.Fatalf("core accepted agent schema: %v", err)
	}
	rebuildM7RTable(t, agentPath, "executions", agentExecutionsDDL(`execution_error TEXT NOT NULL DEFAULT '',`, `status TEXT`), agentExecutionsIndex)
	database, err = openDatabase(context.Background(), Options{Path: agentPath, MigrationSet: "agent", AutoMigrate: false})
	if database != nil {
		_ = database.Close()
	}
	if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
		t.Fatalf("auto_migrate=false accepted drifted schema: %v", err)
	}
}

const agentExecutionsIndex = `CREATE INDEX executions_task_step ON executions(task_id, step_id)`

func agentExecutionsDDL(executionError, status string, replacements ...string) string {
	executionID := `execution_id TEXT PRIMARY KEY`
	idempotencyKey := `idempotency_key TEXT NOT NULL UNIQUE`
	createdAt := `created_at TEXT NOT NULL`
	for _, replacement := range replacements {
		switch {
		case replacement == `execution_id TEXT NOT NULL`:
			executionID = replacement
		case replacement == `idempotency_key TEXT NOT NULL`:
			idempotencyKey = replacement
		case replacement == `created_at INTEGER NOT NULL`:
			createdAt = replacement
		}
	}
	return `CREATE TABLE executions (
` + executionID + `,
task_id TEXT NOT NULL,
step_id TEXT NOT NULL,
` + idempotencyKey + `,
fingerprint_json TEXT NOT NULL,
` + status + `,
result_json TEXT NOT NULL DEFAULT '{}',
` + executionError + `
` + createdAt + `,
updated_at TEXT NOT NULL)`
}

func rebuildM7RTable(t *testing.T, path, table, definition, indexes string) {
	t.Helper()
	db := openM7RawDatabase(t, path)
	defer db.Close()
	if _, err := db.Exec(`PRAGMA foreign_keys=OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE ` + table + ` RENAME TO ` + table + `_m7r_old`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(definition); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE ` + table + `_m7r_old`); err != nil {
		t.Fatal(err)
	}
	if indexes != "" {
		if _, err := db.Exec(indexes); err != nil {
			t.Fatal(err)
		}
	}
}
