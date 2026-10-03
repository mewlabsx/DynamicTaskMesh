package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	storageport "dtm/internal/storage"
)

func TestDatabaseInitializesPragmasMigrationsAndHealth(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "dtm.db")
	database, err := openDatabase(context.Background(), Options{
		Path:         path,
		JournalMode:  "WAL",
		Synchronous:  "NORMAL",
		BusyTimeout:  1250 * time.Millisecond,
		AutoMigrate:  true,
		MigrationSet: "core",
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := database.SQLDB()
	if err != nil {
		t.Fatal(err)
	}
	assertPragma(t, db, "journal_mode", "wal")
	assertPragma(t, db, "synchronous", "1")
	assertPragma(t, db, "foreign_keys", "1")
	assertPragma(t, db, "busy_timeout", "1250")

	var version int
	var name string
	if err := db.QueryRow(`
SELECT version, name
FROM schema_migrations
WHERE version = 1`).Scan(&version, &name); err != nil {
		t.Fatal(err)
	}
	if version != 1 || name != "v026_core_schema" {
		t.Fatalf("migration = (%d, %q), want (1, %q)", version, name, "v026_core_schema")
	}
	if err := db.QueryRow(`
SELECT name
FROM schema_migrations
WHERE version = 2`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "m2_persistence_access" {
		t.Fatalf("migration 2 name = %q, want %q", name, "m2_persistence_access")
	}
	if err := database.Health(context.Background()); err != nil {
		t.Fatalf("Health() error = %v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := database.Health(context.Background()); !errors.Is(err, ErrDatabaseClosed) {
		t.Fatalf("Health() after Close error = %v, want ErrDatabaseClosed", err)
	}
}

func TestRepositoryRepeatedOpenAppliesMigrationOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dtm.db")
	for attempt := 0; attempt < 3; attempt++ {
		repository, err := Open(path)
		if err != nil {
			t.Fatalf("Open() attempt %d error = %v", attempt+1, err)
		}
		if err := repository.Health(context.Background()); err != nil {
			t.Fatalf("Health() attempt %d error = %v", attempt+1, err)
		}
		if err := repository.Close(); err != nil {
			t.Fatalf("Close() attempt %d error = %v", attempt+1, err)
		}
	}
	db, err := sql.Open(defaultSQLDriverName, "file:"+filepath.ToSlash(path)+"?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 6 {
		t.Fatalf("schema_migrations count = %d, want 6", count)
	}
}

func TestOpenDatabaseFailsWhenParentPathIsAFile(t *testing.T) {
	blockingPath := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blockingPath, []byte("blocked"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := openDatabase(context.Background(), Options{
		Path:         filepath.Join(blockingPath, "dtm.db"),
		AutoMigrate:  true,
		MigrationSet: "core",
	})
	if err == nil {
		t.Fatal("openDatabase() error = nil, want path failure")
	}
}

func TestApplyMigrationsRollsBackFailedMigration(t *testing.T) {
	db, err := sql.Open(defaultSQLDriverName, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	failure := []migration{{
		version: 1,
		name:    "fails_after_create",
		sql: `
CREATE TABLE must_be_rolled_back (id INTEGER PRIMARY KEY);
INSERT INTO missing_table(id) VALUES (1);`,
	}}
	err = applyMigrations(context.Background(), db, failure, time.Now)
	if err == nil || !strings.Contains(err.Error(), "apply sqlite migration 1 (fails_after_create)") {
		t.Fatalf("applyMigrations() error = %v, want identified migration failure", err)
	}
	var tableCount int
	if err := db.QueryRow(`
SELECT COUNT(*)
FROM sqlite_master
WHERE type = 'table' AND name = 'must_be_rolled_back'`).Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 0 {
		t.Fatalf("rolled-back table count = %d, want 0", tableCount)
	}
	var migrationCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM schema_migrations").Scan(&migrationCount); err != nil {
		t.Fatal(err)
	}
	if migrationCount != 0 {
		t.Fatalf("recorded failed migrations = %d, want 0", migrationCount)
	}
}

func TestCoreMigrationEnforcesForeignKeys(t *testing.T) {
	repository, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	_, err = repository.db.Exec(`
INSERT INTO task_steps (
	task_id, step_id, sequence_no, capability, input_json, state,
	attempt_count, max_attempts, created_at, updated_at, version
) VALUES ('missing-task', 'step-1', 0, 'temperature_sensor', '{}', 'created', 0, 1, 1, 1, 1)`)
	if err == nil {
		t.Fatal("orphan task step insert error = nil, want foreign-key failure")
	}
}

func TestMemoryDatabasesAreIsolatedByMigrationSet(t *testing.T) {
	core, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer core.Close()
	agent, err := OpenExecutionRepository(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer agent.Close()
	if status := core.StorageStatus(); status.MigrationSet != "core" {
		t.Fatalf("core migration set = %q", status.MigrationSet)
	}
	if status := agent.StorageStatus(); status.MigrationSet != "agent" {
		t.Fatalf("agent migration set = %q", status.MigrationSet)
	}
}

func assertPragma(t *testing.T, db *sql.DB, name, want string) {
	t.Helper()
	var got string
	if err := db.QueryRow("PRAGMA " + name).Scan(&got); err != nil {
		t.Fatalf("PRAGMA %s error = %v", name, err)
	}
	if got != want {
		t.Fatalf("PRAGMA %s = %q, want %q", name, got, want)
	}
}

var _ storageport.Database = (*Database)(nil)
