package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestM7DatabaseRejectsFutureSchemaVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	db := openM7RawDatabase(t, path)
	if _, err := db.Exec(`INSERT INTO schema_migrations(version, name, checksum, applied_at) VALUES (999, 'future_schema', 'future-checksum', ?)`, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	before := fileDigest(t, path)
	reopened, err := Open(path)
	if reopened != nil {
		_ = reopened.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "database schema is newer than this binary") {
		t.Fatalf("Open() error = %v, want stable future-schema rejection", err)
	}
	if after := fileDigest(t, path); after != before {
		t.Fatalf("future-schema database changed: before=%x after=%x", before, after)
	}
}

func TestM7MigrationRejectsChecksumMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	db := openM7RawDatabase(t, path)
	if _, err := db.Exec(`UPDATE schema_migrations SET checksum = 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' WHERE version = 1`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	before := fileDigest(t, path)
	reopened, err := Open(path)
	if reopened != nil {
		_ = reopened.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "migration checksum mismatch") {
		t.Fatalf("Open() error = %v, want checksum mismatch", err)
	}
	if after := fileDigest(t, path); after != before {
		t.Fatalf("checksum-mismatched database changed: before=%x after=%x", before, after)
	}
}

func TestM7MigrationDisabledRejectsEmptyAndLaggingSchema(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, string)
	}{
		{name: "empty"},
		{name: "lagging", setup: func(t *testing.T, path string) {
			repository, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := repository.Close(); err != nil {
				t.Fatal(err)
			}
			db := openM7RawDatabase(t, path)
			if _, err := db.Exec(`DELETE FROM schema_migrations WHERE version = 6`); err != nil {
				t.Fatal(err)
			}
			_ = db.Close()
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "core.db")
			if test.setup != nil {
				test.setup(t, path)
			}
			database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: "core", AutoMigrate: false})
			if database != nil {
				_ = database.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "database schema does not match this binary") {
				t.Fatalf("openDatabase() error = %v, want disabled-migration schema rejection", err)
			}
		})
	}
}

func TestM7DatabaseRejectsForeignKeyIntegrityViolation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	db := openM7RawDatabase(t, path)
	if _, err := db.Exec(`PRAGMA foreign_keys=OFF`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO task_steps(task_id, step_id, sequence_no, capability, input_json, state, attempt_count, max_attempts, created_at, updated_at, version) VALUES ('missing-task', 'orphan-step', 0, 'temperature_sensor', '{}', 'created', 0, 1, 1, 1, 1)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	reopened, err := Open(path)
	if reopened != nil {
		_ = reopened.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "foreign key integrity check failed") {
		t.Fatalf("Open() error = %v, want foreign-key integrity rejection", err)
	}
}

func openM7RawDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open(defaultSQLDriverName, "file:"+filepath.ToSlash(path)+"?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	return db
}
