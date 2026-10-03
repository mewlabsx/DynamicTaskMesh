package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	storageport "dtm/internal/storage"
)

func TestM7R2ApplyMigrationExecutionFaultsRollbackAndRetry(t *testing.T) {
	for _, test := range []struct {
		name     string
		point    migrationFaultPoint
		injected error
		want     error
		exitCode int
	}{
		{name: "sql exec busy", point: migrationFaultBeforeSQLExec, injected: storageport.ErrStorageUnavailable, want: storageport.ErrStorageUnavailable, exitCode: 11},
		{name: "sql exec locked", point: migrationFaultBeforeSQLExec, injected: storageport.ErrStorageUnavailable, want: storageport.ErrStorageUnavailable, exitCode: 11},
		{name: "sql exec readonly", point: migrationFaultBeforeSQLExec, injected: storageport.ErrStorageReadOnly, want: storageport.ErrStorageReadOnly, exitCode: 10},
		{name: "sql exec full", point: migrationFaultBeforeSQLExec, injected: storageport.ErrStorageFull, want: storageport.ErrStorageFull, exitCode: 10},
		{name: "sql exec io", point: migrationFaultBeforeSQLExec, injected: storageport.ErrStorageIO, want: storageport.ErrStorageIO, exitCode: 10},
		{name: "sql exec canceled", point: migrationFaultBeforeSQLExec, injected: context.Canceled, want: context.Canceled, exitCode: 1},
		{name: "sql exec deadline", point: migrationFaultBeforeSQLExec, injected: context.DeadlineExceeded, want: context.DeadlineExceeded, exitCode: 1},
		{name: "metadata full", point: migrationFaultBeforeMetadataInsert, injected: storageport.ErrStorageFull, want: storageport.ErrStorageFull, exitCode: 10},
		{name: "metadata io", point: migrationFaultBeforeMetadataInsert, injected: storageport.ErrStorageIO, want: storageport.ErrStorageIO, exitCode: 10},
		{name: "metadata busy", point: migrationFaultBeforeMetadataInsert, injected: storageport.ErrStorageUnavailable, want: storageport.ErrStorageUnavailable, exitCode: 11},
		{name: "metadata canceled", point: migrationFaultBeforeMetadataInsert, injected: context.Canceled, want: context.Canceled, exitCode: 1},
		{name: "commit boundary", point: migrationFaultBeforeCommit, injected: storageport.ErrStorageIO, want: storageport.ErrStorageIO, exitCode: 10},
		{name: "before begin", point: migrationFaultBeforeBegin, injected: storageport.ErrStorageUnavailable, want: storageport.ErrStorageUnavailable, exitCode: 11},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, err := sql.Open(defaultSQLDriverName, ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			migrations := m7r2ExecutionMigrations()
			injected := false
			err = applyMigrationsWithFaults(context.Background(), db, migrations, time.Now, func(_ context.Context, point migrationFaultPoint, _ migration) error {
				if !injected && point == test.point {
					injected = true
					return test.injected
				}
				return nil
			})
			if !injected {
				t.Fatalf("fault point %s was not reached", test.point)
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
			if code := storageport.StartupExitCode(err); code != test.exitCode {
				t.Fatalf("exit code=%d want=%d", code, test.exitCode)
			}
			assertM7R2MigrationState(t, db, 0, 0, 0)
			if err := applyMigrations(context.Background(), db, migrations, time.Now); err != nil {
				t.Fatalf("retry failed: %v", err)
			}
			assertM7R2MigrationState(t, db, 1, 1, 1)
		})
	}
}

func TestM7R2ApplyMigrationRealLockAtSQLExecution(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration-lock.db")
	db, err := sql.Open(defaultSQLDriverName, databaseSourceNameMode(path, "rwc"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA busy_timeout=25`); err != nil {
		t.Fatal(err)
	}
	if err := createMigrationTable(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	lock, err := sql.Open(defaultSQLDriverName, databaseSourceNameMode(path, "rw"))
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	lock.SetMaxOpenConns(1)
	if _, err := lock.Exec(`PRAGMA busy_timeout=1000`); err != nil {
		t.Fatal(err)
	}
	locked := false
	err = applyMigrationsWithFaults(context.Background(), db, m7r2ExecutionMigrations(), time.Now, func(_ context.Context, point migrationFaultPoint, _ migration) error {
		if point == migrationFaultBeforeSQLExec && !locked {
			if _, lockErr := lock.Exec(`BEGIN EXCLUSIVE`); lockErr != nil {
				t.Fatalf("acquire synchronized migration lock: %v", lockErr)
			}
			locked = true
		}
		return nil
	})
	if !locked {
		t.Fatal("synchronized lock point was not reached")
	}
	if !errors.Is(err, storageport.ErrStorageUnavailable) {
		t.Fatalf("error=%v, want unavailable", err)
	}
	if _, rollbackErr := lock.Exec(`ROLLBACK`); rollbackErr != nil {
		t.Fatal(rollbackErr)
	}
	assertM7R2MigrationState(t, db, 0, 0, 0)
	if err := applyMigrations(context.Background(), db, m7r2ExecutionMigrations(), time.Now); err != nil {
		t.Fatalf("retry after real lock release: %v", err)
	}
	assertM7R2MigrationState(t, db, 1, 1, 1)
}

func m7r2ExecutionMigrations() []migration {
	sqlText := `CREATE TABLE m7r2_migrated(id INTEGER PRIMARY KEY); CREATE INDEX idx_m7r2_migrated ON m7r2_migrated(id)`
	return []migration{{version: 1, name: "m7r2_execution", sql: sqlText, checksum: migrationChecksum(sqlText)}}
}

func assertM7R2MigrationState(t *testing.T, db *sql.DB, versions, tables, indexes int) {
	t.Helper()
	var gotVersions, gotTables, gotIndexes int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&gotVersions); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='m7r2_migrated'`).Scan(&gotTables); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type='index' AND name='idx_m7r2_migrated'`).Scan(&gotIndexes); err != nil {
		t.Fatal(err)
	}
	if gotVersions != versions || gotTables != tables || gotIndexes != indexes {
		t.Fatalf("migration state versions=%d tables=%d indexes=%d, want %d/%d/%d", gotVersions, gotTables, gotIndexes, versions, tables, indexes)
	}
}
