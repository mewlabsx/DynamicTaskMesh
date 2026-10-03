package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	storageport "dtm/internal/storage"
)

func TestM7R2ExistingDatabaseWithoutMigrationMetadataIsPreserved(t *testing.T) {
	path := filepath.Join(t.TempDir(), "valuable.db")
	db := openM7R2CreateDatabase(t, path)
	if _, err := db.Exec(`CREATE TABLE tasks(task_id TEXT PRIMARY KEY, valuable TEXT NOT NULL); INSERT INTO tasks VALUES ('legacy-1','preserve-me')`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	beforeDigest, beforeSize := fileDigest(t, path), fileSize(t, path)

	database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: "core", AutoMigrate: true})
	if database != nil {
		_ = database.Close()
	}
	if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
		t.Fatalf("openDatabase() error = %v, want schema mismatch", err)
	}
	assertFileUnchanged(t, path, beforeDigest, beforeSize)
	db = openM7RawDatabase(t, path)
	defer db.Close()
	var value string
	if err := db.QueryRow(`SELECT valuable FROM tasks WHERE task_id='legacy-1'`).Scan(&value); err != nil || value != "preserve-me" {
		t.Fatalf("preserved value = %q, err=%v", value, err)
	}
	var metadata int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='schema_migrations'`).Scan(&metadata); err != nil || metadata != 0 {
		t.Fatalf("schema_migrations count=%d err=%v", metadata, err)
	}
}

func TestM7R2ExistingEmptySQLiteAndZeroLengthFilesArePreserved(t *testing.T) {
	for _, test := range []struct {
		name   string
		create func(*testing.T, string)
	}{
		{name: "empty sqlite", create: func(t *testing.T, path string) {
			db := openM7R2CreateDatabase(t, path)
			if _, err := db.Exec(`VACUUM`); err != nil {
				t.Fatal(err)
			}
			_ = db.Close()
		}},
		{name: "zero length", create: func(t *testing.T, path string) {
			if err := os.WriteFile(path, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "existing.db")
			test.create(t, path)
			beforeDigest, beforeSize := fileDigest(t, path), fileSize(t, path)
			database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: "core", AutoMigrate: true})
			if database != nil {
				_ = database.Close()
			}
			if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
				t.Fatalf("openDatabase() error = %v, want schema mismatch", err)
			}
			assertFileUnchanged(t, path, beforeDigest, beforeSize)
		})
	}
}

func TestM7R2NewDatabaseInitializationStillWorksForCoreAndAgent(t *testing.T) {
	for _, set := range []string{"core", "agent"} {
		t.Run(set, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), set+".db")
			if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("path unexpectedly exists: %v", err)
			}
			database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: set, AutoMigrate: true})
			if err != nil {
				t.Fatal(err)
			}
			_ = database.Close()
			migrations, _ := embeddedMigrations(set)
			db := openM7R2CreateDatabase(t, path)
			defer db.Close()
			var count int
			if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil || count != len(migrations) {
				t.Fatalf("migration count=%d want=%d err=%v", count, len(migrations), err)
			}
		})
	}
}

func TestM7R2CoreAndAgentRejectOrdinarySQLiteWithoutModification(t *testing.T) {
	for _, set := range []string{"core", "agent"} {
		t.Run(set, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), set+".db")
			db := openM7R2CreateDatabase(t, path)
			if _, err := db.Exec(`CREATE TABLE unrelated(id INTEGER PRIMARY KEY, payload TEXT); INSERT INTO unrelated VALUES(1,'valuable')`); err != nil {
				t.Fatal(err)
			}
			_ = db.Close()
			beforeDigest, beforeSize := fileDigest(t, path), fileSize(t, path)
			database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: set, AutoMigrate: true})
			if database != nil {
				_ = database.Close()
			}
			if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
				t.Fatalf("error=%v, want schema mismatch", err)
			}
			assertFileUnchanged(t, path, beforeDigest, beforeSize)
		})
	}
}

func TestM7R2SchemaPrefixHealthyUpgradePreservesData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core-v4.db")
	createM7R2DatabaseAtVersion(t, path, "core", 4)
	db := openM7RawDatabase(t, path)
	if _, err := db.Exec(`INSERT INTO tasks(task_id,intent,requirements_json,constraints_json,status,created_at,updated_at,version) VALUES('task-prefix','keep','[]','{}','created',1,1,1)`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = repository.Close()
	db = openM7RawDatabase(t, path)
	defer db.Close()
	var versions, data int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&versions); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM tasks WHERE task_id='task-prefix' AND intent='keep'`).Scan(&data); err != nil {
		t.Fatal(err)
	}
	if versions != 6 || data != 1 {
		t.Fatalf("versions=%d data=%d", versions, data)
	}
}

func TestM7R2SchemaPrefixDriftIsRejectedBeforeMigration(t *testing.T) {
	for _, test := range []struct {
		name    string
		version int
		drift   func(*testing.T, string)
	}{
		{name: "missing column", version: 3, drift: func(t *testing.T, path string) {
			db := openM7RawDatabase(t, path)
			defer db.Close()
			if _, err := db.Exec(`ALTER TABLE nodes DROP COLUMN metadata_json`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "missing index", version: 4, drift: func(t *testing.T, path string) {
			db := openM7RawDatabase(t, path)
			defer db.Close()
			if _, err := db.Exec(`DROP INDEX idx_task_steps_recovery`); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "check drift", version: 4, drift: func(t *testing.T, path string) {
			rebuildM7RTable(t, path, "nodes", `CREATE TABLE nodes (
node_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL, capabilities_json TEXT NOT NULL,
status TEXT NOT NULL, generation INTEGER NOT NULL CHECK (generation >= 0),
created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
registration_id TEXT NOT NULL DEFAULT '', metadata_json TEXT NOT NULL DEFAULT '{}',
registered_at INTEGER NOT NULL DEFAULT 0, last_heartbeat_at INTEGER NOT NULL DEFAULT 0,
lease_expires_at INTEGER NOT NULL DEFAULT 0)`, `CREATE INDEX idx_nodes_status_lease_expiry ON nodes(status, lease_expires_at, node_id)`)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "drift.db")
			createM7R2DatabaseAtVersion(t, path, "core", test.version)
			test.drift(t, path)
			beforeDigest, beforeSize := fileDigest(t, path), fileSize(t, path)
			repository, err := Open(path)
			if repository != nil {
				_ = repository.Close()
			}
			if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
				t.Fatalf("Open() error=%v, want schema mismatch", err)
			}
			assertFileUnchanged(t, path, beforeDigest, beforeSize)
			db := openM7RawDatabase(t, path)
			defer db.Close()
			var maxVersion int
			if err := db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&maxVersion); err != nil || maxVersion != test.version {
				t.Fatalf("max version=%d want=%d err=%v", maxVersion, test.version, err)
			}
			var submissionTable int
			if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type='table' AND name='task_submission_keys'`).Scan(&submissionTable); err != nil {
				t.Fatal(err)
			}
			if submissionTable != 0 {
				t.Fatal("migration 5 structure was written before prefix rejection")
			}
		})
	}
}

func TestM7R2LegacyAdoptionValidatesSchemaBeforeWrite(t *testing.T) {
	for _, drift := range []bool{false, true} {
		name := "healthy"
		if drift {
			name = "drifted"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "legacy.db")
			createM7R2DatabaseAtVersion(t, path, "core", 4)
			makeM7R2MigrationMetadataLegacy(t, path)
			if drift {
				db := openM7RawDatabase(t, path)
				if _, err := db.Exec(`DROP INDEX idx_task_steps_recovery`); err != nil {
					t.Fatal(err)
				}
				_ = db.Close()
			}
			beforeDigest, beforeSize := fileDigest(t, path), fileSize(t, path)
			repository, err := Open(path)
			if drift {
				if repository != nil {
					_ = repository.Close()
				}
				if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
					t.Fatalf("error=%v, want schema mismatch", err)
				}
				assertFileUnchanged(t, path, beforeDigest, beforeSize)
				db := openM7RawDatabase(t, path)
				defer db.Close()
				var checksumColumn int
				if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('schema_migrations') WHERE name='checksum'`).Scan(&checksumColumn); err != nil || checksumColumn != 0 {
					t.Fatalf("checksum column=%d err=%v", checksumColumn, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			_ = repository.Close()
			db := openM7RawDatabase(t, path)
			defer db.Close()
			var versions, checksumColumn int
			_ = db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&versions)
			_ = db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('schema_migrations') WHERE name='checksum'`).Scan(&checksumColumn)
			if versions != 6 || checksumColumn != 1 {
				t.Fatalf("versions=%d checksumColumn=%d", versions, checksumColumn)
			}
		})
	}
}

func TestM7R2AgentEmptyManagedPrefixCanUpgrade(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent-empty-managed.db")
	db, err := sql.Open(defaultSQLDriverName, databaseSourceNameMode(path, "rwc"))
	if err != nil {
		t.Fatal(err)
	}
	if err := createMigrationTable(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	repository, err := OpenExecutionRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = repository.Close()
}

func createM7R2DatabaseAtVersion(t *testing.T, path, set string, version int) {
	t.Helper()
	migrations, err := embeddedMigrations(set)
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open(defaultSQLDriverName, databaseSourceNameMode(path, "rwc"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		t.Fatal(err)
	}
	if err := applyMigrations(context.Background(), db, migrations[:version], time.Now); err != nil {
		t.Fatal(err)
	}
}

func openM7R2CreateDatabase(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open(defaultSQLDriverName, databaseSourceNameMode(path, "rwc"))
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func makeM7R2MigrationMetadataLegacy(t *testing.T, path string) {
	t.Helper()
	db := openM7RawDatabase(t, path)
	defer db.Close()
	if _, err := db.Exec(`ALTER TABLE schema_migrations RENAME TO schema_migrations_m7r2;
CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL);
INSERT INTO schema_migrations(version,name,applied_at) SELECT version,name,applied_at FROM schema_migrations_m7r2;
DROP TABLE schema_migrations_m7r2`); err != nil {
		t.Fatal(err)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}

func assertFileUnchanged(t *testing.T, path string, digest [32]byte, size int64) {
	t.Helper()
	if after := fileDigest(t, path); after != digest {
		t.Fatalf("database SHA changed: before=%x after=%x", digest, after)
	}
	if after := fileSize(t, path); after != size {
		t.Fatalf("database size changed: before=%d after=%d", size, after)
	}
}
