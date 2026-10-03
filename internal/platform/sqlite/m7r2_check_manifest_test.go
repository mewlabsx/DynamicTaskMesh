package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	storageport "dtm/internal/storage"
)

const m7r2CheckTable = `CREATE TABLE check_subject (
version INTEGER NOT NULL CHECK (version > 0),
attempt_no INTEGER NOT NULL CHECK (attempt_no > 0),
max_attempts INTEGER NOT NULL CHECK (max_attempts > 0),
idempotency_mode TEXT NOT NULL CHECK (idempotency_mode IN ('unspecified', 'idempotent', 'non_idempotent')),
note TEXT NOT NULL DEFAULT 'literal CHECK(fake)')`

func TestM7R2CheckManifestRejectsMissingBoundaryAndEnumDrift(t *testing.T) {
	for _, test := range []struct {
		name string
		ddl  string
	}{
		{name: "missing version", ddl: `CREATE TABLE check_subject(version INTEGER NOT NULL, attempt_no INTEGER NOT NULL CHECK(attempt_no>0), max_attempts INTEGER NOT NULL CHECK(max_attempts>0), idempotency_mode TEXT NOT NULL CHECK(idempotency_mode IN('unspecified','idempotent','non_idempotent')), note TEXT NOT NULL DEFAULT 'literal CHECK(fake)')`},
		{name: "version boundary", ddl: `CREATE TABLE check_subject(version INTEGER NOT NULL CHECK(version>=0), attempt_no INTEGER NOT NULL CHECK(attempt_no>0), max_attempts INTEGER NOT NULL CHECK(max_attempts>0), idempotency_mode TEXT NOT NULL CHECK(idempotency_mode IN('unspecified','idempotent','non_idempotent')), note TEXT NOT NULL DEFAULT 'literal CHECK(fake)')`},
		{name: "missing attempt number", ddl: `CREATE TABLE check_subject(version INTEGER NOT NULL CHECK(version>0), attempt_no INTEGER NOT NULL, max_attempts INTEGER NOT NULL CHECK(max_attempts>0), idempotency_mode TEXT NOT NULL CHECK(idempotency_mode IN('unspecified','idempotent','non_idempotent')), note TEXT NOT NULL DEFAULT 'literal CHECK(fake)')`},
		{name: "max attempts boundary", ddl: `CREATE TABLE check_subject(version INTEGER NOT NULL CHECK(version>0), attempt_no INTEGER NOT NULL CHECK(attempt_no>0), max_attempts INTEGER NOT NULL CHECK(max_attempts>=0), idempotency_mode TEXT NOT NULL CHECK(idempotency_mode IN('unspecified','idempotent','non_idempotent')), note TEXT NOT NULL DEFAULT 'literal CHECK(fake)')`},
		{name: "missing idempotency check", ddl: `CREATE TABLE check_subject(version INTEGER NOT NULL CHECK(version>0), attempt_no INTEGER NOT NULL CHECK(attempt_no>0), max_attempts INTEGER NOT NULL CHECK(max_attempts>0), idempotency_mode TEXT NOT NULL, note TEXT NOT NULL DEFAULT 'literal CHECK(fake)')`},
		{name: "enum missing value", ddl: `CREATE TABLE check_subject(version INTEGER NOT NULL CHECK(version>0), attempt_no INTEGER NOT NULL CHECK(attempt_no>0), max_attempts INTEGER NOT NULL CHECK(max_attempts>0), idempotency_mode TEXT NOT NULL CHECK(idempotency_mode IN('unspecified','idempotent')), note TEXT NOT NULL DEFAULT 'literal CHECK(fake)')`},
		{name: "enum added value", ddl: `CREATE TABLE check_subject(version INTEGER NOT NULL CHECK(version>0), attempt_no INTEGER NOT NULL CHECK(attempt_no>0), max_attempts INTEGER NOT NULL CHECK(max_attempts>0), idempotency_mode TEXT NOT NULL CHECK(idempotency_mode IN('unspecified','idempotent','non_idempotent','unknown')), note TEXT NOT NULL DEFAULT 'literal CHECK(fake)')`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, migrations := createM7R2CheckFixture(t)
			defer db.Close()
			if _, err := db.Exec(`ALTER TABLE check_subject RENAME TO check_subject_old`); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(test.ddl); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`DROP TABLE check_subject_old`); err != nil {
				t.Fatal(err)
			}
			if err := verifyRequiredSchema(context.Background(), db, "core", migrations); !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
				t.Fatalf("verifyRequiredSchema() error=%v, want schema mismatch", err)
			}
		})
	}
}

func TestM7R2CheckFormattingEquivalenceAndQuotedContent(t *testing.T) {
	db, migrations := createM7R2CheckFixture(t)
	defer db.Close()
	if _, err := db.Exec(`ALTER TABLE check_subject RENAME TO check_subject_old`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE check_subject (
"version" INTEGER NOT NULL check (( "version"  >  0 )),
attempt_no INTEGER NOT NULL ChEcK( ATTEMPT_NO>0 ),
max_attempts INTEGER NOT NULL CHECK
(
MAX_ATTEMPTS > 0
),
idempotency_mode TEXT NOT NULL CHECK ( IDEMPOTENCY_MODE IN ( 'unspecified' , 'idempotent' , 'non_idempotent' ) ),
note TEXT NOT NULL DEFAULT 'literal CHECK(fake)')`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE check_subject_old`); err != nil {
		t.Fatal(err)
	}
	if err := verifyRequiredSchema(context.Background(), db, "core", migrations); err != nil {
		t.Fatalf("format-only CHECK changes rejected: %v", err)
	}
}

func TestM7R2CheckExtractorHandlesNestedParenthesesAndIgnoresStrings(t *testing.T) {
	checks, err := extractCheckConstraints(`CREATE TABLE t(
note TEXT DEFAULT 'CHECK(not_a_constraint)',
attempt_no INTEGER CHECK((attempt_no > 0) AND (coalesce(attempt_no, 0) > 0)))`)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"symbol:(|word:attempt_no|symbol:>|number:0|symbol:)|word:and|symbol:(|word:coalesce|symbol:(|word:attempt_no|symbol:,|number:0|symbol:)|symbol:>|number:0|symbol:)"}
	if !reflect.DeepEqual(checks, want) {
		t.Fatalf("checks=%q want=%q", checks, want)
	}
}

func TestM7R2CheckDriftRejectedWhenAutoMigrateDisabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	createM7R2DatabaseAtVersion(t, path, "core", 5)
	rebuildM7RTable(t, path, "nodes", `CREATE TABLE nodes (
node_id TEXT PRIMARY KEY, endpoint TEXT NOT NULL, capabilities_json TEXT NOT NULL,
status TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation >= 0),
created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
registration_id TEXT NOT NULL DEFAULT '', metadata_json TEXT NOT NULL DEFAULT '{}',
registered_at INTEGER NOT NULL DEFAULT 0, last_heartbeat_at INTEGER NOT NULL DEFAULT 0,
lease_expires_at INTEGER NOT NULL DEFAULT 0)`, `CREATE INDEX idx_nodes_status_lease_expiry ON nodes(status, lease_expires_at, node_id)`)
	database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: "core", AutoMigrate: false})
	if database != nil {
		_ = database.Close()
	}
	if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
		t.Fatalf("error=%v, want schema mismatch", err)
	}
}

func createM7R2CheckFixture(t *testing.T) (*sql.DB, []migration) {
	t.Helper()
	db, err := sql.Open(defaultSQLDriverName, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	migrations := []migration{{version: 1, name: "check_manifest", sql: m7r2CheckTable, checksum: migrationChecksum(m7r2CheckTable)}}
	if err := applyMigrations(context.Background(), db, migrations, time.Now); err != nil {
		db.Close()
		t.Fatal(err)
	}
	return db, migrations
}
