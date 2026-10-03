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

func TestM7R3MigrationMetadataManifestRejectsDriftBeforeWrite(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *sql.DB)
	}{
		{name: "primary key", mutate: rebuildM7R3Metadata(`version INTEGER NOT NULL, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL`)},
		{name: "not null", mutate: rebuildM7R3Metadata(`version INTEGER PRIMARY KEY, name TEXT, checksum TEXT NOT NULL, applied_at TEXT NOT NULL`)},
		{name: "column type", mutate: rebuildM7R3Metadata(`version TEXT PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL`)},
		{name: "extra column", mutate: rebuildM7R3Metadata(`version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT NOT NULL, applied_at TEXT NOT NULL, unexpected TEXT`)},
		{name: "trigger", mutate: func(t *testing.T, db *sql.DB) {
			execM7R3(t, db, `CREATE TRIGGER mutate_after_migration AFTER INSERT ON schema_migrations BEGIN UPDATE schema_migrations SET name='mutated' WHERE version=1; END`)
		}},
		{name: "view", mutate: func(t *testing.T, db *sql.DB) {
			execM7R3(t, db, `CREATE VIEW migration_versions AS SELECT version FROM schema_migrations`)
		}},
		{name: "extra index", mutate: func(t *testing.T, db *sql.DB) {
			execM7R3(t, db, `CREATE UNIQUE INDEX unexpected_migration_name ON schema_migrations(name)`)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "core.db")
			createM7R2DatabaseAtVersion(t, path, "core", 4)
			db := openM7RawDatabase(t, path)
			test.mutate(t, db)
			_ = db.Close()
			before := fileDigest(t, path)
			database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: "core", AutoMigrate: true})
			if database != nil {
				_ = database.Close()
			}
			if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
				t.Fatalf("error=%v, want schema mismatch", err)
			}
			if after := fileDigest(t, path); after != before {
				t.Fatalf("database changed before=%x after=%x", before, after)
			}
			db = openM7RawDatabase(t, path)
			defer db.Close()
			var version int
			if err := db.QueryRow(`SELECT MAX(CAST(version AS INTEGER)) FROM schema_migrations`).Scan(&version); err != nil {
				t.Fatal(err)
			}
			if version != 4 {
				t.Fatalf("migration version=%d want=4", version)
			}
		})
	}
}

func TestM7R3MigrationMetadataManifestAppliesToCoreAndAgent(t *testing.T) {
	for _, set := range []string{"core", "agent"} {
		t.Run(set, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), set+".db")
			createM7R2DatabaseAtVersion(t, path, set, 1)
			db := openM7RawDatabase(t, path)
			execM7R3(t, db, `CREATE TRIGGER reject_metadata_update BEFORE UPDATE ON schema_migrations BEGIN SELECT RAISE(ABORT, 'blocked'); END`)
			_ = db.Close()
			before := fileDigest(t, path)
			database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: set, AutoMigrate: true})
			if database != nil {
				_ = database.Close()
			}
			if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
				t.Fatalf("error=%v, want schema mismatch", err)
			}
			if after := fileDigest(t, path); after != before {
				t.Fatalf("database changed before=%x after=%x", before, after)
			}
		})
	}
}

func TestM7R3TableXInfoRejectsGeneratedColumns(t *testing.T) {
	for _, test := range []struct{ name, generated string }{
		{name: "virtual", generated: `derived TEXT GENERATED ALWAYS AS (value) VIRTUAL`},
		{name: "stored", generated: `derived TEXT GENERATED ALWAYS AS (value) STORED`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, migrations := createM7R3Fixture(t, `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT NOT NULL)`)
			defer db.Close()
			rebuildM7R3Subject(t, db, `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT NOT NULL, `+test.generated+`)`, "")
			assertM7R3SchemaMismatch(t, db, migrations)
		})
	}
}

func TestM7R3IndexXInfoRejectsSemanticDrift(t *testing.T) {
	const ddl = `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT NOT NULL); CREATE INDEX subject_value ON subject(value ASC)`
	for _, test := range []struct{ name, index string }{
		{name: "collation", index: `CREATE INDEX subject_value ON subject(value COLLATE NOCASE ASC)`},
		{name: "descending", index: `CREATE INDEX subject_value ON subject(value DESC)`},
		{name: "expression", index: `CREATE INDEX subject_value ON subject(lower(value))`},
		{name: "partial", index: `CREATE INDEX subject_value ON subject(value) WHERE value <> ''`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, migrations := createM7R3Fixture(t, ddl)
			defer db.Close()
			rebuildM7R3Subject(t, db, `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT NOT NULL)`, test.index)
			assertM7R3SchemaMismatch(t, db, migrations)
		})
	}
}

func TestM7R3ColumnCollationAndTableOptions(t *testing.T) {
	tests := []struct{ name, expected, actual string }{
		{name: "column collation", expected: `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT NOT NULL)`, actual: `CREATE TABLE subject(id TEXT COLLATE NOCASE PRIMARY KEY, value TEXT NOT NULL)`},
		{name: "add strict", expected: `CREATE TABLE subject(id INTEGER PRIMARY KEY, value TEXT NOT NULL)`, actual: `CREATE TABLE subject(id INTEGER PRIMARY KEY, value TEXT NOT NULL) STRICT`},
		{name: "remove strict", expected: `CREATE TABLE subject(id INTEGER PRIMARY KEY, value TEXT NOT NULL) STRICT`, actual: `CREATE TABLE subject(id INTEGER PRIMARY KEY, value TEXT NOT NULL)`},
		{name: "add without rowid", expected: `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT NOT NULL)`, actual: `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID`},
		{name: "remove without rowid", expected: `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID`, actual: `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT NOT NULL)`},
		{name: "remove autoincrement", expected: `CREATE TABLE subject(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL)`, actual: `CREATE TABLE subject(id INTEGER PRIMARY KEY, value TEXT NOT NULL)`},
		{name: "add autoincrement", expected: `CREATE TABLE subject(id INTEGER PRIMARY KEY, value TEXT NOT NULL)`, actual: `CREATE TABLE subject(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL)`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, migrations := createM7R3Fixture(t, test.expected)
			defer db.Close()
			rebuildM7R3Subject(t, db, test.actual, "")
			assertM7R3SchemaMismatch(t, db, migrations)
		})
	}
}

func TestM7R3RejectsUnexpectedTriggerAndView(t *testing.T) {
	for _, test := range []struct{ name, sql string }{
		{name: "trigger", sql: `CREATE TRIGGER subject_insert AFTER INSERT ON subject BEGIN UPDATE subject SET value=value WHERE id=NEW.id; END`},
		{name: "view", sql: `CREATE VIEW subject_values AS SELECT value FROM subject`},
		{name: "extra table", sql: `CREATE TABLE unexpected_table(id INTEGER)`},
		{name: "extra index", sql: `CREATE INDEX unexpected_subject_index ON subject(id, value)`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, migrations := createM7R3Fixture(t, `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT NOT NULL)`)
			defer db.Close()
			execM7R3(t, db, test.sql)
			assertM7R3SchemaMismatch(t, db, migrations)
		})
	}
}

func TestM7R3CheckTokenQuotedIdentifierSemantics(t *testing.T) {
	const expected = `CREATE TABLE subject(version INTEGER NOT NULL CHECK(version > 0), note TEXT NOT NULL CHECK(note IN ('it''s','safe')))`
	tests := []struct {
		name, ddl string
		match     bool
	}{
		{name: "quoted identifier", ddl: `CREATE TABLE subject(version INTEGER NOT NULL CHECK(("version") > 0), note TEXT NOT NULL CHECK(note IN ('it''s','safe')))`, match: true},
		{name: "quoted expression", ddl: `CREATE TABLE subject(version INTEGER NOT NULL CHECK("version>0"), note TEXT NOT NULL CHECK(note IN ('it''s','safe'))) `},
		{name: "unknown quoted identifier", ddl: `CREATE TABLE subject(version INTEGER NOT NULL CHECK("unknown_column" > 0), note TEXT NOT NULL CHECK(note IN ('it''s','safe'))) `},
		{name: "quoted enum expression", ddl: `CREATE TABLE subject(version INTEGER NOT NULL CHECK(version > 0), note TEXT NOT NULL CHECK("notein('it''s')"))`},
		{name: "literal changed", ddl: `CREATE TABLE subject(version INTEGER NOT NULL CHECK(version > 0), note TEXT NOT NULL CHECK(note IN ('its','safe'))) `},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, migrations := createM7R3Fixture(t, expected)
			defer db.Close()
			rebuildM7R3Subject(t, db, test.ddl, "")
			err := verifyRequiredSchema(context.Background(), db, "core", migrations)
			if test.match && err != nil {
				t.Fatalf("equivalent quoted identifier rejected: %v", err)
			}
			if !test.match && !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
				t.Fatalf("error=%v, want schema mismatch", err)
			}
		})
	}
}

func rebuildM7R3Metadata(definition string) func(*testing.T, *sql.DB) {
	return func(t *testing.T, db *sql.DB) {
		execM7R3(t, db, `ALTER TABLE schema_migrations RENAME TO schema_migrations_m7r3_old`)
		execM7R3(t, db, `CREATE TABLE schema_migrations(`+definition+`)`)
		execM7R3(t, db, `INSERT INTO schema_migrations(version,name,checksum,applied_at) SELECT version,name,checksum,applied_at FROM schema_migrations_m7r3_old`)
		execM7R3(t, db, `DROP TABLE schema_migrations_m7r3_old`)
	}
}

func createM7R3Fixture(t *testing.T, ddl string) (*sql.DB, []migration) {
	t.Helper()
	db, err := sql.Open(defaultSQLDriverName, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	migrations := []migration{{version: 1, name: "m7r3_fixture", sql: ddl, checksum: migrationChecksum(ddl)}}
	if err := applyMigrations(context.Background(), db, migrations, time.Now); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db, migrations
}

func rebuildM7R3Subject(t *testing.T, db *sql.DB, ddl, index string) {
	t.Helper()
	execM7R3(t, db, `ALTER TABLE subject RENAME TO subject_m7r3_old`)
	execM7R3(t, db, ddl)
	execM7R3(t, db, `DROP TABLE subject_m7r3_old`)
	if index != "" {
		execM7R3(t, db, index)
	}
}

func assertM7R3SchemaMismatch(t *testing.T, db *sql.DB, migrations []migration) {
	t.Helper()
	if err := verifyRequiredSchema(context.Background(), db, "core", migrations); !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
		t.Fatalf("error=%v, want schema mismatch", err)
	}
}

func execM7R3(t *testing.T, db *sql.DB, statement string) {
	t.Helper()
	if _, err := db.Exec(statement); err != nil {
		t.Fatal(err)
	}
}
