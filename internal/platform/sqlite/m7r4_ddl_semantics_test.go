package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	storageport "dtm/internal/storage"
)

func TestM7R4ColumnCollationQuotedBracketBacktickAndComments(t *testing.T) {
	for _, test := range []struct {
		name, actual string
	}{
		{name: "quoted", actual: `CREATE TABLE subject(id TEXT PRIMARY KEY, failure_code TEXT COLLATE "NOCASE")`},
		{name: "bracket", actual: `CREATE TABLE subject(id TEXT PRIMARY KEY, failure_code TEXT COLLATE [NOCASE])`},
		{name: "backtick", actual: "CREATE TABLE subject(id TEXT PRIMARY KEY, failure_code TEXT COLLATE `NOCASE`)"},
		{name: "rtrim", actual: `CREATE TABLE subject(id TEXT PRIMARY KEY, failure_code TEXT COLLATE RTRIM)`},
		{name: "line comment", actual: "CREATE TABLE subject(id TEXT PRIMARY KEY, -- business field\nfailure_code TEXT COLLATE NOCASE)"},
		{name: "block comment", actual: `CREATE TABLE subject(id TEXT PRIMARY KEY, /* business field */ failure_code TEXT COLLATE NOCASE)`},
		{name: "collate comments", actual: `CREATE TABLE subject(id TEXT PRIMARY KEY, failure_code TEXT /* before */ COLLATE /* after */ NOCASE)`},
	} {
		t.Run(test.name, func(t *testing.T) {
			expected := `CREATE TABLE subject(id TEXT PRIMARY KEY, failure_code TEXT)`
			if test.name == "line comment" || test.name == "block comment" || test.name == "collate comments" {
				expected = `CREATE TABLE subject(id TEXT PRIMARY KEY, failure_code TEXT COLLATE BINARY)`
			}
			db, migrations := createM7R3Fixture(t, expected)
			defer db.Close()
			rebuildM7R3Subject(t, db, test.actual, "")
			assertM7R3SchemaMismatch(t, db, migrations)
		})
	}
}

func TestM7R4ColumnCollationFormattingEquivalence(t *testing.T) {
	for _, actual := range []string{
		`CREATE TABLE subject(id TEXT PRIMARY KEY, failure_code TEXT collate "nocase")`,
		`CREATE TABLE subject(id TEXT PRIMARY KEY, failure_code TEXT COLLATE [NoCase])`,
		"CREATE TABLE subject(id TEXT PRIMARY KEY, failure_code TEXT COLLATE `NOCASE`)",
		"CREATE TABLE subject(id TEXT PRIMARY KEY, -- field\nfailure_code TEXT /* before */ COLLATE /* after */ NOCASE)",
	} {
		db, migrations := createM7R3Fixture(t, `CREATE TABLE subject(id TEXT PRIMARY KEY, failure_code TEXT COLLATE NOCASE)`)
		rebuildM7R3Subject(t, db, actual, "")
		if err := verifyRequiredSchema(context.Background(), db, "core", migrations); err != nil {
			t.Fatalf("equivalent collation rejected for %q: %v", actual, err)
		}
		_ = db.Close()
	}
}

func TestM7R4InvalidCollationFailsClosed(t *testing.T) {
	for _, ddl := range []string{
		`CREATE TABLE subject(intent TEXT COLLATE)`,
		`CREATE TABLE subject(intent TEXT COLLATE 'NOCASE')`,
		`CREATE TABLE subject(intent TEXT COLLATE BINARY COLLATE NOCASE)`,
		`CREATE TABLE subject(intent TEXT /* unterminated)`,
		`CREATE TABLE subject(intent TEXT COLLATE "NOCASE)`,
	} {
		if _, err := parseTableDDLSemantics(ddl); err == nil {
			t.Fatalf("parseTableDDLSemantics(%q) succeeded", ddl)
		}
	}
	semantics, err := parseTableDDLSemantics(`CREATE TABLE subject(intent TEXT)`)
	if err != nil {
		t.Fatal(err)
	}
	if got := semantics.columnCollations["intent"]; got != "BINARY" {
		t.Fatalf("default collation=%q want BINARY", got)
	}
}

func TestM7R4ConflictPolicyPrimaryKeyUniqueNotNullAndExplicitAbort(t *testing.T) {
	for _, test := range []struct{ name, expected, actual string }{
		{name: "primary key replace", expected: `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT)`, actual: `CREATE TABLE subject(id TEXT PRIMARY KEY ON CONFLICT REPLACE, value TEXT)`},
		{name: "table primary key replace", expected: `CREATE TABLE subject(id TEXT NOT NULL, value TEXT, PRIMARY KEY(id))`, actual: `CREATE TABLE subject(id TEXT NOT NULL, value TEXT, PRIMARY KEY(id) ON CONFLICT REPLACE)`},
		{name: "unique ignore", expected: `CREATE TABLE subject(id TEXT, value TEXT, UNIQUE(id))`, actual: `CREATE TABLE subject(id TEXT, value TEXT, UNIQUE(id) ON CONFLICT IGNORE)`},
		{name: "column unique fail", expected: `CREATE TABLE subject(id TEXT UNIQUE, value TEXT)`, actual: `CREATE TABLE subject(id TEXT UNIQUE ON CONFLICT FAIL, value TEXT)`},
		{name: "not null replace", expected: `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT NOT NULL)`, actual: `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT NOT NULL ON CONFLICT REPLACE)`},
		{name: "explicit abort", expected: `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT)`, actual: `CREATE TABLE subject(id TEXT PRIMARY KEY ON CONFLICT ABORT, value TEXT)`},
		{name: "explicit rollback", expected: `CREATE TABLE subject(id TEXT PRIMARY KEY, value TEXT)`, actual: `CREATE TABLE subject(id TEXT PRIMARY KEY ON CONFLICT ROLLBACK, value TEXT)`},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, migrations := createM7R3Fixture(t, test.expected)
			defer db.Close()
			rebuildM7R3Subject(t, db, test.actual, "")
			assertM7R3SchemaMismatch(t, db, migrations)
		})
	}
}

func TestM7R4ConflictPolicyCommentAndStringDoNotMatch(t *testing.T) {
	ddl := `CREATE TABLE subject(
id TEXT PRIMARY KEY, -- ON CONFLICT REPLACE
note TEXT CHECK(note <> 'ON CONFLICT REPLACE'))`
	if _, err := parseTableDDLSemantics(ddl); err != nil {
		t.Fatalf("comment/string false positive: %v", err)
	}
}

func TestM7R4DDLSemanticParserFailsClosed(t *testing.T) {
	for _, ddl := range []string{
		`CREATE TABLE subject(id TEXT PRIMARY KEY ON CONFLICT)`,
		`CREATE TABLE subject(id TEXT PRIMARY KEY ON SOMETHING ABORT)`,
		`CREATE TABLE subject(id TEXT, CHECK(id <> '') ON CONFLICT FAIL)`,
		`CREATE TABLE subject(parent_id TEXT REFERENCES parent(id) MATCH)`,
		`CREATE TABLE subject(parent_id TEXT REFERENCES parent(id) INITIALLY)`,
		`CREATE TABLE subject(parent_id TEXT REFERENCES parent(id) UNKNOWN)`,
	} {
		if _, err := parseTableDDLSemantics(ddl); err == nil {
			t.Fatalf("parseTableDDLSemantics(%q) succeeded", ddl)
		}
	}
}

func TestM7R4ConflictPolicyChangesWriteBehavior(t *testing.T) {
	db := openM7R4MemoryDB(t)
	defer db.Close()
	execM7R3(t, db, `CREATE TABLE ordinary(id TEXT PRIMARY KEY, value TEXT); CREATE TABLE replacing(id TEXT PRIMARY KEY ON CONFLICT REPLACE, value TEXT)`)
	execM7R3(t, db, `INSERT INTO ordinary VALUES('id','first'); INSERT INTO replacing VALUES('id','first')`)
	_, ordinaryErr := db.Exec(`INSERT INTO ordinary VALUES('id','second')`)
	if ordinaryErr == nil {
		t.Fatal("ordinary duplicate primary key succeeded")
	}
	if _, err := db.Exec(`INSERT INTO replacing VALUES('id','second')`); err != nil {
		t.Fatalf("REPLACE duplicate failed: %v", err)
	}
	var value string
	if err := db.QueryRow(`SELECT value FROM replacing WHERE id='id'`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "second" {
		t.Fatalf("replacement value=%q", value)
	}
	t.Logf("ordinary_duplicate_error=%v replace_result=%q", ordinaryErr != nil, value)
}

func TestM7R4ForeignKeyMatchDeferrableAndInitially(t *testing.T) {
	for _, test := range []struct{ name, suffix string }{
		{name: "match full", suffix: ` MATCH FULL`},
		{name: "match simple", suffix: ` MATCH SIMPLE`},
		{name: "deferrable", suffix: ` DEFERRABLE`},
		{name: "initially deferred", suffix: ` DEFERRABLE INITIALLY DEFERRED`},
		{name: "explicit immediate", suffix: ` NOT DEFERRABLE INITIALLY IMMEDIATE`},
	} {
		t.Run(test.name, func(t *testing.T) {
			expected := `CREATE TABLE parent(id TEXT PRIMARY KEY); CREATE TABLE child(id TEXT PRIMARY KEY, parent_id TEXT, FOREIGN KEY(parent_id) REFERENCES parent(id))`
			actual := `CREATE TABLE child(id TEXT PRIMARY KEY, parent_id TEXT, FOREIGN KEY(parent_id) REFERENCES parent(id)` + test.suffix + `)`
			db, migrations := createM7R3Fixture(t, expected)
			defer db.Close()
			execM7R3(t, db, `ALTER TABLE child RENAME TO child_m7r4_old`)
			execM7R3(t, db, actual)
			execM7R3(t, db, `DROP TABLE child_m7r4_old`)
			assertM7R3SchemaMismatch(t, db, migrations)
		})
	}
}

func TestM7R4ForeignKeyDefaultCommentAndStringRemainValid(t *testing.T) {
	ddl := `CREATE TABLE parent(id TEXT PRIMARY KEY);
CREATE TABLE child(id TEXT PRIMARY KEY, note TEXT DEFAULT 'MATCH FULL', parent_id TEXT,
-- DEFERRABLE INITIALLY DEFERRED
FOREIGN KEY(parent_id) REFERENCES parent(id) ON DELETE CASCADE)`
	db, migrations := createM7R3Fixture(t, ddl)
	defer db.Close()
	if err := verifyRequiredSchema(context.Background(), db, "core", migrations); err != nil {
		t.Fatal(err)
	}
}

func TestM7R4CoreAndAgentDDLSemantics(t *testing.T) {
	core, coreMigrations := createM7R3Fixture(t, `CREATE TABLE parent(id TEXT PRIMARY KEY); CREATE TABLE child(parent_id TEXT, FOREIGN KEY(parent_id) REFERENCES parent(id))`)
	if err := verifyRequiredSchema(context.Background(), core, "core", coreMigrations); err != nil {
		t.Fatal(err)
	}
	_ = core.Close()
	agent, agentMigrations := createM7R3Fixture(t, `CREATE TABLE executions(execution_id TEXT PRIMARY KEY, failure_code TEXT)`)
	rebuildM7R3SubjectNamed(t, agent, "executions", `CREATE TABLE executions(execution_id TEXT PRIMARY KEY ON CONFLICT REPLACE, failure_code TEXT)`)
	assertM7R3SchemaMismatch(t, agent, agentMigrations)
	_ = agent.Close()
}

func TestM7R4DeferredForeignKeyChangesFailureBoundary(t *testing.T) {
	db := openM7R4MemoryDB(t)
	defer db.Close()
	execM7R3(t, db, `PRAGMA foreign_keys=ON; CREATE TABLE parent(id TEXT PRIMARY KEY);
CREATE TABLE immediate_child(parent_id TEXT REFERENCES parent(id));
CREATE TABLE deferred_child(parent_id TEXT REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)`)
	_, immediateErr := db.Exec(`INSERT INTO immediate_child(parent_id) VALUES('missing')`)
	if immediateErr == nil {
		t.Fatal("immediate foreign key violation did not fail INSERT")
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`INSERT INTO deferred_child(parent_id) VALUES('missing')`); err != nil {
		_ = tx.Rollback()
		t.Fatalf("deferred INSERT failed before commit: %v", err)
	}
	commitErr := tx.Commit()
	if commitErr == nil {
		t.Fatal("deferred foreign key violation did not fail COMMIT")
	}
	t.Logf("immediate_insert_error=%v deferred_insert_error=false deferred_commit_error=%v", immediateErr != nil, commitErr != nil)
}

func TestM7R4PreMigrationSemanticWriteProtection(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *sql.DB)
	}{
		{name: "collation", mutate: func(t *testing.T, db *sql.DB) {
			rebuildM7R4Nodes(t, db, `metadata_json TEXT COLLATE "NOCASE" NOT NULL DEFAULT '{}'`)
		}},
		{name: "conflict", mutate: func(t *testing.T, db *sql.DB) {
			rebuildM7R4NodesWithID(t, db, `node_id TEXT PRIMARY KEY ON CONFLICT REPLACE`)
		}},
		{name: "foreign key", mutate: rebuildM7R4TaskEventsWithDeferredForeignKey},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "core.db")
			createM7R2DatabaseAtVersion(t, path, "core", 4)
			db := openM7RawDatabase(t, path)
			execM7R3(t, db, `INSERT INTO nodes(node_id,endpoint,capabilities_json,status,generation,created_at,updated_at) VALUES('node-m7r4','endpoint','[]','offline',1,1,1)`)
			test.mutate(t, db)
			_ = db.Close()
			before := readM7R4ProtectionSnapshot(t, path)
			database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: "core", AutoMigrate: true})
			if database != nil {
				_ = database.Close()
			}
			if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
				t.Fatalf("error=%v want schema mismatch", err)
			}
			after := readM7R4ProtectionSnapshot(t, path)
			t.Logf("before=%+v after=%+v", before, after)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("database changed before=%+v after=%+v", before, after)
			}
		})
	}
}

type m7r4ProtectionSnapshot struct {
	size, version                    int64
	digest                           [32]byte
	tables, indexes, triggers, views int
	nodeRows, eventRows              int
}

func readM7R4ProtectionSnapshot(t *testing.T, path string) m7r4ProtectionSnapshot {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	result := m7r4ProtectionSnapshot{size: info.Size(), digest: fileDigest(t, path)}
	db := openM7RawDatabase(t, path)
	defer db.Close()
	if err := db.QueryRow(`SELECT COALESCE(MAX(version),0) FROM schema_migrations`).Scan(&result.version); err != nil {
		t.Fatal(err)
	}
	for objectType, target := range map[string]*int{"table": &result.tables, "index": &result.indexes, "trigger": &result.triggers, "view": &result.views} {
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type=? AND name NOT LIKE 'sqlite_%'`, objectType).Scan(target); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM nodes`).Scan(&result.nodeRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM task_events`).Scan(&result.eventRows); err != nil {
		t.Fatal(err)
	}
	return result
}

func rebuildM7R3SubjectNamed(t *testing.T, db *sql.DB, table, ddl string) {
	t.Helper()
	execM7R3(t, db, `ALTER TABLE `+table+` RENAME TO `+table+`_m7r4_old`)
	execM7R3(t, db, ddl)
	execM7R3(t, db, `DROP TABLE `+table+`_m7r4_old`)
}

func rebuildM7R4Nodes(t *testing.T, db *sql.DB, metadataDefinition string) {
	rebuildM7R4NodesWithDefinitions(t, db, `node_id TEXT PRIMARY KEY`, metadataDefinition)
}

func rebuildM7R4NodesWithID(t *testing.T, db *sql.DB, idDefinition string) {
	rebuildM7R4NodesWithDefinitions(t, db, idDefinition, `metadata_json TEXT NOT NULL DEFAULT '{}'`)
}

func rebuildM7R4NodesWithDefinitions(t *testing.T, db *sql.DB, idDefinition, metadataDefinition string) {
	t.Helper()
	execM7R3(t, db, `PRAGMA foreign_keys=OFF; ALTER TABLE nodes RENAME TO nodes_m7r4_old`)
	execM7R3(t, db, `CREATE TABLE nodes (`+idDefinition+`, endpoint TEXT NOT NULL, capabilities_json TEXT NOT NULL,
status TEXT NOT NULL, generation INTEGER NOT NULL CHECK(generation > 0), created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
registration_id TEXT NOT NULL DEFAULT '', `+metadataDefinition+`, registered_at INTEGER NOT NULL DEFAULT 0,
last_heartbeat_at INTEGER NOT NULL DEFAULT 0, lease_expires_at INTEGER NOT NULL DEFAULT 0)`)
	execM7R3(t, db, `INSERT INTO nodes SELECT * FROM nodes_m7r4_old; DROP TABLE nodes_m7r4_old;
CREATE INDEX idx_nodes_status_lease_expiry ON nodes(status, lease_expires_at, node_id)`)
}

func rebuildM7R4TaskEventsWithDeferredForeignKey(t *testing.T, db *sql.DB) {
	t.Helper()
	execM7R3(t, db, `PRAGMA foreign_keys=OFF; ALTER TABLE task_events RENAME TO task_events_m7r4_old`)
	execM7R3(t, db, `CREATE TABLE task_events(event_id INTEGER PRIMARY KEY AUTOINCREMENT, task_id TEXT NOT NULL, step_id TEXT,
event_type TEXT NOT NULL, from_state TEXT, to_state TEXT, detail_json TEXT, created_at INTEGER NOT NULL,
FOREIGN KEY(task_id) REFERENCES tasks(task_id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
FOREIGN KEY(task_id,step_id) REFERENCES task_steps(task_id,step_id))`)
	execM7R3(t, db, `INSERT INTO task_events SELECT * FROM task_events_m7r4_old; DROP TABLE task_events_m7r4_old;
CREATE INDEX idx_task_events_task_created ON task_events(task_id, created_at, event_id)`)
}

func openM7R4MemoryDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open(defaultSQLDriverName, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	return db
}
