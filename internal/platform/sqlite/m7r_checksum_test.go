package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	storageport "dtm/internal/storage"
)

func TestM7RChecksumMetadataRejectsNullEmptyAndInvalidValuesWithoutModification(t *testing.T) {
	for _, test := range []struct {
		name  string
		value *string
	}{
		{name: "null"},
		{name: "empty", value: stringPointer("")},
		{name: "spaces", value: stringPointer("   ")},
		{name: "invalid format", value: stringPointer("abc123")},
		{name: "uppercase", value: stringPointer("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")},
		{name: "mismatch", value: stringPointer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "core.db")
			repository, err := Open(path)
			if err != nil {
				t.Fatal(err)
			}
			_ = repository.Close()
			db := openM7RawDatabase(t, path)
			if test.value == nil {
				if _, err := db.Exec(`ALTER TABLE schema_migrations RENAME TO schema_migrations_valid`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, name TEXT NOT NULL, checksum TEXT, applied_at TEXT NOT NULL)`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`INSERT INTO schema_migrations SELECT version, name, CASE WHEN version=1 THEN NULL ELSE checksum END, applied_at FROM schema_migrations_valid`); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Exec(`DROP TABLE schema_migrations_valid`); err != nil {
					t.Fatal(err)
				}
			} else if _, err := db.Exec(`UPDATE schema_migrations SET checksum=? WHERE version=1`, *test.value); err != nil {
				t.Fatal(err)
			}
			var beforeValue *string
			if err := db.QueryRow(`SELECT checksum FROM schema_migrations WHERE version=1`).Scan(&beforeValue); err != nil {
				t.Fatal(err)
			}
			_ = db.Close()
			before := fileDigest(t, path)
			repository, err = Open(path)
			if repository != nil {
				_ = repository.Close()
			}
			if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
				t.Fatalf("Open() error = %v, want schema mismatch", err)
			}
			if after := fileDigest(t, path); after != before {
				t.Fatalf("invalid checksum database changed: before=%x after=%x", before, after)
			}
			db = openM7RawDatabase(t, path)
			var afterValue *string
			if err := db.QueryRow(`SELECT checksum FROM schema_migrations WHERE version=1`).Scan(&afterValue); err != nil {
				t.Fatal(err)
			}
			_ = db.Close()
			if !equalNullableString(beforeValue, afterValue) {
				t.Fatalf("checksum metadata changed: before=%v after=%v", beforeValue, afterValue)
			}
		})
	}
}

func TestM7RLegacyChecksumAdoptionDisabledAndRollback(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = repository.Close()
	makeM7RLegacyMigrationTable(t, path)
	database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: "core", AutoMigrate: false})
	if database != nil {
		_ = database.Close()
	}
	if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
		t.Fatalf("auto_migrate=false legacy database error = %v", err)
	}

	db := openM7RawDatabase(t, path)
	// M7-R3 rejects every user-defined view during the read-only metadata
	// manifest preflight, before checksum adoption can create its temporary table.
	if _, err := db.Exec(`CREATE VIEW schema_migrations_m7_adoption AS SELECT 1 AS blocker`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	database, err = openDatabase(context.Background(), Options{Path: path, MigrationSet: "core", AutoMigrate: true})
	if database != nil {
		_ = database.Close()
	}
	if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
		t.Fatalf("failed adoption error = %v, want schema mismatch", err)
	}
	db = openM7RawDatabase(t, path)
	defer db.Close()
	hasChecksum, err := migrationTableHasChecksum(context.Background(), db)
	if err != nil {
		t.Fatal(err)
	}
	if hasChecksum {
		t.Fatal("failed adoption left checksum column behind")
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 6 {
		t.Fatalf("migration count after failed adoption = %d, want 6", count)
	}
}

func makeM7RLegacyMigrationTable(t *testing.T, path string) {
	t.Helper()
	db := openM7RawDatabase(t, path)
	defer db.Close()
	if _, err := db.Exec(`ALTER TABLE schema_migrations RENAME TO schema_migrations_m7r`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO schema_migrations(version, name, applied_at) SELECT version, name, applied_at FROM schema_migrations_m7r`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE schema_migrations_m7r`); err != nil {
		t.Fatal(err)
	}
}

func stringPointer(value string) *string { return &value }

func equalNullableString(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
