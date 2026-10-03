package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	storageport "dtm/internal/storage"
)

func TestM7SQLitePrimaryAndExtendedErrorCodeMatrix(t *testing.T) {
	for _, test := range []struct {
		code int
		want error
	}{
		{5, storageport.ErrStorageUnavailable},
		{6 | 0x100, storageport.ErrStorageUnavailable},
		{8 | 0x300, storageport.ErrStorageReadOnly},
		{10 | 0x500, storageport.ErrStorageIO},
		{11, storageport.ErrStorageCorrupt},
		{13 | 0x200, storageport.ErrStorageFull},
		{14, storageport.ErrStoragePath},
		{26 | 0x100, storageport.ErrStorageCorrupt},
	} {
		if got := storageErrorForSQLiteCode(test.code); !errors.Is(got, test.want) {
			t.Fatalf("code %d classification = %v, want %v", test.code, got, test.want)
		}
	}
	if got := storageErrorForSQLiteCode(19); got != nil {
		t.Fatalf("constraint classification = %v, want nil", got)
	}
}

func TestM7StorageClassificationPrefersContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := classifySQLiteError(ctx, fmt.Errorf("wrapped: %w", storageport.ErrStorageUnavailable))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("classification = %v, want context canceled", err)
	}
}

func TestM7DatabasePathPermissionAndParentFileClassification(t *testing.T) {
	t.Run("mkdir permission", func(t *testing.T) {
		_, err := openDatabase(context.Background(), Options{
			Path: filepath.Join(t.TempDir(), "denied", "core.db"), MigrationSet: "core", AutoMigrate: true,
			mkdirAll: func(string, os.FileMode) error { return os.ErrPermission },
		})
		if !errors.Is(err, storageport.ErrStorageReadOnly) {
			t.Fatalf("openDatabase() error = %v, want read-only", err)
		}
	})
	t.Run("parent file", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "parent-file")
		if err := os.WriteFile(parent, []byte("block"), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Open(filepath.Join(parent, "core.db"))
		if !errors.Is(err, storageport.ErrStoragePath) {
			t.Fatalf("Open() error = %v, want storage path", err)
		}
	})
}

func TestM7DatabaseReadOnlyPreservesOriginal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	before := fileDigest(t, path)
	if err := os.Chmod(path, 0o444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
	reopened, err := Open(path)
	if reopened != nil {
		_ = reopened.Close()
	}
	if !errors.Is(err, storageport.ErrStorageReadOnly) {
		t.Fatalf("Open() error = %v, want read-only", err)
	}
	if after := fileDigest(t, path); after != before {
		t.Fatalf("read-only database changed: before=%x after=%x", before, after)
	}
}

func TestM7DatabaseCorruptFileIsRejectedWithoutModification(t *testing.T) {
	path := filepath.Join(t.TempDir(), "corrupt.db")
	content := []byte("not a sqlite database\x00preserve this evidence")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	before := fileDigest(t, path)
	repository, err := Open(path)
	if repository != nil {
		_ = repository.Close()
	}
	if !errors.Is(err, storageport.ErrStorageCorrupt) {
		t.Fatalf("Open() error = %v, want corrupt", err)
	}
	if after := fileDigest(t, path); after != before {
		t.Fatalf("corrupt database changed: before=%x after=%x", before, after)
	}
}

func TestM7DatabaseCorruptHeaderIsRejectedWithoutModification(t *testing.T) {
	path := filepath.Join(t.TempDir(), "header-corrupt.db")
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	copy(contents[:16], []byte("broken header!!!"))
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	assertCorruptOpenPreservesFile(t, path)
}

func TestM7DatabaseCorruptDataPageIsRejectedWithoutModification(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data-page-corrupt.db")
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	taskID, stepID, version := createDispatchedStep(t, repository, "task-page-corruption")
	startAttempt(t, repository, taskID, stepID, 1, version, "node-a", testTime(4))
	var rootPage int
	if err := repository.db.QueryRow(`SELECT rootpage FROM sqlite_master WHERE type='table' AND name='executions'`).Scan(&rootPage); err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	pageSize := int(binary.BigEndian.Uint16(contents[16:18]))
	if pageSize == 1 {
		pageSize = 65536
	}
	offset := (rootPage - 1) * pageSize
	if rootPage <= 1 || pageSize <= 0 || offset >= len(contents) {
		t.Fatalf("database size=%d page_size=%d root_page=%d cannot exercise a non-header page", len(contents), pageSize, rootPage)
	}
	contents[offset] = 0xff
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	assertCorruptOpenPreservesFile(t, path)
}

func TestM7DatabaseStartupLockIsBoundedAndRecovers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "locked.db")
	database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: "core", AutoMigrate: true, JournalMode: "DELETE", BusyTimeout: 40 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	lock := openM7RawDatabase(t, path)
	if _, err := lock.Exec(`PRAGMA journal_mode=DELETE`); err != nil {
		t.Fatal(err)
	}
	if _, err := lock.Exec(`BEGIN EXCLUSIVE`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = lock.Exec(`ROLLBACK`)
		_ = lock.Close()
	})
	started := time.Now()
	opened, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: "core", AutoMigrate: true, JournalMode: "DELETE", BusyTimeout: 40 * time.Millisecond})
	if opened != nil {
		_ = opened.Close()
	}
	elapsed := time.Since(started)
	if !errors.Is(err, storageport.ErrStorageUnavailable) {
		t.Fatalf("locked Open() error = %v, want unavailable", err)
	}
	// SQLite drivers and host schedulers may return a few milliseconds before
	// the configured timeout. Assert that the failure was not immediate while
	// keeping the upper bound that proves startup remains bounded.
	if elapsed < 10*time.Millisecond || elapsed > time.Second {
		t.Fatalf("locked wait = %s, want bounded busy timeout", elapsed)
	}
	if _, err := lock.Exec(`ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	_ = lock.Close()
	opened, err = openDatabase(context.Background(), Options{Path: path, MigrationSet: "core", AutoMigrate: true, JournalMode: "DELETE", BusyTimeout: 40 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open() after lock release = %v", err)
	}
	_ = opened.Close()
}

func TestM7MigrationAdoptsLegacyChecksumsAndValidatesDisabledMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	db := openM7RawDatabase(t, path)
	if _, err := db.Exec(`ALTER TABLE schema_migrations RENAME TO schema_migrations_m7`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY, name TEXT NOT NULL, applied_at TEXT NOT NULL)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO schema_migrations(version, name, applied_at) SELECT version, name, applied_at FROM schema_migrations_m7`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE schema_migrations_m7`); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()

	repository, err = Open(path)
	if err != nil {
		t.Fatalf("legacy checksum adoption: %v", err)
	}
	_ = repository.Close()
	db = openM7RawDatabase(t, path)
	var empty int
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations WHERE checksum IS NULL OR checksum=''`).Scan(&empty); err != nil {
		t.Fatal(err)
	}
	_ = db.Close()
	if empty != 0 {
		t.Fatalf("empty adopted checksums = %d", empty)
	}
	database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: "core", AutoMigrate: false})
	if err != nil {
		t.Fatalf("disabled migration healthy schema: %v", err)
	}
	_ = database.Close()
}

func TestM7MigrationSetsCannotBeInterchanged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.db")
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_ = repository.Close()
	database, err := openDatabase(context.Background(), Options{Path: path, MigrationSet: "agent", AutoMigrate: false})
	if database != nil {
		_ = database.Close()
	}
	if !errors.Is(err, storageport.ErrStorageSchemaMismatch) {
		t.Fatalf("agent accepted core schema: %v", err)
	}
}

func TestM7MigrationRejectsNameMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "name-mismatch.db")
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := repository.Close(); err != nil {
		t.Fatal(err)
	}
	db := openM7RawDatabase(t, path)
	if _, err := db.Exec(`UPDATE schema_migrations SET name='renamed.sql' WHERE version=1`); err != nil {
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
		t.Fatalf("name-mismatched database changed: before=%x after=%x", before, after)
	}
}

func assertCorruptOpenPreservesFile(t *testing.T, path string) {
	t.Helper()
	beforeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	before := fileDigest(t, path)
	repository, err := Open(path)
	if repository != nil {
		_ = repository.Close()
	}
	if !errors.Is(err, storageport.ErrStorageCorrupt) {
		t.Fatalf("Open() error = %v, want corrupt", err)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if afterInfo.Size() != beforeInfo.Size() {
		t.Fatalf("corrupt database size changed: before=%d after=%d", beforeInfo.Size(), afterInfo.Size())
	}
	if after := fileDigest(t, path); after != before {
		t.Fatalf("corrupt database changed: before=%x after=%x", before, after)
	}
}

func fileDigest(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(contents)
}
