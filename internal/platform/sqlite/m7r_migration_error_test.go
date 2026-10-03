package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	storageport "dtm/internal/storage"
)

func TestM7RMigrationEnvironmentErrorsPreserveClassification(t *testing.T) {
	for _, test := range []struct {
		name     string
		primary  int
		want     error
		exitCode int
	}{
		{name: "busy", primary: sqlitePrimaryBusy, want: storageport.ErrStorageUnavailable, exitCode: 11},
		{name: "locked", primary: sqlitePrimaryLocked, want: storageport.ErrStorageUnavailable, exitCode: 11},
		{name: "read only", primary: sqlitePrimaryReadOnly, want: storageport.ErrStorageReadOnly, exitCode: 10},
		{name: "full", primary: sqlitePrimaryFull, want: storageport.ErrStorageFull, exitCode: 10},
		{name: "io", primary: sqlitePrimaryIOErr, want: storageport.ErrStorageIO, exitCode: 10},
		{name: "path", primary: sqlitePrimaryCantOpen, want: storageport.ErrStoragePath, exitCode: 10},
		{name: "corrupt", primary: sqlitePrimaryCorrupt, want: storageport.ErrStorageCorrupt, exitCode: 13},
	} {
		t.Run(test.name, func(t *testing.T) {
			injected := fmt.Errorf("injected migration failure: %w", storageErrorForSQLiteCode(test.primary|0x500))
			err := classifyMigrationOperationError(context.Background(), "apply migration", injected)
			if !errors.Is(err, test.want) {
				t.Fatalf("classification = %v, want %v", err, test.want)
			}
			if code := storageport.StartupExitCode(err); code != test.exitCode {
				t.Fatalf("exit code = %d, want %d", code, test.exitCode)
			}
		})
	}
}

func TestM7RMigrationContextAndContentErrorsRemainDistinct(t *testing.T) {
	for _, contextError := range []error{context.Canceled, context.DeadlineExceeded} {
		err := classifyMigrationOperationError(context.Background(), "commit migration", contextError)
		if !errors.Is(err, contextError) {
			t.Fatalf("context classification = %v, want %v", err, contextError)
		}
	}
	err := classifyMigrationOperationError(context.Background(), "apply migration", errors.New("syntax error with secret SQL"))
	if !errors.Is(err, storageport.ErrStorageMigration) {
		t.Fatalf("content classification = %v, want migration", err)
	}
	if errors.Is(err, storageport.ErrStorageUnavailable) {
		t.Fatalf("content error misclassified unavailable: %v", err)
	}
}

func TestM7RMigrationSyntaxFailureRollsBackStructureAndVersion(t *testing.T) {
	db, err := sql.Open(defaultSQLDriverName, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	migrations := []migration{{version: 1, name: "invalid", checksum: migrationChecksum("invalid"), sql: `
CREATE TABLE m7r_partial(id INTEGER PRIMARY KEY);
THIS IS NOT VALID SQL;`}}
	err = applyMigrations(context.Background(), db, migrations, time.Now)
	if !errors.Is(err, storageport.ErrStorageMigration) {
		t.Fatalf("applyMigrations() error = %v, want migration", err)
	}
	var tableCount, migrationCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='m7r_partial'`).Scan(&tableCount); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&migrationCount); err != nil {
		t.Fatal(err)
	}
	if tableCount != 0 || migrationCount != 0 {
		t.Fatalf("failed migration left table=%d version=%d", tableCount, migrationCount)
	}
}
