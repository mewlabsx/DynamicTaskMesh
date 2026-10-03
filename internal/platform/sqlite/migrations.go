package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	storageport "dtm/internal/storage"
)

//go:embed migrations/core/*.sql migrations/agent/*.sql
var migrationFiles embed.FS

const migrationTableSchema = `
CREATE TABLE schema_migrations (
	version INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	checksum TEXT NOT NULL,
	applied_at TEXT NOT NULL
)`

const legacyMigrationTableSchema = `
CREATE TABLE schema_migrations (
	version INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	applied_at TEXT NOT NULL
)`

type migration struct {
	version  int
	name     string
	sql      string
	checksum string
}

type appliedMigration struct {
	version  int
	name     string
	checksum string
}

type migrationFaultPoint string

const (
	migrationFaultBeforeBegin          migrationFaultPoint = "before_begin"
	migrationFaultBeforeSQLExec        migrationFaultPoint = "before_sql_exec"
	migrationFaultBeforeMetadataInsert migrationFaultPoint = "before_metadata_insert"
	migrationFaultBeforeCommit         migrationFaultPoint = "before_commit"
)

type migrationFaultInjector func(context.Context, migrationFaultPoint, migration) error

func embeddedMigrations(set string) ([]migration, error) {
	set = strings.TrimSpace(set)
	if set == "" {
		return nil, nil
	}
	directory := path.Join("migrations", set)
	entries, err := fs.ReadDir(migrationFiles, directory)
	if err != nil {
		return nil, fmt.Errorf("%w: read migration set %q", storageport.ErrStorageMigration, set)
	}
	result := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		parts := strings.SplitN(strings.TrimSuffix(entry.Name(), ".sql"), "_", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("%w: invalid migration name %q", storageport.ErrStorageMigration, entry.Name())
		}
		version, err := strconv.Atoi(parts[0])
		if err != nil || version < 1 {
			return nil, fmt.Errorf("%w: invalid migration version %q", storageport.ErrStorageMigration, entry.Name())
		}
		contents, err := migrationFiles.ReadFile(path.Join(directory, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("%w: read migration %q", storageport.ErrStorageMigration, entry.Name())
		}
		sqlText := string(contents)
		result = append(result, migration{version: version, name: parts[1], sql: sqlText, checksum: migrationChecksum(sqlText)})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].version < result[j].version })
	for index, item := range result {
		if item.version != index+1 {
			return nil, fmt.Errorf("%w: migration versions must be continuous", storageport.ErrStorageMigration)
		}
	}
	return result, nil
}

func migrationChecksum(sqlText string) string {
	normalized := strings.ReplaceAll(sqlText, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	sum := sha256.Sum256([]byte(normalized))
	return hex.EncodeToString(sum[:])
}

func ensureMigrationState(ctx context.Context, db *sql.DB, set string, migrations []migration, autoMigrate bool, now func() time.Time) error {
	exists, err := migrationTableExists(ctx, db)
	if err != nil {
		return migrationDatabaseError(ctx, "inspect migration metadata", err)
	}
	if !exists {
		if !autoMigrate {
			return schemaMismatch("database schema does not match this binary")
		}
		if err := createMigrationTable(ctx, db); err != nil {
			return err
		}
	}

	hasChecksum, err := migrationTableHasChecksum(ctx, db)
	if err != nil {
		return migrationDatabaseError(ctx, "inspect migration checksum metadata", err)
	}
	if err := verifyMigrationMetadataSchema(ctx, db, hasChecksum); err != nil {
		return err
	}
	applied, err := loadAppliedMigrations(ctx, db, hasChecksum)
	if err != nil {
		return migrationDatabaseError(ctx, "load migration metadata", err)
	}
	if err := validateAppliedMigrations(applied, migrations, hasChecksum, autoMigrate); err != nil {
		return err
	}
	if err := verifySchemaPrefix(ctx, db, set, migrations, len(applied)); err != nil {
		return err
	}

	if !hasChecksum {
		if !autoMigrate {
			return schemaMismatch("database schema does not match this binary: migration checksum metadata is missing")
		}
		if err := adoptMigrationChecksums(ctx, db, applied, migrations); err != nil {
			return err
		}
		hasChecksum = true
	}
	if autoMigrate {
		if err := applyMigrations(ctx, db, migrations, now); err != nil {
			return err
		}
	}
	finalApplied, err := loadAppliedMigrations(ctx, db, true)
	if err != nil {
		return migrationDatabaseError(ctx, "load final migration metadata", err)
	}
	if err := validateAppliedMigrations(finalApplied, migrations, true, false); err != nil {
		return err
	}
	if len(finalApplied) != len(migrations) {
		return schemaMismatch("database schema does not match this binary")
	}
	if err := verifyRequiredSchema(ctx, db, set, migrations); err != nil {
		return err
	}
	return nil
}

func validateExistingMigrationMetadata(ctx context.Context, db *sql.DB, set string, migrations []migration, autoMigrate bool) error {
	exists, err := migrationTableExists(ctx, db)
	if err != nil {
		return migrationDatabaseError(ctx, "inspect migration metadata", err)
	}
	if !exists {
		return schemaMismatch("database schema does not match this binary")
	}
	hasChecksum, err := migrationTableHasChecksum(ctx, db)
	if err != nil {
		return migrationDatabaseError(ctx, "inspect migration checksum metadata", err)
	}
	if err := verifyMigrationMetadataSchema(ctx, db, hasChecksum); err != nil {
		return err
	}
	applied, err := loadAppliedMigrations(ctx, db, hasChecksum)
	if err != nil {
		return migrationDatabaseError(ctx, "load migration metadata", err)
	}
	if err := validateAppliedMigrations(applied, migrations, hasChecksum, autoMigrate); err != nil {
		return err
	}
	if err := verifySchemaPrefix(ctx, db, set, migrations, len(applied)); err != nil {
		return err
	}
	if autoMigrate {
		return nil
	}
	if !hasChecksum || len(applied) != len(migrations) {
		return schemaMismatch("database schema does not match this binary")
	}
	return verifyRequiredSchema(ctx, db, set, migrations)
}

func migrationTableExists(ctx context.Context, db *sql.DB) (bool, error) {
	var count int
	err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='schema_migrations'`).Scan(&count)
	return count == 1, err
}

func createMigrationTable(ctx context.Context, db *sql.DB) (returnErr error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return migrationDatabaseError(ctx, "begin migration metadata initialization", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, migrationTableSchema); err != nil {
		return migrationDatabaseError(ctx, "initialize migration metadata", err)
	}
	if err := tx.Commit(); err != nil {
		return migrationDatabaseError(ctx, "commit migration metadata initialization", err)
	}
	return nil
}

func migrationTableHasChecksum(ctx context.Context, db *sql.DB) (bool, error) {
	rows, err := db.QueryContext(ctx, `PRAGMA table_xinfo(schema_migrations)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, primaryKey, hidden int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey, &hidden); err != nil {
			return false, err
		}
		if name == "checksum" {
			return true, rows.Err()
		}
	}
	return false, rows.Err()
}

func verifyMigrationMetadataSchema(ctx context.Context, db *sql.DB, hasChecksum bool) error {
	expectedDB, err := sql.Open(defaultSQLDriverName, ":memory:")
	if err != nil {
		return migrationDatabaseError(ctx, "open expected migration metadata manifest", err)
	}
	defer expectedDB.Close()
	expectedDB.SetMaxOpenConns(1)
	ddl := legacyMigrationTableSchema
	if hasChecksum {
		ddl = migrationTableSchema
	}
	if _, err := expectedDB.ExecContext(ctx, ddl); err != nil {
		return migrationDatabaseError(ctx, "build expected migration metadata manifest", err)
	}
	expected, err := readSchemaManifest(ctx, expectedDB, []string{"schema_migrations"})
	if err != nil {
		return migrationDatabaseError(ctx, "read expected migration metadata manifest", err)
	}
	actual, err := readSchemaManifest(ctx, db, []string{"schema_migrations"})
	if err != nil {
		if errors.Is(err, errSchemaManifestMismatch) {
			return schemaMismatch("database schema does not match this binary")
		}
		return migrationDatabaseError(ctx, "inspect migration metadata manifest", err)
	}
	if !reflect.DeepEqual(actual, expected) {
		return schemaMismatch("database schema does not match this binary")
	}
	return nil
}

func loadAppliedMigrations(ctx context.Context, db *sql.DB, hasChecksum bool) ([]appliedMigration, error) {
	query := `SELECT version, name, '' FROM schema_migrations ORDER BY version`
	if hasChecksum {
		query = `SELECT version, name, COALESCE(checksum, '') FROM schema_migrations ORDER BY version`
	}
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []appliedMigration{}
	for rows.Next() {
		var item appliedMigration
		if err := rows.Scan(&item.version, &item.name, &item.checksum); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

func validateAppliedMigrations(applied []appliedMigration, migrations []migration, hasChecksum, _ bool) error {
	for index, item := range applied {
		if item.version > len(migrations) {
			return schemaMismatch("database schema is newer than this binary")
		}
		if item.version < 1 || item.version != index+1 {
			return schemaMismatch("database migration versions are not continuous")
		}
		expected := migrations[item.version-1]
		if item.name != expected.name {
			return schemaMismatch(fmt.Sprintf("migration name mismatch at version %d", item.version))
		}
		if hasChecksum {
			if !validMigrationChecksum(item.checksum) {
				return schemaMismatch(fmt.Sprintf("migration checksum format is invalid at version %d", item.version))
			}
			if item.checksum != expected.checksum {
				return schemaMismatch(fmt.Sprintf("migration checksum mismatch at version %d", item.version))
			}
		}
	}
	return nil
}

func validMigrationChecksum(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func adoptMigrationChecksums(ctx context.Context, db *sql.DB, applied []appliedMigration, migrations []migration) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return migrationDatabaseError(ctx, "begin migration checksum adoption", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `
CREATE TABLE schema_migrations_m7_adoption (
	version INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	checksum TEXT NOT NULL,
	applied_at TEXT NOT NULL
)`); err != nil {
		return migrationDatabaseError(ctx, "create migration checksum metadata", err)
	}
	for _, item := range applied {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO schema_migrations_m7_adoption(version, name, checksum, applied_at)
SELECT version, name, ?, applied_at FROM schema_migrations WHERE version=?`, migrations[item.version-1].checksum, item.version); err != nil {
			return migrationDatabaseError(ctx, "adopt migration checksum", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE schema_migrations`); err != nil {
		return migrationDatabaseError(ctx, "replace migration checksum metadata", err)
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE schema_migrations_m7_adoption RENAME TO schema_migrations`); err != nil {
		return migrationDatabaseError(ctx, "publish migration checksum metadata", err)
	}
	if err := tx.Commit(); err != nil {
		return migrationDatabaseError(ctx, "commit migration checksum adoption", err)
	}
	return nil
}

func applyMigrations(ctx context.Context, db *sql.DB, migrations []migration, now func() time.Time) error {
	return applyMigrationsWithFaults(ctx, db, migrations, now, nil)
}

func applyMigrationsWithFaults(ctx context.Context, db *sql.DB, migrations []migration, now func() time.Time, inject migrationFaultInjector) error {
	exists, err := migrationTableExists(ctx, db)
	if err != nil {
		return migrationDatabaseError(ctx, "inspect migration metadata", err)
	}
	if !exists {
		if err := createMigrationTable(ctx, db); err != nil {
			return err
		}
	}
	if err := verifyMigrationMetadataSchema(ctx, db, true); err != nil {
		return err
	}
	for _, item := range migrations {
		var exists int
		if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version=?`, item.version).Scan(&exists); err != nil {
			return migrationDatabaseError(ctx, fmt.Sprintf("inspect sqlite migration %d", item.version), err)
		}
		if exists != 0 {
			continue
		}
		if err := injectMigrationFailure(ctx, inject, migrationFaultBeforeBegin, item); err != nil {
			return err
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return migrationDatabaseError(ctx, fmt.Sprintf("begin sqlite migration %d", item.version), err)
		}
		if err := injectMigrationFailure(ctx, inject, migrationFaultBeforeSQLExec, item); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, item.sql); err != nil {
			_ = tx.Rollback()
			return classifyMigrationOperationError(ctx, fmt.Sprintf("apply sqlite migration %d (%s)", item.version, item.name), err)
		}
		if err := injectMigrationFailure(ctx, inject, migrationFaultBeforeMetadataInsert, item); err != nil {
			_ = tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version, name, checksum, applied_at) VALUES (?, ?, ?, ?)`, item.version, item.name, item.checksum, now().UTC().Format(time.RFC3339Nano)); err != nil {
			_ = tx.Rollback()
			return migrationDatabaseError(ctx, fmt.Sprintf("record sqlite migration %d (%s)", item.version, item.name), err)
		}
		if err := injectMigrationFailure(ctx, inject, migrationFaultBeforeCommit, item); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return migrationDatabaseError(ctx, fmt.Sprintf("commit sqlite migration %d (%s)", item.version, item.name), err)
		}
	}
	return nil
}

func injectMigrationFailure(ctx context.Context, inject migrationFaultInjector, point migrationFaultPoint, item migration) error {
	if inject == nil {
		return nil
	}
	if err := inject(ctx, point, item); err != nil {
		return classifyMigrationOperationError(ctx, fmt.Sprintf("migration %s at version %d", point, item.version), err)
	}
	return nil
}

func migrationDatabaseError(ctx context.Context, operation string, err error) error {
	return classifyMigrationOperationError(ctx, operation, err)
}

func classifyMigrationOperationError(ctx context.Context, operation string, err error) error {
	classified := classifySQLiteError(ctx, err)
	if storageport.ClassifyError(classified) != storageport.ErrorClassUnknown {
		return fmt.Errorf("%s: %w", operation, classified)
	}
	if errors.Is(classified, context.Canceled) || errors.Is(classified, context.DeadlineExceeded) {
		return classified
	}
	return fmt.Errorf("%w: %s", storageport.ErrStorageMigration, operation)
}

func schemaMismatch(message string) error {
	return fmt.Errorf("%w: %s", storageport.ErrStorageSchemaMismatch, message)
}
