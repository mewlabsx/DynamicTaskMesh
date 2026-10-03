package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	storageport "dtm/internal/storage"

	_ "modernc.org/sqlite"
)

const (
	DriverName           = "sqlite"
	DefaultJournalMode   = "WAL"
	DefaultSynchronous   = "NORMAL"
	DefaultBusyTimeout   = 5 * time.Second
	defaultSQLDriverName = "sqlite"
)

var ErrDatabaseClosed = storageport.ErrClosed

type Options struct {
	Path         string
	JournalMode  string
	Synchronous  string
	BusyTimeout  time.Duration
	AutoMigrate  bool
	MigrationSet string

	mkdirAll func(string, os.FileMode) error
	stat     func(string) (os.FileInfo, error)
}

type Database struct {
	mu              sync.RWMutex
	db              *sql.DB
	closed          bool
	migrationSet    string
	schemaVersion   int
	journalMode     string
	synchronous     string
	foreignKeys     bool
	quickCheck      bool
	foreignKeyCheck bool
}

type Status struct {
	MigrationSet    string
	SchemaVersion   int
	JournalMode     string
	Synchronous     string
	ForeignKeys     bool
	QuickCheck      bool
	ForeignKeyCheck bool
}

func openDatabase(ctx context.Context, options Options) (*Database, error) {
	normalized, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	resolvedPath, existed, err := prepareDatabasePath(normalized)
	if err != nil {
		return nil, err
	}
	normalized.Path = resolvedPath
	if existed && normalized.Path != ":memory:" {
		if err := preflightExistingDatabase(ctx, normalized); err != nil {
			return nil, fmt.Errorf("preflight sqlite database: %w", err)
		}
	}
	dsn := databaseSourceNameForOptions(normalized, "rwc")
	db, err := sql.Open(defaultSQLDriverName, dsn)
	if err != nil {
		return nil, wrapStorageError(ctx, "open sqlite database", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	database := &Database{db: db, migrationSet: normalized.MigrationSet, journalMode: strings.ToLower(normalized.JournalMode), synchronous: strings.ToLower(normalized.Synchronous)}
	closeOnError := func(err error) (*Database, error) {
		_ = database.Close()
		return nil, err
	}
	if err := database.initialize(ctx, normalized); err != nil {
		return closeOnError(fmt.Errorf("initialize sqlite database: %w", err))
	}
	return database, nil
}

func normalizeOptions(options Options) (Options, error) {
	options.Path = strings.TrimSpace(options.Path)
	if options.Path == "" {
		return Options{}, fmt.Errorf("%w: database path is required", storageport.ErrStoragePath)
	}
	options.MigrationSet = strings.TrimSpace(options.MigrationSet)
	if options.MigrationSet != "core" && options.MigrationSet != "agent" {
		return Options{}, fmt.Errorf("%w: migration set is required", storageport.ErrStorageMigration)
	}
	options.JournalMode = strings.ToUpper(strings.TrimSpace(options.JournalMode))
	if options.JournalMode == "" {
		options.JournalMode = DefaultJournalMode
	}
	if options.JournalMode != "WAL" && options.JournalMode != "DELETE" {
		return Options{}, fmt.Errorf("%w: unsupported journal mode", storageport.ErrStorageMigration)
	}
	options.Synchronous = strings.ToUpper(strings.TrimSpace(options.Synchronous))
	if options.Synchronous == "" {
		options.Synchronous = DefaultSynchronous
	}
	switch options.Synchronous {
	case "OFF", "NORMAL", "FULL", "EXTRA":
	default:
		return Options{}, fmt.Errorf("%w: unsupported synchronous mode", storageport.ErrStorageMigration)
	}
	if options.BusyTimeout == 0 {
		options.BusyTimeout = DefaultBusyTimeout
	}
	if options.BusyTimeout < 0 {
		return Options{}, fmt.Errorf("%w: busy timeout must not be negative", storageport.ErrStorageMigration)
	}
	if options.mkdirAll == nil {
		options.mkdirAll = os.MkdirAll
	}
	if options.stat == nil {
		options.stat = os.Stat
	}
	return options, nil
}

func prepareDatabasePath(options Options) (string, bool, error) {
	if options.Path == ":memory:" {
		return options.Path, false, nil
	}
	absolute, err := filepath.Abs(options.Path)
	if err != nil {
		return "", false, fmt.Errorf("%w: resolve database path", storageport.ErrStoragePath)
	}
	info, err := options.stat(absolute)
	switch {
	case err == nil:
		if info.IsDir() {
			return "", false, fmt.Errorf("%w: database path is a directory", storageport.ErrStoragePath)
		}
		if info.Mode().Perm()&0o222 == 0 {
			return "", true, fmt.Errorf("%w: database file is not writable", storageport.ErrStorageReadOnly)
		}
		return absolute, true, nil
	case !errors.Is(err, os.ErrNotExist):
		if errors.Is(err, os.ErrPermission) {
			return "", false, fmt.Errorf("%w: inspect database path", storageport.ErrStorageReadOnly)
		}
		return "", false, fmt.Errorf("%w: inspect database path", storageport.ErrStoragePath)
	}
	if !options.AutoMigrate {
		return "", false, schemaMismatch("database schema does not match this binary")
	}
	if err := options.mkdirAll(filepath.Dir(absolute), 0o755); err != nil {
		if errors.Is(err, os.ErrPermission) {
			return "", false, fmt.Errorf("%w: create database directory", storageport.ErrStorageReadOnly)
		}
		return "", false, fmt.Errorf("%w: create database directory", storageport.ErrStoragePath)
	}
	return absolute, false, nil
}

func databaseSourceName(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("%w: database path is required", storageport.ErrStoragePath)
	}
	if path != ":memory:" {
		absolute, err := filepath.Abs(path)
		if err != nil {
			return "", fmt.Errorf("%w: resolve database path", storageport.ErrStoragePath)
		}
		path = absolute
	}
	return databaseSourceNameMode(path, "rwc"), nil
}

func databaseSourceNameMode(path, mode string) string {
	if path == ":memory:" {
		return "file:dtm-memory?mode=memory&cache=shared"
	}
	values := url.Values{}
	values.Set("mode", mode)
	return "file:" + filepath.ToSlash(path) + "?" + values.Encode()
}

func databaseSourceNameForOptions(options Options, mode string) string {
	if options.Path == ":memory:" {
		return "file:dtm-memory-" + options.MigrationSet + "?mode=memory&cache=shared"
	}
	return databaseSourceNameMode(options.Path, mode)
}

func preflightExistingDatabase(ctx context.Context, options Options) error {
	db, err := sql.Open(defaultSQLDriverName, databaseSourceNameMode(options.Path, "ro"))
	if err != nil {
		return wrapStorageError(ctx, "open existing database read-only", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	defer db.Close()
	if _, err := db.ExecContext(ctx, "PRAGMA busy_timeout = "+strconv.FormatInt(options.BusyTimeout.Milliseconds(), 10)); err != nil {
		return wrapStorageError(ctx, "configure preflight busy timeout", err)
	}
	if err := db.PingContext(ctx); err != nil {
		return wrapStorageError(ctx, "ping existing database", err)
	}
	if err := checkDatabaseIntegrity(ctx, db); err != nil {
		return err
	}
	migrations, err := embeddedMigrations(options.MigrationSet)
	if err != nil {
		return err
	}
	return validateExistingMigrationMetadata(ctx, db, options.MigrationSet, migrations, options.AutoMigrate)
}

func (database *Database) initialize(ctx context.Context, options Options) error {
	if _, err := database.db.ExecContext(ctx, "PRAGMA busy_timeout = "+strconv.FormatInt(options.BusyTimeout.Milliseconds(), 10)); err != nil {
		return wrapStorageError(ctx, "configure busy timeout", err)
	}
	if err := database.db.PingContext(ctx); err != nil {
		return wrapStorageError(ctx, "ping sqlite database", err)
	}
	pragmas := []struct{ name, value string }{
		{"foreign_keys", "ON"},
		{"journal_mode", options.JournalMode},
		{"synchronous", options.Synchronous},
	}
	for _, pragma := range pragmas {
		if _, err := database.db.ExecContext(ctx, "PRAGMA "+pragma.name+" = "+pragma.value); err != nil {
			return wrapStorageError(ctx, "configure "+pragma.name, err)
		}
	}
	migrations, err := embeddedMigrations(options.MigrationSet)
	if err != nil {
		return err
	}
	if err := ensureMigrationState(ctx, database.db, options.MigrationSet, migrations, options.AutoMigrate, time.Now); err != nil {
		return err
	}
	if err := checkDatabaseIntegrity(ctx, database.db); err != nil {
		return err
	}
	database.schemaVersion = len(migrations)
	database.foreignKeys = true
	database.quickCheck = true
	database.foreignKeyCheck = true
	return nil
}

func checkDatabaseIntegrity(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "PRAGMA quick_check")
	if err != nil {
		classified := classifySQLiteError(ctx, err)
		if storageport.ClassifyError(classified) == storageport.ErrorClassUnknown {
			return fmt.Errorf("%w: quick check failed", storageport.ErrStorageCorrupt)
		}
		return classified
	}
	quickOK := false
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			_ = rows.Close()
			return classifySQLiteError(ctx, err)
		}
		if result == "ok" {
			quickOK = true
		} else {
			_ = rows.Close()
			return fmt.Errorf("%w: quick check reported an error", storageport.ErrStorageCorrupt)
		}
	}
	if err := rows.Close(); err != nil {
		return classifySQLiteError(ctx, err)
	}
	if !quickOK {
		return fmt.Errorf("%w: quick check returned no result", storageport.ErrStorageCorrupt)
	}

	foreignRows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return classifySQLiteError(ctx, err)
	}
	defer foreignRows.Close()
	if foreignRows.Next() {
		return fmt.Errorf("%w: foreign key integrity check failed", storageport.ErrStorageIntegrity)
	}
	if err := foreignRows.Err(); err != nil {
		return classifySQLiteError(ctx, err)
	}
	return nil
}

func (database *Database) SQLDB() (*sql.DB, error) {
	database.mu.RLock()
	defer database.mu.RUnlock()
	if database.closed || database.db == nil {
		return nil, ErrDatabaseClosed
	}
	return database.db, nil
}

func (database *Database) Health(ctx context.Context) error {
	db, err := database.SQLDB()
	if err != nil {
		return err
	}
	if err := db.PingContext(ctx); err != nil {
		return wrapStorageError(ctx, "ping sqlite database", err)
	}
	return checkDatabaseIntegrity(ctx, db)
}

func (database *Database) Status() Status {
	database.mu.RLock()
	defer database.mu.RUnlock()
	return Status{MigrationSet: database.migrationSet, SchemaVersion: database.schemaVersion, JournalMode: database.journalMode, Synchronous: database.synchronous, ForeignKeys: database.foreignKeys, QuickCheck: database.quickCheck, ForeignKeyCheck: database.foreignKeyCheck}
}

func (database *Database) Close() error {
	if database == nil {
		return nil
	}
	database.mu.Lock()
	defer database.mu.Unlock()
	if database.closed {
		return nil
	}
	database.closed = true
	if database.db == nil {
		return nil
	}
	database.db.SetMaxIdleConns(0)
	return database.db.Close()
}
