package sqlite

import (
	"context"
	"errors"
	"fmt"

	storageport "dtm/internal/storage"
	moderncsqlite "modernc.org/sqlite"
)

const (
	sqlitePrimaryBusy     = 5
	sqlitePrimaryLocked   = 6
	sqlitePrimaryReadOnly = 8
	sqlitePrimaryIOErr    = 10
	sqlitePrimaryCorrupt  = 11
	sqlitePrimaryFull     = 13
	sqlitePrimaryCantOpen = 14
	sqlitePrimaryNotADB   = 26
)

func classifySQLiteError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		isKnownStorageError(err) {
		return err
	}
	var sqliteErr *moderncsqlite.Error
	if !errors.As(err, &sqliteErr) {
		return err
	}
	category := storageErrorForSQLiteCode(sqliteErr.Code())
	if category == nil {
		return err
	}
	return fmt.Errorf("%w: sqlite primary code %d", category, sqliteErr.Code()&0xff)
}

func storageErrorForSQLiteCode(code int) error {
	switch code & 0xff {
	case sqlitePrimaryBusy, sqlitePrimaryLocked:
		return storageport.ErrStorageUnavailable
	case sqlitePrimaryReadOnly:
		return storageport.ErrStorageReadOnly
	case sqlitePrimaryIOErr:
		return storageport.ErrStorageIO
	case sqlitePrimaryCorrupt, sqlitePrimaryNotADB:
		return storageport.ErrStorageCorrupt
	case sqlitePrimaryFull:
		return storageport.ErrStorageFull
	case sqlitePrimaryCantOpen:
		return storageport.ErrStoragePath
	default:
		return nil
	}
}

func sqliteErrorPrimaryCode(err error) (int, bool) {
	var sqliteErr *moderncsqlite.Error
	if !errors.As(err, &sqliteErr) {
		return 0, false
	}
	return sqliteErr.Code() & 0xff, true
}

func isKnownStorageError(err error) bool {
	return storageport.ClassifyError(err) != storageport.ErrorClassUnknown ||
		errors.Is(err, storageport.ErrNotFound) ||
		errors.Is(err, storageport.ErrInvalidArgument) ||
		errors.Is(err, storageport.ErrAlreadyExists) ||
		errors.Is(err, storageport.ErrConflict) ||
		errors.Is(err, storageport.ErrInvalidData)
}

func wrapStorageError(ctx context.Context, operation string, err error) error {
	if err == nil {
		return nil
	}
	classified := classifySQLiteError(ctx, err)
	if classified == err {
		return fmt.Errorf("%s: %w", operation, err)
	}
	return fmt.Errorf("%s: %w", operation, classified)
}

func classifyRepositoryReturn(ctx context.Context, returnErr *error) {
	if returnErr == nil || *returnErr == nil {
		return
	}
	*returnErr = classifySQLiteError(ctx, *returnErr)
}
