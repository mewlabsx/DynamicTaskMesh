package sqlite

import (
	"context"
	"errors"

	storageport "dtm/internal/storage"
)

func classifyTaskQueryError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, storageport.ErrNotFound) || errors.Is(err, storageport.ErrInvalidArgument) ||
		errors.Is(err, storageport.ErrUnavailable) || errors.Is(err, storageport.ErrClosed) {
		return err
	}
	return classifySQLiteError(ctx, err)
}

func isSQLiteTransientQueryCode(code int) bool {
	switch code & 0xff {
	case sqlitePrimaryBusy, sqlitePrimaryLocked:
		return true
	default:
		return false
	}
}
