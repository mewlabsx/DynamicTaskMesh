package storage

import "errors"

type ErrorClass string

const (
	ErrorClassUnknown     ErrorClass = "unknown"
	ErrorClassPath        ErrorClass = "path"
	ErrorClassUnavailable ErrorClass = "unavailable"
	ErrorClassReadOnly    ErrorClass = "read_only"
	ErrorClassFull        ErrorClass = "full"
	ErrorClassIO          ErrorClass = "io"
	ErrorClassMigration   ErrorClass = "migration"
	ErrorClassSchema      ErrorClass = "schema_mismatch"
	ErrorClassCorrupt     ErrorClass = "corrupt"
	ErrorClassIntegrity   ErrorClass = "integrity"
	ErrorClassClosed      ErrorClass = "closed"
)

func ClassifyError(err error) ErrorClass {
	switch {
	case errors.Is(err, ErrStoragePath):
		return ErrorClassPath
	case errors.Is(err, ErrStorageUnavailable):
		return ErrorClassUnavailable
	case errors.Is(err, ErrStorageReadOnly):
		return ErrorClassReadOnly
	case errors.Is(err, ErrStorageFull):
		return ErrorClassFull
	case errors.Is(err, ErrStorageIO):
		return ErrorClassIO
	case errors.Is(err, ErrStorageMigration):
		return ErrorClassMigration
	case errors.Is(err, ErrStorageSchemaMismatch):
		return ErrorClassSchema
	case errors.Is(err, ErrStorageCorrupt):
		return ErrorClassCorrupt
	case errors.Is(err, ErrStorageIntegrity):
		return ErrorClassIntegrity
	case errors.Is(err, ErrStorageClosed):
		return ErrorClassClosed
	default:
		return ErrorClassUnknown
	}
}

func StartupExitCode(err error) int {
	switch ClassifyError(err) {
	case ErrorClassPath, ErrorClassReadOnly, ErrorClassFull, ErrorClassIO:
		return 10
	case ErrorClassUnavailable:
		return 11
	case ErrorClassMigration, ErrorClassSchema:
		return 12
	case ErrorClassCorrupt, ErrorClassIntegrity:
		return 13
	default:
		return 1
	}
}
