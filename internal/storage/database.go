package storage

import "context"

// Database describes the infrastructure lifecycle exposed to composition code.
// Application and domain packages depend on repository ports instead of this
// interface or a concrete SQL implementation.
type Database interface {
	Health(context.Context) error
	Close() error
}
