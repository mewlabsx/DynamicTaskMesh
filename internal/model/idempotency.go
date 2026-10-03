package model

// IdempotencyMode is the persisted recovery qualification of a task step.
// Unspecified is fail-closed and must never be treated as safely replayable.
type IdempotencyMode string

const (
	IdempotencyUnspecified   IdempotencyMode = "unspecified"
	IdempotencyIdempotent    IdempotencyMode = "idempotent"
	IdempotencyNonIdempotent IdempotencyMode = "non_idempotent"
)

func (mode IdempotencyMode) Valid() bool {
	switch mode {
	case "", IdempotencyUnspecified, IdempotencyIdempotent, IdempotencyNonIdempotent:
		return true
	default:
		return false
	}
}
