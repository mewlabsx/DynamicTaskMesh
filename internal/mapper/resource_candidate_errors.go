package mapper

import (
	"errors"
	"fmt"

	"dtm/internal/model"
)

var (
	// ErrResourceUnavailable reports that the Candidate Source is ready, the
	// query is valid, the underlying query succeeded, and no eligible Resource
	// remains after filtering. Returned errors also satisfy
	// errors.Is(err, ErrCapabilityUnavailable) for v0.3 compatibility.
	ErrResourceUnavailable = errors.New("resource unavailable")

	// ErrResourceCandidateSourceNotReady reports that Resource Scheduling is
	// enabled but the authoritative Resource state has not finished loading,
	// so the query result cannot be interpreted authoritatively.
	ErrResourceCandidateSourceNotReady = errors.New("resource candidate source not ready")

	// ErrResourceCandidateQueryFailed reports an internal failure of the
	// underlying Directory query or adapter. It must never be interpreted as
	// "no candidates".
	ErrResourceCandidateQueryFailed = errors.New("resource candidate query failed")

	// ErrInvalidResourceCandidateQuery reports a caller error in the query
	// (invalid requirement or invalid excluded IDs).
	ErrInvalidResourceCandidateQuery = errors.New("invalid resource candidate query")
)

// resourceUnavailable wraps the sentinel so the returned error satisfies both
// errors.Is(err, ErrResourceUnavailable) and the v0.3 compatibility check
// errors.Is(err, ErrCapabilityUnavailable).
func resourceUnavailableError(requirement model.ResourceRequirement) error {
	return fmt.Errorf(
		"%w: %w: no eligible resource for capability %q",
		ErrResourceUnavailable,
		ErrCapabilityUnavailable,
		requirement.Type,
	)
}
