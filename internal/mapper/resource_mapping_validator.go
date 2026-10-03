package mapper

import (
	"errors"
	"fmt"
	"reflect"

	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
)

var (
	// ErrInvalidResourceMapping means that a Resource Scheduling mapping is
	// malformed or internally inconsistent. It is a caller/mapping invariant
	// failure, not a reason to fall back to Legacy scheduling.
	ErrInvalidResourceMapping = errors.New("invalid resource mapping")

	// ErrStaleResourceMapping means that the mapped ResourceRef no longer
	// describes the authoritative Directory fence. Runtime handles this error
	// before consuming an execution attempt and immediately remaps.
	ErrStaleResourceMapping = errors.New("stale resource mapping")

	// ErrResourceMappingValidationFailed means that the authoritative lookup
	// could not be completed. The underlying lookup error remains in the error
	// chain for recovery and diagnostics.
	ErrResourceMappingValidationFailed = errors.New("resource mapping validation failed")

	// ErrInvalidResourceMappingValidator is returned when a Runtime is given a
	// nil or typed-nil validator dependency.
	ErrInvalidResourceMappingValidator = errors.New("invalid resource mapping validator")
)

// ResourceMappingLookup is the minimal authoritative read dependency needed
// for pre-execution fence validation. resourcedirectory.Directory satisfies
// it without exposing any mutation or lifecycle API to Runtime.
type ResourceMappingLookup interface {
	GetByID(model.ResourceID) (resourcedirectory.ResourceRecordView, error)
}

// ResourceRecordLookup is the concise name used by the M4-C design notes;
// it is an alias so callers can choose either spelling without introducing a
// second dependency contract.
type ResourceRecordLookup = ResourceMappingLookup

// ResourceMappingValidator validates the complete ResourceRef against the
// current authoritative Directory view. It is deliberately separate from the
// Candidate Source: validation must happen for every execution attempt and
// must never be inferred from an earlier scheduling query.
type ResourceMappingValidator struct {
	lookup ResourceMappingLookup
}

// NewResourceMappingValidator constructs a validator over an authoritative
// Resource Directory. Nil and typed-nil dependencies are rejected at the
// boundary so execution cannot fail later with a dependency panic.
func NewResourceMappingValidator(lookup ResourceMappingLookup) (*ResourceMappingValidator, error) {
	if lookup == nil || isNilResourceMappingLookup(lookup) {
		return nil, ErrInvalidResourceMappingValidator
	}
	return &ResourceMappingValidator{lookup: lookup}, nil
}

// Validate checks a Resource-mode mapped step in the frozen order:
// complete ResourceRef, NodeID consistency, authoritative lookup, then the
// Resource and OwnerNode fences plus eligibility. A missing Resource is stale;
// an unexpected lookup failure is a validation failure preserving its cause.
func (validator *ResourceMappingValidator) Validate(step MappedStep) error {
	if validator == nil || validator.lookup == nil {
		return ErrInvalidResourceMappingValidator
	}
	ref := step.ResourceRef
	if err := ref.Validate(); err != nil {
		return fmt.Errorf("%w: resource ref: %w", ErrInvalidResourceMapping, err)
	}
	if step.NodeID != ref.OwnerNodeID {
		return fmt.Errorf(
			"%w: mapped node %q differs from resource owner %q",
			ErrInvalidResourceMapping,
			step.NodeID,
			ref.OwnerNodeID,
		)
	}

	view, err := validator.lookup.GetByID(ref.ResourceID)
	if err != nil {
		if errors.Is(err, resourcedirectory.ErrResourceNotFound) {
			return fmt.Errorf("%w: resource %q: %w", ErrStaleResourceMapping, ref.ResourceID, err)
		}
		return fmt.Errorf("%w: resource %q lookup: %w", ErrResourceMappingValidationFailed, ref.ResourceID, err)
	}
	if !view.Eligible {
		return fmt.Errorf(
			"%w: resource %q is ineligible (%s)",
			ErrStaleResourceMapping,
			ref.ResourceID,
			view.IneligibleReason,
		)
	}
	if view.Descriptor.ID != ref.ResourceID ||
		view.Descriptor.Generation != ref.ResourceGeneration ||
		view.Descriptor.OwnerNodeID != ref.OwnerNodeID ||
		view.NodeGeneration != ref.OwnerNodeGeneration ||
		view.RegistrationID != ref.RegistrationID {
		return fmt.Errorf(
			"%w: resource %q fence differs from current directory view",
			ErrStaleResourceMapping,
			ref.ResourceID,
		)
	}
	return nil
}

func isNilResourceMappingLookup(lookup ResourceMappingLookup) bool {
	value := reflect.ValueOf(lookup)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
