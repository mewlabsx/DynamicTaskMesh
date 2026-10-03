package mapper

import (
	"errors"
	"fmt"
	"testing"

	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
)

type mappingLookupFunc func(model.ResourceID) (resourcedirectory.ResourceRecordView, error)

func (function mappingLookupFunc) GetByID(id model.ResourceID) (resourcedirectory.ResourceRecordView, error) {
	return function(id)
}

func validResourceMappedStep(t *testing.T) MappedStep {
	t.Helper()
	return MappedStep{
		ID: "step-1", Capability: "temperature_sensor", NodeID: "node-a",
		ResourceRef: model.ResourceRef{
			ResourceID: "resource-a", ResourceGeneration: 2, OwnerNodeID: "node-a",
			OwnerNodeGeneration: 3, RegistrationID: "registration-2",
		},
	}
}

func validResourceView(t *testing.T) resourcedirectory.ResourceRecordView {
	t.Helper()
	return candidateView(t, "resource-a", "node-a", 3, "registration-2", 2)
}

func TestResourceMappingValidatorAcceptsCurrentFence(t *testing.T) {
	view := validResourceView(t)
	validator, err := NewResourceMappingValidator(mappingLookupFunc(func(model.ResourceID) (resourcedirectory.ResourceRecordView, error) {
		return view, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := validator.Validate(validResourceMappedStep(t)); err != nil {
		t.Fatalf("Validate() = %v", err)
	}
}

func TestResourceMappingValidatorClassifiesInvalidAndStaleMappings(t *testing.T) {
	view := validResourceView(t)
	validator, err := NewResourceMappingValidator(mappingLookupFunc(func(model.ResourceID) (resourcedirectory.ResourceRecordView, error) {
		return view, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		step MappedStep
		want error
	}{
		{name: "zero ref", step: MappedStep{ID: "step-1", Capability: "temperature_sensor", NodeID: "node-a"}, want: ErrInvalidResourceMapping},
		{name: "partial ref", step: func() MappedStep {
			step := validResourceMappedStep(t)
			step.ResourceRef.RegistrationID = ""
			return step
		}(), want: ErrInvalidResourceMapping},
		{name: "owner mismatch", step: func() MappedStep {
			step := validResourceMappedStep(t)
			step.NodeID = "node-b"
			return step
		}(), want: ErrInvalidResourceMapping},
		{name: "generation stale", step: func() MappedStep {
			step := validResourceMappedStep(t)
			step.ResourceRef.ResourceGeneration = 1
			return step
		}(), want: ErrStaleResourceMapping},
		{name: "node generation stale", step: func() MappedStep {
			step := validResourceMappedStep(t)
			step.ResourceRef.OwnerNodeGeneration = 2
			return step
		}(), want: ErrStaleResourceMapping},
		{name: "registration stale", step: func() MappedStep {
			step := validResourceMappedStep(t)
			step.ResourceRef.RegistrationID = "registration-1"
			return step
		}(), want: ErrStaleResourceMapping},
		{name: "ineligible", step: validResourceMappedStep(t), want: ErrStaleResourceMapping},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lookupValidator := validator
			if test.name == "ineligible" {
				ineligible := view
				ineligible.Eligible = false
				lookupValidator, err = NewResourceMappingValidator(mappingLookupFunc(func(model.ResourceID) (resourcedirectory.ResourceRecordView, error) {
					return ineligible, nil
				}))
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := lookupValidator.Validate(test.step); !errors.Is(err, test.want) {
				t.Fatalf("Validate() = %v, want errors.Is(..., %v)", err, test.want)
			}
		})
	}
}

func TestResourceMappingValidatorPreservesLookupFailure(t *testing.T) {
	lookupErr := errors.New("directory read failed")
	validator, err := NewResourceMappingValidator(mappingLookupFunc(func(model.ResourceID) (resourcedirectory.ResourceRecordView, error) {
		return resourcedirectory.ResourceRecordView{}, lookupErr
	}))
	if err != nil {
		t.Fatal(err)
	}
	err = validator.Validate(validResourceMappedStep(t))
	if !errors.Is(err, ErrResourceMappingValidationFailed) || !errors.Is(err, lookupErr) {
		t.Fatalf("Validate() = %v, want validation sentinel and lookup cause", err)
	}
}

func TestResourceMappingValidatorMissingResourceIsStale(t *testing.T) {
	validator, err := NewResourceMappingValidator(mappingLookupFunc(func(id model.ResourceID) (resourcedirectory.ResourceRecordView, error) {
		return resourcedirectory.ResourceRecordView{}, fmt.Errorf("%w: %s", resourcedirectory.ErrResourceNotFound, id)
	}))
	if err != nil {
		t.Fatal(err)
	}
	if err := validator.Validate(validResourceMappedStep(t)); !errors.Is(err, ErrStaleResourceMapping) || !errors.Is(err, resourcedirectory.ErrResourceNotFound) {
		t.Fatalf("Validate() = %v, want stale and not-found", err)
	}
}
