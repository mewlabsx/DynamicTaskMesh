package model

import (
	"errors"
	"reflect"
	"testing"
)

func TestResourceValueObjects(t *testing.T) {
	tests := []struct {
		name string
		run  func() error
	}{
		{name: "resource id", run: func() error { _, err := NewResourceID("resource-1"); return err }},
		{name: "resource kind", run: func() error { _, err := NewResourceKind("capability"); return err }},
		{name: "resource type", run: func() error { _, err := NewResourceType("temperature_sensor"); return err }},
		{name: "operation id", run: func() error { _, err := NewOperationID("read_temperature"); return err }},
		{name: "generation", run: func() error { _, err := NewResourceGeneration(1); return err }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.run(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestResourceValueObjectsRejectInvalidValuesWithoutNormalization(t *testing.T) {
	for _, value := range []string{"", " ", "Capability", "temperature-sensor", "temperature__sensor", " temperature_sensor", "temperature_sensor "} {
		t.Run("resource_type_"+value, func(t *testing.T) {
			if _, err := NewResourceType(value); !errors.Is(err, ErrInvalidResourceType) {
				t.Fatalf("error=%v want %v", err, ErrInvalidResourceType)
			}
		})
		t.Run("operation_id_"+value, func(t *testing.T) {
			if _, err := NewOperationID(value); !errors.Is(err, ErrInvalidOperationID) {
				t.Fatalf("error=%v want %v", err, ErrInvalidOperationID)
			}
		})
	}
	if _, err := NewResourceID(" \t\n "); !errors.Is(err, ErrInvalidResourceID) {
		t.Fatalf("blank resource id error=%v", err)
	}
	for _, value := range []string{"", "memory", "compute", "unknown"} {
		if _, err := NewResourceKind(value); !errors.Is(err, ErrInvalidResourceKind) {
			t.Fatalf("kind %q error=%v", value, err)
		}
	}
	if _, err := NewResourceGeneration(0); !errors.Is(err, ErrInvalidResourceGeneration) {
		t.Fatalf("generation error=%v", err)
	}
}

func TestOperationDescriptorValidation(t *testing.T) {
	if _, err := NewOperationDescriptor("read_temperature", IdempotencyIdempotent, "input.v1", "output.v1"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		op   OperationDescriptor
	}{
		{name: "empty id", op: OperationDescriptor{IdempotencyMode: IdempotencyIdempotent}},
		{name: "unspecified mode", op: OperationDescriptor{ID: "read_temperature", IdempotencyMode: IdempotencyUnspecified}},
		{name: "empty mode", op: OperationDescriptor{ID: "read_temperature"}},
		{name: "unknown mode", op: OperationDescriptor{ID: "read_temperature", IdempotencyMode: "sometimes"}},
		{name: "nul schema", op: OperationDescriptor{ID: "read_temperature", IdempotencyMode: IdempotencyIdempotent, InputSchemaID: "bad\x00schema"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.op.Validate(); !errors.Is(err, ErrInvalidOperationDescriptor) {
				t.Fatalf("error=%v want %v", err, ErrInvalidOperationDescriptor)
			}
		})
	}
}

func TestResourceDescriptorCanonicalOrderAndDefensiveCopy(t *testing.T) {
	operations := []OperationDescriptor{
		{ID: "set_target_temperature", IdempotencyMode: IdempotencyNonIdempotent},
		{ID: "read_temperature", IdempotencyMode: IdempotencyIdempotent},
	}
	attributes := map[string]string{"zone": "lab", "building": "a"}
	descriptor, err := NewResourceDescriptor("resource-1", ResourceKindCapability, "temperature_sensor", "node-a", 1, operations, attributes)
	if err != nil {
		t.Fatal(err)
	}
	operations[0].ID = "mutated"
	attributes["zone"] = "mutated"
	if got := descriptor.Operations[0].ID; got != "read_temperature" {
		t.Fatalf("first operation=%q", got)
	}
	if got := descriptor.Attributes["zone"]; got != "lab" {
		t.Fatalf("zone=%q", got)
	}
	wantAttributes := []ResourceAttribute{{Key: "building", Value: "a"}, {Key: "zone", Value: "lab"}}
	if got := descriptor.CanonicalAttributes(); !reflect.DeepEqual(got, wantAttributes) {
		t.Fatalf("canonical attributes=%v want=%v", got, wantAttributes)
	}
	wantOperations := []OperationDescriptor{
		{ID: "read_temperature", IdempotencyMode: IdempotencyIdempotent},
		{ID: "set_target_temperature", IdempotencyMode: IdempotencyNonIdempotent},
	}
	if got := descriptor.CanonicalOperations(); !reflect.DeepEqual(got, wantOperations) {
		t.Fatalf("canonical operations=%v want=%v", got, wantOperations)
	}
}

func TestResourceDescriptorCloneDoesNotAlias(t *testing.T) {
	descriptor := validResourceDescriptor(t)
	clone := descriptor.Clone()
	clone.Operations[0].ID = "different_operation"
	clone.Attributes["zone"] = "different"
	if descriptor.Operations[0].ID != "read_temperature" || descriptor.Attributes["zone"] != "lab" {
		t.Fatalf("clone mutation affected original: %+v", descriptor)
	}
}

func TestResourceDescriptorRejectsMissingDuplicateAndInvalidFields(t *testing.T) {
	valid := validResourceDescriptor(t)
	tests := []struct {
		name   string
		mutate func(*ResourceDescriptor)
	}{
		{name: "id", mutate: func(value *ResourceDescriptor) { value.ID = "" }},
		{name: "kind", mutate: func(value *ResourceDescriptor) { value.Kind = "memory" }},
		{name: "type", mutate: func(value *ResourceDescriptor) { value.Type = "Temperature" }},
		{name: "owner", mutate: func(value *ResourceDescriptor) { value.OwnerNodeID = " " }},
		{name: "owner nul", mutate: func(value *ResourceDescriptor) { value.OwnerNodeID = "node\x00a" }},
		{name: "generation", mutate: func(value *ResourceDescriptor) { value.Generation = 0 }},
		{name: "operations", mutate: func(value *ResourceDescriptor) { value.Operations = nil }},
		{name: "duplicate operation", mutate: func(value *ResourceDescriptor) { value.Operations = append(value.Operations, value.Operations[0]) }},
		{name: "unknown idempotency", mutate: func(value *ResourceDescriptor) { value.Operations[0].IdempotencyMode = IdempotencyUnspecified }},
		{name: "invalid attribute", mutate: func(value *ResourceDescriptor) { value.Attributes[""] = "value" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := valid.Clone()
			test.mutate(&value)
			if err := value.Validate(); !errors.Is(err, ErrInvalidResourceDescriptor) {
				t.Fatalf("error=%v want %v", err, ErrInvalidResourceDescriptor)
			}
		})
	}
}

func TestResourceRequirementValidationNormalizationAndClone(t *testing.T) {
	attributes := map[string]string{"zone": "lab", "building": "a"}
	requirement, err := NewResourceRequirement(ResourceKindCapability, "temperature_sensor", "read_temperature", attributes)
	if err != nil {
		t.Fatal(err)
	}
	attributes["zone"] = "mutated"
	if requirement.RequiredAttributes["zone"] != "lab" {
		t.Fatal("constructor did not copy attributes")
	}
	want := []ResourceAttribute{{Key: "building", Value: "a"}, {Key: "zone", Value: "lab"}}
	if got := requirement.CanonicalAttributes(); !reflect.DeepEqual(got, want) {
		t.Fatalf("attributes=%v want=%v", got, want)
	}
	clone := requirement.Clone()
	clone.RequiredAttributes["zone"] = "clone"
	if requirement.RequiredAttributes["zone"] != "lab" {
		t.Fatal("clone aliases original")
	}
	equivalent, err := NewResourceRequirement(ResourceKindCapability, "temperature_sensor", "read_temperature", map[string]string{"building": "a", "zone": "lab"})
	if err != nil {
		t.Fatal(err)
	}
	if !requirement.Equal(equivalent) || requirement.Equal(clone) {
		t.Fatalf("stable comparison failed requirement=%+v equivalent=%+v clone=%+v", requirement, equivalent, clone)
	}
}

func TestResourceRequirementEqualDistinguishesAttributePresence(t *testing.T) {
	base := ResourceRequirement{
		Kind:        ResourceKindCapability,
		Type:        "temperature_sensor",
		OperationID: "read_temperature",
	}
	tests := []struct {
		name  string
		left  map[string]string
		right map[string]string
		want  bool
	}{
		{name: "different empty-valued keys", left: map[string]string{"left": ""}, right: map[string]string{"right": ""}, want: false},
		{name: "same empty-valued key", left: map[string]string{"key": ""}, right: map[string]string{"key": ""}, want: true},
		{name: "empty-valued key versus no attributes", left: map[string]string{"key": ""}, right: map[string]string{}, want: false},
		{name: "no attributes versus empty-valued key", left: map[string]string{}, right: map[string]string{"key": ""}, want: false},
		{name: "same pairs with different insertion order", left: map[string]string{"left": "", "right": "value"}, right: map[string]string{"right": "value", "left": ""}, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			left := base
			left.RequiredAttributes = test.left
			right := base
			right.RequiredAttributes = test.right
			if got := left.Equal(right); got != test.want {
				t.Fatalf("Equal() = %v, want %v; left=%v right=%v", got, test.want, test.left, test.right)
			}
		})
	}
}

func TestResourceRequirementRejectsEmptyOrInvalidRequirement(t *testing.T) {
	for _, requirement := range []ResourceRequirement{
		{},
		{Kind: ResourceKindCapability, Type: "temperature_sensor"},
		{Kind: ResourceKindCapability, Type: "Temperature", OperationID: "read_temperature"},
		{Kind: "memory", Type: "temperature_sensor", OperationID: "read_temperature"},
		{Kind: ResourceKindCapability, Type: "temperature_sensor", OperationID: "read_temperature", RequiredAttributes: map[string]string{"": "value"}},
	} {
		if err := requirement.Validate(); !errors.Is(err, ErrInvalidResourceRequirement) {
			t.Fatalf("requirement=%+v error=%v", requirement, err)
		}
	}
}

func TestResourceRefValidation(t *testing.T) {
	if _, err := NewResourceRef("resource-1", 1, "node-a", 1, "registration-a"); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*ResourceRef)
	}{
		{name: "resource id", mutate: func(value *ResourceRef) { value.ResourceID = " " }},
		{name: "resource generation", mutate: func(value *ResourceRef) { value.ResourceGeneration = 0 }},
		{name: "owner", mutate: func(value *ResourceRef) { value.OwnerNodeID = "" }},
		{name: "owner invalid utf8", mutate: func(value *ResourceRef) { value.OwnerNodeID = NodeID(string([]byte{0xff})) }},
		{name: "owner nul", mutate: func(value *ResourceRef) { value.OwnerNodeID = "node\x00a" }},
		{name: "owner generation", mutate: func(value *ResourceRef) { value.OwnerNodeGeneration = 0 }},
		{name: "registration empty", mutate: func(value *ResourceRef) { value.RegistrationID = "" }},
		{name: "registration leading whitespace", mutate: func(value *ResourceRef) { value.RegistrationID = " registration" }},
		{name: "registration trailing whitespace", mutate: func(value *ResourceRef) { value.RegistrationID = "registration " }},
		{name: "registration nul", mutate: func(value *ResourceRef) { value.RegistrationID = "registration\x00a" }},
		{name: "registration invalid utf8", mutate: func(value *ResourceRef) { value.RegistrationID = string([]byte{0xff}) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := ResourceRef{ResourceID: "resource-1", ResourceGeneration: 1, OwnerNodeID: "node-a", OwnerNodeGeneration: 1, RegistrationID: "registration-a"}
			test.mutate(&value)
			if err := value.Validate(); !errors.Is(err, ErrInvalidResourceRef) {
				t.Fatalf("error=%v want %v", err, ErrInvalidResourceRef)
			}
		})
	}
}

func validResourceDescriptor(t *testing.T) ResourceDescriptor {
	t.Helper()
	descriptor, err := NewResourceDescriptor(
		"resource-1", ResourceKindCapability, "temperature_sensor", "node-a", 1,
		[]OperationDescriptor{{ID: "read_temperature", IdempotencyMode: IdempotencyIdempotent}},
		map[string]string{"zone": "lab"},
	)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}
