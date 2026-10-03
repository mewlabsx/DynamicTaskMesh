package model

import (
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestLegacyCapabilityResourceIDFixedVectors(t *testing.T) {
	tests := []struct {
		nodeID       NodeID
		resourceType ResourceType
		want         ResourceID
	}{
		{nodeID: "node-a", resourceType: "temperature_sensor", want: "legacy-capability:v1:YKATYT3IH6LQ3TEM4C2F4FDTJTRH6CTLEKTUFQWE5LMMZVWCL3TQ"},
		{nodeID: "node-a", resourceType: "cooling_control", want: "legacy-capability:v1:KIXNLMHRO5ZRX4IIXI4LJ7N35MPZQ5S2DKOM5XLYDSR3JDU3XMOQ"},
		{nodeID: "node-b", resourceType: "temperature_sensor", want: "legacy-capability:v1:AJXMH4L7NM5FQWCDT26LTBPSMQGCTZ7PBH45MG4SOOQAN65Q3NDQ"},
		{nodeID: "ab", resourceType: "c", want: "legacy-capability:v1:NQBS4YY5HGQU3BNP67RRSVDK64A6E3EXWV6KSX575HDLVBK7M67Q"},
		{nodeID: "a", resourceType: "bc", want: "legacy-capability:v1:IC5VI7MTNO6TCMMO4N5MQ6M6P3F3ELW2EZI7MXRSCS77XDHJPO2A"},
	}
	for _, test := range tests {
		t.Run(string(test.nodeID)+"_"+string(test.resourceType), func(t *testing.T) {
			got, err := LegacyCapabilityResourceID(test.nodeID, test.resourceType)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("node=%q type=%q id=%q want=%q", test.nodeID, test.resourceType, got, test.want)
			}
			encoded := strings.TrimPrefix(got.String(), legacyCapabilityResourceIDPrefix)
			if len(encoded) != 52 || strings.Contains(encoded, "=") || strings.ToUpper(encoded) != encoded {
				t.Fatalf("non-canonical Base32 %q", encoded)
			}
		})
	}
}

func TestLegacyCapabilityResourceIDStableAndIsolated(t *testing.T) {
	first, err := LegacyCapabilityResourceID("node-a", "temperature_sensor")
	if err != nil {
		t.Fatal(err)
	}
	second, _ := LegacyCapabilityResourceID("node-a", "temperature_sensor")
	otherNode, _ := LegacyCapabilityResourceID("node-b", "temperature_sensor")
	otherType, _ := LegacyCapabilityResourceID("node-a", "cooling_control")
	ambiguousA, _ := LegacyCapabilityResourceID("ab", "c")
	ambiguousB, _ := LegacyCapabilityResourceID("a", "bc")
	if first != second || first == otherNode || first == otherType || ambiguousA == ambiguousB {
		t.Fatalf("first=%q second=%q otherNode=%q otherType=%q ambiguous=(%q,%q)", first, second, otherNode, otherType, ambiguousA, ambiguousB)
	}
}

func TestLegacyCapabilityResourceIDRejectsInvalidInputs(t *testing.T) {
	invalidUTF8 := string([]byte{0xff})
	for _, nodeID := range []NodeID{"", " \t ", "node\x00a", NodeID(invalidUTF8)} {
		if _, err := LegacyCapabilityResourceID(nodeID, "temperature_sensor"); !errors.Is(err, ErrInvalidResourceID) {
			t.Fatalf("node=%q validUTF8=%v error=%v", nodeID, utf8.ValidString(string(nodeID)), err)
		}
	}
	for _, resourceType := range []ResourceType{"", "Temperature", "temperature-sensor", " temperature_sensor"} {
		if _, err := LegacyCapabilityResourceID("node-a", resourceType); !errors.Is(err, ErrInvalidResourceID) {
			t.Fatalf("type=%q error=%v", resourceType, err)
		}
	}
}

func TestAdaptLegacyCapabilityMappings(t *testing.T) {
	tests := []struct {
		capability Capability
		operation  OperationID
		mode       IdempotencyMode
	}{
		{capability: "temperature_sensor", operation: "read_temperature", mode: IdempotencyIdempotent},
		{capability: "cooling_control", operation: "set_target_temperature", mode: IdempotencyNonIdempotent},
	}
	for _, test := range tests {
		t.Run(test.capability.String(), func(t *testing.T) {
			descriptor, err := AdaptLegacyCapability("node-a", test.capability, 7)
			if err != nil {
				t.Fatal(err)
			}
			if descriptor.Kind != ResourceKindCapability || descriptor.Type.String() != test.capability.String() || descriptor.OwnerNodeID != "node-a" || descriptor.Generation != 7 {
				t.Fatalf("descriptor=%+v", descriptor)
			}
			if len(descriptor.Operations) != 1 || descriptor.Operations[0].ID != test.operation || descriptor.Operations[0].IdempotencyMode != test.mode {
				t.Fatalf("operations=%+v", descriptor.Operations)
			}
		})
	}
}

func TestAdaptLegacyCapabilityRejectsUnknownEmptyAndZeroGeneration(t *testing.T) {
	for _, capability := range []Capability{"", "unknown", "temperature-sensor", " temperature_sensor"} {
		if _, err := AdaptLegacyCapability("node-a", capability, 1); !errors.Is(err, ErrUnsupportedLegacyCapability) {
			t.Fatalf("capability=%q error=%v", capability, err)
		}
	}
	if _, err := AdaptLegacyCapability("node-a", "temperature_sensor", 0); !errors.Is(err, ErrInvalidResourceGeneration) {
		t.Fatalf("generation error=%v", err)
	}
}

func TestAdaptLegacyCapabilityGenerationDoesNotAffectIDAndIsNotAllocated(t *testing.T) {
	first, err := AdaptLegacyCapability("node-a", "temperature_sensor", 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := AdaptLegacyCapability("node-a", "temperature_sensor", 9)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || first.Generation != 1 || second.Generation != 9 {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
}

func TestAdaptLegacyCapabilityRepeatedInputIsEquivalentAndIndependent(t *testing.T) {
	first, err := AdaptLegacyCapability("node-a", "temperature_sensor", 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := AdaptLegacyCapability("node-a", "temperature_sensor", 1)
	if err != nil {
		t.Fatal(err)
	}
	first.Attributes["mutated"] = "yes"
	first.Operations[0].ID = "mutated"
	if len(second.Attributes) != 0 || second.Operations[0].ID != "read_temperature" {
		t.Fatalf("adapter results alias: first=%+v second=%+v", first, second)
	}
}

func TestLegacyCapabilityRequirementFromFrozenMappings(t *testing.T) {
	tests := []struct {
		capability Capability
		operation  OperationID
		mode       IdempotencyMode
	}{
		{capability: "temperature_sensor", operation: "read_temperature", mode: IdempotencyIdempotent},
		{capability: "cooling_control", operation: "set_target_temperature", mode: IdempotencyNonIdempotent},
	}
	for _, test := range tests {
		t.Run(test.capability.String(), func(t *testing.T) {
			requirement, err := LegacyCapabilityRequirement(test.capability)
			if err != nil {
				t.Fatal(err)
			}
			if requirement.Kind != ResourceKindCapability || requirement.Type.String() != test.capability.String() || requirement.OperationID != test.operation {
				t.Fatalf("requirement=%+v", requirement)
			}
			if len(requirement.RequiredAttributes) != 0 {
				t.Fatalf("required attributes must be empty, got %v", requirement.RequiredAttributes)
			}
			if err := requirement.Validate(); err != nil {
				t.Fatalf("derived requirement failed validation: %v", err)
			}
			descriptor, err := AdaptLegacyCapability("node-a", test.capability, 1)
			if err != nil {
				t.Fatal(err)
			}
			if len(descriptor.Operations) != 1 || descriptor.Operations[0].IdempotencyMode != test.mode {
				t.Fatalf("operation mode mismatch: %+v", descriptor.Operations)
			}
		})
	}
}

func TestLegacyCapabilityRequirementRejectsUnknownAndEmpty(t *testing.T) {
	for _, capability := range []Capability{"", "unknown", "temperature-sensor", " temperature_sensor"} {
		if _, err := LegacyCapabilityRequirement(capability); !errors.Is(err, ErrUnsupportedLegacyCapability) {
			t.Fatalf("capability=%q error=%v, want ErrUnsupportedLegacyCapability", capability, err)
		}
	}
}

func TestLegacyCapabilityRequirementSharesSingleMappingTable(t *testing.T) {
	requirement, err := LegacyCapabilityRequirement("temperature_sensor")
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := AdaptLegacyCapability("node-a", "temperature_sensor", 1)
	if err != nil {
		t.Fatal(err)
	}
	if requirement.OperationID != descriptor.Operations[0].ID {
		t.Fatalf("requirement operation %q differs from adapter operation %q; mappings must be shared",
			requirement.OperationID, descriptor.Operations[0].ID)
	}
	expected := legacyCapabilityMappings["temperature_sensor"].operationID
	if requirement.OperationID != expected {
		t.Fatalf("requirement operation %q differs from the frozen mapping %q", requirement.OperationID, expected)
	}
}

func TestLegacyCapabilityRequirementIsPure(t *testing.T) {
	first, err := LegacyCapabilityRequirement("cooling_control")
	if err != nil {
		t.Fatal(err)
	}
	second, err := LegacyCapabilityRequirement("cooling_control")
	if err != nil {
		t.Fatal(err)
	}
	first.RequiredAttributes = map[string]string{"mutated": "yes"}
	if len(second.RequiredAttributes) != 0 {
		t.Fatalf("requirement outputs alias: %v", second.RequiredAttributes)
	}
	third, err := LegacyCapabilityRequirement("cooling_control")
	if err != nil {
		t.Fatal(err)
	}
	if !third.Equal(second) {
		t.Fatalf("requirements differ across calls: %+v vs %+v", third, second)
	}
}
