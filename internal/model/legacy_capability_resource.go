package model

import (
	"crypto/sha256"
	"encoding/base32"
	"errors"
	"fmt"
)

const legacyCapabilityResourceIDPrefix = "legacy-capability:v1:"

var ErrUnsupportedLegacyCapability = errors.New("unsupported legacy capability")

type legacyCapabilityMapping struct {
	operationID OperationID
	mode        IdempotencyMode
}

var legacyCapabilityMappings = map[Capability]legacyCapabilityMapping{
	"temperature_sensor": {operationID: "read_temperature", mode: IdempotencyIdempotent},
	"cooling_control":    {operationID: "set_target_temperature", mode: IdempotencyNonIdempotent},
}

func LegacyCapabilityResourceID(nodeID NodeID, resourceType ResourceType) (ResourceID, error) {
	if err := validateNodeID(nodeID); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidResourceID, err)
	}
	if err := resourceType.Validate(); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidResourceID, err)
	}
	payload := make([]byte, 0, len(nodeID)+1+len(resourceType))
	payload = append(payload, []byte(nodeID)...)
	payload = append(payload, 0)
	payload = append(payload, []byte(resourceType)...)
	digest := sha256.Sum256(payload)
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(digest[:])
	return ResourceID(legacyCapabilityResourceIDPrefix + encoded), nil
}

func AdaptLegacyCapability(nodeID NodeID, capability Capability, generation ResourceGeneration) (ResourceDescriptor, error) {
	mapping, exists := legacyCapabilityMappings[capability]
	if !exists {
		return ResourceDescriptor{}, fmt.Errorf("%w: %q", ErrUnsupportedLegacyCapability, capability)
	}
	if err := generation.Validate(); err != nil {
		return ResourceDescriptor{}, err
	}
	resourceType, err := NewResourceType(capability.String())
	if err != nil {
		return ResourceDescriptor{}, err
	}
	resourceID, err := LegacyCapabilityResourceID(nodeID, resourceType)
	if err != nil {
		return ResourceDescriptor{}, err
	}
	operation, err := NewOperationDescriptor(mapping.operationID, mapping.mode, "", "")
	if err != nil {
		return ResourceDescriptor{}, err
	}
	return NewResourceDescriptor(resourceID, ResourceKindCapability, resourceType, nodeID, generation, []OperationDescriptor{operation}, nil)
}

// LegacyCapabilityRequirement derives the ResourceRequirement for a legacy
// Capability from the frozen legacyCapabilityMappings table, the single
// source of truth shared with AdaptLegacyCapability. Unsupported capabilities
// return a distinguishable error via errors.Is.
func LegacyCapabilityRequirement(capability Capability) (ResourceRequirement, error) {
	mapping, exists := legacyCapabilityMappings[capability]
	if !exists {
		return ResourceRequirement{}, fmt.Errorf("%w: %q", ErrUnsupportedLegacyCapability, capability)
	}
	resourceType, err := NewResourceType(capability.String())
	if err != nil {
		return ResourceRequirement{}, err
	}
	return NewResourceRequirement(ResourceKindCapability, resourceType, mapping.operationID, nil)
}

// LegacyCapabilityOperation exposes the frozen Capability -> Operation
// mapping to the Resource Invocation compatibility adapter. Keeping the
// lookup here prevents Runtime, Transport and Service from growing separate
// business mappings.
func LegacyCapabilityOperation(capability Capability) (OperationID, error) {
	mapping, exists := legacyCapabilityMappings[capability]
	if !exists {
		return "", fmt.Errorf("%w: %q", ErrUnsupportedLegacyCapability, capability)
	}
	return mapping.operationID, nil
}

func LegacyCapabilityIdempotencyMode(capability Capability) (IdempotencyMode, error) {
	mapping, exists := legacyCapabilityMappings[capability]
	if !exists {
		return "", fmt.Errorf("%w: %q", ErrUnsupportedLegacyCapability, capability)
	}
	return mapping.mode, nil
}
