package model

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

type ResourceID string
type ResourceKind string
type ResourceType string
type OperationID string
type ResourceGeneration uint64

const ResourceKindCapability ResourceKind = "capability"

var (
	ErrInvalidResourceID          = errors.New("invalid resource id")
	ErrInvalidResourceKind        = errors.New("invalid resource kind")
	ErrInvalidResourceType        = errors.New("invalid resource type")
	ErrInvalidOperationID         = errors.New("invalid operation id")
	ErrInvalidResourceGeneration  = errors.New("invalid resource generation")
	ErrInvalidOperationDescriptor = errors.New("invalid operation descriptor")
	ErrInvalidResourceDescriptor  = errors.New("invalid resource descriptor")
	ErrInvalidResourceRequirement = errors.New("invalid resource requirement")
	ErrInvalidResourceRef         = errors.New("invalid resource ref")
)

var resourceNamePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$`)

func NewResourceID(value string) (ResourceID, error) {
	id := ResourceID(value)
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

func (id ResourceID) String() string { return string(id) }

func (id ResourceID) Validate() error {
	if strings.TrimSpace(string(id)) == "" {
		return ErrInvalidResourceID
	}
	return nil
}

func NewResourceKind(value string) (ResourceKind, error) {
	kind := ResourceKind(value)
	if err := kind.Validate(); err != nil {
		return "", err
	}
	return kind, nil
}

func (kind ResourceKind) String() string { return string(kind) }

func (kind ResourceKind) Validate() error {
	if kind != ResourceKindCapability {
		return ErrInvalidResourceKind
	}
	return nil
}

func NewResourceType(value string) (ResourceType, error) {
	resourceType := ResourceType(value)
	if err := resourceType.Validate(); err != nil {
		return "", err
	}
	return resourceType, nil
}

func (resourceType ResourceType) String() string { return string(resourceType) }

func (resourceType ResourceType) Validate() error {
	if !resourceNamePattern.MatchString(string(resourceType)) {
		return ErrInvalidResourceType
	}
	return nil
}

func NewOperationID(value string) (OperationID, error) {
	id := OperationID(value)
	if err := id.Validate(); err != nil {
		return "", err
	}
	return id, nil
}

func (id OperationID) String() string { return string(id) }

func (id OperationID) Validate() error {
	if !resourceNamePattern.MatchString(string(id)) {
		return ErrInvalidOperationID
	}
	return nil
}

func NewResourceGeneration(value uint64) (ResourceGeneration, error) {
	generation := ResourceGeneration(value)
	if err := generation.Validate(); err != nil {
		return 0, err
	}
	return generation, nil
}

func (generation ResourceGeneration) Validate() error {
	if generation < 1 {
		return ErrInvalidResourceGeneration
	}
	return nil
}

type OperationDescriptor struct {
	ID              OperationID
	IdempotencyMode IdempotencyMode
	InputSchemaID   string
	OutputSchemaID  string
}

func NewOperationDescriptor(id OperationID, mode IdempotencyMode, inputSchemaID, outputSchemaID string) (OperationDescriptor, error) {
	descriptor := OperationDescriptor{ID: id, IdempotencyMode: mode, InputSchemaID: inputSchemaID, OutputSchemaID: outputSchemaID}
	if err := descriptor.Validate(); err != nil {
		return OperationDescriptor{}, err
	}
	return descriptor, nil
}

func (descriptor OperationDescriptor) Validate() error {
	if err := descriptor.ID.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidOperationDescriptor, err)
	}
	if descriptor.IdempotencyMode != IdempotencyIdempotent && descriptor.IdempotencyMode != IdempotencyNonIdempotent {
		return fmt.Errorf("%w: unknown idempotency mode %q", ErrInvalidOperationDescriptor, descriptor.IdempotencyMode)
	}
	if !validOptionalText(descriptor.InputSchemaID) || !validOptionalText(descriptor.OutputSchemaID) {
		return fmt.Errorf("%w: invalid schema id", ErrInvalidOperationDescriptor)
	}
	return nil
}

type ResourceAttribute struct {
	Key   string
	Value string
}

type ResourceDescriptor struct {
	ID          ResourceID
	Kind        ResourceKind
	Type        ResourceType
	OwnerNodeID NodeID
	Generation  ResourceGeneration
	Operations  []OperationDescriptor
	Attributes  map[string]string
}

func NewResourceDescriptor(
	id ResourceID,
	kind ResourceKind,
	resourceType ResourceType,
	ownerNodeID NodeID,
	generation ResourceGeneration,
	operations []OperationDescriptor,
	attributes map[string]string,
) (ResourceDescriptor, error) {
	descriptor := ResourceDescriptor{
		ID: id, Kind: kind, Type: resourceType, OwnerNodeID: ownerNodeID, Generation: generation,
		Operations: cloneOperations(operations), Attributes: cloneStringMap(attributes),
	}
	sort.Slice(descriptor.Operations, func(i, j int) bool { return descriptor.Operations[i].ID < descriptor.Operations[j].ID })
	if err := descriptor.Validate(); err != nil {
		return ResourceDescriptor{}, err
	}
	return descriptor, nil
}

func (descriptor ResourceDescriptor) Validate() error {
	if err := descriptor.ID.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceDescriptor, err)
	}
	if err := descriptor.Kind.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceDescriptor, err)
	}
	if err := descriptor.Type.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceDescriptor, err)
	}
	if err := validateNodeID(descriptor.OwnerNodeID); err != nil {
		return fmt.Errorf("%w: owner node: %v", ErrInvalidResourceDescriptor, err)
	}
	if err := descriptor.Generation.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceDescriptor, err)
	}
	if len(descriptor.Operations) == 0 {
		return fmt.Errorf("%w: operations are required", ErrInvalidResourceDescriptor)
	}
	seen := make(map[OperationID]struct{}, len(descriptor.Operations))
	for _, operation := range descriptor.Operations {
		if err := operation.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrInvalidResourceDescriptor, err)
		}
		if _, exists := seen[operation.ID]; exists {
			return fmt.Errorf("%w: duplicate operation %q", ErrInvalidResourceDescriptor, operation.ID)
		}
		seen[operation.ID] = struct{}{}
	}
	if err := validateAttributes(descriptor.Attributes); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceDescriptor, err)
	}
	return nil
}

func (descriptor ResourceDescriptor) Clone() ResourceDescriptor {
	clone := descriptor
	clone.Operations = cloneOperations(descriptor.Operations)
	clone.Attributes = cloneStringMap(descriptor.Attributes)
	return clone
}

func (descriptor ResourceDescriptor) CanonicalAttributes() []ResourceAttribute {
	return sortedAttributes(descriptor.Attributes)
}

func (descriptor ResourceDescriptor) CanonicalOperations() []OperationDescriptor {
	operations := cloneOperations(descriptor.Operations)
	sort.Slice(operations, func(i, j int) bool { return operations[i].ID < operations[j].ID })
	return operations
}

type ResourceRequirement struct {
	Kind               ResourceKind
	Type               ResourceType
	OperationID        OperationID
	RequiredAttributes map[string]string
}

func NewResourceRequirement(kind ResourceKind, resourceType ResourceType, operationID OperationID, requiredAttributes map[string]string) (ResourceRequirement, error) {
	requirement := ResourceRequirement{Kind: kind, Type: resourceType, OperationID: operationID, RequiredAttributes: cloneStringMap(requiredAttributes)}
	if err := requirement.Validate(); err != nil {
		return ResourceRequirement{}, err
	}
	return requirement, nil
}

func (requirement ResourceRequirement) Validate() error {
	if err := requirement.Kind.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceRequirement, err)
	}
	if err := requirement.Type.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceRequirement, err)
	}
	if err := requirement.OperationID.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceRequirement, err)
	}
	if err := validateAttributes(requirement.RequiredAttributes); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceRequirement, err)
	}
	return nil
}

func (requirement ResourceRequirement) Clone() ResourceRequirement {
	clone := requirement
	clone.RequiredAttributes = cloneStringMap(requirement.RequiredAttributes)
	return clone
}

func (requirement ResourceRequirement) CanonicalAttributes() []ResourceAttribute {
	return sortedAttributes(requirement.RequiredAttributes)
}

func (requirement ResourceRequirement) Equal(other ResourceRequirement) bool {
	if requirement.Kind != other.Kind || requirement.Type != other.Type || requirement.OperationID != other.OperationID || len(requirement.RequiredAttributes) != len(other.RequiredAttributes) {
		return false
	}
	for key, value := range requirement.RequiredAttributes {
		otherValue, exists := other.RequiredAttributes[key]
		if !exists || otherValue != value {
			return false
		}
	}
	return true
}

type ResourceRef struct {
	ResourceID          ResourceID
	ResourceGeneration  ResourceGeneration
	OwnerNodeID         NodeID
	OwnerNodeGeneration int64
	RegistrationID      string
}

func NewResourceRef(resourceID ResourceID, resourceGeneration ResourceGeneration, ownerNodeID NodeID, ownerNodeGeneration int64, registrationID string) (ResourceRef, error) {
	ref := ResourceRef{
		ResourceID: resourceID, ResourceGeneration: resourceGeneration, OwnerNodeID: ownerNodeID,
		OwnerNodeGeneration: ownerNodeGeneration, RegistrationID: registrationID,
	}
	if err := ref.Validate(); err != nil {
		return ResourceRef{}, err
	}
	return ref, nil
}

func (ref ResourceRef) Validate() error {
	if err := ref.ResourceID.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceRef, err)
	}
	if err := ref.ResourceGeneration.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidResourceRef, err)
	}
	if err := validateNodeID(ref.OwnerNodeID); err != nil {
		return fmt.Errorf("%w: owner node: %v", ErrInvalidResourceRef, err)
	}
	if ref.OwnerNodeGeneration < 1 {
		return fmt.Errorf("%w: owner node generation must be positive", ErrInvalidResourceRef)
	}
	if !validRegistrationID(ref.RegistrationID) {
		return fmt.Errorf("%w: invalid registration id", ErrInvalidResourceRef)
	}
	return nil
}

func validateNodeID(nodeID NodeID) error {
	value := string(nodeID)
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || strings.ContainsRune(value, '\x00') {
		return errors.New("invalid node id")
	}
	return nil
}

func validRegistrationID(value string) bool {
	return utf8.ValidString(value) && value != "" && strings.TrimSpace(value) == value && !strings.ContainsRune(value, '\x00')
}

func validOptionalText(value string) bool {
	return utf8.ValidString(value) && !strings.ContainsRune(value, '\x00')
}

func validateAttributes(attributes map[string]string) error {
	for key, value := range attributes {
		if !utf8.ValidString(key) || !utf8.ValidString(value) || key == "" || strings.ContainsRune(key, '\x00') || strings.ContainsRune(value, '\x00') {
			return errors.New("invalid attribute")
		}
	}
	return nil
}

func cloneOperations(operations []OperationDescriptor) []OperationDescriptor {
	if operations == nil {
		return []OperationDescriptor{}
	}
	return append([]OperationDescriptor(nil), operations...)
}

func cloneStringMap(values map[string]string) map[string]string {
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func sortedAttributes(values map[string]string) []ResourceAttribute {
	result := make([]ResourceAttribute, 0, len(values))
	for key, value := range values {
		result = append(result, ResourceAttribute{Key: key, Value: value})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })
	return result
}
