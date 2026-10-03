package sqlite

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"dtm/internal/model"
)

const resourceDescriptorEncodingVersion = 1

type persistedOperationV1 struct {
	OperationID    string `json:"operation_id"`
	Idempotency    string `json:"idempotency"`
	InputSchemaID  string `json:"input_schema_id"`
	OutputSchemaID string `json:"output_schema_id"`
}

func encodeResourceGeneration(value model.ResourceGeneration) (string, error) {
	if err := value.Validate(); err != nil {
		return "", invalidData("resource generation: %v", err)
	}
	return strconv.FormatUint(uint64(value), 10), nil
}

func decodeResourceGeneration(encoded string) (model.ResourceGeneration, error) {
	if encoded == "" || strings.TrimSpace(encoded) != encoded ||
		(encoded != "0" && len(encoded) > 1 && encoded[0] == '0') {
		return 0, invalidData("resource generation %q is not canonical unsigned decimal", encoded)
	}
	for _, character := range encoded {
		if character < '0' || character > '9' {
			return 0, invalidData("resource generation %q is not canonical unsigned decimal", encoded)
		}
	}
	parsed, err := strconv.ParseUint(encoded, 10, 64)
	if err != nil || parsed == 0 {
		return 0, invalidData("resource generation %q is outside the valid uint64 range", encoded)
	}
	if strconv.FormatUint(parsed, 10) != encoded {
		return 0, invalidData("resource generation %q is not canonical unsigned decimal", encoded)
	}
	return model.ResourceGeneration(parsed), nil
}

func encodeResourceOperations(operations []model.OperationDescriptor) (string, error) {
	canonical := append([]model.OperationDescriptor(nil), operations...)
	sort.Slice(canonical, func(left, right int) bool { return canonical[left].ID < canonical[right].ID })
	encoded := make([]persistedOperationV1, len(canonical))
	seen := make(map[model.OperationID]struct{}, len(canonical))
	for index, operation := range canonical {
		if err := operation.Validate(); err != nil {
			return "", invalidData("resource operation %q: %v", operation.ID, err)
		}
		if _, exists := seen[operation.ID]; exists {
			return "", invalidData("duplicate resource operation %q", operation.ID)
		}
		seen[operation.ID] = struct{}{}
		encoded[index] = persistedOperationV1{
			OperationID: operation.ID.String(), Idempotency: string(operation.IdempotencyMode),
			InputSchemaID: operation.InputSchemaID, OutputSchemaID: operation.OutputSchemaID,
		}
	}
	bytes, err := json.Marshal(encoded)
	if err != nil {
		return "", invalidData("encode resource operations: %v", err)
	}
	return string(bytes), nil
}

func decodeResourceOperations(encoded string) ([]model.OperationDescriptor, error) {
	if !utf8.ValidString(encoded) || encoded == "null" {
		return nil, invalidData("resource operations are not valid canonical JSON")
	}
	decoder := json.NewDecoder(strings.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var persisted []persistedOperationV1
	if err := decoder.Decode(&persisted); err != nil {
		return nil, invalidData("decode resource operations: %v", err)
	}
	if err := requireJSONEOF(decoder, "resource operations"); err != nil {
		return nil, err
	}
	operations := make([]model.OperationDescriptor, len(persisted))
	seen := make(map[model.OperationID]struct{}, len(persisted))
	for index, item := range persisted {
		operation, err := model.NewOperationDescriptor(
			model.OperationID(item.OperationID), model.IdempotencyMode(item.Idempotency),
			item.InputSchemaID, item.OutputSchemaID,
		)
		if err != nil {
			return nil, invalidData("decode resource operation %q: %v", item.OperationID, err)
		}
		if _, exists := seen[operation.ID]; exists {
			return nil, invalidData("duplicate resource operation %q", operation.ID)
		}
		seen[operation.ID] = struct{}{}
		operations[index] = operation
	}
	canonical, err := encodeResourceOperations(operations)
	if err != nil {
		return nil, err
	}
	if canonical != encoded {
		return nil, invalidData("resource operations are not canonical JSON")
	}
	return operations, nil
}

func encodeResourceAttributes(attributes map[string]string) (string, error) {
	if attributes == nil {
		attributes = map[string]string{}
	}
	if _, err := model.NewResourceRequirement(model.ResourceKindCapability, "persistence_validation", "validate", attributes); err != nil {
		return "", invalidData("resource attributes: %v", err)
	}
	bytes, err := json.Marshal(attributes)
	if err != nil {
		return "", invalidData("encode resource attributes: %v", err)
	}
	return string(bytes), nil
}

func decodeResourceAttributes(encoded string) (map[string]string, error) {
	if !utf8.ValidString(encoded) || encoded == "null" {
		return nil, invalidData("resource attributes are not valid canonical JSON")
	}
	decoder := json.NewDecoder(strings.NewReader(encoded))
	var attributes map[string]string
	if err := decoder.Decode(&attributes); err != nil {
		return nil, invalidData("decode resource attributes: %v", err)
	}
	if err := requireJSONEOF(decoder, "resource attributes"); err != nil {
		return nil, err
	}
	canonical, err := encodeResourceAttributes(attributes)
	if err != nil {
		return nil, err
	}
	if canonical != encoded {
		return nil, invalidData("resource attributes are not canonical JSON")
	}
	return attributes, nil
}

func requireJSONEOF(decoder *json.Decoder, field string) error {
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return invalidData("decode %s: trailing JSON value", field)
		}
		return invalidData("decode %s: %v", field, err)
	}
	return nil
}

func validateResourceEncodingVersion(value int64) error {
	if value != resourceDescriptorEncodingVersion {
		return invalidData("resource descriptor encoding version %d", value)
	}
	return nil
}

func resourceCodecError(resourceID model.ResourceID, err error) error {
	return fmt.Errorf("resource %q: %w", resourceID, err)
}
