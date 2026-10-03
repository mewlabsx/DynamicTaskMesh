package invocation

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"dtm/internal/mapper"
	"dtm/internal/model"
)

var (
	ErrInvalidLegacyPayload = errors.New("invalid legacy capability payload")
)

// LegacyCapabilityAdapter is the sole M1 adapter for the existing Capability
// step contract. It derives the explicit OperationID from the model's frozen
// mapping and keeps the payload opaque to Invocation Service and Transport.
type LegacyCapabilityAdapter struct{}

func NewLegacyCapabilityAdapter() *LegacyCapabilityAdapter {
	return &LegacyCapabilityAdapter{}
}

func (adapter *LegacyCapabilityAdapter) OperationForCapability(capability model.Capability) (model.OperationID, error) {
	if adapter == nil {
		return "", ErrInvalidLegacyPayload
	}
	return model.LegacyCapabilityOperation(capability)
}

func (adapter *LegacyCapabilityAdapter) IdempotencyModeForCapability(capability model.Capability) (model.IdempotencyMode, error) {
	if adapter == nil {
		return "", ErrInvalidLegacyPayload
	}
	return model.LegacyCapabilityIdempotencyMode(capability)
}

type legacyInvocationPayload struct {
	Capability string            `json:"capability"`
	Inputs     map[string]string `json:"inputs,omitempty"`
}

func (adapter *LegacyCapabilityAdapter) EncodeRequest(step mapper.MappedStep, operation model.OperationID) ([]byte, error) {
	if adapter == nil {
		return nil, ErrInvalidLegacyPayload
	}
	if err := operation.Validate(); err != nil {
		return nil, fmt.Errorf("%w: operation: %v", ErrInvalidLegacyPayload, err)
	}
	if strings.TrimSpace(string(step.Capability)) == "" {
		return nil, fmt.Errorf("%w: capability is required", ErrInvalidLegacyPayload)
	}
	expected, err := adapter.OperationForCapability(step.Capability)
	if err != nil {
		return nil, err
	}
	if expected != operation {
		return nil, fmt.Errorf("%w: operation %q does not match capability %q", ErrInvalidLegacyPayload, operation, step.Capability)
	}
	inputs := cloneStringInputs(step.Inputs)
	// The current temperature handler consumes this operation input. Adding it
	// at the adapter boundary keeps that legacy contract out of Service and
	// Transport and is deterministic for all legacy operations.
	if _, exists := inputs["operation"]; !exists {
		inputs["operation"] = operation.String()
	}
	payload, err := json.Marshal(legacyInvocationPayload{Capability: step.Capability.String(), Inputs: inputs})
	if err != nil {
		return nil, fmt.Errorf("%w: encode request: %v", ErrInvalidLegacyPayload, err)
	}
	return payload, nil
}

func (adapter *LegacyCapabilityAdapter) DecodeRequest(operation model.OperationID, payload []byte) (model.Capability, map[string]string, error) {
	if adapter == nil {
		return "", nil, ErrInvalidLegacyPayload
	}
	if err := operation.Validate(); err != nil {
		return "", nil, fmt.Errorf("%w: operation: %v", ErrInvalidLegacyPayload, err)
	}
	var envelope legacyInvocationPayload
	decoder := json.NewDecoder(bytes.NewReader(payload))
	if err := decoder.Decode(&envelope); err != nil {
		return "", nil, fmt.Errorf("%w: decode request: %v", ErrInvalidLegacyPayload, err)
	}
	if strings.TrimSpace(envelope.Capability) == "" {
		return "", nil, fmt.Errorf("%w: capability is required", ErrInvalidLegacyPayload)
	}
	capability, err := model.NewCapability(envelope.Capability)
	if err != nil {
		return "", nil, fmt.Errorf("%w: capability: %v", ErrInvalidLegacyPayload, err)
	}
	expected, err := adapter.OperationForCapability(capability)
	if err != nil {
		return "", nil, err
	}
	if expected != operation {
		return "", nil, fmt.Errorf("%w: operation %q does not match capability %q", ErrInvalidLegacyPayload, operation, capability)
	}
	inputs := cloneStringInputs(envelope.Inputs)
	if _, exists := inputs["operation"]; !exists {
		inputs["operation"] = operation.String()
	}
	return capability, inputs, nil
}

func (adapter *LegacyCapabilityAdapter) EncodeOutput(output map[string]any) ([]byte, error) {
	if adapter == nil {
		return nil, ErrInvalidLegacyPayload
	}
	payload, err := json.Marshal(output)
	if err != nil {
		return nil, fmt.Errorf("%w: encode output: %v", ErrInvalidLegacyPayload, err)
	}
	return payload, nil
}

func (adapter *LegacyCapabilityAdapter) DecodeOutput(payload []byte) (map[string]any, error) {
	if adapter == nil {
		return nil, ErrInvalidLegacyPayload
	}
	if len(bytes.TrimSpace(payload)) == 0 || bytes.Equal(bytes.TrimSpace(payload), []byte("null")) {
		return map[string]any{}, nil
	}
	var output map[string]any
	if err := json.Unmarshal(payload, &output); err != nil {
		return nil, fmt.Errorf("%w: decode output: %v", ErrInvalidLegacyPayload, err)
	}
	if output == nil {
		return map[string]any{}, nil
	}
	return output, nil
}

func cloneStringInputs(inputs map[string]string) map[string]string {
	if inputs == nil {
		return map[string]string{}
	}
	clone := make(map[string]string, len(inputs))
	for key, value := range inputs {
		clone[key] = value
	}
	return clone
}
