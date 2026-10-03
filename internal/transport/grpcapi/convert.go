package grpcapi

import (
	"fmt"
	"strings"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/node"

	"google.golang.org/protobuf/types/known/structpb"
)

func nodeFromProto(input *dtmv1.Node) (node.Node, string, error) {
	if input == nil {
		return node.Node{}, "", fmt.Errorf("%w: node is required", node.ErrInvalidNode)
	}

	id := model.NodeID(strings.TrimSpace(input.GetId()))
	address := strings.TrimSpace(input.GetExecutionAddress())
	if id == "" {
		return node.Node{}, "", fmt.Errorf("%w: node ID is required", node.ErrInvalidNode)
	}
	if address == "" {
		return node.Node{}, "", fmt.Errorf("%w: execution address is required", ErrInvalidEndpoint)
	}
	if input.GetStatus() != dtmv1.NodeStatus_NODE_STATUS_ONLINE {
		return node.Node{}, "", fmt.Errorf("%w: registration status must be online", node.ErrInvalidNode)
	}
	if len(input.GetCapabilities()) == 0 {
		return node.Node{}, "", fmt.Errorf("%w: at least one capability is required", node.ErrInvalidNode)
	}

	capabilities := make([]model.Capability, 0, len(input.GetCapabilities()))
	seen := make(map[model.Capability]struct{}, len(input.GetCapabilities()))
	for _, rawCapability := range input.GetCapabilities() {
		name := strings.TrimSpace(rawCapability)
		capability, err := model.NewCapability(name)
		if err != nil {
			return node.Node{}, "", fmt.Errorf("%w: capability %q", node.ErrInvalidNode, rawCapability)
		}
		if _, duplicate := seen[capability]; duplicate {
			return node.Node{}, "", fmt.Errorf("%w: duplicate capability %q", node.ErrInvalidNode, capability)
		}
		seen[capability] = struct{}{}
		capabilities = append(capabilities, capability)
	}

	converted, err := node.New(id, capabilities, node.StatusRegistered)
	if err != nil {
		return node.Node{}, "", fmt.Errorf("construct node: %w", err)
	}
	return converted, address, nil
}

func mappedStepFromProto(input *dtmv1.MappedStep) (mapper.MappedStep, error) {
	if input == nil {
		return mapper.MappedStep{}, fmt.Errorf("%w: step is required", mapper.ErrInvalidPlan)
	}

	step := mapper.MappedStep{
		ID:         model.StepID(strings.TrimSpace(input.GetStepId())),
		Capability: model.Capability(strings.TrimSpace(input.GetCapability())),
		NodeID:     model.NodeID(strings.TrimSpace(input.GetNodeId())),
		Inputs:     copyStringMap(input.GetInputs()),
	}
	if step.ID == "" || step.Capability == "" || step.NodeID == "" {
		return mapper.MappedStep{}, fmt.Errorf("%w: step ID, capability, and node ID are required", mapper.ErrInvalidPlan)
	}
	for key := range step.Inputs {
		if strings.TrimSpace(key) == "" {
			return mapper.MappedStep{}, fmt.Errorf("%w: input keys must not be blank", mapper.ErrInvalidPlan)
		}
	}
	return step, nil
}

func mappedStepToProto(step mapper.MappedStep) *dtmv1.MappedStep {
	return &dtmv1.MappedStep{
		StepId:     string(step.ID),
		Capability: string(step.Capability),
		NodeId:     string(step.NodeID),
		Inputs:     copyStringMap(step.Inputs),
	}
}

func stepResultFromProto(input *dtmv1.StepResult) (execution.StepResult, error) {
	if input == nil {
		return execution.StepResult{}, fmt.Errorf("%w: result is required", execution.ErrInvalidResult)
	}

	var resultStatus execution.Status
	switch input.GetStatus() {
	case dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED:
		resultStatus = execution.StatusSucceeded
	case dtmv1.ExecutionStatus_EXECUTION_STATUS_FAILED:
		resultStatus = execution.StatusFailed
	default:
		return execution.StepResult{}, fmt.Errorf(
			"%w: unknown status %q",
			execution.ErrInvalidResult,
			input.GetStatus(),
		)
	}

	var output map[string]any
	if input.GetOutput() != nil {
		output = input.GetOutput().AsMap()
	}
	return execution.NewStepResult(
		model.StepID(strings.TrimSpace(input.GetStepId())),
		model.NodeID(strings.TrimSpace(input.GetNodeId())),
		resultStatus,
		output,
		input.GetError(),
	)
}

func stepResultToProto(result execution.StepResult) (*dtmv1.StepResult, error) {
	var status dtmv1.ExecutionStatus
	switch result.Status {
	case execution.StatusSucceeded:
		status = dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED
	case execution.StatusFailed:
		status = dtmv1.ExecutionStatus_EXECUTION_STATUS_FAILED
	default:
		return nil, fmt.Errorf("%w: unknown status %q", execution.ErrInvalidResult, result.Status)
	}

	var output *structpb.Struct
	var err error
	if result.Output != nil {
		output, err = structpb.NewStruct(result.Output)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid output: %v", execution.ErrInvalidResult, err)
		}
	}
	return &dtmv1.StepResult{
		StepId: string(result.StepID),
		NodeId: string(result.NodeID),
		Status: status,
		Output: output,
		Error:  result.Error,
	}, nil
}

func copyStringMap(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	copied := make(map[string]string, len(input))
	for key, value := range input {
		copied[key] = value
	}
	return copied
}
