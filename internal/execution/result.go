package execution

import (
	"errors"
	"strings"

	"dtm/internal/model"
)

var ErrInvalidResult = errors.New("invalid execution result")

type Status string

const (
	StatusSucceeded Status = "succeeded"
	StatusFailed    Status = "failed"
)

type StepResult struct {
	StepID model.StepID
	NodeID model.NodeID
	Status Status
	Output map[string]any
	Error  string
}

type Result struct {
	TaskID      model.TaskID
	Status      Status
	StepResults []StepResult
	Error       string
}

func NewStepResult(
	stepID model.StepID,
	nodeID model.NodeID,
	status Status,
	output map[string]any,
	message string,
) (StepResult, error) {
	result := StepResult{
		StepID: stepID,
		NodeID: nodeID,
		Status: status,
		Output: copyOutput(output),
		Error:  message,
	}
	if !validStepResult(result) {
		return StepResult{}, ErrInvalidResult
	}
	return result, nil
}

func NewResult(
	taskID model.TaskID,
	status Status,
	stepResults []StepResult,
	message string,
) (Result, error) {
	if strings.TrimSpace(string(taskID)) == "" ||
		!validStatus(status) ||
		len(stepResults) == 0 ||
		(status == StatusSucceeded && message != "") ||
		(status == StatusFailed && message == "") {
		return Result{}, ErrInvalidResult
	}

	copied := make([]StepResult, len(stepResults))
	for index, step := range stepResults {
		if !validStepResult(step) {
			return Result{}, ErrInvalidResult
		}
		if status == StatusSucceeded && step.Status != StatusSucceeded {
			return Result{}, ErrInvalidResult
		}
		if status == StatusFailed {
			isFinal := index == len(stepResults)-1
			if (!isFinal && step.Status != StatusSucceeded) ||
				(isFinal && step.Status != StatusFailed) {
				return Result{}, ErrInvalidResult
			}
		}
		copied[index] = step
		copied[index].Output = copyOutput(step.Output)
	}

	return Result{
		TaskID:      taskID,
		Status:      status,
		StepResults: copied,
		Error:       message,
	}, nil
}

func validStepResult(result StepResult) bool {
	if strings.TrimSpace(string(result.StepID)) == "" ||
		strings.TrimSpace(string(result.NodeID)) == "" ||
		!validStatus(result.Status) {
		return false
	}
	if result.Status == StatusSucceeded {
		return result.Error == ""
	}
	return result.Error != ""
}

func validStatus(status Status) bool {
	return status == StatusSucceeded || status == StatusFailed
}

func copyOutput(output map[string]any) map[string]any {
	if output == nil {
		return nil
	}
	copied := make(map[string]any, len(output))
	for key, value := range output {
		copied[key] = value
	}
	return copied
}
