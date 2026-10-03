package execution

import (
	"errors"
	"testing"

	"dtm/internal/model"
)

func TestNewStepResultCopiesOutput(t *testing.T) {
	output := map[string]any{"temperature": 28}

	result, err := NewStepResult("step-1", "sensor-a", StatusSucceeded, output, "")
	if err != nil {
		t.Fatalf("NewStepResult() error = %v", err)
	}
	output["temperature"] = 12

	if got := result.Output["temperature"]; got != 28 {
		t.Fatalf("result.Output[temperature] = %v, want 28", got)
	}
}

func TestNewStepResultValidation(t *testing.T) {
	tests := []struct {
		name    string
		stepID  model.StepID
		nodeID  model.NodeID
		status  Status
		message string
	}{
		{name: "blank step ID", stepID: " ", nodeID: "node-1", status: StatusSucceeded},
		{name: "blank node ID", stepID: "step-1", nodeID: " ", status: StatusSucceeded},
		{name: "unknown status", stepID: "step-1", nodeID: "node-1", status: "pending"},
		{name: "successful result with error", stepID: "step-1", nodeID: "node-1", status: StatusSucceeded, message: "unexpected"},
		{name: "failed result without error", stepID: "step-1", nodeID: "node-1", status: StatusFailed},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewStepResult(test.stepID, test.nodeID, test.status, nil, test.message)
			if !errors.Is(err, ErrInvalidResult) {
				t.Fatalf("NewStepResult() error = %v, want ErrInvalidResult", err)
			}
		})
	}
}

func TestNewResultCopiesStepResults(t *testing.T) {
	step, err := NewStepResult("step-1", "node-1", StatusSucceeded, nil, "")
	if err != nil {
		t.Fatalf("NewStepResult() error = %v", err)
	}
	steps := []StepResult{step}

	result, err := NewResult("task-1", StatusSucceeded, steps, "")
	if err != nil {
		t.Fatalf("NewResult() error = %v", err)
	}
	steps[0].NodeID = "changed"

	if got := result.StepResults[0].NodeID; got != "node-1" {
		t.Fatalf("result.StepResults[0].NodeID = %q, want node-1", got)
	}
}

func TestNewResultValidation(t *testing.T) {
	success, err := NewStepResult("step-1", "node-1", StatusSucceeded, nil, "")
	if err != nil {
		t.Fatalf("NewStepResult() error = %v", err)
	}
	failed, err := NewStepResult("step-2", "node-2", StatusFailed, nil, "cooling failed")
	if err != nil {
		t.Fatalf("NewStepResult() error = %v", err)
	}

	tests := []struct {
		name    string
		taskID  model.TaskID
		status  Status
		steps   []StepResult
		message string
	}{
		{name: "blank task ID", taskID: " ", status: StatusSucceeded, steps: []StepResult{success}},
		{name: "unknown status", taskID: "task-1", status: "pending", steps: []StepResult{success}},
		{name: "successful result without steps", taskID: "task-1", status: StatusSucceeded},
		{name: "successful result with error", taskID: "task-1", status: StatusSucceeded, steps: []StepResult{success}, message: "unexpected"},
		{name: "successful result with failed step", taskID: "task-1", status: StatusSucceeded, steps: []StepResult{failed}},
		{name: "failed result without steps", taskID: "task-1", status: StatusFailed, message: "failed"},
		{name: "failed result without error", taskID: "task-1", status: StatusFailed, steps: []StepResult{failed}},
		{name: "failed result ending in success", taskID: "task-1", status: StatusFailed, steps: []StepResult{failed, success}, message: "failed"},
		{name: "failed result with preceding failed step", taskID: "task-1", status: StatusFailed, steps: []StepResult{failed, failed}, message: "failed"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewResult(test.taskID, test.status, test.steps, test.message)
			if !errors.Is(err, ErrInvalidResult) {
				t.Fatalf("NewResult() error = %v, want ErrInvalidResult", err)
			}
		})
	}
}

func TestNewResultAcceptsOrderedFailure(t *testing.T) {
	success, _ := NewStepResult("step-1", "node-1", StatusSucceeded, nil, "")
	failed, _ := NewStepResult("step-2", "node-2", StatusFailed, nil, "cooling failed")

	result, err := NewResult("task-1", StatusFailed, []StepResult{success, failed}, "cooling failed")
	if err != nil {
		t.Fatalf("NewResult() error = %v", err)
	}
	if result.Status != StatusFailed {
		t.Fatalf("result.Status = %q, want %q", result.Status, StatusFailed)
	}
}
