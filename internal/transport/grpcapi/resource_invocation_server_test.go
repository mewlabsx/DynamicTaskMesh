package grpcapi

import (
	"context"
	"errors"
	"testing"

	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
)

func TestExecutionFenceValidatorModesStayLocalToTheExecutionBoundary(t *testing.T) {
	defaultServer, err := NewAgentExecutionServer(executionFenceTestHandler{})
	if err != nil {
		t.Fatal(err)
	}
	if got := defaultServer.ExecutionFenceMode(); got != ExecutionFenceModeNone {
		t.Fatalf("default execution fence mode = %q, want %q", got, ExecutionFenceModeNone)
	}

	ref, err := model.NewResourceRef("resource-fence", 1, "node-fence", 1, "registration-fence")
	if err != nil {
		t.Fatal(err)
	}
	localValidator, err := NewStaticExecutionFenceValidator(ref)
	if err != nil {
		t.Fatal(err)
	}
	localServer, err := NewAgentExecutionServer(executionFenceTestHandler{}, WithExecutionFenceValidator(localValidator))
	if err != nil {
		t.Fatal(err)
	}
	if got := ExecutionFenceModeOf(localValidator); got != ExecutionFenceModeLocalValidator || localServer.ExecutionFenceMode() != ExecutionFenceModeLocalValidator {
		t.Fatalf("local execution fence mode = %q/%q, want %q", got, localServer.ExecutionFenceMode(), ExecutionFenceModeLocalValidator)
	}
	if err := localValidator.Validate(context.Background(), ref, "read_temperature"); err != nil {
		t.Fatalf("local validator rejected its matching local snapshot: %v", err)
	}

	_, err = NewAgentExecutionServer(executionFenceTestHandler{}, WithExecutionFenceValidator(authorityConfirmedFenceValidator{}))
	if !errors.Is(err, ErrAuthorityBackedExecutionFenceUnavailable) {
		t.Fatalf("authority-confirmed validator error = %v, want %v", err, ErrAuthorityBackedExecutionFenceUnavailable)
	}
}

type authorityConfirmedFenceValidator struct{}

func (authorityConfirmedFenceValidator) Validate(context.Context, model.ResourceRef, model.OperationID) error {
	return nil
}

func (authorityConfirmedFenceValidator) ExecutionFenceMode() execution.ExecutionFenceMode {
	return ExecutionFenceModeAuthorityConfirmed
}

type executionFenceTestHandler struct{}

func (executionFenceTestHandler) Execute(_ context.Context, step mapper.MappedStep) (execution.StepResult, error) {
	return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, map[string]any{}, "")
}
