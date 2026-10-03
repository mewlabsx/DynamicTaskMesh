package agent

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
)

var (
	ErrInvalidHandler        = errors.New("invalid handler")
	ErrDuplicateCapability   = errors.New("duplicate capability")
	ErrUnsupportedCapability = errors.New("unsupported capability")
)

type Handler interface {
	Capability() model.Capability
	Execute(context.Context, map[string]string) (map[string]any, error)
}

type Router struct {
	handlers map[model.Capability]Handler
}

func NewRouter(handlers ...Handler) (*Router, error) {
	routes := make(map[model.Capability]Handler, len(handlers))
	for _, handler := range handlers {
		if isNilHandler(handler) {
			return nil, ErrInvalidHandler
		}
		capability := handler.Capability()
		if strings.TrimSpace(string(capability)) == "" {
			return nil, ErrInvalidHandler
		}
		if _, exists := routes[capability]; exists {
			return nil, fmt.Errorf("%w: %q", ErrDuplicateCapability, capability)
		}
		routes[capability] = handler
	}
	return &Router{handlers: routes}, nil
}

func (router *Router) Execute(ctx context.Context, step mapper.MappedStep) (execution.StepResult, error) {
	handler, exists := router.handlers[step.Capability]
	if !exists {
		return execution.StepResult{}, fmt.Errorf("%w: %q", ErrUnsupportedCapability, step.Capability)
	}

	output, err := handler.Execute(ctx, copyInputs(step.Inputs))
	if err != nil {
		result, resultErr := execution.NewStepResult(
			step.ID,
			step.NodeID,
			execution.StatusFailed,
			output,
			err.Error(),
		)
		if resultErr != nil {
			return execution.StepResult{}, fmt.Errorf("construct failed step result: %w", resultErr)
		}
		return result, err
	}

	result, err := execution.NewStepResult(
		step.ID,
		step.NodeID,
		execution.StatusSucceeded,
		output,
		"",
	)
	if err != nil {
		return execution.StepResult{}, fmt.Errorf("construct successful step result: %w", err)
	}
	return result, nil
}

func copyInputs(inputs map[string]string) map[string]string {
	if inputs == nil {
		return nil
	}
	copied := make(map[string]string, len(inputs))
	for key, value := range inputs {
		copied[key] = value
	}
	return copied
}

func isNilHandler(handler Handler) bool {
	if handler == nil {
		return true
	}
	value := reflect.ValueOf(handler)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
