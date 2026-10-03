package agent

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
)

type handlerFunc struct {
	capability model.Capability
	execute    func(context.Context, map[string]string) (map[string]any, error)
}

func (handler *handlerFunc) Capability() model.Capability {
	return handler.capability
}

func (handler *handlerFunc) Execute(ctx context.Context, inputs map[string]string) (map[string]any, error) {
	return handler.execute(ctx, inputs)
}

func TestNewRouterRejectsInvalidHandlers(t *testing.T) {
	valid := &handlerFunc{capability: "valid"}
	var typedNil *handlerFunc

	tests := []struct {
		name     string
		handlers []Handler
		wantErr  error
	}{
		{name: "nil handler", handlers: []Handler{nil}, wantErr: ErrInvalidHandler},
		{name: "typed nil handler", handlers: []Handler{typedNil}, wantErr: ErrInvalidHandler},
		{name: "empty capability", handlers: []Handler{&handlerFunc{}}, wantErr: ErrInvalidHandler},
		{
			name: "blank capability",
			handlers: []Handler{
				&handlerFunc{capability: " \t "},
			},
			wantErr: ErrInvalidHandler,
		},
		{
			name: "duplicate capability",
			handlers: []Handler{
				valid,
				&handlerFunc{capability: "valid"},
			},
			wantErr: ErrDuplicateCapability,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRouter(tt.handlers...); !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewRouter() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRouterRoutesByCapabilityAndCopiesInputs(t *testing.T) {
	ctx := context.WithValue(context.Background(), contextKey{}, "preserved")
	inputs := map[string]string{"operation": "read"}
	returnedOutput := map[string]any{"temperature": float64(30)}
	var receivedContext context.Context
	handler := &handlerFunc{
		capability: "temperature_sensor",
		execute: func(received context.Context, receivedInputs map[string]string) (map[string]any, error) {
			receivedContext = received
			receivedInputs["operation"] = "mutated"
			return returnedOutput, nil
		},
	}
	router, err := NewRouter(handler)
	if err != nil {
		t.Fatal(err)
	}

	result, err := router.Execute(ctx, mapper.MappedStep{
		ID:         "step-1",
		Capability: "temperature_sensor",
		NodeID:     "node-1",
		Inputs:     inputs,
	})

	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if receivedContext != ctx {
		t.Fatal("Execute() did not pass the original context")
	}
	if !reflect.DeepEqual(inputs, map[string]string{"operation": "read"}) {
		t.Fatalf("Execute() exposed caller inputs: %v", inputs)
	}
	if result.StepID != "step-1" || result.NodeID != "node-1" || result.Status != execution.StatusSucceeded {
		t.Fatalf("Execute() result = %#v", result)
	}
	returnedOutput["temperature"] = float64(99)
	if got := result.Output["temperature"]; got != float64(30) {
		t.Fatalf("result output changed through handler map: %#v", got)
	}
}

func TestRouterReturnsNormalizedFailedResultAndOriginalHandlerError(t *testing.T) {
	handlerErr := errors.New("fan unavailable")
	handler := &handlerFunc{
		capability: "cooling_control",
		execute: func(context.Context, map[string]string) (map[string]any, error) {
			return map[string]any{"attempted": true}, handlerErr
		},
	}
	router, err := NewRouter(handler)
	if err != nil {
		t.Fatal(err)
	}

	result, err := router.Execute(context.Background(), mapper.MappedStep{
		ID:         "step-from-request",
		Capability: "cooling_control",
		NodeID:     "node-from-request",
	})

	if !errors.Is(err, handlerErr) {
		t.Fatalf("Execute() error = %v, want original handler error", err)
	}
	if result.StepID != "step-from-request" || result.NodeID != "node-from-request" {
		t.Fatalf("Execute() identity = (%q, %q)", result.StepID, result.NodeID)
	}
	if result.Status != execution.StatusFailed || result.Error != handlerErr.Error() {
		t.Fatalf("Execute() failure = %#v", result)
	}
	if got := result.Output["attempted"]; got != true {
		t.Fatalf("Execute() output = %v, want attempted=true", result.Output)
	}
}

func TestRouterRejectsUnsupportedCapabilityWithoutCallingHandlers(t *testing.T) {
	called := false
	router, err := NewRouter(&handlerFunc{
		capability: "temperature_sensor",
		execute: func(context.Context, map[string]string) (map[string]any, error) {
			called = true
			return nil, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := router.Execute(context.Background(), mapper.MappedStep{
		ID:         "step-1",
		Capability: "cooling_control",
		NodeID:     "node-1",
	})

	if !errors.Is(err, ErrUnsupportedCapability) {
		t.Fatalf("Execute() error = %v, want %v", err, ErrUnsupportedCapability)
	}
	if !reflect.DeepEqual(result, execution.StepResult{}) {
		t.Fatalf("Execute() result = %#v, want zero value", result)
	}
	if called {
		t.Fatal("Execute() called a handler for another capability")
	}
}

func TestRouterPreservesContextError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	router, err := NewRouter(&handlerFunc{
		capability: "temperature_sensor",
		execute: func(ctx context.Context, _ map[string]string) (map[string]any, error) {
			return nil, ctx.Err()
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := router.Execute(ctx, mapper.MappedStep{
		ID:         "step-1",
		Capability: "temperature_sensor",
		NodeID:     "node-1",
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", err)
	}
	if result.Status != execution.StatusFailed || result.Error != context.Canceled.Error() {
		t.Fatalf("Execute() result = %#v, want failed context result", result)
	}
}

type contextKey struct{}
