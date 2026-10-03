package demo

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestTemperatureHandlerReadsTemperature(t *testing.T) {
	handler := NewTemperatureHandler()
	inputs := map[string]string{"operation": "read_temperature"}

	output, err := handler.Execute(context.Background(), inputs)

	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got, want := handler.Capability().String(), "temperature_sensor"; got != want {
		t.Fatalf("Capability() = %q, want %q", got, want)
	}
	if got, ok := output["temperature"].(float64); !ok || got != 30 {
		t.Fatalf("temperature = %#v, want numeric 30", output["temperature"])
	}
	if !reflect.DeepEqual(inputs, map[string]string{"operation": "read_temperature"}) {
		t.Fatalf("Execute() mutated inputs: %v", inputs)
	}
}

func TestTemperatureHandlerRejectsInvalidOperation(t *testing.T) {
	handler := NewTemperatureHandler()

	for _, inputs := range []map[string]string{
		nil,
		{},
		{"operation": ""},
		{"operation": "measure_temperature"},
	} {
		if _, err := handler.Execute(context.Background(), inputs); !errors.Is(err, ErrInvalidTemperatureInput) {
			t.Fatalf("Execute(%v) error = %v, want %v", inputs, err, ErrInvalidTemperatureInput)
		}
	}
}

func TestCoolingHandlerStartsCoolingForFiniteNumericTarget(t *testing.T) {
	handler := NewCoolingHandler()

	for _, target := range []string{"26", "-2.5", "0", "1e2"} {
		inputs := map[string]string{"target_temperature": target}
		output, err := handler.Execute(context.Background(), inputs)
		if err != nil {
			t.Fatalf("Execute(%q) error = %v", target, err)
		}
		if got, ok := output["cooling_started"].(bool); !ok || !got {
			t.Fatalf("cooling_started = %#v, want true", output["cooling_started"])
		}
		if got, want := handler.Capability().String(), "cooling_control"; got != want {
			t.Fatalf("Capability() = %q, want %q", got, want)
		}
		if inputs["target_temperature"] != target {
			t.Fatalf("Execute() mutated inputs: %v", inputs)
		}
	}
}

func TestCoolingHandlerRejectsInvalidTarget(t *testing.T) {
	handler := NewCoolingHandler()

	for _, target := range []string{"", " ", " 26 ", "cold", "NaN", "+Inf", "-Inf"} {
		_, err := handler.Execute(context.Background(), map[string]string{"target_temperature": target})
		if !errors.Is(err, ErrInvalidCoolingInput) {
			t.Fatalf("Execute(%q) error = %v, want %v", target, err, ErrInvalidCoolingInput)
		}
	}
}

func TestHandlersPreserveCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name    string
		execute func(context.Context) error
	}{
		{
			name: "temperature",
			execute: func(ctx context.Context) error {
				_, err := NewTemperatureHandler().Execute(ctx, map[string]string{"operation": "read_temperature"})
				return err
			},
		},
		{
			name: "cooling",
			execute: func(ctx context.Context) error {
				_, err := NewCoolingHandler().Execute(ctx, map[string]string{"target_temperature": "26"})
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.execute(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("Execute() error = %v, want context.Canceled", err)
			}
		})
	}
}
