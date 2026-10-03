package demo

import (
	"context"
	"errors"
	"fmt"

	"dtm/internal/capability"
	"dtm/internal/model"
)

var ErrInvalidTemperatureInput = errors.New("invalid temperature input")

type TemperatureHandler struct{}

func NewTemperatureHandler() *TemperatureHandler {
	return &TemperatureHandler{}
}

func (*TemperatureHandler) Capability() model.Capability {
	return capability.TemperatureSensor
}

func (*TemperatureHandler) Execute(ctx context.Context, inputs map[string]string) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if inputs["operation"] != "read_temperature" {
		return nil, fmt.Errorf("%w: operation must be read_temperature", ErrInvalidTemperatureInput)
	}
	return map[string]any{"temperature": float64(30)}, nil
}
