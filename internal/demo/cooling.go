package demo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"dtm/internal/capability"
	"dtm/internal/model"
)

var ErrInvalidCoolingInput = errors.New("invalid cooling input")

type CoolingHandler struct{}

func NewCoolingHandler() *CoolingHandler {
	return &CoolingHandler{}
}

func (*CoolingHandler) Capability() model.Capability {
	return capability.CoolingControl
}

func (*CoolingHandler) Execute(ctx context.Context, inputs map[string]string) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	target := inputs["target_temperature"]
	value, err := strconv.ParseFloat(target, 64)
	if strings.TrimSpace(target) == "" || err != nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, fmt.Errorf("%w: target_temperature must be finite numeric value", ErrInvalidCoolingInput)
	}
	return map[string]any{"cooling_started": true}, nil
}
