package planner

import (
	"errors"
	"math"
	"strconv"

	"dtm/internal/capability"
	"dtm/internal/model"
	"dtm/internal/task"
)

var (
	ErrUnknownIntent       = errors.New("unknown intent")
	ErrRequirementConflict = errors.New("requirement conflict")
)

type Step struct {
	ID              model.StepID
	Capability      model.Capability
	IdempotencyMode model.IdempotencyMode
	Inputs          map[string]string
}

type Plan struct {
	TaskID model.TaskID
	Steps  []Step
}

type Planner struct{}

func New() Planner {
	return Planner{}
}

func (Planner) Plan(input task.Task) (Plan, error) {
	if input.Intent != "cool_environment" {
		return Plan{}, ErrUnknownIntent
	}

	targetTemperature := input.Constraints["target_temperature"]
	targetValue, err := strconv.ParseFloat(targetTemperature, 64)
	if err != nil || math.IsNaN(targetValue) || math.IsInf(targetValue, 0) {
		return Plan{}, ErrRequirementConflict
	}

	steps := []Step{
		{
			ID:              "step-1",
			Capability:      capability.TemperatureSensor,
			IdempotencyMode: model.IdempotencyIdempotent,
			Inputs:          copyInputs(map[string]string{"operation": "read_temperature"}),
		},
		{
			ID:              "step-2",
			Capability:      capability.CoolingControl,
			IdempotencyMode: model.IdempotencyNonIdempotent,
			Inputs: copyInputs(map[string]string{
				"target_temperature": targetTemperature,
			}),
		},
	}
	if len(input.Requirements) != 0 {
		if len(input.Requirements) != len(steps) {
			return Plan{}, ErrRequirementConflict
		}
		for index, requirement := range input.Requirements {
			if requirement != steps[index].Capability {
				return Plan{}, ErrRequirementConflict
			}
		}
	}

	return Plan{
		TaskID: input.ID,
		Steps:  append([]Step(nil), steps...),
	}, nil
}

func copyInputs(inputs map[string]string) map[string]string {
	if inputs == nil {
		return nil
	}

	result := make(map[string]string, len(inputs))
	for key, value := range inputs {
		result[key] = value
	}
	return result
}
