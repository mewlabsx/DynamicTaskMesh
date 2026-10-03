package task

import (
	"errors"
	"strings"

	"dtm/internal/model"
)

var ErrInvalidTask = errors.New("invalid task")

type Constraints map[string]string

type Task struct {
	ID           model.TaskID
	Intent       string
	Requirements []model.Capability
	Constraints  Constraints
}

func New(
	id model.TaskID,
	intent string,
	requirements []model.Capability,
	constraints Constraints,
) (Task, error) {
	if strings.TrimSpace(string(id)) == "" || strings.TrimSpace(intent) == "" {
		return Task{}, ErrInvalidTask
	}

	seen := make(map[model.Capability]struct{}, len(requirements))
	for _, requirement := range requirements {
		if strings.TrimSpace(requirement.String()) == "" {
			return Task{}, ErrInvalidTask
		}
		if _, exists := seen[requirement]; exists {
			return Task{}, ErrInvalidTask
		}
		seen[requirement] = struct{}{}
	}

	requirementsCopy := append([]model.Capability(nil), requirements...)

	var constraintsCopy Constraints
	if constraints != nil {
		constraintsCopy = make(Constraints, len(constraints))
		for key, value := range constraints {
			constraintsCopy[key] = value
		}
	}

	return Task{
		ID:           id,
		Intent:       intent,
		Requirements: requirementsCopy,
		Constraints:  constraintsCopy,
	}, nil
}
