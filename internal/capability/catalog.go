package capability

import (
	"errors"
	"sort"

	"dtm/internal/model"
)

var ErrUnknownCapability = errors.New("unknown capability")

const (
	TemperatureSensor model.Capability = "temperature_sensor"
	CoolingControl    model.Capability = "cooling_control"
)

type Definition struct {
	Name        model.Capability
	Description string
}

var definitions = map[model.Capability]Definition{
	TemperatureSensor: {
		Name:        TemperatureSensor,
		Description: "temperature acquisition",
	},
	CoolingControl: {
		Name:        CoolingControl,
		Description: "cooling control",
	},
}

func Lookup(name model.Capability) (Definition, bool) {
	definition, exists := definitions[name]
	return definition, exists
}

func All() []Definition {
	result := make([]Definition, 0, len(definitions))
	for _, definition := range definitions {
		result = append(result, definition)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].Name < result[right].Name
	})
	return result
}
