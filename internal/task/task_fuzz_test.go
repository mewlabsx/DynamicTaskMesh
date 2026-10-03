package task

import (
	"strings"
	"testing"

	"dtm/internal/model"
)

const maxTaskFuzzInput = 64 << 10

func FuzzNewTask(f *testing.F) {
	f.Add([]byte("task-1\x00cool_environment\x00temperature_sensor,cooling_control\x00latency=100ms"))
	f.Add([]byte{})
	f.Add([]byte("task-1\x00\x00temperature_sensor,temperature_sensor"))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxTaskFuzzInput {
			return
		}
		id, intent, requirements, constraints := fuzzTaskInput(data)
		created, err := New(id, intent, requirements, constraints)
		if err != nil {
			return
		}
		if created.ID != id || created.Intent != intent {
			t.Fatalf("task identity changed: got (%q, %q), want (%q, %q)", created.ID, created.Intent, id, intent)
		}
		if strings.TrimSpace(string(created.ID)) == "" || strings.TrimSpace(created.Intent) == "" {
			t.Fatal("New returned a task with an empty identity or intent")
		}
		seen := make(map[model.Capability]struct{}, len(created.Requirements))
		for _, requirement := range created.Requirements {
			if strings.TrimSpace(requirement.String()) == "" {
				t.Fatal("New returned a blank requirement")
			}
			if _, exists := seen[requirement]; exists {
				t.Fatalf("New returned duplicate requirement %q", requirement)
			}
			seen[requirement] = struct{}{}
		}
		if len(created.Constraints) != len(constraints) {
			t.Fatalf("constraint count = %d, want %d", len(created.Constraints), len(constraints))
		}
	})
}

func fuzzTaskInput(data []byte) (model.TaskID, string, []model.Capability, Constraints) {
	sections := strings.SplitN(string(data), "\x00", 4)
	id := model.TaskID(sections[0])
	intent := string(data)
	if len(sections) > 1 {
		intent = sections[1]
	}
	requirements := make([]model.Capability, 0, 16)
	if len(sections) > 2 {
		for _, value := range strings.Split(sections[2], ",") {
			if len(requirements) == 16 {
				break
			}
			requirements = append(requirements, model.Capability(value))
		}
	}
	constraints := Constraints{}
	if len(sections) > 3 {
		constraints["fuzz"] = sections[3]
	}
	return id, intent, requirements, constraints
}
