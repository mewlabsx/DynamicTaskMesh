package ui

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Timeline stores synthetic observation events produced by snapshot diffs.
// These events are deliberately not authoritative DTM events.
type Timeline struct {
	mu        sync.Mutex
	previous  map[string]string
	events    []ObservationEvent
	maxEvents int
}

func NewTimeline(maxEvents int) *Timeline {
	if maxEvents <= 0 {
		maxEvents = 100
	}
	return &Timeline{previous: make(map[string]string), maxEvents: maxEvents}
}

func (timeline *Timeline) Observe(snapshot Snapshot, now time.Time) []ObservationEvent {
	if timeline == nil {
		return nil
	}
	current := observationState(snapshot)
	timeline.mu.Lock()
	defer timeline.mu.Unlock()
	keys := make([]string, 0, len(current))
	for key := range current {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := current[key]
		previous, exists := timeline.previous[key]
		if !exists || previous != value {
			timeline.events = append(timeline.events, eventForChange(key, previous, value, now))
		}
	}
	for key, previous := range timeline.previous {
		if _, exists := current[key]; !exists {
			timeline.events = append(timeline.events, eventForChange(key, previous, "removed", now))
		}
	}
	timeline.previous = current
	if len(timeline.events) > timeline.maxEvents {
		timeline.events = append([]ObservationEvent(nil), timeline.events[len(timeline.events)-timeline.maxEvents:]...)
	}
	return append([]ObservationEvent(nil), timeline.events...)
}

func observationState(snapshot Snapshot) map[string]string {
	state := map[string]string{
		"mesh.status":      snapshot.Mesh.Status,
		"mesh.coordinator": snapshot.Mesh.Coordinator,
		"mesh.core":        snapshot.Mesh.CoreState,
		"mesh.authority":   snapshot.Mesh.AuthorityState,
		"mesh.ingress":     snapshot.Mesh.IngressState,
	}
	for _, runtime := range snapshot.Runtimes {
		key := "runtime:" + runtime.NodeID + "/" + runtime.RuntimeInstanceID
		state[key] = runtime.MembershipState + ";coordinator=" + fmt.Sprint(runtime.Coordinator) + ";core=" + fmt.Sprint(runtime.CoreReady) + ";authority=" + runtime.LocalAuthorityBinding
	}
	for _, resource := range snapshot.Resources {
		state["resource:"+resource.ResourceID] = resource.Status
	}
	for _, task := range snapshot.Tasks {
		state["task:"+task.TaskID] = task.Status
	}
	return state
}

func eventForChange(key, previous, current string, now time.Time) ObservationEvent {
	switch {
	case key == "mesh.coordinator":
		if current == "NONE" || current == "CONFLICT" {
			return ObservationEvent{Timestamp: now, Type: "COORDINATOR_CHANGED", Message: "coordinator state: " + current}
		}
		return ObservationEvent{Timestamp: now, Type: "COORDINATOR_CHANGED", Message: "coordinator selected: " + current}
	case key == "mesh.core":
		return ObservationEvent{Timestamp: now, Type: "CORE_STATE_CHANGED", Message: "dynamic core state: " + current}
	case key == "mesh.authority":
		return ObservationEvent{Timestamp: now, Type: "AUTHORITY_STATE_CHANGED", Message: "authority state: " + current}
	case key == "mesh.ingress":
		return ObservationEvent{Timestamp: now, Type: "INGRESS_STATE_CHANGED", Message: "ingress state: " + current}
	case key == "mesh.status":
		return ObservationEvent{Timestamp: now, Type: "MESH_STATUS_CHANGED", Message: "mesh status: " + current}
	case len(key) > len("runtime:") && key[:len("runtime:")] == "runtime:":
		return ObservationEvent{Timestamp: now, Type: "RUNTIME_STATE_CHANGED", Message: key[len("runtime:"):] + " observed: " + current}
	case len(key) > len("resource:") && key[:len("resource:")] == "resource:":
		return ObservationEvent{Timestamp: now, Type: "RESOURCE_STATE_CHANGED", Message: "resource " + key[len("resource:"):] + ": " + current}
	case len(key) > len("task:") && key[:len("task:")] == "task:":
		return ObservationEvent{Timestamp: now, Type: "TASK_STATUS_CHANGED", Message: "task " + key[len("task:"):] + ": " + current}
	default:
		return ObservationEvent{Timestamp: now, Type: "OBSERVATION_CHANGED", Message: key + ": " + previous + " -> " + current}
	}
}
