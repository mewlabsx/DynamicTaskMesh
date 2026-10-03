package ui

import (
	"testing"
	"time"
)

func TestTimelineDiffDetectsTaskStatusChange(t *testing.T) {
	timeline := NewTimeline(10)
	first := Snapshot{Timestamp: time.Unix(1, 0).UTC(), Status: "READY", Mesh: MeshOverview{Status: "READY", Coordinator: "node-a", CoreState: "ACTIVE"}, Tasks: []TaskView{{TaskID: "task-1", Status: "RUNNING"}}}
	if events := timeline.Observe(first, first.Timestamp); len(events) == 0 {
		t.Fatal("first observation produced no events")
	}
	second := first
	second.Tasks = []TaskView{{TaskID: "task-1", Status: "SUCCEEDED"}}
	events := timeline.Observe(second, time.Unix(2, 0).UTC())
	if len(events) < 2 {
		t.Fatalf("events = %#v, want initial and changed events", events)
	}
	last := events[len(events)-1]
	if last.Type != "TASK_STATUS_CHANGED" || last.Message != "task task-1: SUCCEEDED" {
		t.Fatalf("last event = %#v", last)
	}
}

func TestTimelineIdentifiesCoordinatorConflict(t *testing.T) {
	timeline := NewTimeline(10)
	timeline.Observe(Snapshot{Mesh: MeshOverview{Status: "DEGRADED", Coordinator: "NONE"}}, time.Unix(1, 0).UTC())
	events := timeline.Observe(Snapshot{Mesh: MeshOverview{Status: "DEGRADED", Coordinator: "CONFLICT"}}, time.Unix(2, 0).UTC())
	last := events[len(events)-1]
	if last.Type != "COORDINATOR_CHANGED" || last.Message != "coordinator state: CONFLICT" {
		t.Fatalf("last event = %#v", last)
	}
}
