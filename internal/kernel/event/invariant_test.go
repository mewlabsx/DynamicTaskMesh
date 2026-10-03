package event_test

import (
	"testing"
	"time"

	"dtm/internal/kernel/event"
	"dtm/internal/kernel/identity"
)

func TestInvariant_EventRecordSnapshotIsImmutable(t *testing.T) {
	manager := event.NewManager()
	original := event.EventRecord{
		Timestamp:      time.Date(2026, time.August, 27, 8, 0, 0, 0, time.UTC),
		SourceObject:   identity.ObjectReference{Kind: identity.ObjectKindResource, ID: "resource-1"},
		EventType:      event.ResourceCreated,
		AffectedObject: identity.ObjectReference{Kind: identity.ObjectKindResource, ID: "resource-1"},
		Cause:          "invariant test",
		Result:         "recorded",
	}
	published, err := manager.PublishAndReturn(original)
	if err != nil {
		t.Fatalf("PublishAndReturn() error = %v", err)
	}

	snapshot := manager.Query()[0]
	snapshot.ID = identity.EventID("event-mutated")
	snapshot.Timestamp = snapshot.Timestamp.Add(time.Hour)
	snapshot.SourceObject = identity.ObjectReference{Kind: identity.ObjectKindCapabilityHandle, ID: "handle-mutated"}
	snapshot.EventType = "mutated.event"
	snapshot.AffectedObject = identity.ObjectReference{Kind: identity.ObjectKindExecutionContext, ID: "context-mutated"}
	snapshot.Cause = "mutated-cause"
	snapshot.Result = "mutated-result"

	got := manager.Query()[0]
	if got.ID != published.ID || !got.Timestamp.Equal(published.Timestamp) ||
		got.SourceObject != published.SourceObject || got.EventType != published.EventType ||
		got.AffectedObject != published.AffectedObject || got.Cause != published.Cause ||
		got.Result != published.Result {
		t.Fatalf("stored EventRecord changed through snapshot mutation: got=%#v published=%#v", got, published)
	}
}
