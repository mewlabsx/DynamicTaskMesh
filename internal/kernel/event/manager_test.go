package event_test

import (
	"testing"
	"time"

	"dtm/internal/kernel/event"
	"dtm/internal/kernel/identity"
	"dtm/internal/kernel/resource"
)

func TestPublishAndQueryAreAppendOnlyFacts(t *testing.T) {
	resources := resource.NewManager()
	resourceObject, err := resources.CreateResource("scope-a")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}
	before, err := resources.GetResource(resourceObject.ID)
	if err != nil {
		t.Fatalf("GetResource(before) error = %v", err)
	}

	manager := event.NewManager()
	occurredAt := time.Date(2026, time.August, 21, 9, 30, 0, 0, time.UTC)
	record := event.EventRecord{
		Timestamp:      occurredAt,
		SourceObject:   identity.ObjectReference{Kind: identity.ObjectKindResource, ID: resourceObject.ID.String()},
		EventType:      event.ResourceCreated,
		AffectedObject: identity.ObjectReference{Kind: identity.ObjectKindResource, ID: resourceObject.ID.String()},
		Cause:          "kernel initialization",
		Result:         "recorded",
	}
	published, err := manager.PublishAndReturn(record)
	if err != nil {
		t.Fatalf("PublishAndReturn() error = %v", err)
	}
	if published.ID == "" || !published.Timestamp.Equal(occurredAt) {
		t.Fatalf("published record missing generated identity or timestamp: %#v", published)
	}

	after, err := resources.GetResource(resourceObject.ID)
	if err != nil {
		t.Fatalf("GetResource(after) error = %v", err)
	}
	if after.State != before.State || after.Scope != before.Scope {
		t.Fatalf("event publication changed resource state: before=%#v after=%#v", before, after)
	}

	query := manager.Query(event.QueryFilter{EventType: event.ResourceCreated})
	if len(query) != 1 || query[0].ID != published.ID {
		t.Fatalf("Query() = %#v, want one published record", query)
	}
	query[0].Cause = "changed-outside-manager"
	if got := manager.Query()[0].Cause; got != "kernel initialization" {
		t.Fatalf("stored event changed through query snapshot: %q", got)
	}
}

func TestPublishRejectsInvalidRecords(t *testing.T) {
	manager := event.NewManager()
	if err := manager.Publish(event.EventRecord{}); err == nil {
		t.Fatal("Publish(invalid record) error = nil")
	}
}
