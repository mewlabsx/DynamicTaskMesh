package event

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"dtm/internal/kernel/identity"
)

type EventID = identity.EventID
type ObjectKind = identity.ObjectKind
type ObjectReference = identity.ObjectReference

var ErrInvalidEvent = errors.New("invalid kernel event record")

const (
	ResourceCreated                      = "Resource.Created"
	ResourceRemoved                      = "Resource.Removed"
	ResourceStateUpdated                 = "Resource.StateUpdated"
	ResourceAvailabilityUpdated          = "Resource.AvailabilityUpdated"
	CapabilityDeclarationRegistered      = "CapabilityDeclaration.Registered"
	CapabilityInstanceCreated            = "CapabilityInstance.Created"
	CapabilityInstanceRevoked            = "CapabilityInstance.Revoked"
	CapabilityInstanceDestroyed          = "CapabilityInstance.Destroyed"
	CapabilityInstanceStateUpdated       = "CapabilityInstance.StateUpdated"
	CapabilityHandleCreated              = "CapabilityHandle.Created"
	CapabilityHandleActivated            = "CapabilityHandle.Activated"
	CapabilityHandleRevoked              = "CapabilityHandle.Revoked"
	CapabilityHandleReleased             = "CapabilityHandle.Released"
	CapabilityHandleInvokeAccepted       = "CapabilityHandle.InvokeAccepted"
	CapabilityHandleInvokeRejected       = "CapabilityHandle.InvokeRejected"
	ExecutionContextCreated              = "ExecutionContext.Created"
	ExecutionContextTerminated           = "ExecutionContext.Terminated"
	ExecutionContextStateUpdated         = "ExecutionContext.StateUpdated"
	ExecutionOccupancyResolved           = "Execution.OccupancyResolved"
	ExecutionOccupancyResolutionAccepted = ExecutionOccupancyResolved
)

type EventRecord struct {
	ID             EventID
	Timestamp      time.Time
	SourceObject   ObjectReference
	EventType      string
	AffectedObject ObjectReference
	Cause          string
	Result         string
}

func NewEventRecord(
	sourceObject ObjectReference,
	eventType string,
	affectedObject ObjectReference,
	cause, result string,
	timestamp ...time.Time,
) (EventRecord, error) {
	id, err := identity.NewEventID()
	if err != nil {
		return EventRecord{}, err
	}
	occurredAt := time.Now().UTC()
	if len(timestamp) > 1 {
		return EventRecord{}, fmt.Errorf("%w: only one timestamp is allowed", ErrInvalidEvent)
	}
	if len(timestamp) == 1 {
		occurredAt = timestamp[0]
	}
	record := EventRecord{
		ID:             id,
		Timestamp:      occurredAt,
		SourceObject:   sourceObject,
		EventType:      eventType,
		AffectedObject: affectedObject,
		Cause:          cause,
		Result:         result,
	}
	if err := record.Validate(); err != nil {
		return EventRecord{}, err
	}
	return record, nil
}

func (record EventRecord) Validate() error {
	if err := record.ID.Validate(); err != nil {
		return fmt.Errorf("%w: identity: %v", ErrInvalidEvent, err)
	}
	if record.Timestamp.IsZero() {
		return fmt.Errorf("%w: timestamp is required", ErrInvalidEvent)
	}
	if err := record.SourceObject.Validate(); err != nil {
		return fmt.Errorf("%w: source object: %v", ErrInvalidEvent, err)
	}
	if strings.TrimSpace(record.EventType) == "" || strings.ContainsRune(record.EventType, '\x00') {
		return fmt.Errorf("%w: event type is required", ErrInvalidEvent)
	}
	if err := record.AffectedObject.Validate(); err != nil {
		return fmt.Errorf("%w: affected object: %v", ErrInvalidEvent, err)
	}
	if strings.TrimSpace(record.Cause) == "" || strings.ContainsRune(record.Cause, '\x00') {
		return fmt.Errorf("%w: cause is required", ErrInvalidEvent)
	}
	if strings.TrimSpace(record.Result) == "" || strings.ContainsRune(record.Result, '\x00') {
		return fmt.Errorf("%w: result is required", ErrInvalidEvent)
	}
	return nil
}

func (record EventRecord) Clone() EventRecord {
	return record
}

type QueryFilter struct {
	SourceObject   ObjectReference
	EventType      string
	AffectedObject ObjectReference
	Since          time.Time
	Until          time.Time
}

func (filter QueryFilter) matches(record EventRecord) bool {
	return (filter.SourceObject == (ObjectReference{}) || filter.SourceObject == record.SourceObject) &&
		(filter.EventType == "" || filter.EventType == record.EventType) &&
		(filter.AffectedObject == (ObjectReference{}) || filter.AffectedObject == record.AffectedObject) &&
		(filter.Since.IsZero() || !record.Timestamp.Before(filter.Since)) &&
		(filter.Until.IsZero() || !record.Timestamp.After(filter.Until))
}
