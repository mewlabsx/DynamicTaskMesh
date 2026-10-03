package event

import (
	"errors"
	"sync"
	"time"

	"dtm/internal/kernel/identity"
)

var ErrDuplicateEvent = errors.New("event already published")

type Manager struct {
	mu     sync.RWMutex
	events []EventRecord
}

type EventManager = Manager

func NewManager() *Manager {
	return &Manager{events: make([]EventRecord, 0)}
}

func NewEventManager() *Manager {
	return NewManager()
}

// Publish appends a validated fact. Missing identity and timestamp are filled
// by the Event Manager; no Resource, Capability, or Execution state is
// changed as a side effect. This public Go method is a Kernel-internal audit
// sink, not a DTM User Space ABI.
func (manager *Manager) Publish(record EventRecord) error {
	_, err := manager.publish(record)
	return err
}

// PublishAndReturn is the Kernel-internal variant that returns the stored
// snapshot; its Go visibility does not define a User Space ABI.
func (manager *Manager) PublishAndReturn(record EventRecord) (EventRecord, error) {
	return manager.publish(record)
}

func (manager *Manager) publish(record EventRecord) (EventRecord, error) {
	if record.ID == "" {
		id, err := identity.NewEventID()
		if err != nil {
			return EventRecord{}, err
		}
		record.ID = id
	}
	if record.Timestamp.IsZero() {
		record.Timestamp = time.Now().UTC()
	}
	if err := record.Validate(); err != nil {
		return EventRecord{}, err
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	for _, existing := range manager.events {
		if existing.ID == record.ID {
			return EventRecord{}, ErrDuplicateEvent
		}
	}
	manager.events = append(manager.events, record.Clone())
	return record.Clone(), nil
}

func (manager *Manager) Query(filters ...QueryFilter) []EventRecord {
	manager.mu.RLock()
	result := make([]EventRecord, 0, len(manager.events))
	for _, record := range manager.events {
		matches := true
		for _, filter := range filters {
			if !filter.matches(record) {
				matches = false
				break
			}
		}
		if matches {
			result = append(result, record.Clone())
		}
	}
	manager.mu.RUnlock()
	return result
}
