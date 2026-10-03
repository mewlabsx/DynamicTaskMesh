package lease

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"dtm/internal/model"
)

var (
	ErrInvalidLease      = errors.New("invalid lease")
	ErrLeaseNotFound     = errors.New("lease not found")
	ErrStaleRegistration = errors.New("stale registration")
)

type Record struct {
	NodeID         model.NodeID
	RegistrationID string
	LastSeen       time.Time
	ExpiresAt      time.Time
}

type Manager struct {
	mu     sync.RWMutex
	ttl    time.Duration
	leases map[model.NodeID]Record
}

func NewManager(ttl time.Duration) (*Manager, error) {
	if ttl <= 0 {
		return nil, ErrInvalidLease
	}
	return &Manager{
		ttl:    ttl,
		leases: make(map[model.NodeID]Record),
	}, nil
}

func (manager *Manager) Register(
	nodeID model.NodeID,
	registrationID string,
	now time.Time,
) (Record, error) {
	record, err := manager.newRecord(nodeID, registrationID, now)
	if err != nil {
		return Record{}, err
	}

	manager.mu.Lock()
	manager.leases[record.NodeID] = record
	manager.mu.Unlock()
	return record, nil
}

func (manager *Manager) Heartbeat(
	nodeID model.NodeID,
	registrationID string,
	now time.Time,
) (Record, error) {
	nodeID = model.NodeID(strings.TrimSpace(string(nodeID)))
	registrationID = strings.TrimSpace(registrationID)
	if nodeID == "" || registrationID == "" || now.IsZero() {
		return Record{}, ErrInvalidLease
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	current, exists := manager.leases[nodeID]
	if !exists {
		return Record{}, ErrLeaseNotFound
	}
	if current.RegistrationID != registrationID {
		return Record{}, ErrStaleRegistration
	}

	current.LastSeen = now
	current.ExpiresAt = now.Add(manager.ttl)
	manager.leases[nodeID] = current
	return current, nil
}

func (manager *Manager) Current(nodeID model.NodeID) (Record, bool) {
	nodeID = model.NodeID(strings.TrimSpace(string(nodeID)))

	manager.mu.RLock()
	record, exists := manager.leases[nodeID]
	manager.mu.RUnlock()
	return record, exists
}

func (manager *Manager) Valid(nodeID model.NodeID, now time.Time) bool {
	nodeID = model.NodeID(strings.TrimSpace(string(nodeID)))
	if nodeID == "" || now.IsZero() {
		return false
	}
	manager.mu.RLock()
	record, exists := manager.leases[nodeID]
	manager.mu.RUnlock()
	return exists && now.Before(record.ExpiresAt)
}

func (manager *Manager) Remove(nodeID model.NodeID, registrationID string) error {
	nodeID = model.NodeID(strings.TrimSpace(string(nodeID)))
	registrationID = strings.TrimSpace(registrationID)
	if nodeID == "" || registrationID == "" {
		return ErrInvalidLease
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()

	current, exists := manager.leases[nodeID]
	if !exists {
		return ErrLeaseNotFound
	}
	if current.RegistrationID != registrationID {
		return ErrStaleRegistration
	}
	delete(manager.leases, nodeID)
	return nil
}

func (manager *Manager) Expired(now time.Time) []Record {
	if now.IsZero() {
		return nil
	}

	manager.mu.RLock()
	expired := make([]Record, 0)
	for _, record := range manager.leases {
		if !now.Before(record.ExpiresAt) {
			expired = append(expired, record)
		}
	}
	manager.mu.RUnlock()

	sort.Slice(expired, func(left, right int) bool {
		return expired[left].NodeID < expired[right].NodeID
	})
	return expired
}

func (manager *Manager) newRecord(
	nodeID model.NodeID,
	registrationID string,
	now time.Time,
) (Record, error) {
	nodeID = model.NodeID(strings.TrimSpace(string(nodeID)))
	registrationID = strings.TrimSpace(registrationID)
	if nodeID == "" || registrationID == "" || now.IsZero() {
		return Record{}, ErrInvalidLease
	}
	return Record{
		NodeID:         nodeID,
		RegistrationID: registrationID,
		LastSeen:       now,
		ExpiresAt:      now.Add(manager.ttl),
	}, nil
}
