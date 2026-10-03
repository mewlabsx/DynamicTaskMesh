package execution

import (
	"fmt"
	"strings"
	"sync"

	"dtm/internal/kernel/event"
	"dtm/internal/kernel/identity"
)

type Manager struct {
	mu       sync.RWMutex
	contexts map[ExecutionContextID]ExecutionContext
	events   *event.Manager
	publish  func(event.EventRecord) error
}

type ExecutionManager = Manager

func NewManager(auditEvents ...*event.Manager) *Manager {
	var events *event.Manager
	if len(auditEvents) > 0 {
		events = auditEvents[0]
	}
	if events == nil {
		events = event.NewManager()
	}
	return &Manager{contexts: make(map[ExecutionContextID]ExecutionContext), events: events, publish: events.Publish}
}

func NewExecutionManager(auditEvents ...*event.Manager) *Manager {
	return NewManager(auditEvents...)
}

func (manager *Manager) EventManager() *event.Manager {
	return manager.events
}

// CreateContext starts in CREATED state. CREATED contexts are valid kernel
// subjects and can be activated through UpdateState when a caller needs the
// explicit ACTIVE transition.
func (manager *Manager) CreateContext(subject string, scope ...string) (ExecutionContext, error) {
	if len(scope) > 1 {
		return ExecutionContext{}, fmt.Errorf("%w: only one scope is allowed", ErrInvalidContext)
	}
	selectedScope := "local"
	if len(scope) == 1 {
		selectedScope = strings.TrimSpace(scope[0])
	}
	if strings.TrimSpace(subject) == "" || selectedScope == "" {
		return ExecutionContext{}, ErrInvalidContext
	}

	for {
		id, err := identity.NewExecutionContextID()
		if err != nil {
			return ExecutionContext{}, err
		}
		created, err := NewContext(id, subject, selectedScope, StateCreated)
		if err != nil {
			return ExecutionContext{}, err
		}

		manager.mu.Lock()
		if _, exists := manager.contexts[id]; !exists {
			manager.contexts[id] = created
			if err := manager.publishAudit(
				event.ExecutionContextCreated,
				identity.ObjectReference{Kind: identity.ObjectKindExecutionContext, ID: created.ID.String()},
				identity.ObjectReference{Kind: identity.ObjectKindExecutionContext, ID: created.ID.String()},
				"execution manager create",
				"created",
			); err != nil {
				delete(manager.contexts, id)
				manager.mu.Unlock()
				return ExecutionContext{}, err
			}
			manager.mu.Unlock()
			return created, nil
		}
		manager.mu.Unlock()
	}
}

func (manager *Manager) GetContext(id ExecutionContextID) (ExecutionContext, error) {
	if err := id.Validate(); err != nil {
		return ExecutionContext{}, err
	}
	manager.mu.RLock()
	context, exists := manager.contexts[id]
	manager.mu.RUnlock()
	if !exists {
		return ExecutionContext{}, ErrContextNotFound
	}
	if context.ID != id {
		return ExecutionContext{}, fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidContext)
	}
	return context.Clone(), nil
}

// UpdateState is a Kernel-internal lifecycle mutation path, not a User Space
// ABI. Its state change and mandatory audit publication are rollback-coupled.
func (manager *Manager) UpdateState(id ExecutionContextID, next LifecycleState) error {
	if err := id.Validate(); err != nil {
		return err
	}
	manager.mu.Lock()
	current, exists := manager.contexts[id]
	if !exists {
		manager.mu.Unlock()
		return ErrContextNotFound
	}
	if current.ID != id {
		manager.mu.Unlock()
		return fmt.Errorf("%w: stored identity does not match lookup key", ErrInvalidContext)
	}
	updated, err := current.Transition(next)
	if err != nil {
		manager.mu.Unlock()
		return err
	}
	manager.contexts[id] = updated
	if current.State != next {
		eventType := event.ExecutionContextStateUpdated
		result := string(next)
		if next == StateTerminated {
			eventType = event.ExecutionContextTerminated
			result = "terminated"
		}
		if err := manager.publishAudit(
			eventType,
			identity.ObjectReference{Kind: identity.ObjectKindExecutionContext, ID: id.String()},
			identity.ObjectReference{Kind: identity.ObjectKindExecutionContext, ID: id.String()},
			"execution manager state transition",
			result,
		); err != nil {
			manager.contexts[id] = current
			manager.mu.Unlock()
			return err
		}
	}
	manager.mu.Unlock()
	return nil
}

func (manager *Manager) TerminateContext(id ExecutionContextID) error {
	return manager.UpdateState(id, StateTerminated)
}

func (manager *Manager) publishAudit(
	eventType string,
	source, affected identity.ObjectReference,
	cause, result string,
) error {
	record, err := event.NewEventRecord(source, eventType, affected, cause, result)
	if err != nil {
		return err
	}
	return manager.publish(record)
}
