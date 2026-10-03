package nodelifecycle

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"dtm/internal/lease"
	"dtm/internal/model"
	"dtm/internal/node"
)

var (
	ErrInvalidDependency = errors.New("invalid node lifecycle dependency")
	ErrStaleRegistration = errors.New("node registration is no longer current")
)

type Registry interface {
	Register(node.Node)
	SetStatus(model.NodeID, node.Status) error
}

type NodeLookup interface {
	Find(model.NodeID) (node.Node, bool)
}

type Endpoints interface {
	Prepare(model.NodeID, string) error
	Activate(model.NodeID)
	Available(model.NodeID) bool
	Delete(model.NodeID)
}

type Controller struct {
	registry  Registry
	endpoints Endpoints
	leases    *lease.Manager
	now       func() time.Time
	mu        sync.Mutex
}

func New(
	registry Registry,
	endpoints Endpoints,
	leases *lease.Manager,
	now func() time.Time,
) (*Controller, error) {
	if registry == nil || endpoints == nil || leases == nil || now == nil {
		return nil, ErrInvalidDependency
	}
	return &Controller{
		registry:  registry,
		endpoints: endpoints,
		leases:    leases,
		now:       now,
	}, nil
}

func (controller *Controller) Register(
	registered node.Node,
	address string,
	registrationID string,
) (lease.Record, error) {
	return controller.RegisterAt(registered, address, registrationID, controller.now())
}

func (controller *Controller) RegisterAt(
	registered node.Node,
	address string,
	registrationID string,
	registeredAt time.Time,
) (lease.Record, error) {
	address = strings.TrimSpace(address)
	registrationID = strings.TrimSpace(registrationID)
	if registered.Status() != node.StatusRegistered || address == "" || registrationID == "" || registeredAt.IsZero() {
		return lease.Record{}, node.ErrInvalidNode
	}

	controller.mu.Lock()
	defer controller.mu.Unlock()

	// Prepare keeps the new endpoint invisible. Endpoint activation is the
	// final runtime commit point after Registry and Lease are ready.
	if err := controller.endpoints.Prepare(registered.ID(), address); err != nil {
		return lease.Record{}, fmt.Errorf("prepare endpoint: %w", err)
	}
	nodeToRegister := registered
	if lookup, ok := controller.registry.(NodeLookup); ok {
		current, exists := lookup.Find(registered.ID())
		if exists && current.Status() == node.StatusOffline {
			if err := controller.registry.SetStatus(registered.ID(), node.StatusRecovering); err != nil {
				controller.endpoints.Delete(registered.ID())
				return lease.Record{}, fmt.Errorf("recover node: %w", err)
			}
			recovering, err := node.New(registered.ID(), registered.Capabilities(), node.StatusRecovering)
			if err != nil {
				controller.endpoints.Delete(registered.ID())
				return lease.Record{}, fmt.Errorf("construct recovering node: %w", err)
			}
			nodeToRegister = recovering
		}
	}
	controller.registry.Register(nodeToRegister)
	record, err := controller.leases.Register(registered.ID(), registrationID, registeredAt)
	if err != nil {
		_ = controller.registry.SetStatus(registered.ID(), node.StatusOffline)
		controller.endpoints.Delete(registered.ID())
		return lease.Record{}, fmt.Errorf("create lease: %w", err)
	}
	controller.endpoints.Activate(registered.ID())
	if err := controller.registry.SetStatus(registered.ID(), node.StatusActive); err != nil {
		_ = controller.leases.Remove(registered.ID(), registrationID)
		_ = controller.registry.SetStatus(registered.ID(), node.StatusOffline)
		controller.endpoints.Delete(registered.ID())
		return lease.Record{}, fmt.Errorf("activate node: %w", err)
	}
	return record, nil
}
func (controller *Controller) Heartbeat(
	nodeID model.NodeID,
	registrationID string,
) (lease.Record, error) {
	return controller.HeartbeatAt(nodeID, registrationID, controller.now())
}

func (controller *Controller) HeartbeatAt(
	nodeID model.NodeID,
	registrationID string,
	heartbeatAt time.Time,
) (lease.Record, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()

	record, err := controller.leases.Heartbeat(
		nodeID,
		registrationID,
		heartbeatAt,
	)
	if err != nil {
		return lease.Record{}, err
	}
	if err := controller.registry.SetStatus(nodeID, node.StatusActive); err != nil {
		return lease.Record{}, fmt.Errorf("activate heartbeat node: %w", err)
	}
	return record, nil
}

func (controller *Controller) Offline(
	nodeID model.NodeID,
	registrationID string,
) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()

	current, exists := controller.leases.Current(nodeID)
	if !exists {
		return lease.ErrLeaseNotFound
	}
	if current.RegistrationID != strings.TrimSpace(registrationID) {
		return ErrStaleRegistration
	}
	if err := controller.registry.SetStatus(nodeID, node.StatusOffline); err != nil {
		return err
	}
	controller.endpoints.Delete(nodeID)
	return controller.leases.Remove(nodeID, registrationID)
}

func (controller *Controller) QuiesceForRegistration(nodeID model.NodeID) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.quiesceRuntimeNode(nodeID)
}

func (controller *Controller) CurrentRegistration(nodeID model.NodeID) (lease.Record, bool) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.leases.Current(nodeID)
}

func (controller *Controller) FailClosed(nodeID model.NodeID) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.quiesceRuntimeNode(nodeID)
}

func (controller *Controller) quiesceRuntimeNode(nodeID model.NodeID) error {
	var result error
	shouldSetOffline := true
	if lookup, ok := controller.registry.(NodeLookup); ok {
		current, exists := lookup.Find(nodeID)
		shouldSetOffline = exists && current.Status() != node.StatusOffline
	}
	if shouldSetOffline {
		if err := controller.registry.SetStatus(nodeID, node.StatusOffline); err != nil {
			result = errors.Join(result, fmt.Errorf("fail-close node %q registry: %w", nodeID, err))
		}
	}
	controller.endpoints.Delete(nodeID)
	if current, exists := controller.leases.Current(nodeID); exists {
		if err := controller.leases.Remove(nodeID, current.RegistrationID); err != nil {
			result = errors.Join(result, fmt.Errorf("fail-close node %q lease: %w", nodeID, err))
		}
	}
	return result
}

func (controller *Controller) SweepExpired(now time.Time) (int, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()

	expired := controller.leases.Expired(now)
	count := 0
	var sweepErr error
	for _, record := range expired {
		current, exists := controller.leases.Current(record.NodeID)
		if !exists ||
			current.RegistrationID != record.RegistrationID ||
			now.Before(current.ExpiresAt) {
			continue
		}
		if err := controller.registry.SetStatus(record.NodeID, node.StatusSuspect); err != nil {
			sweepErr = errors.Join(
				sweepErr,
				fmt.Errorf("suspect node %q: %w", record.NodeID, err),
			)
			continue
		}
		if err := controller.registry.SetStatus(record.NodeID, node.StatusOffline); err != nil {
			sweepErr = errors.Join(
				sweepErr,
				fmt.Errorf("offline node %q: %w", record.NodeID, err),
			)
			continue
		}
		controller.endpoints.Delete(record.NodeID)
		if err := controller.leases.Remove(record.NodeID, record.RegistrationID); err != nil {
			sweepErr = errors.Join(
				sweepErr,
				fmt.Errorf("remove expired lease %q: %w", record.NodeID, err),
			)
			continue
		}
		count++
	}
	return count, sweepErr
}

func (controller *Controller) Eligible(nodeID model.NodeID) bool {
	if !controller.leases.Valid(nodeID, controller.now()) || !controller.endpoints.Available(nodeID) {
		return false
	}
	if lookup, ok := controller.registry.(NodeLookup); ok {
		current, exists := lookup.Find(nodeID)
		return exists && current.Status() == node.StatusActive
	}
	return true
}
