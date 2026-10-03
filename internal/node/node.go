package node

import (
	"errors"
	"strings"

	"dtm/internal/model"
)

type Status string

const (
	StatusRegistered Status = "registered"
	StatusActive     Status = "active"
	StatusSuspect    Status = "suspect"
	StatusOffline    Status = "offline"
	StatusRecovering Status = "recovering"
	StatusStale      Status = "stale"

	// StatusOnline keeps V0.2 source compatibility. ACTIVE is the only
	// schedulable V0.2.5 node state.
	StatusOnline = StatusActive
)

var (
	ErrInvalidNode             = errors.New("invalid node")
	ErrInvalidStatusTransition = errors.New("invalid node status transition")
)

type Node struct {
	id           model.NodeID
	capabilities []model.Capability
	status       Status
}

func New(id model.NodeID, capabilities []model.Capability, status Status) (Node, error) {
	if strings.TrimSpace(string(id)) == "" || !isValidStatus(status) {
		return Node{}, ErrInvalidNode
	}

	unique := make([]model.Capability, 0, len(capabilities))
	seen := make(map[model.Capability]struct{}, len(capabilities))
	for _, capability := range capabilities {
		if strings.TrimSpace(string(capability)) == "" {
			return Node{}, ErrInvalidNode
		}
		if _, exists := seen[capability]; exists {
			continue
		}
		seen[capability] = struct{}{}
		unique = append(unique, capability)
	}

	return Node{
		id:           id,
		capabilities: unique,
		status:       status,
	}, nil
}

func (n Node) ID() model.NodeID {
	return n.id
}

func (n Node) Capabilities() []model.Capability {
	return append([]model.Capability(nil), n.capabilities...)
}

func (n Node) Status() Status {
	return n.status
}

func (n Node) Has(capability model.Capability) bool {
	for _, provided := range n.capabilities {
		if provided == capability {
			return true
		}
	}

	return false
}

func (n Node) Transition(next Status) (Node, error) {
	if !isValidStatus(next) {
		return Node{}, ErrInvalidStatusTransition
	}
	if next == n.status {
		return n, nil
	}
	if !canTransition(n.status, next) {
		return Node{}, ErrInvalidStatusTransition
	}
	n.status = next
	return n, nil
}

func isValidStatus(status Status) bool {
	switch status {
	case StatusRegistered, StatusActive, StatusSuspect, StatusOffline, StatusRecovering, StatusStale:
		return true
	default:
		return false
	}
}

func canTransition(current, next Status) bool {
	switch current {
	case StatusRegistered:
		return next == StatusActive || next == StatusOffline
	case StatusActive:
		return next == StatusSuspect || next == StatusOffline
	case StatusSuspect:
		return next == StatusActive || next == StatusOffline
	case StatusOffline:
		return next == StatusRecovering || next == StatusRegistered
	case StatusRecovering:
		return next == StatusActive || next == StatusOffline
	case StatusStale:
		return next == StatusRegistered || next == StatusOffline
	default:
		return false
	}
}
