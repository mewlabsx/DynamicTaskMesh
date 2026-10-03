package capability

import (
	"errors"
	"sort"
	"sync"

	"dtm/internal/model"
	"dtm/internal/node"
)

var ErrNodeNotFound = errors.New("node not found")

type Registry struct {
	mu    sync.RWMutex
	nodes map[model.NodeID]node.Node
}

func NewRegistry() *Registry {
	return &Registry{
		nodes: make(map[model.NodeID]node.Node),
	}
}

func (r *Registry) Register(n node.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nodes[n.ID()] = n
}

func (r *Registry) Find(id model.NodeID) (node.Node, bool) {
	r.mu.RLock()
	current, exists := r.nodes[id]
	r.mu.RUnlock()
	return current, exists
}

func (r *Registry) SetStatus(id model.NodeID, status node.Status) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	current, exists := r.nodes[id]
	if !exists {
		return ErrNodeNotFound
	}

	updated, err := current.Transition(status)
	if err != nil {
		return err
	}
	r.nodes[id] = updated
	return nil
}

func (r *Registry) Discover(capability model.Capability) []node.Node {
	r.mu.RLock()
	defer r.mu.RUnlock()

	matches := make([]node.Node, 0)
	for _, candidate := range r.nodes {
		if candidate.Status() == node.StatusActive && candidate.Has(capability) {
			matches = append(matches, candidate)
		}
	}

	sort.Slice(matches, func(left, right int) bool {
		return string(matches[left].ID()) < string(matches[right].ID())
	})
	return matches
}
