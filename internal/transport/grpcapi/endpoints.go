package grpcapi

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"dtm/internal/model"
)

var (
	ErrInvalidEndpoint  = errors.New("invalid endpoint")
	ErrEndpointNotFound = errors.New("endpoint not found")
)

type EndpointDirectory struct {
	mu        sync.RWMutex
	endpoints map[model.NodeID]string
	pending   map[model.NodeID]string
}

func NewEndpointDirectory() *EndpointDirectory {
	return &EndpointDirectory{
		endpoints: make(map[model.NodeID]string),
		pending:   make(map[model.NodeID]string),
	}
}

func (d *EndpointDirectory) Set(id model.NodeID, address string) error {
	if err := d.Prepare(id, address); err != nil {
		return err
	}
	d.Activate(id)
	return nil
}

func (d *EndpointDirectory) Prepare(id model.NodeID, address string) error {
	normalizedID := model.NodeID(strings.TrimSpace(string(id)))
	normalizedAddress := strings.TrimSpace(address)
	if normalizedID == "" || normalizedAddress == "" {
		return ErrInvalidEndpoint
	}
	d.mu.Lock()
	delete(d.endpoints, normalizedID)
	d.pending[normalizedID] = normalizedAddress
	d.mu.Unlock()
	return nil
}

func (d *EndpointDirectory) Activate(id model.NodeID) {
	normalizedID := model.NodeID(strings.TrimSpace(string(id)))
	d.mu.Lock()
	if address, exists := d.pending[normalizedID]; exists {
		d.endpoints[normalizedID] = address
		delete(d.pending, normalizedID)
	}
	d.mu.Unlock()
}

func (d *EndpointDirectory) Available(id model.NodeID) bool {
	normalizedID := model.NodeID(strings.TrimSpace(string(id)))
	d.mu.RLock()
	_, exists := d.endpoints[normalizedID]
	d.mu.RUnlock()
	return exists
}
func (d *EndpointDirectory) Resolve(id model.NodeID) (string, error) {
	normalizedID := model.NodeID(strings.TrimSpace(string(id)))

	d.mu.RLock()
	address, exists := d.endpoints[normalizedID]
	d.mu.RUnlock()
	if !exists {
		return "", fmt.Errorf("%w: node %q", ErrEndpointNotFound, normalizedID)
	}
	return address, nil
}

func (d *EndpointDirectory) Delete(id model.NodeID) {
	normalizedID := model.NodeID(strings.TrimSpace(string(id)))

	d.mu.Lock()
	delete(d.endpoints, normalizedID)
	delete(d.pending, normalizedID)
	d.mu.Unlock()
}
