package identity

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

var ErrInvalidID = errors.New("invalid kernel object id")

type ResourceID string
type CapabilityDeclarationID string
type CapabilityInstanceID string
type CapabilityHandleID string
type ExecutionContextID string
type ExecutionAllocationID string
type EventID string

func NewResourceID() (ResourceID, error) {
	value, err := newID("resource")
	return ResourceID(value), err
}

func NewCapabilityDeclarationID() (CapabilityDeclarationID, error) {
	value, err := newID("capability-declaration")
	return CapabilityDeclarationID(value), err
}

func NewCapabilityInstanceID() (CapabilityInstanceID, error) {
	value, err := newID("capability-instance")
	return CapabilityInstanceID(value), err
}

func NewCapabilityHandleID() (CapabilityHandleID, error) {
	value, err := newID("capability-handle")
	return CapabilityHandleID(value), err
}

func NewExecutionContextID() (ExecutionContextID, error) {
	value, err := newID("execution-context")
	return ExecutionContextID(value), err
}

func NewExecutionAllocationID() (ExecutionAllocationID, error) {
	value, err := newID("execution-allocation")
	return ExecutionAllocationID(value), err
}

func NewEventID() (EventID, error) {
	value, err := newID("event")
	return EventID(value), err
}

func (id ResourceID) String() string               { return string(id) }
func (id CapabilityDeclarationID) String() string  { return string(id) }
func (id CapabilityInstanceID) String() string     { return string(id) }
func (id CapabilityHandleID) String() string       { return string(id) }
func (id ExecutionContextID) String() string       { return string(id) }
func (id ExecutionAllocationID) String() string    { return string(id) }
func (id EventID) String() string                  { return string(id) }
func (id ResourceID) Validate() error              { return validate(string(id)) }
func (id CapabilityDeclarationID) Validate() error { return validate(string(id)) }
func (id CapabilityInstanceID) Validate() error    { return validate(string(id)) }
func (id CapabilityHandleID) Validate() error      { return validate(string(id)) }
func (id ExecutionContextID) Validate() error      { return validate(string(id)) }
func (id ExecutionAllocationID) Validate() error   { return validate(string(id)) }
func (id EventID) Validate() error                 { return validate(string(id)) }

func newID(prefix string) (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate %s id: %w", prefix, err)
	}
	return prefix + "-" + hex.EncodeToString(bytes[:]), nil
}

func validate(value string) error {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || strings.ContainsRune(value, '\x00') {
		return ErrInvalidID
	}
	return nil
}
