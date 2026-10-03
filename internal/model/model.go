package model

import (
	"errors"
	"strings"
)

type TaskID string

type NodeID string

type StepID string
type ExecutionID string

type Capability string

var ErrInvalidCapability = errors.New("invalid capability")

func NewCapability(name string) (Capability, error) {
	if strings.TrimSpace(name) == "" {
		return "", ErrInvalidCapability
	}

	return Capability(name), nil
}

func (capability Capability) String() string {
	return string(capability)
}
