package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

const DTMVersion = "v0.5.0"

var ErrInvalidPresence = errors.New("invalid mesh presence")

type Identity struct {
	MeshNamespace   string `json:"mesh_namespace"`
	ProtocolMajor   uint32 `json:"protocol_major"`
	ProtocolMinor   uint32 `json:"protocol_minor"`
	DTMVersion      string `json:"dtm_version"`
	NodeID          string `json:"node_id"`
	RuntimeInstance string `json:"runtime_instance_id"`
	ControlEndpoint string `json:"runtime_control_endpoint"`
}

func NewRuntimeInstanceID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate runtime instance ID: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func (identity Identity) Validate() error {
	if strings.TrimSpace(identity.MeshNamespace) == "" || identity.ProtocolMajor == 0 ||
		strings.TrimSpace(identity.DTMVersion) == "" || strings.TrimSpace(identity.NodeID) == "" ||
		strings.TrimSpace(identity.RuntimeInstance) == "" {
		return ErrInvalidPresence
	}
	host, portText, err := net.SplitHostPort(identity.ControlEndpoint)
	port, portErr := strconv.Atoi(portText)
	if err != nil || strings.TrimSpace(host) == "" || portErr != nil || port < 1 || port > 65535 {
		return ErrInvalidPresence
	}
	return nil
}

func Encode(identity Identity) ([]byte, error) {
	if err := identity.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(identity)
}

func Decode(value []byte) (Identity, error) {
	var identity Identity
	if err := json.Unmarshal(value, &identity); err != nil {
		return Identity{}, fmt.Errorf("%w: %v", ErrInvalidPresence, err)
	}
	if err := identity.Validate(); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

func (identity Identity) SameSession(other Identity) bool {
	return identity.NodeID == other.NodeID && identity.RuntimeInstance == other.RuntimeInstance
}
