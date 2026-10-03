package main

import (
	"strings"
	"testing"

	"dtm/internal/config"
	"dtm/internal/model"
	"dtm/internal/node"
)

func TestComposeBuildsRegisteredExecutionAgent(t *testing.T) {
	cfg := config.Agent{Node: config.Node{ID: "agent-1", Capabilities: []string{"temperature_sensor", "cooling_control"}}}

	deps, err := compose(cfg)
	if err != nil {
		t.Fatalf("compose() error = %v", err)
	}
	defer deps.ExecutionRepository.Close()
	if deps.Node.ID() != "agent-1" || deps.Node.Status() != node.StatusActive {
		t.Fatalf("node = %q/%q, want agent-1/active", deps.Node.ID(), deps.Node.Status())
	}
	got := deps.Node.Capabilities()
	if len(got) != 2 || got[0] != model.Capability("temperature_sensor") || got[1] != model.Capability("cooling_control") {
		t.Fatalf("node capabilities = %v", got)
	}
	if deps.Router == nil ||
		deps.ExecutionServer == nil ||
		deps.ExecutionRepository == nil ||
		deps.Server == nil {
		t.Fatalf("dependencies must expose router, execution server and grpc server: %#v", deps)
	}
}

func TestComposeRejectsUnknownCapability(t *testing.T) {
	_, err := compose(config.Agent{Node: config.Node{ID: "agent-1", Capabilities: []string{"not_implemented"}}})
	if err == nil || !strings.Contains(err.Error(), "unsupported agent capability") {
		t.Fatalf("compose() error = %v, want stable unsupported capability error", err)
	}
}
