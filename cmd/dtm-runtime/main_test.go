package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dtm/internal/mesh/coordinator"
	"dtm/internal/runtimehost"
)

func TestRuntimeRejectsStaticProfile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("node:\n  id: node-a\n  capabilities: [temperature_sensor]\n  advertise_address: 127.0.0.1:50061\ncore:\n  address: 127.0.0.1:50051\nserver:\n  listen_address: 127.0.0.1:50061\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	if code := run(context.Background(), []string{"-config", path}, &bytes.Buffer{}, &stderr); code != 2 || !strings.Contains(stderr.String(), "runtime mode") {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
}

func TestFormatRuntimeStatusLabelsCoordinatorAndLocalFields(t *testing.T) {
	line := formatRuntimeStatus("node-b", "local-session-b", runtimehost.RuntimeStatus{
		Role: coordinator.RoleSnapshot{
			Selection:          coordinator.Selection{NodeID: "node-a", RuntimeInstanceID: "coordinator-session-a", ControlEndpoint: "127.0.0.1:46020"},
			HasCoordinator:     true,
			LocalIsCoordinator: false,
		},
		CoreReady:       false,
		AuthorityReady:  false,
		IngressReady:    false,
		AuthorityStatus: runtimehost.AuthorityStatusReady,
	}, false, 0, 0)
	fields := make(map[string]string)
	for _, field := range strings.Fields(line) {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) == 2 {
			fields[parts[0]] = parts[1]
		}
	}
	if fields["local_runtime_instance_id"] != "local-session-b" {
		t.Fatalf("local runtime field = %q, line=%s", fields["local_runtime_instance_id"], line)
	}
	if fields["coordinator_runtime_instance_id"] != "coordinator-session-a" || fields["coordinator_control_endpoint"] != "127.0.0.1:46020" {
		t.Fatalf("coordinator fields missing or ambiguous: %s", line)
	}
	if _, exists := fields["runtime_instance_id"]; exists {
		t.Fatalf("ambiguous runtime_instance_id field remains: %s", line)
	}
	if fields["local_authority_binding"] != runtimehost.AuthorityStatusReady || fields["authority_ready"] != "false" {
		t.Fatalf("local/aggregate authority fields = %q/%q, line=%s", fields["local_authority_binding"], fields["authority_ready"], line)
	}
}
