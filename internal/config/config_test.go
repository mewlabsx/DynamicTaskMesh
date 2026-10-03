package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadCoreLoadsValidYAML(t *testing.T) {
	path := writeConfig(t, "server:\n  address: :8080\n")

	got, err := LoadCore(path)

	if err != nil {
		t.Fatalf("LoadCore() error = %v", err)
	}
	if got.Server.Address != ":8080" {
		t.Fatalf("LoadCore() = %+v, want configured server", got)
	}
	if got.NATS.URL != "" {
		t.Fatalf("LoadCore() NATS URL = %q, want optional NATS to remain empty", got.NATS.URL)
	}
	if got.Lease.TTL.Duration != DefaultLeaseTTL ||
		got.Lease.SweepInterval.Duration != DefaultLeaseSweepInterval {
		t.Fatalf("LoadCore() lease = %+v, want defaults", got.Lease)
	}
	if got.Storage.Path != filepath.Join(filepath.Dir(path), "dtm.db") {
		t.Fatalf("LoadCore() storage path = %q, want path relative to config", got.Storage.Path)
	}
	if got.Storage.Driver != DefaultStorageDriver ||
		got.Storage.JournalMode != DefaultJournalMode ||
		got.Storage.Synchronous != DefaultSynchronous ||
		got.Storage.BusyTimeout.Duration != DefaultBusyTimeout ||
		!got.Storage.AutoMigrationEnabled() {
		t.Fatalf("LoadCore() storage defaults = %+v", got.Storage)
	}
	if got.Retry.MaxAttempts != DefaultRetryMaxAttempts ||
		got.Retry.Backoff.Duration != DefaultRetryBackoff {
		t.Fatalf("LoadCore() retry = %+v, want defaults", got.Retry)
	}
}

func TestLoadCoreResolvesConfiguredStorageRelativeToConfig(t *testing.T) {
	path := writeConfig(t, "server:\n  address: :8080\nstorage:\n  path: data/state.db\n")
	got, err := LoadCore(path)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(filepath.Dir(path), "data", "state.db")
	if got.Storage.Path != want {
		t.Fatalf("LoadCore() storage path = %q, want %q", got.Storage.Path, want)
	}
}

func TestLoadCoreLoadsConfiguredSQLiteSettings(t *testing.T) {
	path := writeConfig(t, `server:
  address: :8080
storage:
  driver: sqlite
  path: data/state.db
  journal_mode: delete
  synchronous: full
  busy_timeout: 750ms
  auto_migrate: false
`)
	got, err := LoadCore(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Storage.Driver != "sqlite" || got.Storage.JournalMode != "DELETE" ||
		got.Storage.Synchronous != "FULL" || got.Storage.BusyTimeout.Duration != 750*time.Millisecond ||
		got.Storage.AutoMigrationEnabled() {
		t.Fatalf("LoadCore() storage = %+v", got.Storage)
	}
}

func TestLoadCoreRejectsUnsupportedSQLiteSettings(t *testing.T) {
	for name, storage := range map[string]string{
		"driver":       "driver: postgres",
		"journal_mode": "journal_mode: unsupported",
		"synchronous":  "synchronous: sometimes",
	} {
		t.Run(name, func(t *testing.T) {
			path := writeConfig(t, "server:\n  address: :8080\nstorage:\n  path: state.db\n  "+storage+"\n")
			if _, err := LoadCore(path); err == nil {
				t.Fatal("LoadCore() error = nil, want storage validation failure")
			}
		})
	}
}

func TestLoadAgentLoadsValidYAML(t *testing.T) {
	path := writeConfig(t, `node:
  id: " agent-1 "
  capabilities:
    - " temperature_sensor "
    - cooling_control
  advertise_address: " agent.internal:50061 "
core:
  address: " localhost:8080 "
server:
  listen_address: " 0.0.0.0:50061 "
`)

	got, err := LoadAgent(path)

	if err != nil {
		t.Fatalf("LoadAgent() error = %v", err)
	}
	if got.Node.ID != "agent-1" {
		t.Fatalf("LoadAgent() node ID = %q, want trimmed ID", got.Node.ID)
	}
	if got.Core.Address != "localhost:8080" {
		t.Fatalf("LoadAgent() core address = %q, want trimmed address", got.Core.Address)
	}
	if got.Server.ListenAddress != "0.0.0.0:50061" {
		t.Fatalf("LoadAgent() listen address = %q, want trimmed address", got.Server.ListenAddress)
	}
	if got.Node.AdvertiseAddress != "agent.internal:50061" {
		t.Fatalf("LoadAgent() advertise address = %q, want trimmed address", got.Node.AdvertiseAddress)
	}
	if got.Server.Address != got.Server.ListenAddress ||
		got.Server.AdvertisedAddress != got.Node.AdvertiseAddress {
		t.Fatalf("legacy aliases not normalized: %+v", got)
	}
	wantCapabilities := []string{"temperature_sensor", "cooling_control"}
	if len(got.Node.Capabilities) != len(wantCapabilities) {
		t.Fatalf("LoadAgent() capabilities = %v, want %v", got.Node.Capabilities, wantCapabilities)
	}
	for i, want := range wantCapabilities {
		if got.Node.Capabilities[i] != want {
			t.Fatalf("LoadAgent() capability[%d] = %q, want %q", i, got.Node.Capabilities[i], want)
		}
	}
	if cap(got.Node.Capabilities) != len(got.Node.Capabilities) {
		t.Fatalf("LoadAgent() capability capacity = %d, want %d to avoid retaining unused backing storage", cap(got.Node.Capabilities), len(got.Node.Capabilities))
	}
	if got.NATS.URL != "" {
		t.Fatalf("LoadAgent() NATS URL = %q, want optional NATS to remain empty", got.NATS.URL)
	}
	if got.Heartbeat.Interval.Duration != DefaultHeartbeatInterval {
		t.Fatalf("LoadAgent() heartbeat = %v, want default", got.Heartbeat.Interval.Duration)
	}
	if got.Storage.Path != filepath.Join(filepath.Dir(path), "agent.db") {
		t.Fatalf("LoadAgent() storage path = %q, want config-relative default", got.Storage.Path)
	}
}

func TestLoadAgentResolvesConfiguredStorageRelativeToConfig(t *testing.T) {
	path := writeConfig(t, validAgentYAML(
		"agent-1",
		"temperature_sensor",
		"127.0.0.1:50061",
		"127.0.0.1:50061",
	)+"storage:\n  path: data/executions.db\n")
	agent, err := LoadAgent(path)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(filepath.Dir(path), "data", "executions.db")
	if agent.Storage.Path != want {
		t.Fatalf("storage path = %q, want %q", agent.Storage.Path, want)
	}
}

func TestRepositoryAgentConfigsUseDistinctPersistentStorage(t *testing.T) {
	paths := []string{
		filepath.Join("..", "..", "configs", "agent.yaml"),
		filepath.Join("..", "..", "configs", "demo", "sensor-agent.yaml"),
		filepath.Join("..", "..", "configs", "demo", "cooling-agent-001.yaml"),
		filepath.Join("..", "..", "configs", "demo", "cooling-agent-002.yaml"),
		filepath.Join("..", "..", "configs", "lab", "sensor-agent.yaml"),
		filepath.Join("..", "..", "configs", "lab", "cooling-agent.yaml"),
	}
	seen := make(map[string]string, len(paths))
	for _, path := range paths {
		agent, err := LoadAgent(path)
		if err != nil {
			t.Fatalf("LoadAgent(%q) error = %v", path, err)
		}
		if previousNode, exists := seen[agent.Storage.Path]; exists &&
			previousNode != agent.Node.ID {
			t.Fatalf(
				"nodes %q and %q share storage %q",
				previousNode,
				agent.Node.ID,
				agent.Storage.Path,
			)
		}
		seen[agent.Storage.Path] = agent.Node.ID
	}
}

func TestLoadersParseHeartbeatAndLeaseDurations(t *testing.T) {
	corePath := writeConfig(t, "server:\n  address: :8080\nlease:\n  ttl: 6s\n  sweep_interval: 250ms\n")
	core, err := LoadCore(corePath)
	if err != nil {
		t.Fatal(err)
	}
	if core.Lease.TTL.Duration != 6*time.Second || core.Lease.SweepInterval.Duration != 250*time.Millisecond {
		t.Fatalf("core lease = %+v", core.Lease)
	}

	agentPath := writeConfig(t, validAgentYAML("agent-1", "temperature_sensor", "127.0.0.1:50061", "127.0.0.1:50061")+
		"heartbeat:\n  interval: 500ms\n")
	agent, err := LoadAgent(agentPath)
	if err != nil {
		t.Fatal(err)
	}
	if agent.Heartbeat.Interval.Duration != 500*time.Millisecond {
		t.Fatalf("agent heartbeat = %v", agent.Heartbeat.Interval.Duration)
	}
}

func TestLoadCoreParsesRetryPolicy(t *testing.T) {
	core, err := LoadCore(writeConfig(t, "server:\n  address: :8080\nretry:\n  max_attempts: 5\n  backoff: 250ms\n"))
	if err != nil {
		t.Fatal(err)
	}
	if core.Retry.MaxAttempts != 5 || core.Retry.Backoff.Duration != 250*time.Millisecond {
		t.Fatalf("core retry = %+v", core.Retry)
	}
}

func TestLoadCoreRejectsInvalidRetryPolicy(t *testing.T) {
	for _, yaml := range []string{
		"server:\n  address: :8080\nretry:\n  max_attempts: 0\n",
		"server:\n  address: :8080\nretry:\n  backoff: 0s\n",
	} {
		if _, err := LoadCore(writeConfig(t, yaml)); err == nil {
			t.Fatalf("LoadCore() error = nil for %q", yaml)
		}
	}
}

func TestLoadersRejectNonPositiveDurations(t *testing.T) {
	if _, err := LoadCore(writeConfig(t, "server:\n  address: :8080\nlease:\n  ttl: 0s\n")); err == nil {
		t.Fatal("LoadCore() error = nil, want invalid TTL")
	}
	if _, err := LoadAgent(writeConfig(t, validAgentYAML("agent-1", "temperature_sensor", "127.0.0.1:50061", "127.0.0.1:50061")+
		"heartbeat:\n  interval: -1s\n")); err == nil {
		t.Fatal("LoadAgent() error = nil, want invalid heartbeat interval")
	}
}

func TestLoadersRejectUnreadableFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing.yaml")
	for name, load := range map[string]func(string) error{
		"core": func(path string) error {
			_, err := LoadCore(path)
			return err
		},
		"agent": func(path string) error {
			_, err := LoadAgent(path)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := load(path); err == nil || !strings.Contains(err.Error(), "read") {
				t.Fatalf("load() error = %v, want wrapped read error", err)
			}
		})
	}
}

func TestLoadersRejectMalformedYAML(t *testing.T) {
	path := writeConfig(t, "server: [\n")
	for name, load := range map[string]func(string) error{
		"core": func(path string) error {
			_, err := LoadCore(path)
			return err
		},
		"agent": func(path string) error {
			_, err := LoadAgent(path)
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			if err := load(path); err == nil || !strings.Contains(err.Error(), "parse") {
				t.Fatalf("load() error = %v, want wrapped parse error", err)
			}
		})
	}
}

func TestLoadCoreRejectsMissingRequiredFields(t *testing.T) {
	_, err := LoadCore(writeConfig(t, "server:\n  address: \"  \"\n"))
	if err == nil || !strings.Contains(err.Error(), "server.address") {
		t.Fatalf("LoadCore() error = %v, want validation error containing server.address", err)
	}
}

func TestLoadCoreRejectsBlankStoragePath(t *testing.T) {
	_, err := LoadCore(writeConfig(t, "server:\n  address: :8080\nstorage:\n  path: \"  \"\n"))
	if err == nil || !strings.Contains(err.Error(), "storage.path") {
		t.Fatalf("LoadCore() error = %v, want validation error containing storage.path", err)
	}
}

func TestLoadAgentRejectsMissingRequiredFields(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "node ID",
			yaml: validAgentYAML("  ", "temperature_sensor", "127.0.0.1:50061", "127.0.0.1:50061"),
			want: "node.id",
		},
		{
			name: "capabilities",
			yaml: validAgentYAML("agent-1", "", "127.0.0.1:50061", "127.0.0.1:50061"),
			want: "node.capabilities",
		},
		{
			name: "core address",
			yaml: `node:
  id: agent-1
  capabilities: [temperature_sensor]
core:
  address: " "
server:
  listen_address: 127.0.0.1:50061
`,
			want: "core.address",
		},
		{
			name: "listen address",
			yaml: validAgentYAML("agent-1", "temperature_sensor", "  ", "127.0.0.1:50061"),
			want: "server.listen_address",
		},
		{
			name: "advertise address",
			yaml: validAgentYAML("agent-1", "temperature_sensor", "127.0.0.1:50061", "  "),
			want: "node.advertise_address",
		},
		{
			name: "storage path",
			yaml: validAgentYAML("agent-1", "temperature_sensor", "127.0.0.1:50061", "127.0.0.1:50061") +
				"storage:\n  path: \"  \"\n",
			want: "storage.path",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadAgent(writeConfig(t, test.yaml))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadAgent() error = %v, want validation error containing %q", err, test.want)
			}
		})
	}
}

func TestLoadAgentRuntimeModeDoesNotRequireCoreAddress(t *testing.T) {
	path := writeConfig(t, `mode: runtime
node:
  id: runtime-a
  capabilities: [temperature_sensor]
  advertise_address: 127.0.0.1:50061
server:
  listen_address: 127.0.0.1:50061
mesh:
  namespace: test-mesh
  control:
    listen: 127.0.0.1:50061
    advertise: 127.0.0.1:50061
`)
	agent, err := LoadAgent(path)
	if err != nil {
		t.Fatal(err)
	}
	if agent.Mode != ModeRuntime || agent.Core.Address != "" {
		t.Fatalf("runtime config = mode %q core %q", agent.Mode, agent.Core.Address)
	}
	if agent.Mesh.Discovery.MulticastGroup != DefaultMulticastGroup || agent.Mesh.Discovery.Port != DefaultMulticastPort {
		t.Fatalf("runtime discovery defaults = %+v", agent.Mesh.Discovery)
	}
	if agent.Mesh.Membership.SuspectAfter.Duration != DefaultMembershipSuspectAfter || agent.Mesh.Membership.ExpireAfter.Duration != DefaultMembershipExpireAfter {
		t.Fatalf("runtime membership defaults = %+v", agent.Mesh.Membership)
	}
}

func TestLoadAgentStaticModeRemainsValidWithoutMesh(t *testing.T) {
	agent, err := LoadAgent(writeConfig(t, validAgentYAML("agent-1", "temperature_sensor", "127.0.0.1:50061", "127.0.0.1:50061")))
	if err != nil || agent.Mode != ModeStatic {
		t.Fatalf("LoadAgent() = %+v, %v", agent, err)
	}
}

func TestLoadAgentRuntimeRejectsInvalidMesh(t *testing.T) {
	tests := []struct{ name, mesh, want string }{
		{"namespace", "namespace: ' '", "mesh.namespace"},
		{"group", "namespace: mesh-a\n  discovery:\n    multicast_group: 127.0.0.1", "multicast_group"},
		{"port", "namespace: mesh-a\n  discovery:\n    port: 80", "mesh.discovery.port"},
		{"control", "namespace: mesh-a", "mesh.control.listen"},
		{"membership suspect", "namespace: mesh-a\n  membership:\n    suspect_after: 0s", "duration must be positive"},
		{"membership refresh cadence", "namespace: mesh-a\n  discovery:\n    interval: 4s\n  membership:\n    suspect_after: 6s\n    expire_after: 9s", "confirmed refresh bound"},
		{"membership refresh boundary", "namespace: mesh-a\n  discovery:\n    interval: 1s\n  membership:\n    suspect_after: 5s\n    expire_after: 9s", "confirmed refresh bound"},
		{"membership ordering", "namespace: mesh-a\n  membership:\n    suspect_after: 7s\n    expire_after: 6s", "expire_after"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			yaml := "mode: runtime\nnode:\n  id: runtime-a\n  capabilities: [temperature_sensor]\n  advertise_address: 127.0.0.1:50061\nserver:\n  listen_address: 127.0.0.1:50061\nmesh:\n  " + test.mesh + "\n"
			_, err := LoadAgent(writeConfig(t, yaml))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %s", err, test.want)
			}
		})
	}
}

func TestLoadAgentRuntimeAcceptsMembershipTimingAboveConfirmedRefreshBound(t *testing.T) {
	yaml := "mode: runtime\nnode:\n  id: runtime-a\n  capabilities: [temperature_sensor]\n  advertise_address: 127.0.0.1:50061\nserver:\n  listen_address: 127.0.0.1:50061\nmesh:\n  namespace: mesh-a\n  discovery:\n    interval: 1s\n  membership:\n    suspect_after: 5001ms\n    expire_after: 9s\n  control:\n    listen: 127.0.0.1:50061\n    advertise: 127.0.0.1:50061\n"
	if _, err := LoadAgent(writeConfig(t, yaml)); err != nil {
		t.Fatal(err)
	}
}

func TestMembershipTimingValidation(t *testing.T) {
	base := Mesh{
		Namespace: "mesh-a", ProtocolMajor: 1,
		Discovery: MeshDiscovery{MulticastGroup: DefaultMulticastGroup, Port: DefaultMulticastPort, Interval: Duration{Duration: time.Second}},
		Control:   MeshControl{Listen: "127.0.0.1:50061", Advertise: "127.0.0.1:50061"},
	}
	tests := []struct {
		name                      string
		interval, suspect, expire time.Duration
		wantErr                   bool
	}{
		{name: "TimingDefaultsValid", interval: time.Second, suspect: DefaultMembershipSuspectAfter, expire: DefaultMembershipExpireAfter},
		{name: "TimingRejectsSuspectBeforeConfirmedRefreshBound", interval: time.Second, suspect: 4999 * time.Millisecond, expire: 9 * time.Second, wantErr: true},
		{name: "TimingRejectsEqualBoundary", interval: time.Second, suspect: 5 * time.Second, expire: 9 * time.Second, wantErr: true},
		{name: "TimingAcceptsSafeMargin", interval: time.Second, suspect: 5001 * time.Millisecond, expire: 9 * time.Second},
		{name: "TimingRejectsExpireNotGreaterThanSuspect", interval: time.Second, suspect: 6 * time.Second, expire: 6 * time.Second, wantErr: true},
		{name: "TimingTracksDiscoveryIntervalChange", interval: 4 * time.Second, suspect: DefaultMembershipSuspectAfter, expire: DefaultMembershipExpireAfter, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mesh := base
			mesh.Discovery.Interval.Duration = test.interval
			mesh.Membership = MeshMembership{SuspectAfter: Duration{Duration: test.suspect}, ExpireAfter: Duration{Duration: test.expire}}
			err := validateMesh(mesh)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateMesh() = %v, wantErr=%v", err, test.wantErr)
			}
		})
	}
}

func TestLoadAgentDefaultsToStaticModeAndRequiresCoreAddress(t *testing.T) {
	path := writeConfig(t, `node:
  id: runtime-a
  capabilities: [temperature_sensor]
  advertise_address: 127.0.0.1:50061
server:
  listen_address: 127.0.0.1:50061
`)
	if _, err := LoadAgent(path); err == nil || !strings.Contains(err.Error(), "core.address") {
		t.Fatalf("LoadAgent() error = %v, want static core.address requirement", err)
	}
}

func TestLoadAgentRejectsUnknownCompositionMode(t *testing.T) {
	path := writeConfig(t, "mode: mesh-magic\n"+validAgentYAML("runtime-a", "temperature_sensor", "127.0.0.1:50061", "127.0.0.1:50061"))
	if _, err := LoadAgent(path); err == nil || !strings.Contains(err.Error(), "mode") {
		t.Fatalf("LoadAgent() error = %v, want mode validation", err)
	}
}

func TestLoadAgentRejectsInvalidCapabilities(t *testing.T) {
	tests := []struct {
		name         string
		capabilities string
		want         string
	}{
		{name: "missing list", capabilities: "", want: "node.capabilities"},
		{name: "blank item", capabilities: "temperature_sensor\n    - \"  \"", want: "node.capabilities"},
		{name: "duplicate", capabilities: "temperature_sensor\n    - \" temperature_sensor \"", want: "duplicate"},
		{name: "unknown", capabilities: "not_implemented", want: "unknown capability"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := LoadAgent(writeConfig(t, validAgentYAML(
				"agent-1",
				test.capabilities,
				"127.0.0.1:50061",
				"127.0.0.1:50061",
			)))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LoadAgent() error = %v, want validation error containing %q", err, test.want)
			}
		})
	}
}

func TestLoadAgentAcceptsLegacyEndpointFields(t *testing.T) {
	path := writeConfig(t, `node:
  id: agent-1
  capabilities: [temperature_sensor]
core:
  address: 127.0.0.1:50051
server:
  address: 0.0.0.0:50061
  advertised_address: 192.168.100.11:50061
`)
	agent, err := LoadAgent(path)
	if err != nil {
		t.Fatal(err)
	}
	if agent.Server.ListenAddress != "0.0.0.0:50061" ||
		agent.Node.AdvertiseAddress != "192.168.100.11:50061" {
		t.Fatalf("legacy endpoints = %+v", agent)
	}
}

func TestLoadAgentRejectsConflictingEndpointAliases(t *testing.T) {
	path := writeConfig(t, `node:
  id: agent-1
  capabilities: [temperature_sensor]
  advertise_address: 192.168.100.11:50061
core:
  address: 127.0.0.1:50051
server:
  listen_address: 0.0.0.0:50061
  address: 127.0.0.1:50061
  advertised_address: 127.0.0.1:50061
`)
	if _, err := LoadAgent(path); err == nil || !strings.Contains(err.Error(), "conflicts") {
		t.Fatalf("LoadAgent() error = %v, want conflict", err)
	}
}

func validAgentYAML(id, capabilities, serverAddress, advertisedAddress string) string {
	capabilityYAML := "  capabilities: []\n"
	if capabilities != "" {
		capabilityYAML = "  capabilities:\n    - " + capabilities + "\n"
	}
	return "node:\n" +
		"  id: \"" + id + "\"\n" +
		capabilityYAML +
		"  advertise_address: \"" + advertisedAddress + "\"\n" +
		"core:\n" +
		"  address: localhost:8080\n" +
		"server:\n" +
		"  listen_address: \"" + serverAddress + "\"\n"
}

func writeConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
