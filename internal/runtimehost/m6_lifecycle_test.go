package runtimehost

import (
	"context"
	"net"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/authoritybinding"
	"dtm/internal/config"
	"dtm/internal/mesh/discovery"
	"dtm/internal/mesh/handshake"
	"dtm/internal/mesh/membership"
	"dtm/internal/mesh/protocol"
	"dtm/internal/model"
	"dtm/internal/planner"
)

type blockedAuthoritySession struct {
	started atomic.Bool
	closed  atomic.Bool
}

func (session *blockedAuthoritySession) Start(context.Context, string) error {
	session.started.Store(true)
	return nil
}

func (session *blockedAuthoritySession) Ready() bool          { return false }
func (session *blockedAuthoritySession) Errors() <-chan error { return nil }
func (session *blockedAuthoritySession) Close() error {
	session.closed.Store(true)
	return nil
}

var _ AuthoritySession = (*blockedAuthoritySession)(nil)

func TestM6RoleLossClosesIngressAndRegainBuildsFreshCore(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := listener.Addr().String()
	execution, err := NewResourceExecutionCapability(config.Agent{
		Mode:    config.ModeRuntime,
		Node:    config.Node{ID: "node-a", Capabilities: []string{"temperature_sensor"}},
		Storage: config.Storage{Path: filepath.Join(t.TempDir(), "agent.db")},
	}, testHandler{capability: "temperature_sensor"})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	network := discovery.NewMemoryNetwork()
	identity := protocol.Identity{
		MeshNamespace: "m6-lifecycle", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion,
		NodeID: "node-a", RuntimeInstance: "session-a", ControlEndpoint: endpoint,
	}
	var nowNanos atomic.Int64
	nowNanos.Store(time.Now().UnixNano())
	currentTime := func() time.Time { return time.Unix(0, nowNanos.Load()) }
	var host *RuntimeHost
	var factoryCalls atomic.Int32
	var authorityCalls atomic.Int32
	var authorityCallsDuringConflict atomic.Int32
	host, err = New(Options{
		NodeID: "node-a", Execution: execution, ExecutionAddress: endpoint,
		AuthorityFactory: func() (AuthoritySession, error) {
			if host != nil && host.identityConflict() {
				authorityCallsDuringConflict.Add(1)
			}
			authorityCalls.Add(1)
			return authoritybinding.New(authoritybinding.Options{
				NodeID: "node-a", Capabilities: []string{"temperature_sensor"}, ExecutionAddress: endpoint,
				HeartbeatInterval: 100 * time.Millisecond,
			})
		},
		CoreFactory: func(ctx context.Context) (*CoreCapability, error) {
			factoryCalls.Add(1)
			return NewCoreCapability(ctx, coreConfig(filepath.Join(t.TempDir(), "core.db")), CoreOptions{Readiness: func() bool {
				return host != nil && host.IngressReady()
			}})
		},
		Listen: func(network, address string) (net.Listener, error) {
			if network == "tcp" && address == endpoint {
				return listener, nil
			}
			return net.Listen(network, address)
		},
		Mesh: &MeshOptions{
			Identity: identity, Transport: network.NewTransport(), AnnouncementInterval: 20 * time.Millisecond,
			HandshakeTimeout: 250 * time.Millisecond, Now: currentTime,
			MembershipTiming: membership.Timing{SuspectAfter: time.Second, ExpireAfter: 2 * time.Second},
		},
	})
	if err != nil {
		_ = listener.Close()
		_ = execution.Close()
		t.Fatal(err)
	}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	waitForRuntimeHost(t, func() bool { return host.IngressReady() })
	firstCore := host.Core()
	if firstCore == nil || factoryCalls.Load() != 1 || authorityCalls.Load() != 1 {
		t.Fatalf("initial Core state=%v factory_calls=%d authority_calls=%d", firstCore != nil, factoryCalls.Load(), authorityCalls.Load())
	}
	stableAuthorityCalls := authorityCalls.Load()
	stableAuthoritySamples := 0
	waitForRuntimeHost(t, func() bool {
		calls := authorityCalls.Load()
		if calls != stableAuthorityCalls {
			stableAuthorityCalls = calls
			stableAuthoritySamples = 0
			return false
		}
		stableAuthoritySamples++
		return stableAuthoritySamples >= 20 && host.IngressReady()
	})

	duplicate := identity
	duplicate.RuntimeInstance = "session-duplicate"
	duplicate.ControlEndpoint = "127.0.0.1:49999"
	if err := host.Membership().Observe(handshake.Result{Identity: duplicate}, currentTime()); err != nil {
		t.Fatal(err)
	}
	host.refreshCoordinatorRole()
	host.notifyRoleController()
	waitForRuntimeHost(t, func() bool { return !host.IngressReady() && host.Core() == nil })
	waitForRuntimeHost(t, func() bool {
		return host.StatusSnapshot().AuthorityStatus == AuthorityStatusIdentityConflict
	})
	if status := host.StatusSnapshot(); status.AuthorityStatus != AuthorityStatusIdentityConflict {
		t.Fatalf("identity conflict status = %+v", status)
	}
	if authorityCallsDuringConflict.Load() != 0 {
		t.Fatalf("identity conflict started a new authority session: calls=%d", authorityCallsDuringConflict.Load())
	}

	nowNanos.Store(currentTime().Add(3 * time.Second).UnixNano())
	if err := host.Membership().Tick(currentTime()); err != nil {
		t.Fatal(err)
	}
	host.refreshCoordinatorRole()
	host.notifyRoleController()
	waitForRuntimeHost(t, func() bool { return host.IngressReady() && host.Core() != nil })
	if host.Core() == firstCore || factoryCalls.Load() < 2 || authorityCalls.Load() < 2 {
		t.Fatalf("regained Core/authority was not freshly constructed: first=%p current=%p core_calls=%d authority_calls=%d", firstCore, host.Core(), factoryCalls.Load(), authorityCalls.Load())
	}
}

func TestM6PreAuthorityMeshViewCannotFeedCandidateOrMapper(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := listener.Addr().String()
	execution, err := NewResourceExecutionCapability(config.Agent{
		Mode:    config.ModeRuntime,
		Node:    config.Node{ID: "node-a", Capabilities: []string{"temperature_sensor"}},
		Storage: config.Storage{Path: filepath.Join(t.TempDir(), "agent.db")},
	}, testHandler{capability: "temperature_sensor"})
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	identity := protocol.Identity{
		MeshNamespace: "m6-pre-authority", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion,
		NodeID: "node-a", RuntimeInstance: "session-a", ControlEndpoint: endpoint,
	}
	blocked := &blockedAuthoritySession{}
	var host *RuntimeHost
	host, err = New(Options{
		NodeID: "node-a", Execution: execution, ExecutionAddress: endpoint,
		AuthorityFactory: func() (AuthoritySession, error) { return blocked, nil },
		CoreFactory: func(ctx context.Context) (*CoreCapability, error) {
			// This test covers authority gating, not persistence. An in-memory Core
			// also avoids an unrelated Windows SQLite cleanup race under -race.
			return NewCoreCapability(ctx, coreConfig(":memory:"), CoreOptions{Readiness: func() bool {
				return host != nil && host.IngressReady()
			}})
		},
		Listen: func(network, address string) (net.Listener, error) {
			if network == "tcp" && address == endpoint {
				return listener, nil
			}
			return net.Listen(network, address)
		},
		Mesh: &MeshOptions{
			Identity: identity, Transport: discovery.NewMemoryNetwork().NewTransport(),
			AnnouncementInterval: 20 * time.Millisecond, HandshakeTimeout: 250 * time.Millisecond,
			MembershipTiming: membership.Timing{SuspectAfter: time.Second, ExpireAfter: 2 * time.Second},
		},
	})
	if err != nil {
		_ = listener.Close()
		_ = execution.Close()
		t.Fatal(err)
	}
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	defer host.Close()
	waitForRuntimeHost(t, func() bool { return host.CoreReady() && blocked.started.Load() })
	if host.AuthorityReady() || host.IngressReady() {
		t.Fatalf("pre-authority readiness unexpectedly open: %+v", host.StatusSnapshot())
	}
	if status := host.StatusSnapshot(); status.AuthorityStatus != AuthorityStatusBinding {
		t.Fatalf("pre-authority local authority status = %+v", status)
	}
	meshResources := host.ResourceViewSnapshot().ActiveResources
	if len(meshResources) != 1 || meshResources[0].Descriptor.Type != model.ResourceType("temperature_sensor") {
		t.Fatalf("mesh resource view = %+v", meshResources)
	}
	core := host.Core()
	if len(core.Resources.ListEligible()) != 0 {
		t.Fatalf("unbound resources became eligible: %+v", core.Resources.ListEligible())
	}
	_, err = core.Mapper.Map(planner.Plan{TaskID: "pre-authority", Steps: []planner.Step{{ID: "temperature-step", Capability: "temperature_sensor"}}})
	if err == nil {
		t.Fatal("mapper produced a ResourceRef before authority binding")
	}
	response, err := host.runtimeStatus(context.Background(), &dtmv1.GetRuntimeStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetReadiness() != dtmv1.RuntimeReadiness_RUNTIME_READINESS_AUTHORITY_NOT_READY {
		t.Fatalf("runtime status = %+v", response)
	}
}

func waitForRuntimeHost(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for RuntimeHost state")
}
