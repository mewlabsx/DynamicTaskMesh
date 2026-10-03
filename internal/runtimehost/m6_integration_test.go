package runtimehost

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/agent"
	"dtm/internal/authoritybinding"
	"dtm/internal/config"
	"dtm/internal/mapper"
	"dtm/internal/mesh/discovery"
	"dtm/internal/mesh/membership"
	"dtm/internal/mesh/protocol"
	"dtm/internal/model"
	"dtm/internal/planner"
)

func TestM6ThreeRuntimeAuthorityConvergence(t *testing.T) {
	network := discovery.NewMemoryNetwork()
	ids := []string{"node-a", "node-b", "node-c"}
	capabilities := [][]string{{"temperature_sensor"}, {"cooling_control"}, nil}
	testDirs := make([]string, 0, len(ids)*2)
	type runtime struct {
		host     *RuntimeHost
		endpoint string
	}
	runtimes := make([]runtime, 0, len(ids))
	for index, nodeID := range ids {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		endpoint := listener.Addr().String()
		agentDir := t.TempDir()
		testDirs = append(testDirs, agentDir)
		configured := config.Agent{Mode: config.ModeRuntime, Node: config.Node{ID: nodeID, Capabilities: capabilities[index], AdvertiseAddress: endpoint}, Storage: config.Storage{Path: filepath.Join(agentDir, nodeID+".db")}}
		handlers := make([]agent.Handler, 0, len(capabilities[index]))
		for _, capability := range capabilities[index] {
			handlers = append(handlers, testHandler{capability: model.Capability(capability)})
		}
		execution, err := NewResourceExecutionCapability(configured, handlers...)
		if err != nil {
			_ = listener.Close()
			t.Fatal(err)
		}
		identity := protocol.Identity{
			MeshNamespace: "m6-test", ProtocolMajor: 1, ProtocolMinor: 0,
			DTMVersion: protocol.DTMVersion, NodeID: nodeID, RuntimeInstance: "session-" + nodeID,
			ControlEndpoint: endpoint,
		}
		var host *RuntimeHost
		corePath := ":memory:"
		if nodeID == "node-a" {
			coreDir := t.TempDir()
			testDirs = append(testDirs, coreDir)
			corePath = filepath.Join(coreDir, nodeID+"-core.db")
		}
		coreFactory := func(coreCtx context.Context) (*CoreCapability, error) {
			return NewCoreCapability(coreCtx, coreConfig(corePath), CoreOptions{Readiness: func() bool { return host != nil && host.IngressReady() }})
		}
		authorityFactory := func() (AuthoritySession, error) {
			return authoritybinding.New(authoritybinding.Options{
				NodeID: nodeID, Capabilities: capabilities[index], ExecutionAddress: endpoint,
				HeartbeatInterval: 100 * time.Millisecond,
			})
		}
		host, err = New(Options{
			NodeID: nodeID, Execution: execution, ExecutionAddress: endpoint,
			CoreFactory: coreFactory, AuthorityFactory: authorityFactory,
			Listen: func(network, address string) (net.Listener, error) {
				if network == "tcp" && address == endpoint {
					return listener, nil
				}
				return net.Listen(network, address)
			},
			Mesh: &MeshOptions{
				Identity: identity, Transport: network.NewTransport(), AnnouncementInterval: 20 * time.Millisecond,
				HandshakeTimeout: 250 * time.Millisecond,
				MembershipTiming: membership.Timing{SuspectAfter: time.Second, ExpireAfter: 2 * time.Second},
			},
		})
		if err != nil {
			_ = listener.Close()
			_ = execution.Close()
			t.Fatal(err)
		}
		runtimes = append(runtimes, runtime{host: host, endpoint: endpoint})
	}
	for _, item := range runtimes {
		if err := item.host.Start(); err != nil {
			for _, started := range runtimes {
				_ = started.host.Close()
			}
			t.Fatal(err)
		}
	}
	// Register host shutdown after all per-runtime TempDir cleanups. Testing
	// executes cleanups in LIFO order, so every Core/Agent handle is released
	// before its temporary database directory is removed on Windows.
	t.Cleanup(func() {
		for _, item := range runtimes {
			_ = item.host.Close()
		}
		cleanupRuntimeHostTestDirs(t, testDirs...)
	})

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if runtimes[0].host.IngressReady() && len(runtimes[0].host.ResourceViewSnapshot().ActiveResources) == 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !runtimes[0].host.IngressReady() {
		for index, item := range runtimes {
			t.Logf("runtime[%d] status=%+v membership=%+v resources=%+v", index, item.host.StatusSnapshot(), item.host.MembershipSnapshot(), item.host.ResourceViewSnapshot())
		}
		t.Fatal("selected coordinator did not become ingress ready")
	}
	statusDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(statusDeadline) {
		coordinatorStatus := runtimes[0].host.StatusSnapshot()
		ownerStatus := runtimes[1].host.StatusSnapshot()
		leafStatus := runtimes[2].host.StatusSnapshot()
		if coordinatorStatus.CoreReady && coordinatorStatus.AuthorityReady && coordinatorStatus.IngressReady &&
			coordinatorStatus.AuthorityStatus == AuthorityStatusReady &&
			ownerStatus.AuthorityStatus == AuthorityStatusReady &&
			leafStatus.AuthorityStatus == AuthorityStatusNotRequired {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	for index, item := range runtimes {
		status := item.host.StatusSnapshot()
		if status.Role.Selection.NodeID != "node-a" {
			t.Fatalf("runtime[%d] selected coordinator = %+v", index, status.Role)
		}
		if index == 0 {
			if !status.CoreReady || !status.AuthorityReady || !status.IngressReady || status.AuthorityStatus != AuthorityStatusReady {
				t.Fatalf("runtime-a status = %+v", status)
			}
		} else if status.CoreReady || status.IngressReady {
			t.Fatalf("runtime[%d] unexpectedly ready = %+v", index, status)
		} else if index == 1 && status.AuthorityStatus != AuthorityStatusReady {
			t.Fatalf("runtime-b local authority status = %+v", status)
		} else if index == 2 && status.AuthorityStatus != AuthorityStatusNotRequired {
			t.Fatalf("runtime-c local authority status = %+v", status)
		}
	}
	core := runtimes[0].host.Core()
	if core == nil {
		t.Fatal("selected coordinator Core is nil")
	}
	for _, nodeID := range []model.NodeID{"node-a", "node-b"} {
		record, err := core.Repository.GetNode(context.Background(), nodeID)
		if err != nil {
			t.Fatalf("GetNode(%s): %v", nodeID, err)
		}
		if record.Generation < 1 || record.RegistrationID == "" || record.Endpoint == "" || !core.RegistryAPI.Eligible(nodeID) {
			t.Fatalf("authoritative node record %s = %+v eligible=%v", nodeID, record, core.RegistryAPI.Eligible(nodeID))
		}
		t.Logf("authority_node node_id=%s node_generation=%d registration_id=%s status=%s endpoint=%s lease_expires_at=%s eligible=%t", record.ID, record.Generation, record.RegistrationID, record.Status, record.Endpoint, record.LeaseExpiresAt.UTC().Format(time.RFC3339Nano), core.RegistryAPI.Eligible(nodeID))
	}
	resources := core.Resources.ListEligible()
	if len(resources) != 2 {
		t.Fatalf("authoritative eligible resources = %+v", resources)
	}
	for _, record := range resources {
		if record.Descriptor.Generation < 1 || record.NodeGeneration < 1 || record.RegistrationID == "" {
			t.Fatalf("incomplete authoritative resource = %+v", record)
		}
		t.Logf("authority_resource resource_id=%s owner_node_id=%s resource_generation=%d node_generation=%d registration_id=%s publication=%s eligible=%t", record.Descriptor.ID, record.Descriptor.OwnerNodeID, record.Descriptor.Generation, record.NodeGeneration, record.RegistrationID, record.PublicationState, record.Eligible)
	}
	bootstrapStatus, err := runtimes[2].host.runtimeClient.GetRuntimeStatus(context.Background(), runtimes[2].endpoint)
	if err != nil {
		t.Fatalf("runtime-c status: %v", err)
	}
	if bootstrapStatus.GetReadiness() != dtmv1.RuntimeReadiness_RUNTIME_READINESS_NOT_COORDINATOR ||
		bootstrapStatus.GetCoordinator().GetNodeId() != "node-a" || bootstrapStatus.GetCoreAddress() != "" {
		t.Fatalf("runtime-c bootstrap status = %+v", bootstrapStatus)
	}
	coordinatorStatus, err := runtimes[2].host.runtimeClient.GetRuntimeStatus(context.Background(), runtimes[0].endpoint)
	if err != nil {
		t.Fatalf("runtime-a status through bootstrap client: %v", err)
	}
	if coordinatorStatus.GetReadiness() != dtmv1.RuntimeReadiness_RUNTIME_READINESS_READY ||
		!coordinatorStatus.GetIngressReady() || coordinatorStatus.GetCoreAddress() == "" {
		t.Fatalf("runtime-a bootstrap status = %+v", coordinatorStatus)
	}
	mapped, err := core.Mapper.Map(planner.Plan{TaskID: "m6-authority-proof", Steps: []planner.Step{
		{ID: "temperature-step", Capability: "temperature_sensor"},
		{ID: "cooling-step", Capability: "cooling_control"},
	}})
	if err != nil {
		t.Fatalf("authoritative mapper: %v", err)
	}
	validator, err := mapper.NewResourceMappingValidator(core.Resources)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range mapped.Steps {
		if err := step.ResourceRef.Validate(); err != nil {
			t.Fatalf("mapped ResourceRef = %+v: %v", step.ResourceRef, err)
		}
		if err := validator.Validate(step); err != nil {
			t.Fatalf("mapped fence validation for %s: %v", step.ID, err)
		}
		t.Logf("resource_ref step_id=%s resource_id=%s owner_node_id=%s owner_node_generation=%d resource_generation=%d registration_id=%s", step.ID, step.ResourceRef.ResourceID, step.ResourceRef.OwnerNodeID, step.ResourceRef.OwnerNodeGeneration, step.ResourceRef.ResourceGeneration, step.ResourceRef.RegistrationID)
		if step.ResourceRef.RegistrationID == string(step.ResourceRef.OwnerNodeID) {
			t.Fatalf("ResourceRef registration ID copied from node ID: %+v", step.ResourceRef)
		}
	}
}
