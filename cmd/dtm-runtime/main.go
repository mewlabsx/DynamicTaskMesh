package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"dtm/internal/agent"
	"dtm/internal/authoritybinding"
	"dtm/internal/capability"
	"dtm/internal/config"
	"dtm/internal/demo"
	"dtm/internal/mesh/discovery"
	"dtm/internal/mesh/membership"
	"dtm/internal/mesh/protocol"
	"dtm/internal/model"
	"dtm/internal/runtimehost"
)

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("dtm-runtime", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to the Runtime YAML configuration")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "missing required -config flag")
		return 2
	}
	runtimeConfig, err := config.LoadAgent(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if runtimeConfig.Mode != config.ModeRuntime {
		fmt.Fprintln(stderr, "dtm-runtime requires runtime mode")
		return 2
	}
	handlers, err := handlers(runtimeConfig.Node.Capabilities)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	execution, err := runtimehost.NewResourceExecutionCapability(runtimeConfig, handlers...)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	transport, err := discovery.NewUDPTransport(discovery.MulticastConfig{Group: runtimeConfig.Mesh.Discovery.MulticastGroup, Port: runtimeConfig.Mesh.Discovery.Port, Interface: runtimeConfig.Mesh.Discovery.Interface})
	if err != nil {
		_ = execution.Close()
		fmt.Fprintln(stderr, err)
		return 1
	}
	identity := protocol.Identity{MeshNamespace: runtimeConfig.Mesh.Namespace, ProtocolMajor: runtimeConfig.Mesh.ProtocolMajor, ProtocolMinor: runtimeConfig.Mesh.ProtocolMinor, DTMVersion: protocol.DTMVersion, NodeID: runtimeConfig.Node.ID, ControlEndpoint: runtimeConfig.Mesh.Control.Advertise}
	var host *runtimehost.RuntimeHost
	coreFactory := func(coreCtx context.Context) (*runtimehost.CoreCapability, error) {
		coreConfig := coreConfigForRuntime(runtimeConfig)
		return runtimehost.NewCoreCapability(coreCtx, coreConfig, runtimehost.CoreOptions{
			ResourceEvidenceWriter: stdout,
			Readiness: func() bool {
				return host != nil && host.IngressReady()
			},
		})
	}
	authorityFactory := func() (runtimehost.AuthoritySession, error) {
		interval := runtimeConfig.Heartbeat.Interval.Duration
		if interval <= 0 {
			interval = config.DefaultHeartbeatInterval
		}
		return authoritybinding.New(authoritybinding.Options{
			NodeID: runtimeConfig.Node.ID, Capabilities: runtimeConfig.Node.Capabilities,
			ExecutionAddress: runtimeConfig.Node.AdvertiseAddress, HeartbeatInterval: interval,
		})
	}
	host, err = runtimehost.New(runtimehost.Options{
		NodeID: runtimeConfig.Node.ID, Execution: execution, ExecutionAddress: runtimeConfig.Mesh.Control.Listen,
		CoreFactory: coreFactory, AuthorityFactory: authorityFactory,
		Mesh: &runtimehost.MeshOptions{
			Identity: identity, Transport: transport, AnnouncementInterval: runtimeConfig.Mesh.Discovery.Interval.Duration,
			MembershipTiming: membership.Timing{SuspectAfter: runtimeConfig.Mesh.Membership.SuspectAfter.Duration, ExpireAfter: runtimeConfig.Mesh.Membership.ExpireAfter.Duration},
		},
	})
	if err != nil {
		_ = transport.Close()
		_ = execution.Close()
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := host.Start(); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	defer host.Close()
	fmt.Fprintf(stdout, "runtime ready node_id=%s local_runtime_instance_id=%s local_control_endpoint=%s static_core_address=absent\n", runtimeConfig.Node.ID, host.RuntimeInstanceID(), runtimeConfig.Mesh.Control.Advertise)
	statusTicker := time.NewTicker(500 * time.Millisecond)
	defer statusTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return 0
		case <-statusTicker.C:
			writeStatus(stdout, runtimeConfig.Node.ID, host)
		case result, ok := <-host.PeerHandshakes():
			if !ok {
				return 0
			}
			fmt.Fprintf(stdout, "peer handshake accepted node_id=%s peer_runtime_instance_id=%s peer_control_endpoint=%s protocol=%d.%d\n", result.Identity.NodeID, result.Identity.RuntimeInstance, result.Identity.ControlEndpoint, result.Identity.ProtocolMajor, result.Identity.ProtocolMinor)
			snapshot := host.MembershipSnapshot()
			members := make([]string, len(snapshot.ActiveMembers))
			for index, member := range snapshot.ActiveMembers {
				members[index] = member.Identity.NodeID + "/" + member.Identity.RuntimeInstance
			}
			fmt.Fprintf(stdout, "membership snapshot self_node_id=%s active=%s conflicts=%s\n", runtimeConfig.Node.ID, strings.Join(members, ","), strings.Join(snapshot.IdentityConflicts, ","))
			writeStatus(stdout, runtimeConfig.Node.ID, host)
			resourceSnapshot := host.ResourceViewSnapshot()
			resources := make([]string, len(resourceSnapshot.ActiveResources))
			for index, entry := range resourceSnapshot.ActiveResources {
				resources[index] = entry.Owner.NodeID + "/" + entry.Owner.RuntimeInstance + "/" + entry.Descriptor.ID.String()
			}
			fmt.Fprintf(stdout, "resource view snapshot self_node_id=%s active=%s local_authority_binding=%s\n", runtimeConfig.Node.ID, strings.Join(resources, ","), host.StatusSnapshot().AuthorityStatus)
		}
	}
}

func writeStatus(stdout io.Writer, nodeID string, host *runtimehost.RuntimeHost) {
	snapshot := host.StatusSnapshot()
	registeredNodes, registeredResources := authoritativeCounts(host.Core())
	line := formatRuntimeStatus(nodeID, host.RuntimeInstanceID(), snapshot, host.Core() != nil, registeredNodes, registeredResources)
	fmt.Fprintln(stdout, line)
}

func formatRuntimeStatus(nodeID, localRuntimeInstanceID string, snapshot runtimehost.RuntimeStatus, coreActive bool, registeredNodes, registeredResources int) string {
	coordinatorNodeID := "none"
	coordinatorRuntimeInstanceID := "none"
	coordinatorControlEndpoint := "none"
	if snapshot.Role.HasCoordinator {
		coordinatorNodeID = snapshot.Role.Selection.NodeID
		coordinatorRuntimeInstanceID = snapshot.Role.Selection.RuntimeInstanceID
		coordinatorControlEndpoint = snapshot.Role.Selection.ControlEndpoint
	}
	return fmt.Sprintf("runtime status self_node_id=%s local_runtime_instance_id=%s coordinator=%s coordinator_runtime_instance_id=%s coordinator_control_endpoint=%s local_is_coordinator=%t core_active=%t core_ready=%t authority_ready=%t ingress_ready=%t dynamic_core_address=%s local_authority_binding=%s authoritative_registered_nodes=%d authoritative_registered_resources=%d", nodeID, localRuntimeInstanceID, coordinatorNodeID, coordinatorRuntimeInstanceID, coordinatorControlEndpoint, snapshot.Role.LocalIsCoordinator, coreActive, snapshot.CoreReady, snapshot.AuthorityReady, snapshot.IngressReady, snapshot.CoreAddress, snapshot.AuthorityStatus, registeredNodes, registeredResources)
}

func authoritativeCounts(core *runtimehost.CoreCapability) (int, int) {
	if core == nil || core.Resources == nil {
		return 0, 0
	}
	owners := make(map[string]struct{})
	for _, record := range core.Resources.ListEligible() {
		owners[string(record.Descriptor.OwnerNodeID)] = struct{}{}
	}
	return len(owners), len(core.Resources.ListEligible())
}

func coreConfigForRuntime(agentConfig config.Agent) config.Core {
	storagePath := strings.TrimSpace(agentConfig.Storage.Path)
	if storagePath == "" || storagePath == ":memory:" {
		storagePath = ":memory:"
	} else {
		storagePath = filepath.Clean(storagePath + ".core")
	}
	return config.Core{
		Lease: config.Lease{
			TTL:           config.Duration{Duration: config.DefaultLeaseTTL},
			SweepInterval: config.Duration{Duration: config.DefaultLeaseSweepInterval},
		},
		Storage: config.Storage{
			Driver: agentConfig.Storage.Driver, Path: storagePath,
			JournalMode: agentConfig.Storage.JournalMode, Synchronous: agentConfig.Storage.Synchronous,
			BusyTimeout: agentConfig.Storage.BusyTimeout, AutoMigrate: agentConfig.Storage.AutoMigrate,
		},
		Retry: config.Retry{MaxAttempts: config.DefaultRetryMaxAttempts, Backoff: config.Duration{Duration: config.DefaultRetryBackoff}},
	}
}

func handlers(configured []string) ([]agent.Handler, error) {
	result := make([]agent.Handler, 0, len(configured))
	for _, value := range configured {
		switch model.Capability(value) {
		case capability.TemperatureSensor:
			result = append(result, demo.NewTemperatureHandler())
		case capability.CoolingControl:
			result = append(result, demo.NewCoolingHandler())
		default:
			return nil, fmt.Errorf("unsupported Runtime capability %q", value)
		}
	}
	return result, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
