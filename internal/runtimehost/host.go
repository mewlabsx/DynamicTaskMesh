package runtimehost

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/mesh/coordinator"
	"dtm/internal/mesh/discovery"
	"dtm/internal/mesh/handshake"
	"dtm/internal/mesh/membership"
	"dtm/internal/mesh/protocol"
	"dtm/internal/mesh/resourcesync"
	"dtm/internal/mesh/resourceview"
)

var ErrInvalidExecutionCapability = errors.New("resource execution capability is required")
var ErrInvalidNodeIdentity = errors.New("stable node identity is required")

type CoreFactory func(context.Context) (*CoreCapability, error)
type ListenFunc func(network, address string) (net.Listener, error)

type Options struct {
	NodeID           string
	Execution        *ResourceExecutionCapability
	ExecutionAddress string
	CoreFactory      CoreFactory
	CoreAddress      string
	AuthorityFactory AuthorityFactory
	Listen           ListenFunc
	Mesh             *MeshOptions
}

type MeshOptions struct {
	Identity             protocol.Identity
	Transport            discovery.Transport
	HandshakeDial        handshake.DialFunc
	ResourceClient       resourcesync.Client
	HandshakeTimeout     time.Duration
	AnnouncementInterval time.Duration
	MembershipTiming     membership.Timing
	Now                  func() time.Time
}

// RuntimeHost owns local execution, optional M2 peer discovery/control, and an
// optional explicitly activated Core capability. Discovery never activates Core.
type RuntimeHost struct {
	nodeID             string
	execution          *ResourceExecutionCapability
	executionAddress   string
	coreFactory        CoreFactory
	coreAddress        string
	coreEndpoint       string
	dynamicCore        bool
	ownsResources      bool
	listen             ListenFunc
	runtimeInstanceID  string
	meshIdentity       protocol.Identity
	runtimeClient      *handshake.Client
	statusServer       *handshake.Server
	coordinatorView    *coordinator.View
	discovery          *discovery.Service
	resourceView       *resourceview.View
	resourceSync       *resourcesync.Sync
	membership         *membership.Table
	membershipCancel   context.CancelFunc
	membershipDone     chan struct{}
	membershipInterval time.Duration
	now                func() time.Time
	peerHandshakes     chan handshake.Result
	peerHandshakeClose sync.Once
	roleWake           chan struct{}
	roleControlCancel  context.CancelFunc
	roleControlDone    chan struct{}
	authorityFactory   AuthorityFactory
	authority          AuthoritySession
	authorityTarget    string
	authorityStatus    string
	authorityError     string

	mu                   sync.RWMutex
	core                 *CoreCapability
	starting             bool
	activatingCore       bool
	coreActivationCancel context.CancelFunc
	activationDone       chan struct{}
	ready                bool
	closed               bool
	close                sync.Once
}

func New(options Options) (*RuntimeHost, error) {
	if options.Execution == nil {
		return nil, ErrInvalidExecutionCapability
	}
	if strings.TrimSpace(options.NodeID) == "" || string(options.Execution.Node.ID()) != strings.TrimSpace(options.NodeID) {
		return nil, ErrInvalidNodeIdentity
	}
	var discoveryService *discovery.Service
	var runtimeInstanceID string
	var meshIdentity protocol.Identity
	var coordinatorView *coordinator.View
	var membershipTable *membership.Table
	var meshResourceView *resourceview.View
	var resourceClient resourcesync.Client
	var meshResourceSync *resourcesync.Sync
	var membershipInterval time.Duration
	var runtimeClient *handshake.Client
	var statusServer *handshake.Server
	now := time.Now
	if options.Mesh != nil {
		identity := options.Mesh.Identity
		if identity.NodeID != strings.TrimSpace(options.NodeID) {
			return nil, ErrInvalidNodeIdentity
		}
		if identity.RuntimeInstance == "" {
			generated, err := protocol.NewRuntimeInstanceID()
			if err != nil {
				return nil, err
			}
			identity.RuntimeInstance = generated
		}
		localResources, err := options.Execution.LocalResourceFacts()
		if err != nil {
			return nil, fmt.Errorf("derive local resource facts: %w", err)
		}
		server, err := handshake.NewServer(identity, localResources...)
		if err != nil {
			return nil, fmt.Errorf("create handshake server: %w", err)
		}
		dtmv1.RegisterRuntimeControlServiceServer(options.Execution.Server, server)
		timeout := options.Mesh.HandshakeTimeout
		if timeout == 0 {
			timeout = handshake.DefaultTimeout
		}
		client, err := handshake.NewClient(identity, timeout, options.Mesh.HandshakeDial)
		if err != nil {
			return nil, err
		}
		interval := options.Mesh.AnnouncementInterval
		if interval == 0 {
			interval = time.Second
		}
		discoveryService, err = discovery.New(identity, options.Mesh.Transport, client, interval)
		if err != nil {
			return nil, err
		}
		runtimeInstanceID = identity.RuntimeInstance
		meshIdentity = identity
		if options.Mesh.Now != nil {
			now = options.Mesh.Now
		}
		timing := options.Mesh.MembershipTiming
		if timing.SuspectAfter == 0 && timing.ExpireAfter == 0 {
			timing = membership.Timing{SuspectAfter: membership.DefaultSuspectAfter, ExpireAfter: membership.DefaultExpireAfter}
		}
		refreshBound, timingErr := discovery.ConfirmedRefreshUpperBound(interval, timeout)
		if timingErr != nil || timing.SuspectAfter <= refreshBound {
			return nil, fmt.Errorf("create membership: %w", membership.ErrInvalidTiming)
		}
		membershipTable, err = membership.New(identity, timing, now())
		if err != nil {
			return nil, fmt.Errorf("create membership: %w", err)
		}
		coordinatorView = &coordinator.View{}
		coordinatorView.Update(membershipTable.Snapshot(), identity)
		membershipInterval = interval
		resourceClient = options.Mesh.ResourceClient
		if resourceClient == nil {
			resourceClient = client
		}
		meshResourceView, err = resourceview.New(identity, func() resourceview.MembershipSnapshot {
			snapshot := membershipTable.Snapshot()
			result := resourceview.MembershipSnapshot{Members: make([]protocol.Identity, len(snapshot.Members)), ActiveMembers: make([]protocol.Identity, len(snapshot.ActiveMembers))}
			for i, member := range snapshot.Members {
				result.Members[i] = member.Identity
			}
			for i, member := range snapshot.ActiveMembers {
				result.ActiveMembers[i] = member.Identity
			}
			return result
		}, localResources)
		if err != nil {
			return nil, fmt.Errorf("create mesh resource view: %w", err)
		}
		meshResourceSync, err = resourcesync.New(resourceClient, meshResourceView.Observe)
		if err != nil {
			return nil, fmt.Errorf("create mesh resource sync: %w", err)
		}
		runtimeClient = client
		statusServer = server
	}
	listen := options.Listen
	if listen == nil {
		listen = net.Listen
	}
	coreAddress := strings.TrimSpace(options.CoreAddress)
	if coreAddress == "" && options.Mesh != nil {
		if host, _, err := net.SplitHostPort(meshIdentity.ControlEndpoint); err == nil && host != "" {
			coreAddress = net.JoinHostPort(host, "0")
		} else {
			coreAddress = "127.0.0.1:0"
		}
	}
	if coreAddress == "" {
		coreAddress = "127.0.0.1:0"
	}
	host := &RuntimeHost{
		nodeID: options.NodeID, execution: options.Execution,
		executionAddress: options.ExecutionAddress,
		coreFactory:      options.CoreFactory, coreAddress: coreAddress,
		listen:            listen,
		runtimeInstanceID: runtimeInstanceID, meshIdentity: meshIdentity, runtimeClient: runtimeClient, statusServer: statusServer,
		coordinatorView: coordinatorView, discovery: discoveryService, resourceView: meshResourceView, resourceSync: meshResourceSync,
		membership: membershipTable, membershipInterval: membershipInterval, now: now,
		peerHandshakes: make(chan handshake.Result, 32), roleWake: make(chan struct{}, 1),
		authorityFactory: options.AuthorityFactory, dynamicCore: options.Mesh != nil && options.CoreFactory != nil,
		ownsResources:   len(options.Execution.Node.Capabilities()) > 0,
		authorityStatus: AuthorityStatusNotReady,
	}
	if statusServer != nil {
		statusServer.SetRuntimeStatusHandler(host.runtimeStatus)
	}
	return host, nil
}

func (host *RuntimeHost) Start() error {
	host.mu.Lock()
	if host.closed {
		host.mu.Unlock()
		return ErrCapabilityClosed
	}
	if host.ready {
		host.mu.Unlock()
		return ErrCapabilityActive
	}
	if host.starting {
		host.mu.Unlock()
		return ErrCapabilityActive
	}
	host.starting = true
	host.mu.Unlock()
	listener, err := host.listen("tcp", host.executionAddress)
	if err != nil {
		host.failStart()
		return fmt.Errorf("listen for resource execution: %w", err)
	}
	if err := host.execution.Start(listener); err != nil {
		_ = listener.Close()
		host.failStart()
		return err
	}
	if host.discovery != nil {
		if err := host.discovery.Start(context.Background()); err != nil {
			_ = host.execution.Close()
			host.failStart()
			return fmt.Errorf("start peer discovery: %w", err)
		}
	}
	host.mu.Lock()
	if host.closed {
		host.starting = false
		host.mu.Unlock()
		_ = host.execution.Close()
		return ErrCapabilityClosed
	}
	if host.membership != nil {
		membershipCtx, cancel := context.WithCancel(context.Background())
		host.membershipCancel = cancel
		host.membershipDone = make(chan struct{})
		if host.resourceSync != nil {
			if err := host.resourceSync.Start(membershipCtx); err != nil {
				cancel()
				host.membershipCancel = nil
				host.membershipDone = nil
				host.mu.Unlock()
				host.failStart()
				return fmt.Errorf("start mesh resource sync: %w", err)
			}
		}
		go host.runMembership(membershipCtx)
	}
	host.starting = false
	host.ready = true
	host.mu.Unlock()
	host.startRoleController(context.Background())
	return nil
}

func (host *RuntimeHost) failStart() {
	host.mu.Lock()
	host.starting = false
	host.mu.Unlock()
	_ = host.Close()
}

func (host *RuntimeHost) ActivateCore(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	host.mu.RLock()
	if host.closed {
		host.mu.RUnlock()
		return ErrCapabilityClosed
	}
	if !host.ready {
		host.mu.RUnlock()
		return errors.New("runtime host is not ready")
	}
	if host.core != nil {
		host.mu.RUnlock()
		return ErrCapabilityActive
	}
	if host.activatingCore {
		host.mu.RUnlock()
		return ErrCapabilityActive
	}
	factory := host.coreFactory
	host.mu.RUnlock()
	if factory == nil {
		return errors.New("core capability is not configured")
	}
	host.mu.Lock()
	if host.closed || !host.ready || host.core != nil || host.activatingCore {
		host.mu.Unlock()
		return ErrCapabilityActive
	}
	host.activatingCore = true
	activationDone := make(chan struct{})
	host.activationDone = activationDone
	activationCtx, activationCancel := context.WithCancel(ctx)
	host.coreActivationCancel = activationCancel
	host.mu.Unlock()
	core, err := factory(activationCtx)
	if err != nil {
		host.failCoreActivation(activationDone, nil)
		return fmt.Errorf("construct core capability: %w", err)
	}
	if core == nil {
		host.failCoreActivation(activationDone, nil)
		return errors.New("construct core capability: factory returned nil")
	}
	listener, err := host.listen("tcp", host.coreAddress)
	if err != nil {
		host.failCoreActivation(activationDone, core)
		return fmt.Errorf("listen for core capability: %w", err)
	}
	if err := core.Activate(activationCtx, listener); err != nil {
		_ = listener.Close()
		host.failCoreActivation(activationDone, core)
		return err
	}
	endpoint := coreEndpointForListener(listener, host.coreAddress)
	host.mu.Lock()
	if host.closed {
		host.mu.Unlock()
		activationCancel()
		_ = core.Close()
		host.mu.Lock()
		host.activatingCore = false
		host.coreActivationCancel = nil
		if host.activationDone == activationDone {
			close(activationDone)
			host.activationDone = nil
		}
		host.mu.Unlock()
		return ErrCapabilityClosed
	}
	host.activatingCore = false
	host.coreActivationCancel = nil
	host.core = core
	host.coreEndpoint = endpoint
	close(activationDone)
	host.activationDone = nil
	host.mu.Unlock()
	return nil
}

func (host *RuntimeHost) failCoreActivation(done chan struct{}, core *CoreCapability) {
	if core != nil {
		_ = core.Close()
	}
	host.mu.Lock()
	host.activatingCore = false
	activationCancel := host.coreActivationCancel
	host.coreActivationCancel = nil
	if host.activationDone == done {
		close(done)
		host.activationDone = nil
	}
	dynamic := host.dynamicCore
	host.mu.Unlock()
	if activationCancel != nil {
		activationCancel()
	}
	if !dynamic {
		_ = host.Close()
	}
}

func (host *RuntimeHost) Ready() bool {
	if host == nil {
		return false
	}
	host.mu.RLock()
	defer host.mu.RUnlock()
	return host.ready && !host.closed && host.execution.Ready()
}

func (host *RuntimeHost) CoreReady() bool {
	if host == nil {
		return false
	}
	return host.coreReadyForRole(host.CoordinatorRoleSnapshot())
}

func (host *RuntimeHost) Execution() *ResourceExecutionCapability { return host.execution }
func (host *RuntimeHost) RuntimeInstanceID() string               { return host.runtimeInstanceID }
func (host *RuntimeHost) PeerHandshakes() <-chan handshake.Result {
	if host.discovery == nil || host.peerHandshakes == nil {
		return nil
	}
	return host.peerHandshakes
}
func (host *RuntimeHost) MembershipSnapshot() membership.Snapshot {
	if host.membership == nil {
		return membership.Snapshot{}
	}
	return host.membership.Snapshot()
}
func (host *RuntimeHost) Membership() *membership.Table { return host.membership }
func (host *RuntimeHost) CoordinatorRoleSnapshot() coordinator.RoleSnapshot {
	if host.coordinatorView == nil {
		return coordinator.RoleSnapshot{}
	}
	host.refreshCoordinatorRole()
	return host.coordinatorView.Snapshot()
}
func (host *RuntimeHost) ResourceViewSnapshot() resourceview.Snapshot {
	if host.resourceView == nil {
		return resourceview.Snapshot{}
	}
	return host.resourceView.Snapshot()
}
func (host *RuntimeHost) ResourceView() *resourceview.View { return host.resourceView }
func (host *RuntimeHost) ResourceSyncErrors() <-chan error {
	if host.resourceSync == nil {
		return nil
	}
	return host.resourceSync.Errors()
}

func (host *RuntimeHost) runMembership(ctx context.Context) {
	defer close(host.membershipDone)
	defer host.closePeerHandshakes()
	ticker := time.NewTicker(host.membershipInterval)
	defer ticker.Stop()
	results := host.discovery.Results()
	for {
		select {
		case <-ctx.Done():
			return
		case result, ok := <-results:
			if !ok {
				return
			}
			if err := host.membership.Observe(result, host.now()); err != nil {
				continue
			}
			host.refreshCoordinatorRole()
			host.notifyRoleController()
			select {
			case host.peerHandshakes <- result:
			default:
			}
			if host.resourceSync != nil {
				host.resourceSync.Request(result.Identity)
			}
		case <-ticker.C:
			_ = host.membership.Tick(host.now())
			host.refreshCoordinatorRole()
			host.notifyRoleController()
		}
	}
}
func (host *RuntimeHost) refreshCoordinatorRole() {
	if host.coordinatorView != nil && host.membership != nil {
		host.coordinatorView.Update(host.membership.Snapshot(), host.meshIdentity)
	}
}
func (host *RuntimeHost) closePeerHandshakes() {
	host.peerHandshakeClose.Do(func() { close(host.peerHandshakes) })
}
func (host *RuntimeHost) Core() *CoreCapability {
	host.mu.RLock()
	defer host.mu.RUnlock()
	return host.core
}

func (host *RuntimeHost) Close() error {
	if host == nil {
		return nil
	}
	var result error
	host.close.Do(func() {
		host.mu.Lock()
		host.ready = false
		host.closed = true
		activationDone := host.activationDone
		activationCancel := host.coreActivationCancel
		roleCancel := host.roleControlCancel
		roleDone := host.roleControlDone
		host.mu.Unlock()
		if activationCancel != nil {
			activationCancel()
		}
		if roleCancel != nil {
			roleCancel()
		}
		if roleDone != nil {
			<-roleDone
		}
		if activationDone != nil {
			<-activationDone
		}
		host.mu.RLock()
		core := host.core
		authority := host.authority
		host.mu.RUnlock()
		if authority != nil {
			result = errors.Join(result, authority.Close())
		}
		if core != nil {
			result = errors.Join(result, core.Close())
		}
		if host.discovery != nil {
			result = errors.Join(result, host.discovery.Close())
		}
		if host.membershipCancel != nil {
			host.membershipCancel()
			<-host.membershipDone
		} else if host.membership != nil {
			host.closePeerHandshakes()
		}
		if host.resourceSync != nil {
			result = errors.Join(result, host.resourceSync.Close())
		}
		if host.membership != nil {
			result = errors.Join(result, host.membership.Close())
		}
		if host.resourceView != nil {
			result = errors.Join(result, host.resourceView.Close())
		}
		result = errors.Join(result, host.execution.Close())
	})
	return result
}
