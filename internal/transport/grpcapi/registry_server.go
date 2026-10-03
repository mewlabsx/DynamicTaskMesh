package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/capability"
	"dtm/internal/lease"
	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/nodelifecycle"
	"dtm/internal/resourcedirectory"
	storageport "dtm/internal/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidRegistryPort = errors.New("invalid registry port")
	ErrInvalidEndpointPort = errors.New("invalid endpoint port")
	ErrInvalidLeaseTTL     = errors.New("invalid lease TTL")
	ErrInvalidRegistryLog  = errors.New("invalid registry logger")
)

const defaultLeaseTTL = 10 * time.Second

type RegistryPort = nodelifecycle.Registry

type EndpointPort = nodelifecycle.Endpoints

type NodeRepository interface {
	GetNode(context.Context, model.NodeID) (storageport.NodeRecord, error)
	RegisterNode(context.Context, storageport.RegisterNodeRequest) (storageport.NodeRecord, error)
	RenewNodeLease(context.Context, storageport.RenewNodeLeaseRequest) error
	SetNodeOffline(context.Context, storageport.SetNodeOfflineRequest) error
	FailClosedNode(context.Context, model.NodeID, time.Time) error
	ExpireNodeLeases(context.Context, time.Time) (int64, error)
}

type ResourceNodeRepository interface {
	RegisterNodeWithResources(context.Context, storageport.RegisterNodeRequest, []model.ResourceDescriptor, bool) (storageport.NodeRecord, resourcedirectory.TransitionPlan, error)
}

type registrationKind int

const (
	registrationFirst registrationKind = iota
	registrationReplay
	registrationUpdate
	registrationRebuild
	registrationOwnershipConflict
)

type RegistryServer struct {
	dtmv1.UnimplementedNodeRegistryServiceServer

	lifecycle          *nodelifecycle.Controller
	repository         NodeRepository
	resources          *resourcedirectory.Directory
	resourceRepository ResourceNodeRepository
	writeMu            sync.Mutex
	leaseTTL           time.Duration
	now                func() time.Time
	logger             *log.Logger
}

type RegistryServerOption func(*registryServerOptions) error

type registryServerOptions struct {
	leaseTTL   time.Duration
	now        func() time.Time
	logger     *log.Logger
	repository NodeRepository
	resources  *resourcedirectory.Directory
}

func WithNodeRepository(repository NodeRepository) RegistryServerOption {
	return func(options *registryServerOptions) error {
		if isNilDependency(repository) {
			return ErrInvalidRegistryPort
		}
		options.repository = repository
		return nil
	}
}

func WithResourceDirectory(directory *resourcedirectory.Directory) RegistryServerOption {
	return func(options *registryServerOptions) error {
		if directory == nil {
			return ErrInvalidRegistryPort
		}
		options.resources = directory
		return nil
	}
}

func WithLeaseTTL(ttl time.Duration) RegistryServerOption {
	return func(options *registryServerOptions) error {
		if ttl <= 0 {
			return ErrInvalidLeaseTTL
		}
		options.leaseTTL = ttl
		return nil
	}
}

func WithRegistryClock(now func() time.Time) RegistryServerOption {
	return func(options *registryServerOptions) error {
		if now == nil {
			return ErrInvalidLeaseTTL
		}
		options.now = now
		return nil
	}
}

func WithRegistryLogger(logger *log.Logger) RegistryServerOption {
	return func(options *registryServerOptions) error {
		if logger == nil {
			return ErrInvalidRegistryLog
		}
		options.logger = logger
		return nil
	}
}

func NewRegistryServer(
	registry RegistryPort,
	endpoints EndpointPort,
	serverOptions ...RegistryServerOption,
) (*RegistryServer, error) {
	if isNilDependency(registry) {
		return nil, ErrInvalidRegistryPort
	}
	if isNilDependency(endpoints) {
		return nil, ErrInvalidEndpointPort
	}
	options := registryServerOptions{
		leaseTTL: defaultLeaseTTL,
		now:      time.Now,
		logger:   log.New(io.Discard, "", 0),
	}
	for _, apply := range serverOptions {
		if apply == nil {
			return nil, ErrInvalidLeaseTTL
		}
		if err := apply(&options); err != nil {
			return nil, err
		}
	}
	leaseManager, err := lease.NewManager(options.leaseTTL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidLeaseTTL, err)
	}
	controller, err := nodelifecycle.New(registry, endpoints, leaseManager, options.now)
	if err != nil {
		return nil, fmt.Errorf("create node lifecycle controller: %w", err)
	}
	server := &RegistryServer{lifecycle: controller, repository: options.repository, resources: options.resources, leaseTTL: options.leaseTTL, now: options.now, logger: options.logger}
	if options.resources != nil {
		resourceRepository, ok := options.repository.(ResourceNodeRepository)
		if !ok {
			return nil, ErrInvalidRegistryPort
		}
		server.resourceRepository = resourceRepository
	}
	return server, nil
}

func (server *RegistryServer) RegisterNode(
	ctx context.Context,
	request *dtmv1.RegisterNodeRequest,
) (*dtmv1.RegisterNodeResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "register request is required")
	}
	registrationID := strings.TrimSpace(request.GetRegistrationId())
	if registrationID == "" {
		return nil, status.Error(codes.InvalidArgument, "registration ID is required")
	}
	registeredNode, address, err := nodeFromProto(request.GetNode())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid node registration: %v", err)
	}

	server.writeMu.Lock()
	defer server.writeMu.Unlock()
	registeredAt := server.now().UTC()
	leaseExpiresAt := registeredAt.Add(server.leaseTTL)
	kind, persisted, err := server.classifyRegistration(ctx, registeredNode, address, registrationID, registeredAt)
	if err != nil {
		return nil, nodeMutationStatus(err)
	}
	if kind == registrationOwnershipConflict {
		return nil, nodeMutationStatus(fmt.Errorf("%w: node %q is owned by registration %q", storageport.ErrNodeOwnershipConflict, registeredNode.ID(), persisted.RegistrationID))
	}

	if server.resources == nil && (kind == registrationUpdate || kind == registrationRebuild) {
		if err := server.lifecycle.QuiesceForRegistration(registeredNode.ID()); err != nil {
			return nil, status.Errorf(codes.Unavailable, "quiesce node registration: %v", err)
		}
	}
	// A same-owner rebuild from an active persisted snapshot needs an explicit
	// offline fence so RegisterNode advances the generation.
	if server.resources == nil && kind == registrationRebuild && server.repository != nil &&
		persisted.ID != "" && persisted.RegistrationID == registrationID &&
		persisted.Status != node.StatusOffline && persisted.Status != node.StatusStale {
		if err := server.repository.FailClosedNode(ctx, registeredNode.ID(), registeredAt); err != nil {
			return nil, nodeMutationStatus(fmt.Errorf("persist rebuild fence: %w", err))
		}
	}

	registrationRequest := storageport.RegisterNodeRequest{
		ID: registeredNode.ID(), Endpoint: address, Capabilities: registeredNode.Capabilities(),
		RegistrationID: registrationID, Metadata: map[string]string{},
		RegisteredAt: registeredAt, LeaseExpiresAt: leaseExpiresAt,
	}
	var persistedRegistration storageport.NodeRecord
	var resourcePlan resourcedirectory.TransitionPlan
	if server.resourceRepository != nil {
		descriptors, descriptorErr := legacyResourceDescriptors(registeredNode.ID(), registeredNode.Capabilities())
		if descriptorErr != nil {
			return nil, status.Errorf(codes.InvalidArgument, "adapt node resources: %v", descriptorErr)
		}
		persistedRegistration, resourcePlan, err = server.resourceRepository.RegisterNodeWithResources(ctx, registrationRequest, descriptors, kind == registrationRebuild)
	} else if server.repository != nil {
		persistedRegistration, err = server.repository.RegisterNode(ctx, registrationRequest)
	}
	if server.repository != nil {
		if err != nil {
			if server.resources != nil {
				// The persistence outcome is unknown (it may or may not have
				// committed), so the owner is strongly isolated until a full
				// Register converges with the authoritative SQLite state.
				server.resources.QuiesceNode(registeredNode.ID(), resourcedirectory.IneligibleReasonCommitOutcomeUnknown)
			}
			err = server.failClosedNode(registeredNode.ID(), registeredAt, fmt.Errorf("persist node registration: %w", err))
			return nil, nodeMutationStatus(err)
		}
	}
	if server.resources != nil {
		if err := server.resources.PublishTransition(resourcePlan); err != nil {
			// The Node+Resource transaction already committed in SQLite; only
			// the in-memory publication failed. The owner must stay strongly
			// isolated (post_commit_publication_failure) until a full Register
			// converges. failClosedNode below records a recoverable reason, but
			// the merge priority never downgrades strong isolation.
			server.resources.QuiesceNode(registeredNode.ID(), resourcedirectory.IneligibleReasonPostCommitFailure)
			err = server.failClosedNode(registeredNode.ID(), registeredAt, fmt.Errorf("publish committed resource registration: %w", err))
			return nil, nodeMutationStatus(err)
		}
	}

	if kind == registrationReplay {
		_, err = server.lifecycle.HeartbeatAt(registeredNode.ID(), registrationID, registeredAt)
	} else {
		_, err = server.lifecycle.RegisterAt(registeredNode, address, registrationID, registeredAt)
	}
	if err != nil {
		err = fmt.Errorf("publish node registration: %w", err)
		if server.repository != nil {
			err = server.failClosedNode(registeredNode.ID(), registeredAt, err)
		}
		return nil, status.Error(codes.Unavailable, "node registration failed")
	}
	if server.resources != nil {
		if err := server.activateResourceLifecycle(persistedRegistration, node.StatusActive, true, true); err != nil {
			err = server.failClosedNode(registeredNode.ID(), registeredAt, fmt.Errorf("publish resource lifecycle: %w", err))
			return nil, nodeMutationStatus(err)
		}
	}
	capabilities := registeredNode.Capabilities()
	names := make([]string, len(capabilities))
	for index, item := range capabilities {
		names[index] = item.String()
	}
	server.logger.Printf("node registered node_id=%s capabilities=%s endpoint=%s", registeredNode.ID(), strings.Join(names, ","), address)
	return &dtmv1.RegisterNodeResponse{Accepted: true}, nil
}

func legacyResourceDescriptors(nodeID model.NodeID, capabilities []model.Capability) ([]model.ResourceDescriptor, error) {
	descriptors := make([]model.ResourceDescriptor, len(capabilities))
	for index, capability := range capabilities {
		descriptor, err := model.AdaptLegacyCapability(nodeID, capability, 1)
		if err != nil {
			return nil, err
		}
		descriptors[index] = descriptor
	}
	return descriptors, nil
}

func (server *RegistryServer) updateResourceLifecycle(record storageport.NodeRecord, statusValue node.Status, leaseValid, endpointActive bool) error {
	if server.resources == nil {
		return nil
	}
	view, err := resourcedirectory.NewNodeLifecycleView(record.ID, record.Generation, record.RegistrationID, statusValue, leaseValid, endpointActive)
	if err != nil {
		return err
	}
	return server.resources.UpdateNodeLifecycle(view)
}

// synchronizeResourceLifecycle routes a successful authoritative lifecycle
// observation (committed NodeRecord plus current runtime Lease and Endpoint)
// to the correct Directory API: ReconcileNodeLifecycle lifts recoverable
// isolation under an exact healthy fence; UpdateNodeLifecycle only refreshes
// the view and never clears any isolation (including strong and unknown).
func (server *RegistryServer) synchronizeResourceLifecycle(record storageport.NodeRecord, statusValue node.Status, leaseValid, endpointActive bool) error {
	if server.resources == nil {
		return nil
	}
	view, err := resourcedirectory.NewNodeLifecycleView(record.ID, record.Generation, record.RegistrationID, statusValue, leaseValid, endpointActive)
	if err != nil {
		return err
	}
	if resourcedirectory.IsRecoverableQuiesceReason(server.resources.NodeQuiesceReason(record.ID)) {
		return server.resources.ReconcileNodeLifecycle(view)
	}
	return server.resources.UpdateNodeLifecycle(view)
}

func (server *RegistryServer) activateResourceLifecycle(record storageport.NodeRecord, statusValue node.Status, leaseValid, endpointActive bool) error {
	if server.resources == nil {
		return nil
	}
	view, err := resourcedirectory.NewNodeLifecycleView(record.ID, record.Generation, record.RegistrationID, statusValue, leaseValid, endpointActive)
	if err != nil {
		return err
	}
	return server.resources.ActivateNodeLifecycle(view)
}

func (server *RegistryServer) classifyRegistration(
	ctx context.Context,
	registeredNode node.Node,
	address string,
	registrationID string,
	now time.Time,
) (registrationKind, storageport.NodeRecord, error) {
	currentLease, hasLease := server.lifecycle.CurrentRegistration(registeredNode.ID())
	runtimeValid := hasLease && now.Before(currentLease.ExpiresAt) && server.lifecycle.Eligible(registeredNode.ID())
	if runtimeValid && currentLease.RegistrationID != registrationID {
		return registrationOwnershipConflict, storageport.NodeRecord{ID: registeredNode.ID(), RegistrationID: currentLease.RegistrationID}, nil
	}
	if server.repository == nil {
		if !hasLease {
			return registrationFirst, storageport.NodeRecord{}, nil
		}
		if runtimeValid && currentLease.RegistrationID == registrationID {
			return registrationUpdate, storageport.NodeRecord{}, nil
		}
		return registrationRebuild, storageport.NodeRecord{}, nil
	}
	persisted, err := server.repository.GetNode(ctx, registeredNode.ID())
	if errors.Is(err, storageport.ErrNodeNotFound) {
		return registrationFirst, storageport.NodeRecord{}, nil
	}
	if err != nil {
		return registrationFirst, storageport.NodeRecord{}, fmt.Errorf("inspect node registration: %w", err)
	}
	if runtimeValid && currentLease.RegistrationID == registrationID && persisted.RegistrationID == registrationID {
		if sameRegistrationPayload(persisted, address, registeredNode.Capabilities()) {
			return registrationReplay, persisted, nil
		}
		return registrationUpdate, persisted, nil
	}
	return registrationRebuild, persisted, nil
}

func sameRegistrationPayload(persisted storageport.NodeRecord, address string, capabilities []model.Capability) bool {
	if persisted.Endpoint != address || len(persisted.Metadata) != 0 || len(persisted.Capabilities) != len(capabilities) {
		return false
	}
	persistedCapabilities := make([]string, len(persisted.Capabilities))
	incomingCapabilities := make([]string, len(capabilities))
	for index := range persisted.Capabilities {
		persistedCapabilities[index] = persisted.Capabilities[index].String()
		incomingCapabilities[index] = capabilities[index].String()
	}
	sort.Strings(persistedCapabilities)
	sort.Strings(incomingCapabilities)
	for index := range persistedCapabilities {
		if persistedCapabilities[index] != incomingCapabilities[index] {
			return false
		}
	}
	return true
}

func (server *RegistryServer) Heartbeat(
	ctx context.Context,
	request *dtmv1.HeartbeatRequest,
) (*dtmv1.HeartbeatResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "heartbeat request is required")
	}
	nodeID := model.NodeID(strings.TrimSpace(request.GetNodeId()))
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node ID is required")
	}
	registrationID := strings.TrimSpace(request.GetRegistrationId())
	if registrationID == "" {
		return nil, status.Error(codes.InvalidArgument, "registration ID is required")
	}

	server.writeMu.Lock()
	defer server.writeMu.Unlock()
	heartbeatAt := server.now().UTC()
	leaseExpiresAt := heartbeatAt.Add(server.leaseTTL)
	if server.repository != nil {
		if err := server.repository.RenewNodeLease(ctx, storageport.RenewNodeLeaseRequest{
			ID: nodeID, RegistrationID: registrationID,
			LastHeartbeatAt: heartbeatAt, LeaseExpiresAt: leaseExpiresAt,
		}); err != nil {
			err = fmt.Errorf("persist node heartbeat: %w", err)
			switch {
			case errors.Is(err, storageport.ErrStaleRegistration):
				// A heartbeat from an old owner must not fail-close the current owner.
				return nil, nodeMutationStatus(err)
			case errors.Is(err, storageport.ErrNodeReregistrationRequired),
				errors.Is(err, storageport.ErrNodeNotFound):
				if quiesceErr := server.lifecycle.QuiesceForRegistration(nodeID); quiesceErr != nil {
					err = errors.Join(err, quiesceErr)
				}
				return nil, nodeMutationStatus(err)
			default:
				err = server.failClosedNode(nodeID, heartbeatAt, err)
				return nil, nodeMutationStatus(err)
			}
		}
	}
	record, err := server.lifecycle.HeartbeatAt(nodeID, registrationID, heartbeatAt)
	if err != nil {
		if server.repository != nil {
			if errors.Is(err, lease.ErrLeaseNotFound) {
				err = fmt.Errorf("%w: persisted heartbeat has no runtime lease: %w", storageport.ErrNodeReregistrationRequired, err)
			} else {
				err = fmt.Errorf("publish node heartbeat: %w", err)
			}
			err = server.failClosedNode(nodeID, heartbeatAt, err)
			return nil, nodeMutationStatus(err)
		}
		switch {
		case errors.Is(err, lease.ErrLeaseNotFound):
			return nil, status.Error(codes.NotFound, "node lease not found")
		case errors.Is(err, lease.ErrStaleRegistration):
			return nil, status.Error(codes.FailedPrecondition, "node registration is no longer current")
		default:
			return nil, status.Errorf(codes.Unavailable, "renew node lease: %v", err)
		}
	}
	if server.resources != nil && server.repository != nil {
		persisted, loadErr := server.repository.GetNode(ctx, nodeID)
		if loadErr != nil {
			loadErr = server.failClosedNode(nodeID, heartbeatAt, fmt.Errorf("load committed heartbeat lifecycle: %w", loadErr))
			return nil, nodeMutationStatus(loadErr)
		}
		if syncErr := server.synchronizeResourceLifecycle(persisted, node.StatusActive, true, true); syncErr != nil {
			if !errors.Is(syncErr, resourcedirectory.ErrQuiesceRequiresRegistration) {
				syncErr = server.failClosedNode(nodeID, heartbeatAt, fmt.Errorf("publish resource heartbeat lifecycle: %w", syncErr))
				return nil, nodeMutationStatus(syncErr)
			}
			server.logger.Printf("resource lifecycle stays quiesced node_id=%s heartbeat=ok reason=strong", nodeID)
		}
	}
	return &dtmv1.HeartbeatResponse{
		Status:                 dtmv1.NodeStatus_NODE_STATUS_ONLINE,
		LeaseExpiresUnixMillis: record.ExpiresAt.UnixMilli(),
	}, nil
}

func (server *RegistryServer) UpdateNodeStatus(
	ctx context.Context,
	request *dtmv1.UpdateNodeStatusRequest,
) (*dtmv1.UpdateNodeStatusResponse, error) {
	if request == nil {
		return nil, status.Error(codes.InvalidArgument, "status update request is required")
	}
	id := model.NodeID(strings.TrimSpace(request.GetNodeId()))
	if id == "" {
		return nil, status.Error(codes.InvalidArgument, "node ID is required")
	}
	if request.GetStatus() != dtmv1.NodeStatus_NODE_STATUS_OFFLINE {
		return nil, status.Error(codes.InvalidArgument, "only offline status updates are allowed")
	}
	registrationID := strings.TrimSpace(request.GetRegistrationId())
	if registrationID == "" {
		return nil, status.Error(codes.InvalidArgument, "registration ID is required")
	}

	server.writeMu.Lock()
	defer server.writeMu.Unlock()
	if err := server.lifecycle.Offline(id, registrationID); err != nil {
		if errors.Is(err, lease.ErrLeaseNotFound) {
			return nil, status.Error(codes.NotFound, "node registration session not found")
		}
		if errors.Is(err, nodelifecycle.ErrStaleRegistration) || errors.Is(err, lease.ErrStaleRegistration) {
			return nil, status.Error(codes.FailedPrecondition, "node registration is no longer current")
		}
		if errors.Is(err, capability.ErrNodeNotFound) {
			return nil, status.Errorf(codes.NotFound, "update node status: %v", err)
		}
		return nil, status.Errorf(codes.Unavailable, "update node status: %v", err)
	}
	if server.repository != nil {
		updatedAt := server.now().UTC()
		if err := server.repository.SetNodeOffline(ctx, storageport.SetNodeOfflineRequest{
			ID: id, RegistrationID: registrationID, UpdatedAt: updatedAt,
		}); err != nil {
			err = server.failClosedNode(id, updatedAt, fmt.Errorf("persist node offline status: %w", err))
			return nil, nodeMutationStatus(err)
		}
	}
	if server.resources != nil && server.repository != nil {
		persisted, err := server.repository.GetNode(ctx, id)
		if err != nil {
			server.resources.QuiesceNode(id, resourcedirectory.IneligibleReasonOfflinePersistenceFailure)
			return nil, nodeMutationStatus(err)
		}
		if err := server.updateResourceLifecycle(persisted, node.StatusOffline, false, false); err != nil {
			return nil, nodeMutationStatus(err)
		}
	}
	return &dtmv1.UpdateNodeStatusResponse{Accepted: true}, nil
}

func (server *RegistryServer) SweepExpired(now time.Time) (int, error) {
	server.writeMu.Lock()
	defer server.writeMu.Unlock()
	count, sweepErr := server.lifecycle.SweepExpired(now)
	if server.repository == nil {
		return count, sweepErr
	}
	_, persistenceErr := server.repository.ExpireNodeLeases(context.Background(), now.UTC())
	if persistenceErr != nil {
		persistenceErr = fmt.Errorf("persist expired node leases: %w", persistenceErr)
		server.quiesceAllResourceNodes(resourcedirectory.IneligibleReasonSweepPersistenceFailure)
	}
	resourceErr := server.refreshResourceLifecycles(context.Background())
	return count, errors.Join(sweepErr, persistenceErr, resourceErr)
}

func (server *RegistryServer) refreshResourceLifecycles(ctx context.Context) error {
	if server.resources == nil || server.repository == nil {
		return nil
	}
	seen := make(map[model.NodeID]struct{})
	var result error
	for _, resource := range server.resources.ListAll() {
		nodeID := resource.Descriptor.OwnerNodeID
		if _, exists := seen[nodeID]; exists {
			continue
		}
		seen[nodeID] = struct{}{}
		record, err := server.repository.GetNode(ctx, nodeID)
		if err != nil {
			server.resources.QuiesceNode(nodeID, resourcedirectory.IneligibleReasonRepositoryReadFailure)
			result = errors.Join(result, err)
			continue
		}
		leaseValid := record.Status == node.StatusActive && server.lifecycle.Eligible(nodeID)
		if err := server.synchronizeResourceLifecycle(record, record.Status, leaseValid, leaseValid); err != nil {
			switch {
			case errors.Is(err, resourcedirectory.ErrNodeNotActive),
				errors.Is(err, resourcedirectory.ErrLeaseInvalid),
				errors.Is(err, resourcedirectory.ErrEndpointInactive),
				errors.Is(err, resourcedirectory.ErrQuiesceRequiresRegistration):
				server.logger.Printf("resource lifecycle remains quiesced node_id=%s err=%v", nodeID, err)
			default:
				result = errors.Join(result, err)
			}
		}
	}
	return result
}

func (server *RegistryServer) failClosedNode(nodeID model.NodeID, at time.Time, primary error) error {
	result := primary
	if server.resources != nil {
		server.resources.QuiesceNode(nodeID, resourcedirectory.IneligibleReasonPersistenceFailure)
	}
	if err := server.lifecycle.FailClosed(nodeID); err != nil {
		result = errors.Join(result, err)
	}
	if server.repository != nil {
		if err := server.repository.FailClosedNode(context.Background(), nodeID, at.UTC()); err != nil {
			result = errors.Join(result, fmt.Errorf("persist fail-closed node %q: %w", nodeID, err))
		}
	}
	if server.resources != nil && server.repository != nil {
		if record, err := server.repository.GetNode(context.Background(), nodeID); err != nil {
			result = errors.Join(result, err)
		} else if err := server.updateResourceLifecycle(record, record.Status, false, false); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (server *RegistryServer) quiesceAllResourceNodes(reason resourcedirectory.IneligibleReason) {
	if server.resources == nil {
		return
	}
	seen := make(map[model.NodeID]struct{})
	for _, resource := range server.resources.ListAll() {
		nodeID := resource.Descriptor.OwnerNodeID
		if _, exists := seen[nodeID]; exists {
			continue
		}
		seen[nodeID] = struct{}{}
		server.resources.QuiesceNode(nodeID, reason)
	}
}

func nodeMutationStatus(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "storage operation canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "storage operation deadline exceeded")
	case errors.Is(err, storageport.ErrStorageUnavailable):
		return status.Error(codes.Unavailable, "storage is temporarily unavailable")
	case errors.Is(err, storageport.ErrStorageReadOnly),
		errors.Is(err, storageport.ErrStorageFull),
		errors.Is(err, storageport.ErrStorageIO),
		errors.Is(err, storageport.ErrStorageCorrupt),
		errors.Is(err, storageport.ErrStorageIntegrity),
		errors.Is(err, storageport.ErrStorageClosed):
		return status.Error(codes.Internal, "persistent storage operation failed")
	case errors.Is(err, storageport.ErrNodeReregistrationRequired):
		return status.Error(codes.NotFound, "node registration session must be re-established")
	case errors.Is(err, storageport.ErrNodeNotFound):
		return status.Error(codes.NotFound, "node registration session not found")
	case errors.Is(err, storageport.ErrNodeConcurrentMutation):
		return status.Error(codes.Aborted, "node persistence mutation was concurrent")
	case errors.Is(err, storageport.ErrNodeOwnershipConflict),
		errors.Is(err, storageport.ErrStaleRegistration),
		errors.Is(err, storageport.ErrNodeStateConflict),
		errors.Is(err, storageport.ErrNodeGenerationConflict),
		errors.Is(err, storageport.ErrNodeTimeRegression):
		return status.Error(codes.FailedPrecondition, "node persistence precondition failed")
	default:
		return status.Error(codes.Unavailable, "storage is temporarily unavailable")
	}
}

func (server *RegistryServer) Eligible(nodeID model.NodeID) bool {
	return server.lifecycle.Eligible(nodeID)
}

func isNilDependency(dependency any) bool {
	if dependency == nil {
		return true
	}
	value := reflect.ValueOf(dependency)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

var (
	_ RegistryPort = (*capability.Registry)(nil)
	_ EndpointPort = (*EndpointDirectory)(nil)
)
