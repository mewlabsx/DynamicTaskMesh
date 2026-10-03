package runtimehost

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/application"
	"dtm/internal/capability"
	"dtm/internal/config"
	"dtm/internal/invocation"
	"dtm/internal/lifecycle"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/planner"
	sqliteplatform "dtm/internal/platform/sqlite"
	"dtm/internal/resourcedirectory"
	meshruntime "dtm/internal/runtime"
	"dtm/internal/transport/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type LifecycleFactory func(model.TaskID) (*lifecycle.Lifecycle, error)

type CoreOptions struct {
	Logger                 *log.Logger
	ResourceEvidenceWriter io.Writer
	Dialer                 grpcapi.Dialer
	TaskServiceOptions     []application.TaskServiceOption
	Readiness              func() bool
}

// CoreCapability owns the v0.4/v0.5 Core composition plus the v0.6 Resource
// Invocation path. Ready describes service/composition lifecycle only; it
// grants no Node or Resource authority.
type CoreCapability struct {
	NewLifecycle        LifecycleFactory
	Planner             planner.Planner
	Registry            *capability.Registry
	Resources           *resourcedirectory.Directory
	Endpoints           *grpcapi.EndpointDirectory
	Executor            *grpcapi.RemoteExecutor
	Invocation          *invocation.InvocationExecutor
	InvocationService   *invocation.InvocationService
	InvocationTransport invocation.Transport
	TargetResolver      *invocation.AuthoritativeTargetResolver
	Mapper              *mapper.Mapper
	Runtime             *meshruntime.Runtime
	TaskService         *application.TaskService
	TaskQueries         *application.TaskQueryService
	AsyncTasks          *application.AsyncTaskService
	Recovery            *application.RecoveryController
	RegistryAPI         *grpcapi.RegistryServer
	Repository          *sqliteplatform.Repository
	Server              *grpc.Server

	leaseSweepInterval time.Duration
	mu                 sync.RWMutex
	cancel             context.CancelFunc
	listener           net.Listener
	backgroundErrors   chan error
	serveErrors        chan error
	activated          bool
	ready              bool
	closed             bool
	close              sync.Once
	active             sync.WaitGroup
}

func NewCoreCapability(ctx context.Context, coreConfig config.Core, options CoreOptions) (*CoreCapability, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	repository, err := sqliteplatform.OpenWithOptions(sqliteOptions(coreConfig.Storage))
	if err != nil {
		return nil, err
	}
	var invocationTransport *invocation.NativeGrpcInvocationTransport
	closeOnError := func(err error) (*CoreCapability, error) {
		if invocationTransport != nil {
			_ = invocationTransport.Close()
		}
		_ = repository.Close()
		return nil, err
	}
	if _, err := repository.MarkNodesStale(ctx, time.Now().UTC()); err != nil {
		return closeOnError(err)
	}
	registry := capability.NewRegistry()
	resourceDirectory := resourcedirectory.New()
	resourceRecords, err := repository.LoadAllResourceRecords(ctx)
	if err != nil {
		return closeOnError(err)
	}
	if err := resourceDirectory.RestoreRecords(resourceRecords); err != nil {
		return closeOnError(err)
	}
	restoredOwners := make(map[model.NodeID]struct{})
	for _, resourceRecord := range resourceRecords {
		nodeID := resourceRecord.Descriptor.OwnerNodeID
		if _, exists := restoredOwners[nodeID]; exists {
			continue
		}
		restoredOwners[nodeID] = struct{}{}
		nodeRecord, loadErr := repository.GetNode(ctx, nodeID)
		if loadErr != nil {
			return closeOnError(loadErr)
		}
		view, viewErr := resourcedirectory.NewNodeLifecycleView(nodeRecord.ID, nodeRecord.Generation, nodeRecord.RegistrationID, nodeRecord.Status, false, false)
		if viewErr != nil {
			return closeOnError(viewErr)
		}
		if viewErr = resourceDirectory.UpdateNodeLifecycle(view); viewErr != nil {
			return closeOnError(viewErr)
		}
	}
	endpoints := grpcapi.NewEndpointDirectory()
	registryOptions := []grpcapi.RegistryServerOption{
		grpcapi.WithLeaseTTL(coreConfig.Lease.TTL.Duration),
		grpcapi.WithNodeRepository(repository),
		grpcapi.WithResourceDirectory(resourceDirectory),
	}
	if options.Logger != nil {
		registryOptions = append(registryOptions, grpcapi.WithRegistryLogger(options.Logger))
	}
	registryServer, err := grpcapi.NewRegistryServer(registry, endpoints, registryOptions...)
	if err != nil {
		return closeOnError(err)
	}
	dialer := options.Dialer
	if dialer == nil {
		dialer = func(ctx context.Context, address string) (*grpc.ClientConn, error) {
			return grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}
	}
	executor, err := grpcapi.NewRemoteExecutor(endpoints, dialer)
	if err != nil {
		return closeOnError(err)
	}
	candidateSource, err := mapper.NewResourceCandidateSource(resourceDirectory, restoredResourceReadiness{})
	if err != nil {
		return closeOnError(err)
	}
	meshMapper, err := mapper.New(registry, mapper.WithEligibility(registryServer), mapper.WithResourceCandidateSource(candidateSource))
	if err != nil {
		return closeOnError(err)
	}
	validator, err := mapper.NewResourceMappingValidator(resourceDirectory)
	if err != nil {
		return closeOnError(err)
	}
	legacyAdapter := invocation.NewLegacyCapabilityAdapter()
	targetResolver, err := invocation.NewAuthoritativeTargetResolver(resourceDirectory, endpoints)
	if err != nil {
		return closeOnError(err)
	}
	invocationTransport, err = invocation.NewNativeGrpcInvocationTransport(dialer)
	if err != nil {
		return closeOnError(err)
	}
	var evidenceSink *taskResourceEvidenceSink
	if options.ResourceEvidenceWriter != nil {
		evidenceSink = newTaskResourceEvidenceSink(options.ResourceEvidenceWriter, resourceDirectory, endpoints)
	}
	serviceOptions := []invocation.InvocationServiceOption{}
	if evidenceSink != nil {
		serviceOptions = append(serviceOptions, invocation.WithInvocationObserver(evidenceSink.ObserveInvocation))
	}
	invocationService, err := invocation.NewInvocationService(targetResolver, invocationTransport, serviceOptions...)
	if err != nil {
		return closeOnError(err)
	}
	invocationExecutor, err := invocation.NewInvocationExecutor(invocationService, legacyAdapter)
	if err != nil {
		return closeOnError(err)
	}
	meshRuntime, err := meshruntime.New(invocationExecutor,
		meshruntime.WithRetryPolicy(meshruntime.RetryPolicy{MaxAttempts: coreConfig.Retry.MaxAttempts, Backoff: coreConfig.Retry.Backoff.Duration}),
		meshruntime.WithRemapping(meshMapper, meshruntime.DefaultRemappingPolicy),
		meshruntime.WithResourceMappingValidator(validator),
	)
	if err != nil {
		return closeOnError(err)
	}
	taskOptions := []application.TaskServiceOption{
		application.WithTaskRepository(repository),
		application.WithRecoveryEligibility(registryServer),
		application.WithResourceRecoveryMapper(meshMapper),
	}
	if evidenceSink != nil {
		taskOptions = append(taskOptions, application.WithTaskResourceEvidenceObserver(evidenceSink.Observe))
	}
	taskOptions = append(taskOptions, options.TaskServiceOptions...)
	taskService, err := application.NewTaskService(lifecycle.New, planner.New(), meshMapper, meshRuntime, taskOptions...)
	if err != nil {
		return closeOnError(err)
	}
	taskQueries, err := application.NewTaskQueryService(repository)
	if err != nil {
		return closeOnError(err)
	}
	asyncTasks, err := application.NewAsyncTaskService(ctx, taskService)
	if err != nil {
		return closeOnError(err)
	}
	recovery, err := application.NewRecoveryController(taskService, taskService, time.Second)
	if err != nil {
		asyncTasks.Close()
		return closeOnError(err)
	}
	coreOptions := []grpcapi.CoreServerOption{grpcapi.WithAsyncTaskService(asyncTasks), grpcapi.WithTaskQueryService(taskQueries)}
	if options.Readiness != nil {
		coreOptions = append(coreOptions, grpcapi.WithReadiness(options.Readiness))
	}
	if options.Logger != nil {
		coreOptions = append(coreOptions, grpcapi.WithCoreLogger(options.Logger))
	}
	coreServer, err := grpcapi.NewCoreServer(taskService, coreOptions...)
	if err != nil {
		asyncTasks.Close()
		return closeOnError(err)
	}
	server := grpc.NewServer()
	dtmv1.RegisterCoreServiceServer(server, coreServer)
	dtmv1.RegisterNodeRegistryServiceServer(server, registryServer)
	return &CoreCapability{
		NewLifecycle: lifecycle.New, Planner: planner.New(), Registry: registry,
		Resources: resourceDirectory, Endpoints: endpoints, Executor: executor,
		Invocation: invocationExecutor, InvocationService: invocationService, InvocationTransport: invocationTransport, TargetResolver: targetResolver,
		Mapper: meshMapper, Runtime: meshRuntime, TaskService: taskService,
		TaskQueries: taskQueries, AsyncTasks: asyncTasks, Recovery: recovery,
		RegistryAPI: registryServer, Repository: repository, Server: server,
		leaseSweepInterval: coreConfig.Lease.SweepInterval.Duration,
	}, nil
}

func (capability *CoreCapability) Activate(parent context.Context, listener net.Listener) error {
	if listener == nil {
		return ErrInvalidListener
	}
	if parent == nil {
		parent = context.Background()
	}
	capability.mu.Lock()
	if capability.closed {
		capability.mu.Unlock()
		return ErrCapabilityClosed
	}
	if capability.activated {
		capability.mu.Unlock()
		return ErrCapabilityActive
	}
	ctx, cancel := context.WithCancel(parent)
	capability.cancel = cancel
	capability.listener = listener
	capability.backgroundErrors = make(chan error, 2)
	capability.serveErrors = make(chan error, 1)
	capability.activated = true
	capability.ready = true
	backgroundErrors := capability.backgroundErrors
	serveErrors := capability.serveErrors
	capability.active.Add(3)
	capability.mu.Unlock()

	go func() { defer capability.active.Done(); capability.serve(listener, serveErrors) }()
	go func() { defer capability.active.Done(); capability.sweepLeases(ctx, backgroundErrors) }()
	go func() {
		defer capability.active.Done()
		capability.forwardRecovery(capability.Recovery.Run(ctx), backgroundErrors)
	}()
	return nil
}

func (capability *CoreCapability) Ready() bool {
	if capability == nil {
		return false
	}
	capability.mu.RLock()
	defer capability.mu.RUnlock()
	return capability.ready && !capability.closed
}

func (capability *CoreCapability) Errors() <-chan error {
	if capability == nil {
		return nil
	}
	capability.mu.RLock()
	defer capability.mu.RUnlock()
	return capability.backgroundErrors
}

// ServeErrors reports the terminal result of the Core gRPC serving loop.
// Lease and recovery diagnostics remain on Errors so static dtm-core can keep
// its v0.4 behavior of logging those background errors without exiting.
func (capability *CoreCapability) ServeErrors() <-chan error {
	if capability == nil {
		return nil
	}
	capability.mu.RLock()
	defer capability.mu.RUnlock()
	return capability.serveErrors
}

func (capability *CoreCapability) Close() error {
	if capability == nil {
		return nil
	}
	var closeErr error
	capability.close.Do(func() {
		capability.mu.Lock()
		capability.ready = false
		capability.closed = true
		cancel := capability.cancel
		listener := capability.listener
		capability.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if listener != nil {
			_ = listener.Close()
		}
		stopServer(capability.Server, 5*time.Second)
		capability.active.Wait()
		capability.AsyncTasks.Close()
		if closer, ok := capability.InvocationTransport.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("close invocation transport: %w", err))
			}
		}
		if err := capability.Repository.Close(); err != nil {
			closeErr = errors.Join(closeErr, fmt.Errorf("close core repository: %w", err))
		}
	})
	return closeErr
}

func (capability *CoreCapability) serve(listener net.Listener, errorsOut chan<- error) {
	err := capability.Server.Serve(listener)
	capability.mu.Lock()
	capability.ready = false
	capability.mu.Unlock()
	if err == grpc.ErrServerStopped || errors.Is(err, net.ErrClosed) {
		err = nil
	}
	reportError(errorsOut, err)
}

func (capability *CoreCapability) sweepLeases(ctx context.Context, errorsOut chan<- error) {
	ticker := time.NewTicker(capability.leaseSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if _, err := capability.RegistryAPI.SweepExpired(now); err != nil {
				reportError(errorsOut, err)
			}
		}
	}
}

func (capability *CoreCapability) forwardRecovery(recoveryErrors <-chan error, errorsOut chan<- error) {
	for err := range recoveryErrors {
		if err != nil && !errors.Is(err, context.Canceled) {
			reportError(errorsOut, err)
		}
	}
}

func reportError(errorsOut chan<- error, err error) {
	select {
	case errorsOut <- err:
	default:
	}
}

func sqliteOptions(storage config.Storage) sqliteplatform.Options {
	return sqliteplatform.Options{Path: storage.Path, JournalMode: storage.JournalMode, Synchronous: storage.Synchronous, BusyTimeout: storage.BusyTimeout.Duration, AutoMigrate: storage.AutoMigrationEnabled()}
}

type restoredResourceReadiness struct{}

func (restoredResourceReadiness) Ready() bool { return true }
