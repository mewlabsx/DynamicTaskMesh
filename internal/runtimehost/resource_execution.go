package runtimehost

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/agent"
	"dtm/internal/config"
	"dtm/internal/invocation"
	"dtm/internal/mesh/resourceview"
	"dtm/internal/model"
	"dtm/internal/node"
	sqliteplatform "dtm/internal/platform/sqlite"
	"dtm/internal/transport/grpcapi"
	"google.golang.org/grpc"
)

var (
	ErrCapabilityClosed = errors.New("runtime capability is closed")
	ErrCapabilityActive = errors.New("runtime capability is already active")
	ErrInvalidListener  = errors.New("runtime capability listener is required")
)

// ResourceExecutionCapability owns the existing Agent execution composition.
// It deliberately has no Core registration or heartbeat responsibility.
type ResourceExecutionCapability struct {
	Node                node.Node
	Router              *agent.Router
	ExecutionServer     *grpcapi.AgentExecutionServer
	ExecutionRepository *sqliteplatform.ExecutionRepository
	Server              *grpc.Server

	mu       sync.RWMutex
	listener net.Listener
	serveErr chan error
	started  bool
	ready    bool
	closed   bool
	close    sync.Once
	active   sync.WaitGroup
}

func (capability *ResourceExecutionCapability) LocalResourceFacts() ([]resourceview.Descriptor, error) {
	if capability == nil {
		return nil, ErrInvalidExecutionCapability
	}
	capabilities := capability.Node.Capabilities()
	descriptors := make([]resourceview.Descriptor, len(capabilities))
	for i, provided := range capabilities {
		descriptor, err := resourceview.ProjectLocalCapability(capability.Node.ID(), provided)
		if err != nil {
			return nil, err
		}
		descriptors[i] = descriptor
	}
	return descriptors, nil
}

func NewResourceExecutionCapability(
	executionConfig config.Agent,
	handlers ...agent.Handler,
) (*ResourceExecutionCapability, error) {
	capabilities := make([]model.Capability, len(executionConfig.Node.Capabilities))
	for i, configured := range executionConfig.Node.Capabilities {
		capabilities[i] = model.Capability(configured)
	}
	agentNode, err := node.New(model.NodeID(executionConfig.Node.ID), capabilities, node.StatusOnline)
	if err != nil {
		return nil, err
	}
	router, err := agent.NewRouter(handlers...)
	if err != nil {
		return nil, err
	}
	storagePath := executionConfig.Storage.Path
	if storagePath == "" {
		storagePath = ":memory:"
	}
	repository, err := sqliteplatform.OpenExecutionRepositoryWithOptions(sqliteplatform.Options{
		Path: storagePath, JournalMode: executionConfig.Storage.JournalMode,
		Synchronous: executionConfig.Storage.Synchronous,
		BusyTimeout: executionConfig.Storage.BusyTimeout.Duration,
		AutoMigrate: executionConfig.Storage.AutoMigrationEnabled(),
	})
	if err != nil {
		return nil, err
	}
	executionServer, err := grpcapi.NewAgentExecutionServer(
		router,
		grpcapi.WithExecutionRepository(repository),
		grpcapi.WithInvocationAdapter(invocation.NewLegacyCapabilityAdapter()),
	)
	if err != nil {
		_ = repository.Close()
		return nil, err
	}
	server := grpc.NewServer()
	dtmv1.RegisterAgentExecutionServiceServer(server, executionServer)
	dtmv1.RegisterResourceInvocationServiceServer(server, executionServer)
	return &ResourceExecutionCapability{
		Node: agentNode, Router: router, ExecutionServer: executionServer,
		ExecutionRepository: repository, Server: server,
	}, nil
}

func (capability *ResourceExecutionCapability) Start(listener net.Listener) error {
	if listener == nil {
		return ErrInvalidListener
	}
	capability.mu.Lock()
	if capability.closed {
		capability.mu.Unlock()
		return ErrCapabilityClosed
	}
	if capability.started {
		capability.mu.Unlock()
		return ErrCapabilityActive
	}
	capability.listener = listener
	capability.serveErr = make(chan error, 1)
	capability.started = true
	capability.ready = true
	serveErr := capability.serveErr
	capability.active.Add(1)
	capability.mu.Unlock()
	go func() {
		defer capability.active.Done()
		err := capability.Server.Serve(listener)
		capability.mu.Lock()
		capability.ready = false
		capability.mu.Unlock()
		serveErr <- err
		close(serveErr)
	}()
	return nil
}

func (capability *ResourceExecutionCapability) Ready() bool {
	if capability == nil {
		return false
	}
	capability.mu.RLock()
	defer capability.mu.RUnlock()
	return capability.ready && !capability.closed
}

func (capability *ResourceExecutionCapability) Errors() <-chan error {
	if capability == nil {
		return nil
	}
	capability.mu.RLock()
	defer capability.mu.RUnlock()
	return capability.serveErr
}

func (capability *ResourceExecutionCapability) Close() error {
	if capability == nil {
		return nil
	}
	var closeErr error
	capability.close.Do(func() {
		capability.mu.Lock()
		capability.ready = false
		capability.closed = true
		listener := capability.listener
		capability.mu.Unlock()
		if listener != nil {
			_ = listener.Close()
		}
		stopServer(capability.Server, 2*time.Second)
		capability.active.Wait()
		if err := capability.ExecutionRepository.Close(); err != nil {
			closeErr = fmt.Errorf("close execution repository: %w", err)
		}
	})
	return closeErr
}

func stopServer(server *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		server.Stop()
		<-done
	}
}
