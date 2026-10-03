package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/capability"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/planner"
	sqliteplatform "dtm/internal/platform/sqlite"
	storageport "dtm/internal/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegistryWritePathPersistsRegistrationHeartbeatCapabilitiesAndOffline(t *testing.T) {
	repository, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	now := time.Date(2026, 8, 1, 1, 2, 3, 0, time.UTC)
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	server, err := NewRegistryServer(registry, endpoints,
		WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return now }),
		WithNodeRepository(repository),
	)
	if err != nil {
		t.Fatal(err)
	}
	first := registerRequest("node-persisted", []string{"temperature_sensor", "cooling_control"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "127.0.0.1:5001")
	first.RegistrationId = "registration-1"
	if _, err := server.RegisterNode(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	record, err := repository.GetNode(context.Background(), "node-persisted")
	if err != nil {
		t.Fatal(err)
	}
	if record.Generation != 1 || record.Status != node.StatusActive ||
		!reflect.DeepEqual(record.Capabilities, []model.Capability{"cooling_control", "temperature_sensor"}) {
		t.Fatalf("first persisted registration = %#v", record)
	}

	now = now.Add(time.Second)
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-persisted", RegistrationId: "registration-1", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	second := registerRequest("node-persisted", []string{"cooling_control"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "127.0.0.1:5002")
	second.RegistrationId = "registration-2"
	if _, err := server.RegisterNode(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	record, err = repository.GetNode(context.Background(), "node-persisted")
	if err != nil {
		t.Fatal(err)
	}
	if record.Generation != 2 || record.Endpoint != "127.0.0.1:5002" ||
		!reflect.DeepEqual(record.Capabilities, []model.Capability{"cooling_control"}) {
		t.Fatalf("replacement persisted registration = %#v", record)
	}

	now = now.Add(2 * time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-persisted", RegistrationId: "registration-2"}); err != nil {
		t.Fatal(err)
	}
	record, err = repository.GetNode(context.Background(), "node-persisted")
	if err != nil || !record.LastHeartbeatAt.Equal(now) || !record.LeaseExpiresAt.Equal(now.Add(10*time.Second)) {
		t.Fatalf("persisted heartbeat = %#v, %v", record, err)
	}
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-persisted", RegistrationId: "registration-2", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	record, err = repository.GetNode(context.Background(), "node-persisted")
	if err != nil || record.Status != node.StatusOffline || server.Eligible("node-persisted") {
		t.Fatalf("persisted offline = %#v eligible=%v err=%v", record, server.Eligible("node-persisted"), err)
	}
}

func TestRegistryLeaseSweepAndConcurrentRegistrationRemainPersistedAndPaired(t *testing.T) {
	repository, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	now := time.Date(2026, 8, 1, 2, 0, 0, 0, time.UTC)
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	server, err := NewRegistryServer(registry, endpoints,
		WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return now }), WithNodeRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	requests := []*dtmv1.RegisterNodeRequest{
		registerRequest("node-concurrent", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-a"),
		registerRequest("node-concurrent", []string{"cooling_control"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-b"),
	}
	requests[0].RegistrationId = "registration-a"
	requests[1].RegistrationId = "registration-b"
	var wait sync.WaitGroup
	errorsFound := make(chan error, len(requests))
	for _, request := range requests {
		request := request
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := server.RegisterNode(context.Background(), request)
			errorsFound <- err
		}()
	}
	wait.Wait()
	close(errorsFound)
	successes, conflicts := 0, 0
	for err := range errorsFound {
		if err == nil {
			successes++
			continue
		}
		if status.Code(err) == codes.FailedPrecondition {
			conflicts++
			continue
		}
		t.Fatal(err)
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent registrations: successes=%d conflicts=%d", successes, conflicts)
	}
	persisted, err := repository.GetNode(context.Background(), "node-concurrent")
	if err != nil {
		t.Fatal(err)
	}
	current, exists := registry.Find("node-concurrent")
	endpoint, resolveErr := endpoints.Resolve("node-concurrent")
	if !exists || resolveErr != nil || persisted.Generation != 1 || persisted.Endpoint != endpoint ||
		!reflect.DeepEqual(persisted.Capabilities, current.Capabilities()) {
		t.Fatalf("paired state: persisted=%#v registry=%#v endpoint=%q exists=%v resolve=%v", persisted, current, endpoint, exists, resolveErr)
	}

	now = now.Add(5 * time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-concurrent", RegistrationId: persisted.RegistrationID}); err != nil {
		t.Fatal(err)
	}
	if count, err := server.SweepExpired(now.Add(5 * time.Second)); err != nil || count != 0 {
		t.Fatalf("sweep before renewed expiry = %d, %v", count, err)
	}
	if count, err := server.SweepExpired(now.Add(10 * time.Second)); err != nil || count != 1 {
		t.Fatalf("sweep at renewed expiry = %d, %v", count, err)
	}
	persisted, err = repository.GetNode(context.Background(), "node-concurrent")
	if err != nil || persisted.Status != node.StatusOffline || server.Eligible("node-concurrent") {
		t.Fatalf("swept state = %#v eligible=%v err=%v", persisted, server.Eligible("node-concurrent"), err)
	}
}

func TestThreeNodeRegistryAndLeaseStateMatchSQLite(t *testing.T) {
	repository, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	now := time.Date(2026, 8, 1, 3, 0, 0, 0, time.UTC)
	registry := capability.NewRegistry()
	server, err := NewRegistryServer(registry, NewEndpointDirectory(),
		WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return now }), WithNodeRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	for index, capabilities := range [][]string{{"temperature_sensor"}, {"cooling_control"}, {"temperature_sensor", "cooling_control"}} {
		id := model.NodeID("node-" + string(rune('a'+index)))
		request := registerRequest(string(id), capabilities, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-"+string(id))
		request.RegistrationId = "registration-" + string(id)
		if _, err := server.RegisterNode(context.Background(), request); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(5 * time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-a", RegistrationId: "registration-node-a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-b", RegistrationId: "registration-node-b", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	if count, err := server.SweepExpired(now.Add(5 * time.Second)); err != nil || count != 1 {
		t.Fatalf("three-node sweep = %d, %v", count, err)
	}
	want := map[model.NodeID]node.Status{"node-a": node.StatusActive, "node-b": node.StatusOffline, "node-c": node.StatusOffline}
	for id, status := range want {
		record, err := repository.GetNode(context.Background(), id)
		if err != nil || record.Status != status {
			t.Fatalf("node %s persisted state = %#v, %v", id, record, err)
		}
		if server.Eligible(id) != (status == node.StatusActive) {
			t.Fatalf("node %s eligibility=%v status=%s", id, server.Eligible(id), status)
		}
	}
	if got := registry.Discover("temperature_sensor"); len(got) != 1 || got[0].ID() != "node-a" {
		t.Fatalf("three-node discovery = %#v", got)
	}
}

func TestRegistryPersistenceFailureFailsClosed(t *testing.T) {
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	failure := errors.New("node persistence failed")
	server, err := NewRegistryServer(registry, endpoints, WithNodeRepository(failingNodeRepository{err: failure}))
	if err != nil {
		t.Fatal(err)
	}
	response, err := server.RegisterNode(context.Background(), validRegisterRequest())
	if response != nil || err == nil {
		t.Fatalf("RegisterNode() = %#v, %v", response, err)
	}
	if server.Eligible("node-1") || len(registry.Discover("temperature_sensor")) != 0 {
		t.Fatalf("failed persistence left node schedulable: eligible=%v nodes=%v", server.Eligible("node-1"), registry.Discover("temperature_sensor"))
	}
	if _, resolveErr := endpoints.Resolve("node-1"); !errors.Is(resolveErr, ErrEndpointNotFound) {
		t.Fatalf("failed persistence endpoint error = %v", resolveErr)
	}
}

func TestM7RegistryStorageErrorsAreStableAndRedacted(t *testing.T) {
	for _, test := range []struct {
		name    string
		failure error
		code    codes.Code
		message string
	}{
		{"busy", fmt.Errorf("%w: SQLite SELECT C:\\secret.db full-key", storageport.ErrStorageUnavailable), codes.Unavailable, "storage is temporarily unavailable"},
		{"read_only", fmt.Errorf("%w: SQLite UPDATE C:\\secret.db full-key", storageport.ErrStorageReadOnly), codes.Internal, "persistent storage operation failed"},
		{"corrupt", fmt.Errorf("%w: SQLite page C:\\secret.db full-key", storageport.ErrStorageCorrupt), codes.Internal, "persistent storage operation failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, err := NewRegistryServer(capability.NewRegistry(), NewEndpointDirectory(), WithNodeRepository(failingNodeRepository{err: test.failure}))
			if err != nil {
				t.Fatal(err)
			}
			_, err = server.RegisterNode(context.Background(), validRegisterRequest())
			if status.Code(err) != test.code || status.Convert(err).Message() != test.message {
				t.Fatalf("RegisterNode() error = %v", err)
			}
			for _, forbidden := range []string{"SQLite", "SELECT", "UPDATE", "secret.db", "full-key"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("client error leaked %q: %v", forbidden, err)
				}
			}
		})
	}
}

type failingNodeRepository struct{ err error }

func (repository failingNodeRepository) GetNode(context.Context, model.NodeID) (storageport.NodeRecord, error) {
	return storageport.NodeRecord{}, repository.err
}
func (repository failingNodeRepository) RegisterNode(context.Context, storageport.RegisterNodeRequest) (storageport.NodeRecord, error) {
	return storageport.NodeRecord{}, repository.err
}
func (repository failingNodeRepository) RenewNodeLease(context.Context, storageport.RenewNodeLeaseRequest) error {
	return repository.err
}
func (repository failingNodeRepository) SetNodeOffline(context.Context, storageport.SetNodeOfflineRequest) error {
	return repository.err
}
func (repository failingNodeRepository) FailClosedNode(context.Context, model.NodeID, time.Time) error {
	return repository.err
}
func (repository failingNodeRepository) ExpireNodeLeases(context.Context, time.Time) (int64, error) {
	return 0, repository.err
}

type blockingNodeRepository struct {
	NodeRepository
	entered chan struct{}
	release chan struct{}
	err     error
}

func (repository *blockingNodeRepository) RegisterNode(ctx context.Context, request storageport.RegisterNodeRequest) (storageport.NodeRecord, error) {
	close(repository.entered)
	select {
	case <-repository.release:
	case <-ctx.Done():
		return storageport.NodeRecord{}, ctx.Err()
	}
	if repository.err != nil {
		return storageport.NodeRecord{}, repository.err
	}
	return repository.NodeRepository.RegisterNode(ctx, request)
}

type injectedNodeRepository struct {
	NodeRepository
	mu           sync.Mutex
	registerErr  error
	heartbeatErr error
	offlineErr   error
	expireErr    error
}

func (repository *injectedNodeRepository) errors() (error, error, error, error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	return repository.registerErr, repository.heartbeatErr, repository.offlineErr, repository.expireErr
}
func (repository *injectedNodeRepository) RegisterNode(ctx context.Context, request storageport.RegisterNodeRequest) (storageport.NodeRecord, error) {
	registerErr, _, _, _ := repository.errors()
	if registerErr != nil {
		return storageport.NodeRecord{}, registerErr
	}
	return repository.NodeRepository.RegisterNode(ctx, request)
}
func (repository *injectedNodeRepository) RenewNodeLease(ctx context.Context, request storageport.RenewNodeLeaseRequest) error {
	_, heartbeatErr, _, _ := repository.errors()
	if heartbeatErr != nil {
		return heartbeatErr
	}
	return repository.NodeRepository.RenewNodeLease(ctx, request)
}
func (repository *injectedNodeRepository) SetNodeOffline(ctx context.Context, request storageport.SetNodeOfflineRequest) error {
	_, _, offlineErr, _ := repository.errors()
	if offlineErr != nil {
		return offlineErr
	}
	return repository.NodeRepository.SetNodeOffline(ctx, request)
}
func (repository *injectedNodeRepository) ExpireNodeLeases(ctx context.Context, now time.Time) (int64, error) {
	_, _, _, expireErr := repository.errors()
	if expireErr != nil {
		return 0, expireErr
	}
	return repository.NodeRepository.ExpireNodeLeases(ctx, now)
}

func newPersistentRegistryServer(t *testing.T, now *time.Time) (*RegistryServer, *capability.Registry, *EndpointDirectory, *sqliteplatform.Repository, *injectedNodeRepository) {
	t.Helper()
	database, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	repository := &injectedNodeRepository{NodeRepository: database}
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	server, err := NewRegistryServer(registry, endpoints, WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return *now }), WithNodeRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	return server, registry, endpoints, database, repository
}

func TestRegisterNodeIsNotSchedulableBeforePersistenceCommit(t *testing.T) {
	for _, shouldFail := range []bool{false, true} {
		t.Run(map[bool]string{false: "commit", true: "failure"}[shouldFail], func(t *testing.T) {
			database, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			blocked := &blockingNodeRepository{NodeRepository: database, entered: make(chan struct{}), release: make(chan struct{})}
			if shouldFail {
				blocked.err = errors.New("blocked registration failed")
			}
			registry := capability.NewRegistry()
			endpoints := NewEndpointDirectory()
			server, err := NewRegistryServer(registry, endpoints, WithNodeRepository(blocked))
			if err != nil {
				t.Fatal(err)
			}
			meshMapper, err := mapper.New(registry, mapper.WithEligibility(server))
			if err != nil {
				t.Fatal(err)
			}
			request := validRegisterRequest()
			done := make(chan error, 1)
			go func() { _, err := server.RegisterNode(context.Background(), request); done <- err }()
			<-blocked.entered
			plan := planner.Plan{TaskID: "task-blocked", Steps: []planner.Step{{ID: "step-1", Capability: "temperature_sensor"}}}
			if server.Eligible("node-1") || len(registry.Discover("temperature_sensor")) != 0 {
				t.Fatal("node became discoverable or eligible before persistence commit")
			}
			if _, err := meshMapper.Map(plan); !errors.Is(err, mapper.ErrCapabilityUnavailable) {
				t.Fatalf("Map() before commit error = %v", err)
			}
			if _, err := endpoints.Resolve("node-1"); !errors.Is(err, ErrEndpointNotFound) {
				t.Fatalf("Resolve() before commit error = %v", err)
			}
			close(blocked.release)
			err = <-done
			if shouldFail {
				if err == nil || server.Eligible("node-1") || len(registry.Discover("temperature_sensor")) != 0 {
					t.Fatalf("failed registration state: err=%v eligible=%v", err, server.Eligible("node-1"))
				}
				if _, err := meshMapper.Map(plan); !errors.Is(err, mapper.ErrCapabilityUnavailable) {
					t.Fatalf("Map() after failure error = %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err := meshMapper.Map(plan); err != nil {
				t.Fatalf("Map() after commit error = %v", err)
			}
			if _, err := endpoints.Resolve("node-1"); err != nil {
				t.Fatalf("Resolve() after commit error = %v", err)
			}
		})
	}
}

func TestHeartbeatPersistenceFailureFailsClosedAndCanReregister(t *testing.T) {
	now := time.Date(2026, 8, 1, 4, 0, 0, 0, time.UTC)
	server, registry, endpoints, database, repository := newPersistentRegistryServer(t, &now)
	request := validRegisterRequest()
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	repository.mu.Lock()
	repository.heartbeatErr = errors.New("heartbeat write failed")
	repository.mu.Unlock()
	now = now.Add(time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-1", RegistrationId: request.RegistrationId}); err == nil {
		t.Fatal("Heartbeat() error = nil")
	}
	if server.Eligible("node-1") || len(registry.Discover("temperature_sensor")) != 0 {
		t.Fatal("failed heartbeat left node schedulable")
	}
	if _, err := endpoints.Resolve("node-1"); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("endpoint after heartbeat failure = %v", err)
	}
	repository.mu.Lock()
	repository.heartbeatErr = nil
	repository.mu.Unlock()
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-1", RegistrationId: request.RegistrationId}); status.Code(err) != codes.FailedPrecondition && status.Code(err) != codes.NotFound {
		t.Fatalf("next heartbeat code = %v, %v", status.Code(err), err)
	}
	now = now.Add(time.Second)
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	record, err := database.GetNode(context.Background(), "node-1")
	if err != nil || record.Generation != 2 || !server.Eligible("node-1") {
		t.Fatalf("reregistered record = %#v eligible=%v err=%v", record, server.Eligible("node-1"), err)
	}
}

func TestOfflinePersistenceFailureLeavesNodeUnschedulable(t *testing.T) {
	now := time.Date(2026, 8, 1, 5, 0, 0, 0, time.UTC)
	server, registry, endpoints, _, repository := newPersistentRegistryServer(t, &now)
	request := validRegisterRequest()
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	repository.mu.Lock()
	repository.offlineErr = errors.New("offline write failed")
	repository.mu.Unlock()
	now = now.Add(time.Second)
	_, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-1", RegistrationId: request.RegistrationId, Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE})
	if err == nil {
		t.Fatal("UpdateNodeStatus() error = nil")
	}
	if server.Eligible("node-1") || len(registry.Discover("temperature_sensor")) != 0 {
		t.Fatal("offline failure left node schedulable")
	}
	if _, err := endpoints.Resolve("node-1"); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("endpoint after offline failure = %v", err)
	}
	repository.mu.Lock()
	repository.offlineErr = nil
	repository.mu.Unlock()
	now = now.Add(time.Second)
	if _, err := server.RegisterNode(context.Background(), request); err != nil || !server.Eligible("node-1") {
		t.Fatalf("reregister after offline failure = %v eligible=%v", err, server.Eligible("node-1"))
	}
}

func TestSweepPersistenceFailureReturnsErrorAndKeepsNodeIneligible(t *testing.T) {
	now := time.Date(2026, 8, 1, 6, 0, 0, 0, time.UTC)
	server, registry, endpoints, _, repository := newPersistentRegistryServer(t, &now)
	request := validRegisterRequest()
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	repository.mu.Lock()
	repository.expireErr = errors.New("expiry write failed")
	repository.mu.Unlock()
	if count, err := server.SweepExpired(now.Add(10 * time.Second)); count != 1 || err == nil {
		t.Fatalf("SweepExpired() = %d, %v", count, err)
	}
	if server.Eligible("node-1") || len(registry.Discover("temperature_sensor")) != 0 {
		t.Fatal("sweep failure left node schedulable")
	}
	if _, err := endpoints.Resolve("node-1"); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("endpoint after sweep failure = %v", err)
	}
	repository.mu.Lock()
	repository.expireErr = nil
	repository.mu.Unlock()
	now = now.Add(11 * time.Second)
	if _, err := server.RegisterNode(context.Background(), request); err != nil || !server.Eligible("node-1") {
		t.Fatalf("reregister after sweep failure = %v eligible=%v", err, server.Eligible("node-1"))
	}
}

func TestFailedReplacementRegistrationDoesNotExposeMixedState(t *testing.T) {
	now := time.Date(2026, 8, 1, 7, 0, 0, 0, time.UTC)
	server, registry, endpoints, database, repository := newPersistentRegistryServer(t, &now)
	first := validRegisterRequest()
	if _, err := server.RegisterNode(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-1", RegistrationId: first.RegistrationId, Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	repository.mu.Lock()
	repository.registerErr = errors.New("replacement write failed")
	repository.mu.Unlock()
	now = now.Add(time.Second)
	replacement := registerRequest("node-1", []string{"cooling_control"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "replacement-endpoint")
	replacement.RegistrationId = "registration-2"
	if _, err := server.RegisterNode(context.Background(), replacement); err == nil {
		t.Fatal("replacement RegisterNode() error = nil")
	}
	if server.Eligible("node-1") || len(registry.Discover("temperature_sensor")) != 0 || len(registry.Discover("cooling_control")) != 0 {
		t.Fatal("failed replacement exposed old or new schedulable state")
	}
	if _, err := endpoints.Resolve("node-1"); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("endpoint after replacement failure = %v", err)
	}
	record, err := database.GetNode(context.Background(), "node-1")
	if err != nil || record.Generation != 1 || record.RegistrationID != first.RegistrationId || record.Status != node.StatusOffline {
		t.Fatalf("database after replacement failure = %#v, %v", record, err)
	}
}

type controlledRegistrationRepository struct {
	NodeRepository
	mu      sync.Mutex
	block   bool
	err     error
	entered chan struct{}
	release chan struct{}
}

func (repository *controlledRegistrationRepository) configure(block bool, err error) {
	repository.mu.Lock()
	repository.block = block
	repository.err = err
	if block {
		repository.entered = make(chan struct{})
		repository.release = make(chan struct{})
	}
	repository.mu.Unlock()
}

func (repository *controlledRegistrationRepository) RegisterNode(ctx context.Context, request storageport.RegisterNodeRequest) (storageport.NodeRecord, error) {
	repository.mu.Lock()
	block, injectedErr := repository.block, repository.err
	entered, release := repository.entered, repository.release
	repository.mu.Unlock()
	if block {
		close(entered)
		select {
		case <-release:
		case <-ctx.Done():
			return storageport.NodeRecord{}, ctx.Err()
		}
	}
	if injectedErr != nil {
		return storageport.NodeRecord{}, injectedErr
	}
	return repository.NodeRepository.RegisterNode(ctx, request)
}

func replacementPlan() planner.Plan {
	return planner.Plan{TaskID: "task-replacement", Steps: []planner.Step{{ID: "step-1", Capability: "temperature_sensor"}}}
}

func TestMissingRuntimeLeaseRequiresReregistration(t *testing.T) {
	database, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	now := time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC)
	if _, err := database.RegisterNode(context.Background(), nodeRegistrationRequestForTransport("node-runtime-missing", "registration", "endpoint", now)); err != nil {
		t.Fatal(err)
	}
	server, err := NewRegistryServer(capability.NewRegistry(), NewEndpointDirectory(), WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return now.Add(time.Second) }), WithNodeRepository(database))
	if err != nil {
		t.Fatal(err)
	}
	_, err = server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-runtime-missing", RegistrationId: "registration"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("Heartbeat() code = %v, error=%v", status.Code(err), err)
	}
	record, getErr := database.GetNode(context.Background(), "node-runtime-missing")
	if getErr != nil || record.Status != node.StatusOffline || server.Eligible("node-runtime-missing") {
		t.Fatalf("fail-closed missing lease record=%#v eligible=%v err=%v", record, server.Eligible("node-runtime-missing"), getErr)
	}
}

func nodeRegistrationRequestForTransport(id model.NodeID, registrationID, endpoint string, at time.Time) storageport.RegisterNodeRequest {
	return storageport.RegisterNodeRequest{ID: id, Endpoint: endpoint, Capabilities: []model.Capability{"temperature_sensor"}, RegistrationID: registrationID, Metadata: map[string]string{}, RegisteredAt: at, LeaseExpiresAt: at.Add(10 * time.Second)}
}

func TestReplacementRegistrationIsNotSchedulableBeforePersistenceCommit(t *testing.T) {
	database, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repository := &controlledRegistrationRepository{NodeRepository: database}
	now := time.Date(2026, 8, 1, 9, 0, 0, 0, time.UTC)
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	server, err := NewRegistryServer(registry, endpoints, WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return now }), WithNodeRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	meshMapper, _ := mapper.New(registry, mapper.WithEligibility(server))
	first := registerRequest("node-replacement", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-a")
	first.RegistrationId = "registration-a"
	if _, err := server.RegisterNode(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := meshMapper.Map(replacementPlan()); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-replacement", RegistrationId: "registration-a", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	repository.configure(true, nil)
	now = now.Add(time.Second)
	second := registerRequest("node-replacement", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-b")
	second.RegistrationId = "registration-b"
	done := make(chan error, 1)
	go func() { _, err := server.RegisterNode(context.Background(), second); done <- err }()
	<-repository.entered
	if len(registry.Discover("temperature_sensor")) != 0 || server.Eligible("node-replacement") {
		t.Fatal("replacement persistence block left old registration schedulable")
	}
	if _, err := meshMapper.Map(replacementPlan()); !errors.Is(err, mapper.ErrCapabilityUnavailable) {
		t.Fatalf("Map() while replacement blocked = %v", err)
	}
	if _, err := endpoints.Resolve("node-replacement"); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("Resolve() while replacement blocked = %v", err)
	}
	close(repository.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	record, err := database.GetNode(context.Background(), "node-replacement")
	if err != nil || record.Generation != 2 || record.RegistrationID != "registration-b" {
		t.Fatalf("replacement record = %#v, %v", record, err)
	}
	if address, err := endpoints.Resolve("node-replacement"); err != nil || address != "endpoint-b" {
		t.Fatalf("replacement endpoint = %q, %v", address, err)
	}
	if _, err := meshMapper.Map(replacementPlan()); err != nil {
		t.Fatalf("Map() after replacement = %v", err)
	}
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-replacement", RegistrationId: "registration-a"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old owner heartbeat = %v, %v", status.Code(err), err)
	}
	if !server.Eligible("node-replacement") {
		t.Fatal("old heartbeat disturbed new registration")
	}
}

func TestReplacementRegistrationFailureRemainsQuiesced(t *testing.T) {
	database, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repository := &controlledRegistrationRepository{NodeRepository: database}
	now := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	server, err := NewRegistryServer(registry, endpoints, WithRegistryClock(func() time.Time { return now }), WithNodeRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	meshMapper, _ := mapper.New(registry, mapper.WithEligibility(server))
	first := registerRequest("node-replacement-fail", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-a")
	first.RegistrationId = "registration-a"
	if _, err := server.RegisterNode(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-replacement-fail", RegistrationId: "registration-a", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	repository.configure(true, errors.New("replacement persistence failed"))
	now = now.Add(time.Second)
	second := registerRequest("node-replacement-fail", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-b")
	second.RegistrationId = "registration-b"
	done := make(chan error, 1)
	go func() { _, err := server.RegisterNode(context.Background(), second); done <- err }()
	<-repository.entered
	if server.Eligible("node-replacement-fail") {
		t.Fatal("old registration eligible while failed replacement blocked")
	}
	close(repository.release)
	if err := <-done; err == nil {
		t.Fatal("replacement error = nil")
	}
	if len(registry.Discover("temperature_sensor")) != 0 || server.Eligible("node-replacement-fail") {
		t.Fatal("failed replacement restored old registration")
	}
	if _, err := endpoints.Resolve("node-replacement-fail"); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("failed replacement endpoint = %v", err)
	}
	if _, err := meshMapper.Map(replacementPlan()); !errors.Is(err, mapper.ErrCapabilityUnavailable) {
		t.Fatalf("Map() after failed replacement = %v", err)
	}
	repository.configure(false, nil)
	now = now.Add(time.Second)
	if _, err := server.RegisterNode(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if !server.Eligible("node-replacement-fail") {
		t.Fatal("explicit retry did not restore replacement")
	}
}
