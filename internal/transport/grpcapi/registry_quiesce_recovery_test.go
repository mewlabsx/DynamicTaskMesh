package grpcapi

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/capability"
	"dtm/internal/model"
	"dtm/internal/node"
	sqliteplatform "dtm/internal/platform/sqlite"
	"dtm/internal/resourcedirectory"
	storageport "dtm/internal/storage"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// resourceInjectedRepository wraps a real SQLite resource repository and
// injects transient faults per operation. Every other call delegates to the
// authoritative persisted repository.
type resourceInjectedRepository struct {
	*sqliteplatform.Repository
	mu           sync.Mutex
	getNodeErr   map[model.NodeID]error
	heartbeatErr error
	offlineErr   error
	expireErr    error
}

func (repository *resourceInjectedRepository) setGetNodeErr(nodeID model.NodeID, err error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if err == nil {
		delete(repository.getNodeErr, nodeID)
		return
	}
	repository.getNodeErr[nodeID] = err
}

func (repository *resourceInjectedRepository) GetNode(ctx context.Context, nodeID model.NodeID) (storageport.NodeRecord, error) {
	repository.mu.Lock()
	err := repository.getNodeErr[nodeID]
	repository.mu.Unlock()
	if err != nil {
		return storageport.NodeRecord{}, err
	}
	return repository.Repository.GetNode(ctx, nodeID)
}

func (repository *resourceInjectedRepository) RenewNodeLease(ctx context.Context, request storageport.RenewNodeLeaseRequest) error {
	repository.mu.Lock()
	err := repository.heartbeatErr
	repository.mu.Unlock()
	if err != nil {
		return err
	}
	return repository.Repository.RenewNodeLease(ctx, request)
}

func (repository *resourceInjectedRepository) SetNodeOffline(ctx context.Context, request storageport.SetNodeOfflineRequest) error {
	repository.mu.Lock()
	err := repository.offlineErr
	repository.mu.Unlock()
	if err != nil {
		return err
	}
	return repository.Repository.SetNodeOffline(ctx, request)
}

func (repository *resourceInjectedRepository) ExpireNodeLeases(ctx context.Context, now time.Time) (int64, error) {
	repository.mu.Lock()
	err := repository.expireErr
	repository.mu.Unlock()
	if err != nil {
		return 0, err
	}
	return repository.Repository.ExpireNodeLeases(ctx, now)
}

func newResourcePersistentRegistryServer(t *testing.T, now *time.Time) (*RegistryServer, *resourceInjectedRepository, *resourcedirectory.Directory, *sqliteplatform.Repository) {
	t.Helper()
	database, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "quiesce-registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	repository := &resourceInjectedRepository{Repository: database, getNodeErr: make(map[model.NodeID]error)}
	directory := resourcedirectory.New()
	server, err := NewRegistryServer(capability.NewRegistry(), NewEndpointDirectory(),
		WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return *now }),
		WithNodeRepository(repository), WithResourceDirectory(directory))
	if err != nil {
		t.Fatal(err)
	}
	return server, repository, directory, database
}

func resourceRegisterRequest(nodeID string, capabilities []string, address string) *dtmv1.RegisterNodeRequest {
	request := registerRequest(nodeID, capabilities, dtmv1.NodeStatus_NODE_STATUS_ONLINE, address)
	request.RegistrationId = "registration-" + nodeID
	return request
}

func TestHeartbeatLiftsRecoverableQuiesceUnderExactHealthyFence(t *testing.T) {
	now := time.Date(2026, 8, 6, 2, 0, 0, 0, time.UTC)
	server, repository, directory, _ := newResourcePersistentRegistryServer(t, &now)
	request := resourceRegisterRequest("node-heartbeat-recover", []string{"temperature_sensor"}, "endpoint-1")
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 1 {
		t.Fatalf("eligible after register = %d, want 1", len(got))
	}

	repository.setGetNodeErr("node-heartbeat-recover", errors.New("transient read failure"))
	if count, err := server.SweepExpired(now.Add(time.Second)); count != 0 || err == nil {
		t.Fatalf("SweepExpired() = %d, %v; want refresh error", count, err)
	}
	repository.setGetNodeErr("node-heartbeat-recover", nil)
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("resource eligible after transient read failure: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-heartbeat-recover"); reason != resourcedirectory.IneligibleReasonRepositoryReadFailure {
		t.Fatalf("quiesce reason = %q, want repository_read_failure", reason)
	}

	now = now.Add(2 * time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-heartbeat-recover", RegistrationId: request.RegistrationId}); err != nil {
		t.Fatal(err)
	}
	got := directory.ListEligible()
	if len(got) != 1 || got[0].Descriptor.Generation != 1 {
		t.Fatalf("healthy heartbeat did not restore the resource: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-heartbeat-recover"); reason != resourcedirectory.IneligibleReasonNone {
		t.Fatalf("quiesce reason after heartbeat = %q", reason)
	}
}

func TestHeartbeatDoesNotClearStrongIsolation(t *testing.T) {
	tests := []struct {
		name   string
		reason resourcedirectory.IneligibleReason
		apply  func(*testing.T, *RegistryServer, *sqliteplatform.Repository, *resourcedirectory.Directory) error
	}{
		{
			name:   "publication_in_progress",
			reason: resourcedirectory.IneligibleReasonPublicationInProgress,
			apply: func(t *testing.T, server *RegistryServer, database *sqliteplatform.Repository, directory *resourcedirectory.Directory) error {
				t.Helper()
				record, err := database.GetNode(context.Background(), "node-strong-heartbeat")
				if err != nil {
					return err
				}
				descriptors := make([]model.ResourceDescriptor, 0)
				for _, view := range directory.ListAll() {
					descriptors = append(descriptors, view.Descriptor.Clone())
				}
				snapshot, err := resourcedirectory.NewNodeResourceSnapshot(record.ID, record.Generation, record.RegistrationID, descriptors)
				if err != nil {
					return err
				}
				plan, err := resourcedirectory.PlanNodeResourceSnapshot(directory.ListAll(), snapshot)
				if err != nil {
					return err
				}
				return directory.PublishTransition(plan)
			},
		},
		{
			name:   "post_commit_publication_failure",
			reason: resourcedirectory.IneligibleReasonPostCommitFailure,
			apply: func(t *testing.T, server *RegistryServer, database *sqliteplatform.Repository, directory *resourcedirectory.Directory) error {
				directory.QuiesceNode("node-strong-heartbeat", resourcedirectory.IneligibleReasonPostCommitFailure)
				return nil
			},
		},
		{
			name:   "commit_outcome_unknown",
			reason: resourcedirectory.IneligibleReasonCommitOutcomeUnknown,
			apply: func(t *testing.T, server *RegistryServer, database *sqliteplatform.Repository, directory *resourcedirectory.Directory) error {
				directory.QuiesceNode("node-strong-heartbeat", resourcedirectory.IneligibleReasonCommitOutcomeUnknown)
				return nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			now := time.Date(2026, 8, 6, 3, 0, 0, 0, time.UTC)
			server, _, directory, database := newResourcePersistentRegistryServer(t, &now)
			request := resourceRegisterRequest("node-strong-heartbeat", []string{"temperature_sensor"}, "endpoint-1")
			if _, err := server.RegisterNode(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			if len(directory.ListEligible()) != 1 {
				t.Fatal("resource not eligible before strong isolation")
			}
			if err := test.apply(t, server, database, directory); err != nil {
				t.Fatal(err)
			}
			now = now.Add(2 * time.Second)
			response, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-strong-heartbeat", RegistrationId: request.RegistrationId})
			if err != nil || response == nil || response.GetStatus() != dtmv1.NodeStatus_NODE_STATUS_ONLINE {
				t.Fatalf("Heartbeat() = %#v, %v; strong isolation must not change v0.3 heartbeat success", response, err)
			}
			if got := directory.ListEligible(); len(got) != 0 {
				t.Fatalf("strong isolation was cleared by heartbeat: %#v", got)
			}
			if reason := directory.NodeQuiesceReason("node-strong-heartbeat"); reason != test.reason {
				t.Fatalf("strong reason changed to %q", reason)
			}
			if err := server.refreshResourceLifecycles(context.Background()); err != nil {
				t.Fatalf("refresh error = %v", err)
			}
			if got := directory.ListEligible(); len(got) != 0 {
				t.Fatalf("strong isolation was cleared by refresh: %#v", got)
			}
			now = now.Add(time.Second)
			if _, err := server.RegisterNode(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			if got := directory.ListEligible(); len(got) != 1 {
				t.Fatalf("full re-register did not restore the resource: %#v", got)
			}
		})
	}
}

func TestRefreshRecoveryRestoresOnlyReconciledNode(t *testing.T) {
	now := time.Date(2026, 8, 6, 4, 0, 0, 0, time.UTC)
	server, repository, directory, _ := newResourcePersistentRegistryServer(t, &now)
	for _, nodeID := range []string{"node-refresh-a", "node-refresh-b"} {
		if _, err := server.RegisterNode(context.Background(), resourceRegisterRequest(nodeID, []string{"temperature_sensor"}, "endpoint-"+nodeID)); err != nil {
			t.Fatal(err)
		}
	}
	if got := directory.ListEligible(); len(got) != 2 {
		t.Fatalf("eligible after register = %d, want 2", len(got))
	}

	repository.setGetNodeErr("node-refresh-a", errors.New("transient read failure"))
	if count, err := server.SweepExpired(now.Add(time.Second)); count != 0 || err == nil {
		t.Fatalf("SweepExpired() = %d, %v; want aggregated refresh error", count, err)
	}
	repository.setGetNodeErr("node-refresh-a", nil)
	got := directory.ListEligible()
	if len(got) != 1 || got[0].Descriptor.OwnerNodeID != "node-refresh-b" {
		t.Fatalf("refresh failure disturbed unrelated node: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-refresh-a"); reason != resourcedirectory.IneligibleReasonRepositoryReadFailure {
		t.Fatalf("node-refresh-a reason = %q", reason)
	}
	if reason := directory.NodeQuiesceReason("node-refresh-b"); reason != resourcedirectory.IneligibleReasonNone {
		t.Fatalf("node-refresh-b reason = %q", reason)
	}

	now = now.Add(time.Second)
	if count, err := server.SweepExpired(now); count != 0 || err != nil {
		t.Fatalf("SweepExpired() after recovery = %d, %v", count, err)
	}
	got = directory.ListEligible()
	if len(got) != 2 || got[0].Descriptor.OwnerNodeID != "node-refresh-a" || got[1].Descriptor.OwnerNodeID != "node-refresh-b" {
		t.Fatalf("refresh did not restore target node: %#v", got)
	}
}

func TestSweepPersistenceFailureKeepsResourceIneligibleUntilReregister(t *testing.T) {
	now := time.Date(2026, 8, 6, 5, 0, 0, 0, time.UTC)
	server, repository, directory, database := newResourcePersistentRegistryServer(t, &now)
	request := resourceRegisterRequest("node-sweep-fail", []string{"temperature_sensor"}, "endpoint-1")
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(directory.ListEligible()) != 1 {
		t.Fatal("resource not eligible before sweep failure")
	}

	repository.mu.Lock()
	repository.expireErr = errors.New("expiry write failed")
	repository.mu.Unlock()
	now = now.Add(10 * time.Second)
	if count, err := server.SweepExpired(now); count != 1 || err == nil {
		t.Fatalf("SweepExpired() = %d, %v; want persistence error", count, err)
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("resource eligible after sweep persistence failure: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-sweep-fail"); reason != resourcedirectory.IneligibleReasonSweepPersistenceFailure {
		t.Fatalf("sweep quiesce reason = %q", reason)
	}

	repository.mu.Lock()
	repository.expireErr = nil
	repository.mu.Unlock()
	now = now.Add(time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-sweep-fail", RegistrationId: request.RegistrationId}); status.Code(err) != codes.NotFound {
		t.Fatalf("heartbeat after sweep failure code = %v, %v; runtime lease was removed by the sweep", status.Code(err), err)
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("offline node recovered without re-registration: %#v", got)
	}
	if record, err := database.GetNode(context.Background(), "node-sweep-fail"); err != nil || record.Status != node.StatusOffline {
		t.Fatalf("node after failed heartbeat = %#v, %v", record, err)
	}

	now = now.Add(time.Second)
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 1 {
		t.Fatalf("re-register did not restore the resource: %#v", got)
	}
}

func TestOfflinePersistenceFailureKeepsResourceIneligibleUntilReregister(t *testing.T) {
	now := time.Date(2026, 8, 6, 6, 0, 0, 0, time.UTC)
	server, repository, directory, _ := newResourcePersistentRegistryServer(t, &now)
	request := resourceRegisterRequest("node-offline-fail", []string{"temperature_sensor"}, "endpoint-1")
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(directory.ListEligible()) != 1 {
		t.Fatal("resource not eligible before offline failure")
	}

	repository.setGetNodeErr("node-offline-fail", errors.New("transient read failure"))
	now = now.Add(time.Second)
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-offline-fail", RegistrationId: request.RegistrationId, Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err == nil {
		t.Fatal("UpdateNodeStatus() error = nil")
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("resource eligible after offline persistence failure: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-offline-fail"); reason != resourcedirectory.IneligibleReasonOfflinePersistenceFailure {
		t.Fatalf("offline quiesce reason = %q", reason)
	}

	repository.setGetNodeErr("node-offline-fail", nil)
	now = now.Add(time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-offline-fail", RegistrationId: request.RegistrationId}); status.Code(err) != codes.NotFound {
		t.Fatalf("heartbeat after offline failure code = %v, %v; runtime lease was removed", status.Code(err), err)
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("offline node recovered without re-registration: %#v", got)
	}

	now = now.Add(time.Second)
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 1 {
		t.Fatalf("re-register did not restore the resource: %#v", got)
	}
}

func TestOfflineWriteFailureFailsClosedThroughGeneralRecoverableReason(t *testing.T) {
	now := time.Date(2026, 8, 6, 6, 30, 0, 0, time.UTC)
	server, repository, directory, _ := newResourcePersistentRegistryServer(t, &now)
	request := resourceRegisterRequest("node-offline-write-fail", []string{"temperature_sensor"}, "endpoint-1")
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	repository.mu.Lock()
	repository.offlineErr = errors.New("offline write failed")
	repository.mu.Unlock()
	now = now.Add(time.Second)
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-offline-write-fail", RegistrationId: request.RegistrationId, Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err == nil {
		t.Fatal("UpdateNodeStatus() error = nil")
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("resource eligible after offline write failure: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-offline-write-fail"); reason != resourcedirectory.IneligibleReasonPersistenceFailure {
		t.Fatalf("offline write quiesce reason = %q", reason)
	}
	repository.mu.Lock()
	repository.offlineErr = nil
	repository.mu.Unlock()
	now = now.Add(time.Second)
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 1 {
		t.Fatalf("re-register did not restore the resource: %#v", got)
	}
}

func TestHeartbeatLiftsSweepAndOfflineRecoverableReasons(t *testing.T) {
	for _, reason := range []resourcedirectory.IneligibleReason{
		resourcedirectory.IneligibleReasonSweepPersistenceFailure,
		resourcedirectory.IneligibleReasonOfflinePersistenceFailure,
	} {
		t.Run(string(reason), func(t *testing.T) {
			now := time.Date(2026, 8, 6, 7, 0, 0, 0, time.UTC)
			server, _, directory, _ := newResourcePersistentRegistryServer(t, &now)
			request := resourceRegisterRequest("node-recover-reason", []string{"temperature_sensor"}, "endpoint-1")
			if _, err := server.RegisterNode(context.Background(), request); err != nil {
				t.Fatal(err)
			}
			directory.QuiesceNode("node-recover-reason", reason)
			if got := directory.ListEligible(); len(got) != 0 {
				t.Fatalf("resource eligible after %q: %#v", reason, got)
			}
			now = now.Add(2 * time.Second)
			if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-recover-reason", RegistrationId: request.RegistrationId}); err != nil {
				t.Fatal(err)
			}
			got := directory.ListEligible()
			if len(got) != 1 || got[0].Descriptor.Generation != 1 {
				t.Fatalf("heartbeat did not lift %q: %#v", reason, got)
			}
			if reason := directory.NodeQuiesceReason("node-recover-reason"); reason != resourcedirectory.IneligibleReasonNone {
				t.Fatalf("reason after heartbeat = %q", reason)
			}
		})
	}
}

func TestStaleFenceHeartbeatCannotDisturbNewRegistration(t *testing.T) {
	now := time.Date(2026, 8, 6, 8, 0, 0, 0, time.UTC)
	server, _, directory, database := newResourcePersistentRegistryServer(t, &now)
	first := registerRequest("node-stale-fence", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	first.RegistrationId = "registration-old"
	if _, err := server.RegisterNode(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-stale-fence", RegistrationId: first.RegistrationId, Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	second := registerRequest("node-stale-fence", []string{"temperature_sensor", "cooling_control"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-2")
	second.RegistrationId = "registration-new"
	if _, err := server.RegisterNode(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 2 {
		t.Fatalf("new registration resources = %#v", got)
	}
	persisted, err := database.GetNode(context.Background(), "node-stale-fence")
	if err != nil || persisted.Generation != 2 || persisted.RegistrationID != "registration-new" {
		t.Fatalf("new registration fence = %#v, %v", persisted, err)
	}

	now = now.Add(time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-stale-fence", RegistrationId: first.RegistrationId}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale heartbeat code = %v, %v", status.Code(err), err)
	}
	if got := directory.ListEligible(); len(got) != 2 {
		t.Fatalf("stale heartbeat disturbed new resources: %#v", got)
	}
	after, err := database.GetNode(context.Background(), "node-stale-fence")
	if err != nil || after.Generation != persisted.Generation || after.RegistrationID != persisted.RegistrationID || after.Status != node.StatusActive {
		t.Fatalf("stale heartbeat changed new registration: before=%#v after=%#v", persisted, after)
	}

	directory.QuiesceNode("node-stale-fence", resourcedirectory.IneligibleReasonPublicationInProgress)
	now = now.Add(time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-stale-fence", RegistrationId: first.RegistrationId}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("stale heartbeat during publication code = %v, %v", status.Code(err), err)
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("stale heartbeat cleared publication isolation: %#v", got)
	}
	now = now.Add(time.Second)
	if _, err := server.RegisterNode(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 2 {
		t.Fatalf("re-register did not complete activation: %#v", got)
	}
}
