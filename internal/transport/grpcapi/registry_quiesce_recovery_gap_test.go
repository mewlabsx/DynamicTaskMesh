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
	sqliteplatform "dtm/internal/platform/sqlite"
	"dtm/internal/resourcedirectory"
	storageport "dtm/internal/storage"
)

// transientGetNodeRepository wraps a real SQLite resource repository and
// injects transient GetNode failures per node. Everything else delegates to
// the authoritative persisted repository.
type transientGetNodeRepository struct {
	*sqliteplatform.Repository
	mu      sync.Mutex
	getErr  map[model.NodeID]error
	gotNode bool
}

func (repository *transientGetNodeRepository) setGetNodeErr(nodeID model.NodeID, err error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if err == nil {
		delete(repository.getErr, nodeID)
		return
	}
	repository.getErr[nodeID] = err
}

func (repository *transientGetNodeRepository) GetNode(ctx context.Context, nodeID model.NodeID) (storageport.NodeRecord, error) {
	repository.mu.Lock()
	err := repository.getErr[nodeID]
	repository.mu.Unlock()
	if err != nil {
		return storageport.NodeRecord{}, err
	}
	return repository.Repository.GetNode(ctx, nodeID)
}

func newTransientRegistryServer(t *testing.T, now *time.Time) (*RegistryServer, *transientGetNodeRepository, *resourcedirectory.Directory) {
	t.Helper()
	database, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "quiesce-registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	repository := &transientGetNodeRepository{Repository: database, getErr: make(map[model.NodeID]error)}
	directory := resourcedirectory.New()
	server, err := NewRegistryServer(capability.NewRegistry(), NewEndpointDirectory(),
		WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return *now }),
		WithNodeRepository(repository), WithResourceDirectory(directory))
	if err != nil {
		t.Fatal(err)
	}
	return server, repository, directory
}

// TestRecoveryGapHeartbeatCannotRestoreEligibleResource reproduces the M3-B
// gap through the production Heartbeat path using only the M3-A API surface:
//
//  1. Resource is Eligible after Register;
//  2. a transient GetNode failure during the lifecycle refresh quiesces the
//     owner (recoverable isolation);
//  3. the next Heartbeat succeeds with a fully valid Node Fence, Lease and
//     Endpoint;
//  4. the production Heartbeat path only refreshes the lifecycle view, so the
//     Resource stays permanently ineligible.
//
// BEFORE FIX: this test fails (Eligible remains 0), proving the recovery gap.
func TestRecoveryGapHeartbeatCannotRestoreEligibleResource(t *testing.T) {
	now := time.Date(2026, 8, 6, 1, 0, 0, 0, time.UTC)
	server, repository, directory := newTransientRegistryServer(t, &now)
	request := registerRequest("node-gap-heartbeat", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	request.RegistrationId = "registration-1"
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 1 {
		t.Fatalf("eligible resources after register = %d, want 1: %#v", len(got), got)
	}

	// Transient GetNode failure observed by the refresh that SweepExpired runs.
	repository.setGetNodeErr("node-gap-heartbeat", errors.New("transient read failure"))
	if count, err := server.SweepExpired(now.Add(time.Second)); count != 0 || err == nil {
		t.Fatalf("SweepExpired() = %d, %v; want refresh error", count, err)
	}
	repository.setGetNodeErr("node-gap-heartbeat", nil)
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("resource remained eligible after transient read failure: %#v", got)
	}

	// Next Heartbeat succeeds: persistence, runtime lease, endpoint and the
	// authoritative Node Fence are all valid.
	now = now.Add(2 * time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-gap-heartbeat", RegistrationId: request.RegistrationId}); err != nil {
		t.Fatal(err)
	}
	got := directory.ListEligible()
	if len(got) != 1 {
		t.Fatalf("healthy heartbeat did not restore the resource: eligible = %#v", got)
	}
	if got[0].Descriptor.Generation != 1 {
		t.Fatalf("heartbeat recovery changed generation: %d", got[0].Descriptor.Generation)
	}
}
