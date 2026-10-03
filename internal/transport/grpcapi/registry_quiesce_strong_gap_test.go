package grpcapi

import (
	"context"
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
)

// postCommitPublishRepository wraps a real SQLite resource repository. It
// lets the real Node+Resource registration transaction commit and then
// corrupts the returned TransitionPlan so the in-memory PublishTransition
// fails after the SQLite commit. GetNode and heartbeat faults are injected
// per operation; everything else delegates to the persisted repository.
type postCommitPublishRepository struct {
	*sqliteplatform.Repository
	mu                sync.Mutex
	corruptNextPlan   bool
	getNodeErr        map[model.NodeID]error
	heartbeatErr      error
	failClosedNodeErr error
}

func (repository *postCommitPublishRepository) setCorruptNextPlan() {
	repository.mu.Lock()
	repository.corruptNextPlan = true
	repository.mu.Unlock()
}

func (repository *postCommitPublishRepository) setGetNodeErr(nodeID model.NodeID, err error) {
	repository.mu.Lock()
	defer repository.mu.Unlock()
	if err == nil {
		delete(repository.getNodeErr, nodeID)
		return
	}
	repository.getNodeErr[nodeID] = err
}

func (repository *postCommitPublishRepository) setHeartbeatErr(err error) {
	repository.mu.Lock()
	repository.heartbeatErr = err
	repository.mu.Unlock()
}

func (repository *postCommitPublishRepository) setFailClosedNodeErr(err error) {
	repository.mu.Lock()
	repository.failClosedNodeErr = err
	repository.mu.Unlock()
}

func (repository *postCommitPublishRepository) FailClosedNode(ctx context.Context, nodeID model.NodeID, at time.Time) error {
	repository.mu.Lock()
	err := repository.failClosedNodeErr
	repository.mu.Unlock()
	if err != nil {
		return err
	}
	return repository.Repository.FailClosedNode(ctx, nodeID, at)
}

func (repository *postCommitPublishRepository) RegisterNodeWithResources(
	ctx context.Context,
	request storageport.RegisterNodeRequest,
	requested []model.ResourceDescriptor,
	forceNodeGenerationAdvance bool,
) (storageport.NodeRecord, resourcedirectory.TransitionPlan, error) {
	record, plan, err := repository.Repository.RegisterNodeWithResources(ctx, request, requested, forceNodeGenerationAdvance)
	if err == nil {
		repository.mu.Lock()
		corrupt := repository.corruptNextPlan
		repository.corruptNextPlan = false
		repository.mu.Unlock()
		if corrupt {
			// The SQLite transaction already committed; make the in-memory
			// plan impossible to publish so PublishTransition fails.
			plan.RegistrationID = ""
		}
	}
	return record, plan, err
}

func (repository *postCommitPublishRepository) GetNode(ctx context.Context, nodeID model.NodeID) (storageport.NodeRecord, error) {
	repository.mu.Lock()
	err := repository.getNodeErr[nodeID]
	repository.mu.Unlock()
	if err != nil {
		return storageport.NodeRecord{}, err
	}
	return repository.Repository.GetNode(ctx, nodeID)
}

func (repository *postCommitPublishRepository) RenewNodeLease(ctx context.Context, request storageport.RenewNodeLeaseRequest) error {
	repository.mu.Lock()
	err := repository.heartbeatErr
	repository.mu.Unlock()
	if err != nil {
		return err
	}
	return repository.Repository.RenewNodeLease(ctx, request)
}

func newPostCommitPublishServer(t *testing.T, now *time.Time) (*RegistryServer, *postCommitPublishRepository, *resourcedirectory.Directory, *sqliteplatform.Repository) {
	t.Helper()
	database, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "strong-quiesce-registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	repository := &postCommitPublishRepository{Repository: database, getNodeErr: make(map[model.NodeID]error)}
	directory := resourcedirectory.New()
	server, err := NewRegistryServer(capability.NewRegistry(), NewEndpointDirectory(),
		WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return *now }),
		WithNodeRepository(repository), WithResourceDirectory(directory))
	if err != nil {
		t.Fatal(err)
	}
	return server, repository, directory, database
}

// TestGapPostCommitPublicationFailureIsNotStrong reproduces the M3-C gap
// through the production Register path: the Node+Resource transaction commits
// in SQLite, the in-memory PublishTransition fails afterwards, and the owner
// must enter the post_commit_publication_failure strong isolation.
//
// BEFORE FIX: the production path only runs failClosedNode which records the
// recoverable persistence_failure reason, so this test fails.
func TestGapPostCommitPublicationFailureIsNotStrong(t *testing.T) {
	now := time.Date(2026, 8, 6, 11, 0, 0, 0, time.UTC)
	server, repository, directory, database := newPostCommitPublishServer(t, &now)
	request := registerRequest("node-gap-postcommit", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	request.RegistrationId = "registration-1"
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 1 {
		t.Fatalf("eligible after register = %d, want 1", len(got))
	}
	now = now.Add(time.Second)
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-gap-postcommit", RegistrationId: request.RegistrationId, Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}

	repository.setCorruptNextPlan()
	now = now.Add(time.Second)
	second := registerRequest("node-gap-postcommit", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	second.RegistrationId = "registration-2"
	if _, err := server.RegisterNode(context.Background(), second); err == nil {
		t.Fatal("RegisterNode() error = nil; in-memory publish must fail after commit")
	}
	persisted, err := database.GetNode(context.Background(), "node-gap-postcommit")
	if err != nil || persisted.Generation != 2 || persisted.RegistrationID != "registration-2" || persisted.Status != node.StatusOffline {
		t.Fatalf("persisted committed registration = %#v, %v", persisted, err)
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("resource eligible after failed publish: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-gap-postcommit"); reason != resourcedirectory.IneligibleReasonPostCommitFailure {
		t.Fatalf("quiesce reason = %q, want post_commit_publication_failure", reason)
	}
}
