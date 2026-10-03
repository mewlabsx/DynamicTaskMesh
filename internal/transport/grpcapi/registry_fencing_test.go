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
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/planner"
	sqliteplatform "dtm/internal/platform/sqlite"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type activationBlockingEndpoints struct {
	*EndpointDirectory
	mu        sync.Mutex
	blockNext bool
	entered   chan struct{}
	release   chan struct{}
}

func newActivationBlockingEndpoints() *activationBlockingEndpoints {
	return &activationBlockingEndpoints{EndpointDirectory: NewEndpointDirectory()}
}

func (endpoints *activationBlockingEndpoints) blockNextActivation() (<-chan struct{}, chan<- struct{}) {
	endpoints.mu.Lock()
	defer endpoints.mu.Unlock()
	endpoints.blockNext = true
	endpoints.entered = make(chan struct{})
	endpoints.release = make(chan struct{})
	return endpoints.entered, endpoints.release
}

func (endpoints *activationBlockingEndpoints) Activate(nodeID model.NodeID) {
	endpoints.mu.Lock()
	block := endpoints.blockNext
	entered, release := endpoints.entered, endpoints.release
	endpoints.blockNext = false
	endpoints.mu.Unlock()
	if block {
		close(entered)
		<-release
	}
	endpoints.EndpointDirectory.Activate(nodeID)
}

func TestNewRegistrationSucceedsAfterPreviousLeaseExpires(t *testing.T) {
	repository, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	now := time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	server, err := NewRegistryServer(registry, endpoints, WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return now }), WithNodeRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	first := registerRequest("lease-owner", []string{"capability-a"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-a")
	first.RegistrationId = "registration-a"
	if _, err := server.RegisterNode(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := registerRequest("lease-owner", []string{"capability-b"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-b")
	second.RegistrationId = "registration-b"
	if _, err := server.RegisterNode(context.Background(), second); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RegisterNode() before expiry code=%v error=%v", status.Code(err), err)
	}
	record, err := repository.GetNode(context.Background(), "lease-owner")
	if err != nil || record.Generation != 1 || record.RegistrationID != "registration-a" || record.Status != node.StatusActive {
		t.Fatalf("owner changed before expiry: record=%#v error=%v", record, err)
	}
	if address, err := endpoints.Resolve("lease-owner"); err != nil || address != "endpoint-a" {
		t.Fatalf("owner endpoint before expiry=%q error=%v", address, err)
	}
	now = now.Add(10 * time.Second)
	if count, err := server.SweepExpired(now); err != nil || count != 1 {
		t.Fatalf("SweepExpired()=%d error=%v", count, err)
	}
	if _, err := server.RegisterNode(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	record, err = repository.GetNode(context.Background(), "lease-owner")
	if err != nil || record.Generation != 2 || record.RegistrationID != "registration-b" || record.Status != node.StatusActive {
		t.Fatalf("owner after expiry: record=%#v error=%v", record, err)
	}
}

func TestExpiredLeaseAllowsTakeoverBeforeSweepAndFencesOldOwner(t *testing.T) {
	repository, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	now := time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC)
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	server, err := NewRegistryServer(registry, endpoints, WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return now }), WithNodeRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	meshMapper, err := mapper.New(registry, mapper.WithEligibility(server))
	if err != nil {
		t.Fatal(err)
	}

	requestA := registerRequest("unswept-takeover", []string{"capability-a"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-a")
	requestA.RegistrationId = "registration-a"
	if _, err := server.RegisterNode(context.Background(), requestA); err != nil {
		t.Fatal(err)
	}
	beforeConflict, err := repository.GetNode(context.Background(), "unswept-takeover")
	if err != nil {
		t.Fatal(err)
	}
	requestB := registerRequest("unswept-takeover", []string{"capability-b"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-b")
	requestB.RegistrationId = "registration-b"
	if _, err := server.RegisterNode(context.Background(), requestB); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("RegisterNode(B) before A expiry code=%v error=%v", status.Code(err), err)
	}
	afterConflict, err := repository.GetNode(context.Background(), "unswept-takeover")
	if err != nil || afterConflict.Generation != beforeConflict.Generation || afterConflict.RegistrationID != beforeConflict.RegistrationID ||
		!afterConflict.LeaseExpiresAt.Equal(beforeConflict.LeaseExpiresAt) || afterConflict.Endpoint != beforeConflict.Endpoint {
		t.Fatalf("A changed after rejected takeover: before=%#v after=%#v error=%v", beforeConflict, afterConflict, err)
	}
	if address, err := endpoints.Resolve("unswept-takeover"); err != nil || address != "endpoint-a" || !server.Eligible("unswept-takeover") {
		t.Fatalf("A runtime after rejected takeover: endpoint=%q eligible=%v error=%v", address, server.Eligible("unswept-takeover"), err)
	}

	now = beforeConflict.LeaseExpiresAt
	if server.Eligible("unswept-takeover") {
		t.Fatal("A remained eligible at absolute Lease expiry before Sweep")
	}
	if _, err := server.RegisterNode(context.Background(), requestB); err != nil {
		t.Fatalf("RegisterNode(B) after expiry before Sweep=%v", err)
	}
	ownedByB, err := repository.GetNode(context.Background(), "unswept-takeover")
	if err != nil || ownedByB.Generation != beforeConflict.Generation+1 || ownedByB.RegistrationID != "registration-b" || ownedByB.Status != node.StatusActive || ownedByB.Endpoint != "endpoint-b" {
		t.Fatalf("B persisted takeover: record=%#v error=%v", ownedByB, err)
	}
	if address, err := endpoints.Resolve("unswept-takeover"); err != nil || address != "endpoint-b" || !server.Eligible("unswept-takeover") {
		t.Fatalf("B runtime takeover: endpoint=%q eligible=%v error=%v", address, server.Eligible("unswept-takeover"), err)
	}
	if len(registry.Discover("capability-a")) != 0 || len(registry.Discover("capability-b")) != 1 {
		t.Fatalf("mixed capability ownership: A=%v B=%v", registry.Discover("capability-a"), registry.Discover("capability-b"))
	}
	if _, err := meshMapper.Map(registrationPlan("capability-a")); !errors.Is(err, mapper.ErrCapabilityUnavailable) {
		t.Fatalf("Map(A) after B takeover=%v", err)
	}
	if _, err := meshMapper.Map(registrationPlan("capability-b")); err != nil {
		t.Fatalf("Map(B) after takeover=%v", err)
	}

	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "unswept-takeover", RegistrationId: "registration-a"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old A Heartbeat code=%v error=%v", status.Code(err), err)
	}
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "unswept-takeover", RegistrationId: "registration-a", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old A Offline code=%v error=%v", status.Code(err), err)
	}
	afterOldRequests, err := repository.GetNode(context.Background(), "unswept-takeover")
	if err != nil || afterOldRequests.Generation != ownedByB.Generation || afterOldRequests.RegistrationID != ownedByB.RegistrationID ||
		!afterOldRequests.LeaseExpiresAt.Equal(ownedByB.LeaseExpiresAt) || afterOldRequests.Status != node.StatusActive || !server.Eligible("unswept-takeover") {
		t.Fatalf("B changed after old A requests: before=%#v after=%#v eligible=%v error=%v", ownedByB, afterOldRequests, server.Eligible("unswept-takeover"), err)
	}

	if count, err := server.SweepExpired(now); err != nil || count != 0 {
		t.Fatalf("SweepExpired() after B takeover count=%d error=%v", count, err)
	}
	afterSweep, err := repository.GetNode(context.Background(), "unswept-takeover")
	if err != nil || afterSweep.Generation != ownedByB.Generation || afterSweep.RegistrationID != "registration-b" || afterSweep.Status != node.StatusActive ||
		afterSweep.Endpoint != "endpoint-b" || !server.Eligible("unswept-takeover") {
		t.Fatalf("B changed by post-takeover Sweep: record=%#v eligible=%v error=%v", afterSweep, server.Eligible("unswept-takeover"), err)
	}
}

func TestSameRegistrationUpdateDoesNotExposeOldEndpointWithNewCapabilities(t *testing.T) {
	database, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repository := &controlledRegistrationRepository{NodeRepository: database}
	now := time.Date(2026, 8, 1, 11, 0, 0, 0, time.UTC)
	registry := capability.NewRegistry()
	endpoints := newActivationBlockingEndpoints()
	server, err := NewRegistryServer(registry, endpoints, WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return now }), WithNodeRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	meshMapper, err := mapper.New(registry, mapper.WithEligibility(server))
	if err != nil {
		t.Fatal(err)
	}
	first := registerRequest("update-node", []string{"capability-old"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-old")
	first.RegistrationId = "registration-stable"
	if _, err := server.RegisterNode(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := meshMapper.Map(registrationPlan("capability-old")); err != nil {
		t.Fatalf("Map(old) before update=%v", err)
	}
	if address, err := endpoints.Resolve("update-node"); err != nil || address != "endpoint-old" {
		t.Fatalf("Resolve() before update=%q error=%v", address, err)
	}
	repository.configure(true, nil)
	entered, release := endpoints.blockNextActivation()
	updated := registerRequest("update-node", []string{"capability-new"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-new")
	updated.RegistrationId = first.RegistrationId
	done := make(chan error, 1)
	go func() { _, err := server.RegisterNode(context.Background(), updated); done <- err }()
	<-repository.entered
	assertUpdateQuiesced(t, server, registry, endpoints.EndpointDirectory, meshMapper, "before SQLite commit")
	close(repository.release)
	<-entered
	assertUpdateQuiesced(t, server, registry, endpoints.EndpointDirectory, meshMapper, "after SQLite commit before ACTIVE")
	record, err := database.GetNode(context.Background(), "update-node")
	if err != nil || record.Endpoint != "endpoint-new" || record.Generation != 1 {
		t.Fatalf("persisted update before activation: record=%#v error=%v", record, err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if address, err := endpoints.Resolve("update-node"); err != nil || address != "endpoint-new" {
		t.Fatalf("Resolve() after update=%q error=%v", address, err)
	}
	if got := registry.Discover("capability-new"); len(got) != 1 || !server.Eligible("update-node") {
		t.Fatalf("new runtime state not published: nodes=%v eligible=%v", got, server.Eligible("update-node"))
	}
	if got := registry.Discover("capability-old"); len(got) != 0 {
		t.Fatalf("old capability after update=%v", got)
	}
	if _, err := meshMapper.Map(registrationPlan("capability-old")); !errors.Is(err, mapper.ErrCapabilityUnavailable) {
		t.Fatalf("Map(old) after update=%v", err)
	}
	if _, err := meshMapper.Map(registrationPlan("capability-new")); err != nil {
		t.Fatalf("Map(new) after update=%v", err)
	}
}

func registrationPlan(capabilityName model.Capability) planner.Plan {
	return planner.Plan{TaskID: "task-registration-update", Steps: []planner.Step{{ID: "step-1", Capability: capabilityName}}}
}

func assertUpdateQuiesced(t *testing.T, server *RegistryServer, registry *capability.Registry, endpoints *EndpointDirectory, meshMapper *mapper.Mapper, checkpoint string) {
	t.Helper()
	if server.Eligible("update-node") || len(registry.Discover("capability-old")) != 0 || len(registry.Discover("capability-new")) != 0 {
		t.Fatalf("%s: update became schedulable", checkpoint)
	}
	if _, err := endpoints.Resolve("update-node"); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("%s: Resolve() error=%v", checkpoint, err)
	}
	for _, capabilityName := range []model.Capability{"capability-old", "capability-new"} {
		if _, err := meshMapper.Map(registrationPlan(capabilityName)); !errors.Is(err, mapper.ErrCapabilityUnavailable) {
			t.Fatalf("%s: Map(%s)=%v", checkpoint, capabilityName, err)
		}
	}
}

func TestIdenticalRegistrationReplayKeepsRuntimeStable(t *testing.T) {
	database, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repository := &controlledRegistrationRepository{NodeRepository: database}
	now := time.Date(2026, 8, 1, 12, 0, 0, 0, time.UTC)
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	server, err := NewRegistryServer(registry, endpoints, WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return now }), WithNodeRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	request := registerRequest("replay-node", []string{"capability-z", "capability-a"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-stable")
	request.RegistrationId = "registration-stable"
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	repository.configure(true, nil)
	now = now.Add(time.Second)
	done := make(chan error, 1)
	replay := registerRequest("replay-node", []string{"capability-a", "capability-z"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-stable")
	replay.RegistrationId = request.RegistrationId
	go func() { _, err := server.RegisterNode(context.Background(), replay); done <- err }()
	<-repository.entered
	if !server.Eligible("replay-node") || len(registry.Discover("capability-a")) != 1 || len(registry.Discover("capability-z")) != 1 {
		t.Fatal("identical replay quiesced stable runtime state")
	}
	if address, err := endpoints.Resolve("replay-node"); err != nil || address != "endpoint-stable" {
		t.Fatalf("stable endpoint during replay=%q error=%v", address, err)
	}
	close(repository.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	record, err := database.GetNode(context.Background(), "replay-node")
	if err != nil || record.Generation != 1 || record.RegistrationID != "registration-stable" {
		t.Fatalf("identical replay changed ownership: record=%#v error=%v", record, err)
	}
}

func TestSameRegistrationUpdateFailureRemainsQuiesced(t *testing.T) {
	database, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	repository := &controlledRegistrationRepository{NodeRepository: database}
	now := time.Date(2026, 8, 1, 13, 0, 0, 0, time.UTC)
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	server, err := NewRegistryServer(registry, endpoints, WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return now }), WithNodeRepository(repository))
	if err != nil {
		t.Fatal(err)
	}
	first := registerRequest("failed-update", []string{"capability-old"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-old")
	first.RegistrationId = "registration-stable"
	if _, err := server.RegisterNode(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	repository.configure(false, errors.New("injected update failure"))
	updated := registerRequest("failed-update", []string{"capability-new"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-new")
	updated.RegistrationId = first.RegistrationId
	if _, err := server.RegisterNode(context.Background(), updated); status.Code(err) != codes.Unavailable {
		t.Fatalf("failed update code=%v error=%v", status.Code(err), err)
	}
	if server.Eligible("failed-update") || len(registry.Discover("capability-old")) != 0 || len(registry.Discover("capability-new")) != 0 {
		t.Fatal("failed same-registration update restored schedulability")
	}
	if _, err := endpoints.Resolve("failed-update"); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("failed update endpoint error=%v", err)
	}
	record, err := database.GetNode(context.Background(), "failed-update")
	if err != nil || record.Status != node.StatusOffline || record.Generation != 1 {
		t.Fatalf("failed update persistence state: record=%#v error=%v", record, err)
	}
	repository.configure(false, nil)
	now = now.Add(time.Second)
	if _, err := server.RegisterNode(context.Background(), updated); err != nil {
		t.Fatal(err)
	}
	record, err = database.GetNode(context.Background(), "failed-update")
	if err != nil || record.Status != node.StatusActive || record.Generation != 2 {
		t.Fatalf("rebuild after failed update: record=%#v error=%v", record, err)
	}
}
