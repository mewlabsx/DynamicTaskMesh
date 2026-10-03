package grpcapi

import (
	"context"
	"errors"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/node"
	"dtm/internal/resourcedirectory"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegisterPostCommitPublishFailureEstablishesStrongQuiesce(t *testing.T) {
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	server, repository, directory, database := newPostCommitPublishServer(t, &now)
	first := registerRequest("node-postcommit", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	first.RegistrationId = "registration-1"
	if _, err := server.RegisterNode(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 1 {
		t.Fatalf("eligible after first register = %d, want 1", len(got))
	}
	now = now.Add(time.Second)
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-postcommit", RegistrationId: first.RegistrationId, Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}

	repository.setCorruptNextPlan()
	now = now.Add(time.Second)
	second := registerRequest("node-postcommit", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	second.RegistrationId = "registration-2"
	if _, err := server.RegisterNode(context.Background(), second); err == nil {
		t.Fatal("RegisterNode() error = nil; in-memory publish must fail after commit")
	}
	persisted, err := database.GetNode(context.Background(), "node-postcommit")
	if err != nil || persisted.Generation != 2 || persisted.RegistrationID != "registration-2" {
		t.Fatalf("committed registration = %#v, %v", persisted, err)
	}
	if reason := directory.NodeQuiesceReason("node-postcommit"); reason != resourcedirectory.IneligibleReasonPostCommitFailure {
		t.Fatalf("quiesce reason = %q, want post_commit_publication_failure", reason)
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("resource eligible after post-commit publish failure: %#v", got)
	}

	now = now.Add(2 * time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-postcommit", RegistrationId: second.RegistrationId}); status.Code(err) != codes.NotFound {
		t.Fatalf("heartbeat after failed publish code = %v, %v; runtime lease was removed by fail-closed", status.Code(err), err)
	}
	if reason := directory.NodeQuiesceReason("node-postcommit"); reason != resourcedirectory.IneligibleReasonPostCommitFailure {
		t.Fatalf("heartbeat degraded strong quiesce to %q", reason)
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("heartbeat restored eligibility: %#v", got)
	}

	now = now.Add(time.Second)
	if _, err := server.RegisterNode(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 1 {
		t.Fatalf("full re-register did not finalize activation: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-postcommit"); reason != resourcedirectory.IneligibleReasonNone {
		t.Fatalf("reason after full re-register = %q", reason)
	}
	persisted, err = database.GetNode(context.Background(), "node-postcommit")
	if err != nil || persisted.RegistrationID != "registration-2" {
		t.Fatalf("final registration = %#v, %v", persisted, err)
	}
	got := directory.ListByNode("node-postcommit")
	if len(got) != 1 || got[0].NodeGeneration != persisted.Generation || got[0].RegistrationID != persisted.RegistrationID {
		t.Fatalf("new registration fence not effective: %#v", got)
	}

	now = now.Add(time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-postcommit", RegistrationId: first.RegistrationId}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old registration heartbeat code = %v, %v", status.Code(err), err)
	}
	if got := directory.ListEligible(); len(got) != 1 {
		t.Fatalf("old registration heartbeat disturbed new state: %#v", got)
	}
}

func TestPostCommitStrongSurvivesHealthyHeartbeatAndRefresh(t *testing.T) {
	now := time.Date(2026, 8, 6, 13, 0, 0, 0, time.UTC)
	server, _, directory, _ := newPostCommitPublishServer(t, &now)
	request := registerRequest("node-healthy-strong", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	request.RegistrationId = "registration-1"
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(directory.ListEligible()) != 1 {
		t.Fatal("resource not eligible before strong isolation")
	}

	directory.QuiesceNode("node-healthy-strong", resourcedirectory.IneligibleReasonPostCommitFailure)
	now = now.Add(2 * time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-healthy-strong", RegistrationId: request.RegistrationId}); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("healthy heartbeat cleared strong quiesce: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-healthy-strong"); reason != resourcedirectory.IneligibleReasonPostCommitFailure {
		t.Fatalf("reason after heartbeat = %q", reason)
	}
	if count, err := server.SweepExpired(now.Add(time.Second)); count != 0 || err != nil {
		t.Fatalf("SweepExpired() = %d, %v", count, err)
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("refresh cleared strong quiesce: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-healthy-strong"); reason != resourcedirectory.IneligibleReasonPostCommitFailure {
		t.Fatalf("reason after refresh = %q", reason)
	}
}

func TestRecoverableReasonCannotOverridePostCommitStrong(t *testing.T) {
	now := time.Date(2026, 8, 6, 14, 0, 0, 0, time.UTC)
	server, repository, directory, _ := newPostCommitPublishServer(t, &now)
	request := registerRequest("node-no-override", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	request.RegistrationId = "registration-1"
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	directory.QuiesceNode("node-no-override", resourcedirectory.IneligibleReasonPostCommitFailure)

	repository.setHeartbeatErr(errors.New("transient heartbeat write failure"))
	now = now.Add(2 * time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-no-override", RegistrationId: request.RegistrationId}); err == nil {
		t.Fatal("Heartbeat() error = nil")
	}
	if reason := directory.NodeQuiesceReason("node-no-override"); reason != resourcedirectory.IneligibleReasonPostCommitFailure {
		t.Fatalf("recoverable fail-closed overrode post-commit strong: %q", reason)
	}
	repository.setHeartbeatErr(nil)
	now = now.Add(time.Second)
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 1 {
		t.Fatalf("full re-register did not restore eligibility: %#v", got)
	}
}

func TestPostCommitPublishFailureIsolatesOnlyTargetNode(t *testing.T) {
	now := time.Date(2026, 8, 6, 15, 0, 0, 0, time.UTC)
	server, repository, directory, _ := newPostCommitPublishServer(t, &now)
	healthy := registerRequest("node-healthy", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-healthy")
	healthy.RegistrationId = "registration-healthy"
	if _, err := server.RegisterNode(context.Background(), healthy); err != nil {
		t.Fatal(err)
	}

	repository.setCorruptNextPlan()
	failing := registerRequest("node-failing", []string{"cooling_control"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-failing")
	failing.RegistrationId = "registration-failing"
	if _, err := server.RegisterNode(context.Background(), failing); err == nil {
		t.Fatal("failing node RegisterNode() error = nil")
	}
	if reason := directory.NodeQuiesceReason("node-failing"); reason != resourcedirectory.IneligibleReasonPostCommitFailure {
		t.Fatalf("failing node reason = %q", reason)
	}
	if reason := directory.NodeQuiesceReason("node-healthy"); reason != resourcedirectory.IneligibleReasonNone {
		t.Fatalf("healthy node was quiesced: %q", reason)
	}
	if got := directory.ListEligible(); len(got) != 1 || got[0].Descriptor.OwnerNodeID != "node-healthy" {
		t.Fatalf("unrelated node disturbed: %#v", got)
	}
	now = now.Add(2 * time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-healthy", RegistrationId: healthy.RegistrationId}); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 1 || got[0].Descriptor.OwnerNodeID != "node-healthy" {
		t.Fatalf("healthy node heartbeat disturbed: %#v", got)
	}
	if count, err := server.SweepExpired(now.Add(time.Second)); count != 0 || err != nil {
		t.Fatalf("refresh error = %d, %v", count, err)
	}
	if got := directory.ListEligible(); len(got) != 1 || got[0].Descriptor.OwnerNodeID != "node-healthy" {
		t.Fatalf("refresh disturbed healthy node: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-failing"); reason != resourcedirectory.IneligibleReasonPostCommitFailure {
		t.Fatalf("refresh cleared failing node strong quiesce: %q", reason)
	}
}

func TestFailClosedInternalFailurePreservesStrongQuiesce(t *testing.T) {
	now := time.Date(2026, 8, 6, 16, 0, 0, 0, time.UTC)
	server, repository, directory, _ := newPostCommitPublishServer(t, &now)
	first := registerRequest("node-failclosed-internal", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	first.RegistrationId = "registration-1"
	if _, err := server.RegisterNode(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-failclosed-internal", RegistrationId: first.RegistrationId, Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}

	repository.setFailClosedNodeErr(errors.New("fail-closed persistence write failed"))
	repository.setCorruptNextPlan()
	now = now.Add(time.Second)
	second := registerRequest("node-failclosed-internal", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	second.RegistrationId = "registration-2"
	if _, err := server.RegisterNode(context.Background(), second); err == nil {
		t.Fatal("RegisterNode() error = nil")
	}
	if reason := directory.NodeQuiesceReason("node-failclosed-internal"); reason != resourcedirectory.IneligibleReasonPostCommitFailure {
		t.Fatalf("failClosedNode internal failure degraded strong quiesce to %q", reason)
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("resource eligible after failed publish: %#v", got)
	}
	repository.setFailClosedNodeErr(nil)
	now = now.Add(time.Second)
	if _, err := server.RegisterNode(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 1 {
		t.Fatalf("re-register after internal failure did not restore eligibility: %#v", got)
	}
}

func TestRegisterHappyPathWithPostCommitWrapperNoRegression(t *testing.T) {
	now := time.Date(2026, 8, 6, 17, 0, 0, 0, time.UTC)
	server, _, directory, database := newPostCommitPublishServer(t, &now)
	request := registerRequest("node-happy-path", []string{"temperature_sensor", "cooling_control"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	request.RegistrationId = "registration-1"
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 2 {
		t.Fatalf("eligible after register = %d, want 2", len(got))
	}
	if reason := directory.NodeQuiesceReason("node-happy-path"); reason != resourcedirectory.IneligibleReasonNone {
		t.Fatalf("happy path quiesce reason = %q", reason)
	}
	persisted, err := database.GetNode(context.Background(), "node-happy-path")
	if err != nil || persisted.Generation != 1 || persisted.Status != node.StatusActive {
		t.Fatalf("happy path record = %#v, %v", persisted, err)
	}
	now = now.Add(2 * time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-happy-path", RegistrationId: request.RegistrationId}); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 2 {
		t.Fatalf("happy path heartbeat disturbed resources: %#v", got)
	}
}

func TestZeroValueQuiesceThroughProductionHeartbeatStaysStrong(t *testing.T) {
	now := time.Date(2026, 8, 6, 18, 0, 0, 0, time.UTC)
	server, _, directory, _ := newPostCommitPublishServer(t, &now)
	request := registerRequest("node-zero-value", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	request.RegistrationId = "registration-1"
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	directory.QuiesceNode("node-zero-value", resourcedirectory.IneligibleReasonNone)
	directory.QuiesceNode("node-zero-value", resourcedirectory.IneligibleReasonRepositoryReadFailure)
	if reason := directory.NodeQuiesceReason("node-zero-value"); reason != resourcedirectory.IneligibleReasonUnknownQuiesce {
		t.Fatalf("zero-value quiesce normalized to %q", reason)
	}
	now = now.Add(2 * time.Second)
	if _, err := server.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "node-zero-value", RegistrationId: request.RegistrationId}); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 0 {
		t.Fatalf("healthy heartbeat cleared zero-value strong quiesce: %#v", got)
	}
	if reason := directory.NodeQuiesceReason("node-zero-value"); reason != resourcedirectory.IneligibleReasonUnknownQuiesce {
		t.Fatalf("reason after heartbeat = %q", reason)
	}
	now = now.Add(time.Second)
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 1 {
		t.Fatalf("full re-register did not clear zero-value quiesce: %#v", got)
	}
}
