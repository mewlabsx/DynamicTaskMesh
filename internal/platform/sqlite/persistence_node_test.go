package sqlite

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"dtm/internal/model"
	"dtm/internal/node"
	storageport "dtm/internal/storage"
)

func TestRegisterNodePersistsGenerationAndReplacesCapabilitySnapshot(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	firstAt := testTime(0)
	first, err := repository.RegisterNode(ctx, nodeRegistration(
		"node-m3", "registration-1", "127.0.0.1:5001",
		[]model.Capability{"temperature_sensor", "cooling_control"}, firstAt,
	))
	if err != nil {
		t.Fatal(err)
	}
	if first.Generation != 1 || first.Status != node.StatusActive ||
		!reflect.DeepEqual(first.Capabilities, []model.Capability{"cooling_control", "temperature_sensor"}) ||
		first.RegistrationID != "registration-1" || first.Metadata == nil {
		t.Fatalf("first registration = %#v", first)
	}

	replayedAt := firstAt.Add(time.Second)
	replayed, err := repository.RegisterNode(ctx, nodeRegistration(
		"node-m3", "registration-1", "127.0.0.1:5002",
		[]model.Capability{"temperature_sensor"}, replayedAt,
	))
	if err != nil {
		t.Fatal(err)
	}
	if replayed.Generation != 1 || replayed.Endpoint != "127.0.0.1:5002" ||
		!reflect.DeepEqual(replayed.Capabilities, []model.Capability{"temperature_sensor"}) ||
		!replayed.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("idempotent registration replay = %#v", replayed)
	}

	replacedAt := replayedAt.Add(time.Second)
	replaced, err := repository.RegisterNode(ctx, nodeRegistration(
		"node-m3", "registration-2", "127.0.0.1:5003",
		[]model.Capability{"cooling_control"}, replacedAt,
	))
	if err != nil {
		t.Fatal(err)
	}
	if replaced.Generation != 2 || replaced.RegistrationID != "registration-2" ||
		!reflect.DeepEqual(replaced.Capabilities, []model.Capability{"cooling_control"}) ||
		!replaced.CreatedAt.Equal(first.CreatedAt) {
		t.Fatalf("replacement registration = %#v", replaced)
	}
}

func TestNodeHeartbeatOfflineAndExpiryRequireCurrentRegistration(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	registeredAt := testTime(0)
	if _, err := repository.RegisterNode(ctx, nodeRegistration("node-heartbeat", "current", "endpoint", []model.Capability{"temperature_sensor"}, registeredAt)); err != nil {
		t.Fatal(err)
	}
	heartbeatAt := registeredAt.Add(2 * time.Second)
	if err := repository.RenewNodeLease(ctx, storageport.RenewNodeLeaseRequest{
		ID: "node-heartbeat", RegistrationID: "current", LastHeartbeatAt: heartbeatAt,
		LeaseExpiresAt: heartbeatAt.Add(10 * time.Second),
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.RenewNodeLease(ctx, storageport.RenewNodeLeaseRequest{
		ID: "node-heartbeat", RegistrationID: "old", LastHeartbeatAt: heartbeatAt.Add(time.Second),
		LeaseExpiresAt: heartbeatAt.Add(11 * time.Second),
	}); !errors.Is(err, storageport.ErrConflict) {
		t.Fatalf("stale heartbeat error = %v", err)
	}
	record, err := repository.GetNode(ctx, "node-heartbeat")
	if err != nil {
		t.Fatal(err)
	}
	if !record.LastHeartbeatAt.Equal(heartbeatAt) || !record.LeaseExpiresAt.Equal(heartbeatAt.Add(10*time.Second)) || record.Status != node.StatusActive {
		t.Fatalf("heartbeat record = %#v", record)
	}
	if err := repository.SetNodeOffline(ctx, storageport.SetNodeOfflineRequest{ID: record.ID, RegistrationID: "current", UpdatedAt: heartbeatAt.Add(3 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	record, err = repository.GetNode(ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != node.StatusOffline {
		t.Fatalf("offline record = %#v", record)
	}

	if _, err := repository.RegisterNode(ctx, nodeRegistration("node-expire", "expiry", "endpoint", []model.Capability{"temperature_sensor"}, registeredAt)); err != nil {
		t.Fatal(err)
	}
	if count, err := repository.ExpireNodeLeases(ctx, registeredAt.Add(9*time.Second)); err != nil || count != 0 {
		t.Fatalf("early expiry = %d, %v", count, err)
	}
	if count, err := repository.ExpireNodeLeases(ctx, registeredAt.Add(10*time.Second)); err != nil || count != 1 {
		t.Fatalf("due expiry = %d, %v", count, err)
	}
	expired, err := repository.GetNode(ctx, "node-expire")
	if err != nil || expired.Status != node.StatusOffline {
		t.Fatalf("expired record = %#v, %v", expired, err)
	}
}

func TestMarkNodesStaleIsRepeatableAndDoesNotChangeOfflineNodes(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	now := testTime(0)
	for _, id := range []model.NodeID{"node-active", "node-offline"} {
		if _, err := repository.RegisterNode(ctx, nodeRegistration(id, "registration-"+string(id), "endpoint", []model.Capability{"temperature_sensor"}, now)); err != nil {
			t.Fatal(err)
		}
	}
	if err := repository.SetNodeOffline(ctx, storageport.SetNodeOfflineRequest{ID: "node-offline", RegistrationID: "registration-node-offline", UpdatedAt: now.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	if count, err := repository.MarkNodesStale(ctx, now.Add(2*time.Second)); err != nil || count != 1 {
		t.Fatalf("MarkNodesStale() = %d, %v", count, err)
	}
	if count, err := repository.MarkNodesStale(ctx, now.Add(3*time.Second)); err != nil || count != 0 {
		t.Fatalf("repeated MarkNodesStale() = %d, %v", count, err)
	}
	active, _ := repository.GetNode(ctx, "node-active")
	offline, _ := repository.GetNode(ctx, "node-offline")
	if active.Status != node.StatusStale || offline.Status != node.StatusOffline {
		t.Fatalf("startup statuses: active=%s offline=%s", active.Status, offline.Status)
	}
}

func TestRegisterNodeRollsBackOnWriteFailureAndRejectsInvalidStoredSnapshot(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	if _, err := repository.db.Exec(`CREATE TRIGGER fail_node_insert BEFORE INSERT ON nodes BEGIN SELECT RAISE(ABORT, "node insert failed"); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.RegisterNode(ctx, nodeRegistration("node-failed", "registration", "endpoint", []model.Capability{"temperature_sensor"}, testTime(0))); err == nil {
		t.Fatal("RegisterNode() error = nil")
	}
	if _, err := repository.GetNode(ctx, "node-failed"); !errors.Is(err, storageport.ErrNotFound) {
		t.Fatalf("failed registration remained in database: %v", err)
	}
	if _, err := repository.db.Exec("DROP TRIGGER fail_node_insert"); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.RegisterNode(ctx, nodeRegistration("node-invalid", "registration", "endpoint", []model.Capability{"temperature_sensor"}, testTime(0))); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []string{
		"UPDATE nodes SET status='unknown-status' WHERE node_id='node-invalid'",
		"UPDATE nodes SET status='active', capabilities_json='{' WHERE node_id='node-invalid'",
		"UPDATE nodes SET capabilities_json='[]', lease_expires_at=last_heartbeat_at WHERE node_id='node-invalid'",
	} {
		if _, err := repository.db.Exec(mutation); err != nil {
			t.Fatal(err)
		}
		if _, err := repository.GetNode(ctx, "node-invalid"); !errors.Is(err, storageport.ErrInvalidData) {
			t.Fatalf("invalid stored snapshot error = %v after %s", err, mutation)
		}
	}
}

func nodeRegistration(id model.NodeID, registrationID, endpoint string, capabilities []model.Capability, at time.Time) storageport.RegisterNodeRequest {
	return storageport.RegisterNodeRequest{
		ID: id, Endpoint: endpoint, Capabilities: capabilities,
		RegistrationID: registrationID, Metadata: map[string]string{},
		RegisteredAt: at, LeaseExpiresAt: at.Add(10 * time.Second),
	}
}

func TestRenewNodeLeaseRejectsBackwardHeartbeat(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	registeredAt := testTime(0)
	if _, err := repository.RegisterNode(ctx, nodeRegistration("node-backward-heartbeat", "registration", "endpoint", []model.Capability{"temperature_sensor"}, registeredAt)); err != nil {
		t.Fatal(err)
	}
	forward := registeredAt.Add(2 * time.Second)
	if err := repository.RenewNodeLease(ctx, storageport.RenewNodeLeaseRequest{ID: "node-backward-heartbeat", RegistrationID: "registration", LastHeartbeatAt: forward, LeaseExpiresAt: forward.Add(10 * time.Second)}); err != nil {
		t.Fatal(err)
	}
	err := repository.RenewNodeLease(ctx, storageport.RenewNodeLeaseRequest{ID: "node-backward-heartbeat", RegistrationID: "registration", LastHeartbeatAt: forward.Add(-time.Nanosecond), LeaseExpiresAt: forward.Add(10 * time.Second)})
	if !errors.Is(err, storageport.ErrNodeTimeRegression) {
		t.Fatalf("backward heartbeat error = %v", err)
	}
}

func TestRegisterNodeReplayDoesNotRegressTimestamps(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	registeredAt := testTime(0)
	if _, err := repository.RegisterNode(ctx, nodeRegistration("node-replay-time", "registration", "endpoint-a", []model.Capability{"temperature_sensor"}, registeredAt)); err != nil {
		t.Fatal(err)
	}
	forward := registeredAt.Add(2 * time.Second)
	if _, err := repository.RegisterNode(ctx, nodeRegistration("node-replay-time", "registration", "endpoint-b", []model.Capability{"temperature_sensor"}, forward)); err != nil {
		t.Fatal(err)
	}
	_, err := repository.RegisterNode(ctx, nodeRegistration("node-replay-time", "registration", "endpoint-c", []model.Capability{"temperature_sensor"}, forward.Add(-time.Nanosecond)))
	if !errors.Is(err, storageport.ErrNodeTimeRegression) {
		t.Fatalf("backward replay error = %v", err)
	}
	record, err := repository.GetNode(ctx, "node-replay-time")
	if err != nil || record.Endpoint != "endpoint-b" || !record.UpdatedAt.Equal(forward) {
		t.Fatalf("record after rejected replay = %#v, %v", record, err)
	}
}

func TestOfflineDoesNotRegressUpdatedAt(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	registeredAt := testTime(0)
	if _, err := repository.RegisterNode(ctx, nodeRegistration("node-offline-time", "registration", "endpoint", []model.Capability{"temperature_sensor"}, registeredAt)); err != nil {
		t.Fatal(err)
	}
	err := repository.SetNodeOffline(ctx, storageport.SetNodeOfflineRequest{ID: "node-offline-time", RegistrationID: "registration", UpdatedAt: registeredAt.Add(-time.Nanosecond)})
	if !errors.Is(err, storageport.ErrNodeTimeRegression) {
		t.Fatalf("backward offline error = %v", err)
	}
}

func TestExpireNodeLeaseDoesNotRegressUpdatedAt(t *testing.T) {
	repository := newRepository(t)
	now := testTime(0)
	record := storageport.NodeRecord{ID: "node-expire-time", Endpoint: "endpoint", Capabilities: []model.Capability{"temperature_sensor"}, Status: node.StatusActive, Generation: 1, RegistrationID: "registration", Metadata: map[string]string{}, RegisteredAt: now, LastHeartbeatAt: now, LeaseExpiresAt: now.Add(5 * time.Second), CreatedAt: now, UpdatedAt: now.Add(10 * time.Second)}
	if err := repository.UpsertNode(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.ExpireNodeLeases(context.Background(), now.Add(5*time.Second)); !errors.Is(err, storageport.ErrNodeTimeRegression) {
		t.Fatalf("backward expiry error = %v", err)
	}
}

func TestMarkNodesStaleDoesNotRegressUpdatedAt(t *testing.T) {
	repository := newRepository(t)
	now := testTime(0)
	record := storageport.NodeRecord{ID: "node-stale-time", Endpoint: "endpoint", Capabilities: []model.Capability{"temperature_sensor"}, Status: node.StatusActive, Generation: 1, RegistrationID: "registration", Metadata: map[string]string{}, RegisteredAt: now, LastHeartbeatAt: now, LeaseExpiresAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now.Add(10 * time.Second)}
	if err := repository.UpsertNode(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.MarkNodesStale(context.Background(), now.Add(5*time.Second)); !errors.Is(err, storageport.ErrNodeTimeRegression) {
		t.Fatalf("backward stale error = %v", err)
	}
}

func TestNodeRecordValidationRejectsInconsistentTimestamps(t *testing.T) {
	now := testTime(0)
	base := storageport.NodeRecord{ID: "node-invalid-time", Endpoint: "endpoint", Capabilities: []model.Capability{"temperature_sensor"}, Status: node.StatusActive, Generation: 1, RegistrationID: "registration", Metadata: map[string]string{}, RegisteredAt: now, LastHeartbeatAt: now, LeaseExpiresAt: now.Add(time.Minute), CreatedAt: now, UpdatedAt: now}
	tests := []struct {
		name   string
		mutate func(*storageport.NodeRecord)
	}{
		{name: "registered_before_created", mutate: func(record *storageport.NodeRecord) { record.CreatedAt = now.Add(time.Second) }},
		{name: "heartbeat_before_registered", mutate: func(record *storageport.NodeRecord) { record.RegisteredAt = now.Add(time.Second) }},
		{name: "lease_not_after_heartbeat", mutate: func(record *storageport.NodeRecord) { record.LeaseExpiresAt = now }},
		{name: "updated_before_registered", mutate: func(record *storageport.NodeRecord) {
			record.RegisteredAt = now.Add(time.Second)
			record.LastHeartbeatAt = now.Add(time.Second)
			record.LeaseExpiresAt = now.Add(time.Minute)
		}},
		{name: "updated_before_heartbeat", mutate: func(record *storageport.NodeRecord) { record.LastHeartbeatAt = now.Add(time.Second) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository := newRepository(t)
			record := base
			test.mutate(&record)
			if err := repository.UpsertNode(context.Background(), record); !errors.Is(err, storageport.ErrInvalidData) {
				t.Fatalf("invalid timestamp error = %v", err)
			}
		})
	}
}

func TestStaleNodeHeartbeatRequiresReregistration(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	registeredAt := testTime(0)
	if _, err := repository.RegisterNode(ctx, nodeRegistration("node-reregister", "registration", "endpoint", []model.Capability{"temperature_sensor"}, registeredAt)); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.MarkNodesStale(ctx, registeredAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	err := repository.RenewNodeLease(ctx, storageport.RenewNodeLeaseRequest{ID: "node-reregister", RegistrationID: "registration", LastHeartbeatAt: registeredAt.Add(2 * time.Second), LeaseExpiresAt: registeredAt.Add(12 * time.Second)})
	if !errors.Is(err, storageport.ErrNodeReregistrationRequired) || errors.Is(err, storageport.ErrStaleRegistration) {
		t.Fatalf("stale heartbeat error = %v", err)
	}
}
