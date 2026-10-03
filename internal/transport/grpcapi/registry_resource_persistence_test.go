package grpcapi

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/capability"
	"dtm/internal/model"
	sqliteplatform "dtm/internal/platform/sqlite"
	"dtm/internal/resourcedirectory"
)

func TestRegistryPersistsLegacyResourceShadowAndSynchronizesLifecycle(t *testing.T) {
	repository, err := sqliteplatform.Open(filepath.Join(t.TempDir(), "resource-registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	now := time.Date(2026, 8, 5, 1, 2, 3, 0, time.UTC)
	directory := resourcedirectory.New()
	server, err := NewRegistryServer(capability.NewRegistry(), NewEndpointDirectory(),
		WithLeaseTTL(10*time.Second), WithRegistryClock(func() time.Time { return now }),
		WithNodeRepository(repository), WithResourceDirectory(directory),
	)
	if err != nil {
		t.Fatal(err)
	}
	request := registerRequest("node-resource-shadow", []string{"temperature_sensor", "cooling_control"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-1")
	request.RegistrationId = "registration-1"
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if got := directory.ListEligible(); len(got) != 2 {
		t.Fatalf("eligible resources=%d want=2: %#v", len(got), got)
	}
	persisted, err := repository.LoadResourceRecordsByNode(context.Background(), "node-resource-shadow")
	if err != nil || len(persisted) != 2 {
		t.Fatalf("persisted resources=%d err=%v", len(persisted), err)
	}

	now = now.Add(time.Second)
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-resource-shadow", RegistrationId: "registration-1", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	request = registerRequest("node-resource-shadow", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-2")
	request.RegistrationId = "registration-2"
	if _, err := server.RegisterNode(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if len(directory.ListEligible()) != 1 || len(directory.ListWithdrawn()) != 1 {
		t.Fatalf("after replacement eligible=%d withdrawn=%d", len(directory.ListEligible()), len(directory.ListWithdrawn()))
	}
	temperatureID, _ := model.LegacyCapabilityResourceID("node-resource-shadow", "temperature_sensor")
	temperature, err := directory.GetByID(temperatureID)
	if err != nil || temperature.Descriptor.Generation != 2 || temperature.NodeGeneration != 2 {
		t.Fatalf("temperature after replacement=%#v err=%v", temperature, err)
	}

	now = now.Add(10 * time.Second)
	if count, err := server.SweepExpired(now); err != nil || count != 1 {
		t.Fatalf("SweepExpired() count=%d err=%v", count, err)
	}
	if len(directory.ListEligible()) != 0 || len(directory.ListIneligible()) != 1 {
		t.Fatalf("after sweep eligible=%d ineligible=%d", len(directory.ListEligible()), len(directory.ListIneligible()))
	}
}
