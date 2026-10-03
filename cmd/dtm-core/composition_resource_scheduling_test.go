package main

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/config"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/task"
)

func resourceSchedulingConfig(path string) config.Core {
	return config.Core{
		Lease:   config.Lease{TTL: config.Duration{Duration: 10 * time.Second}},
		Storage: config.Storage{Path: path},
		Retry:   config.Retry{MaxAttempts: 1, Backoff: config.Duration{}},
	}
}

func coolEnvironmentInput(taskID model.TaskID) task.Task {
	input, err := task.New(taskID, "cool_environment", []model.Capability{"temperature_sensor", "cooling_control"}, task.Constraints{"target_temperature": "26"})
	if err != nil {
		panic(err)
	}
	return input
}

func assertCompleteResourceRefs(t *testing.T, mapped mapper.MappedPlan) {
	t.Helper()
	if len(mapped.Steps) != 2 {
		t.Fatalf("mapped steps = %d, want 2", len(mapped.Steps))
	}
	for _, step := range mapped.Steps {
		if err := step.ResourceRef.Validate(); err != nil {
			t.Fatalf("step %q ResourceRef.Validate() = %v (%+v)", step.ID, err, step.ResourceRef)
		}
		if step.ResourceRef.OwnerNodeID != step.NodeID {
			t.Fatalf("step %q ResourceRef.OwnerNodeID %q != NodeID %q", step.ID, step.ResourceRef.OwnerNodeID, step.NodeID)
		}
		if step.ResourceRef == (model.ResourceRef{}) {
			t.Fatalf("step %q ResourceRef is zero in resource scheduling mode", step.ID)
		}
	}
}

func TestComposeMapperUsesResourceSchedulingWithCompleteResourceRefs(t *testing.T) {
	ctx := context.Background()
	deps, err := composeWithConfig(ctx, resourceSchedulingConfig(filepath.Join(t.TempDir(), "core.db")))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deps.AsyncTasks.Close()
		_ = deps.Repository.Close()
	})
	registration := &dtmv1.RegisterNodeRequest{
		Node:           &dtmv1.Node{Id: "node-resource-sched", Capabilities: []string{"temperature_sensor", "cooling_control"}, Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, ExecutionAddress: "127.0.0.1:5001"},
		RegistrationId: "registration-1",
	}
	if _, err := deps.RegistryAPI.RegisterNode(ctx, registration); err != nil {
		t.Fatal(err)
	}
	plan, err := deps.Planner.Plan(coolEnvironmentInput("task-resource-sched"))
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := deps.Mapper.Map(plan)
	if err != nil {
		t.Fatalf("production Map() error = %v", err)
	}
	assertCompleteResourceRefs(t, mapped)
	for _, step := range mapped.Steps {
		if step.NodeID != "node-resource-sched" {
			t.Fatalf("step %q mapped to %q, want node-resource-sched", step.ID, step.NodeID)
		}
	}
	// The production Mapper must be in Resource Scheduling Mode.
	persisted, err := deps.Repository.GetNode(ctx, "node-resource-sched")
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range mapped.Steps {
		if step.ResourceRef.OwnerNodeGeneration != persisted.Generation || step.ResourceRef.RegistrationID != persisted.RegistrationID {
			t.Fatalf("step %q ref fence %+v does not match persisted node %+v", step.ID, step.ResourceRef, persisted)
		}
	}
}

func TestComposeResourceMappingSurvivesRestartOnlyAfterReregistration(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "core.db")
	first, err := composeWithConfig(ctx, resourceSchedulingConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	registration := &dtmv1.RegisterNodeRequest{
		Node:           &dtmv1.Node{Id: "node-restart-resource", Capabilities: []string{"temperature_sensor", "cooling_control"}, Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, ExecutionAddress: "127.0.0.1:5001"},
		RegistrationId: "registration-1",
	}
	if _, err := first.RegistryAPI.RegisterNode(ctx, registration); err != nil {
		t.Fatal(err)
	}
	plan, err := first.Planner.Plan(coolEnvironmentInput("task-restart-resource"))
	if err != nil {
		t.Fatal(err)
	}
	mapped, err := first.Mapper.Map(plan)
	if err != nil {
		t.Fatal(err)
	}
	assertCompleteResourceRefs(t, mapped)
	before, err := first.Repository.GetNode(ctx, "node-restart-resource")
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range mapped.Steps {
		if step.ResourceRef.OwnerNodeGeneration != before.Generation || step.ResourceRef.RegistrationID != before.RegistrationID {
			t.Fatalf("pre-restart ref fence %+v != node %+v", step.ResourceRef, before)
		}
	}
	first.AsyncTasks.Close()
	if err := first.Repository.Close(); err != nil {
		t.Fatal(err)
	}

	second, err := composeWithConfig(ctx, resourceSchedulingConfig(path))
	if err != nil {
		t.Fatal(err)
	}
	defer second.AsyncTasks.Close()
	defer second.Repository.Close()
	// After restart the authoritative Resource state is restored (Ready) but
	// every Resource is ineligible until the agent re-registers; the Resource
	// path must report authoritative unavailability, never fall back to the
	// Capability Registry, and never report NotReady.
	if len(second.Resources.ListEligible()) != 0 || len(second.Resources.ListIneligible()) != 2 {
		t.Fatalf("restored resources: eligible=%d ineligible=%d", len(second.Resources.ListEligible()), len(second.Resources.ListIneligible()))
	}
	if _, err := second.Mapper.Map(plan); !errors.Is(err, mapper.ErrResourceUnavailable) {
		t.Fatalf("Map() after restart = %v, want %v", err, mapper.ErrResourceUnavailable)
	}
	if _, err := second.Mapper.Map(plan); !errors.Is(err, mapper.ErrCapabilityUnavailable) {
		t.Fatalf("Map() after restart must remain compatible with %v: %v", mapper.ErrCapabilityUnavailable, err)
	}
	if len(second.Registry.Discover("temperature_sensor")) != 0 {
		t.Fatalf("legacy registry was consulted after restart: %v", second.Registry.Discover("temperature_sensor"))
	}

	// Re-registration restores eligibility and a complete new ResourceRef
	// fence matching the new generation.
	registration.Node.ExecutionAddress = "127.0.0.1:5002"
	if _, err := second.RegistryAPI.RegisterNode(ctx, registration); err != nil {
		t.Fatal(err)
	}
	mapped, err = second.Mapper.Map(plan)
	if err != nil {
		t.Fatalf("Map() after reregistration = %v", err)
	}
	assertCompleteResourceRefs(t, mapped)
	restored, err := second.Repository.GetNode(ctx, "node-restart-resource")
	if err != nil || restored.Status != node.StatusActive {
		t.Fatalf("reregistered node = %#v, %v", restored, err)
	}
	for _, step := range mapped.Steps {
		if step.ResourceRef.OwnerNodeGeneration != restored.Generation || step.ResourceRef.RegistrationID != restored.RegistrationID {
			t.Fatalf("post-restart ref fence %+v != node %+v", step.ResourceRef, restored)
		}
	}
}
