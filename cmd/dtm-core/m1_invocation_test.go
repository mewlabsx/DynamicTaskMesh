package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"dtm/internal/lifecycle"
	"dtm/internal/runtimehost"
)

func TestM1GreenhouseRunsThroughInvocationCore(t *testing.T) {
	var evidence bytes.Buffer
	capability, err := runtimehost.NewCoreCapability(context.Background(), resourceSchedulingConfig(":memory:"), runtimehost.CoreOptions{
		ResourceEvidenceWriter: &evidence,
	})
	if err != nil {
		t.Fatal(err)
	}
	deps := dependencies{CoreCapability: capability}
	t.Cleanup(func() { closeM4DDependencies(deps) })
	agentRecorder, address := startM4DAgent(t)
	registerM4DNode(t, deps, "m1-greenhouse-node", address, "m1-registration-1", "temperature_sensor", "cooling_control")

	outcome, err := deps.TaskService.Submit(context.Background(), coolEnvironmentInput("m1-invocation-greenhouse"))
	if err != nil || outcome.State != lifecycle.StateSuccess {
		t.Fatalf("TaskService.Submit() outcome=%#v error=%v", outcome, err)
	}
	if agentRecorder.total() != 2 {
		t.Fatalf("native ResourceInvocationService calls=%d, want 2", agentRecorder.total())
	}
	text := evidence.String()
	if strings.Count(text, "invocation_evidence ") != 2 {
		t.Fatalf("invocation evidence=%q, want two invocation records", text)
	}
	if !strings.Contains(text, `"operation":"read_temperature"`) || !strings.Contains(text, `"operation":"set_target_temperature"`) {
		t.Fatalf("evidence missing explicit operations: %q", text)
	}
	if !strings.Contains(text, `"execution_fence_validation":"legacy/unconfirmed"`) || !strings.Contains(text, `"execution_fence_mode":"none"`) ||
		!strings.Contains(text, `"accepted_target_confirmation":"legacy/unconfirmed"`) || strings.Contains(text, `"execution_fence_mode":"authority-confirmed"`) {
		t.Fatalf("evidence missing legacy fence status: %q", text)
	}
	if strings.Count(text, `"transport_profile":"native/grpc"`) != 2 || strings.Contains(text, `"transport_profile":"legacy/compatibility"`) {
		t.Fatalf("evidence did not distinguish native transport profile: %q", text)
	}
	if !strings.Contains(text, `"caller_fence_validation":"passed/current"`) || !strings.Contains(text, `"status":"success"`) {
		t.Fatalf("evidence missing resolved caller fence or service success status: %q", text)
	}
}
