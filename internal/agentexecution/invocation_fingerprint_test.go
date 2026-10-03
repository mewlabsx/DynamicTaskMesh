package agentexecution

import (
	"testing"

	"dtm/internal/mapper"
	"dtm/internal/model"
)

func TestInvocationFingerprintIncludesTargetOperationAndPayloadDigest(t *testing.T) {
	ref, err := model.NewResourceRef("resource-1", 3, "node-1", 4, "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	step := mapper.MappedStep{
		ID: "step-1", Capability: "temperature_sensor", NodeID: ref.OwnerNodeID,
		ResourceRef: ref, Inputs: map[string]string{"operation": "read_temperature"},
	}
	first := NewInvocationFingerprint("task-1", step, "read_temperature", []byte(`{"temperature":26}`))
	second := NewInvocationFingerprint("task-1", step, "read_temperature", []byte(`{"temperature":27}`))
	if first.ResourceRef == nil || *first.ResourceRef != ref {
		t.Fatalf("resource ref = %#v, want %#v", first.ResourceRef, ref)
	}
	if first.OperationID != "read_temperature" || first.PayloadDigest == "" {
		t.Fatalf("native fingerprint = %#v, want operation and digest", first)
	}
	if first.Equal(second) {
		t.Fatal("different opaque payloads reused the same native fingerprint")
	}
	changedTarget := step
	changedTarget.ResourceRef.ResourceGeneration++
	third := NewInvocationFingerprint("task-1", changedTarget, "read_temperature", []byte(`{"temperature":26}`))
	if first.Equal(third) {
		t.Fatal("different target generations reused the same native fingerprint")
	}
}
