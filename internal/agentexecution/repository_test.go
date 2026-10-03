package agentexecution

import (
	"testing"

	"dtm/internal/mapper"
)

func TestFingerprintCopiesInputsAndComparesRequestIdentity(t *testing.T) {
	inputs := map[string]string{"operation": "read_temperature"}
	fingerprint := NewFingerprint("task-1", mapper.MappedStep{
		ID:         "step-1",
		Capability: "temperature_sensor",
		NodeID:     "node-1",
		Inputs:     inputs,
	})
	inputs["operation"] = "mutated"

	same := NewFingerprint("task-1", mapper.MappedStep{
		ID:         "step-1",
		Capability: "temperature_sensor",
		NodeID:     "node-1",
		Inputs:     map[string]string{"operation": "read_temperature"},
	})
	if !fingerprint.Equal(same) {
		t.Fatalf("fingerprints differ: %#v %#v", fingerprint, same)
	}
	different := same
	different.NodeID = "node-2"
	if fingerprint.Equal(different) {
		t.Fatal("fingerprints with different node IDs compare equal")
	}
}
