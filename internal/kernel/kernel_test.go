package kernel

import "testing"

func TestNewWiresManagers(t *testing.T) {
	k := New()
	if k == nil || k.Resources == nil || k.Capabilities == nil || k.Executions == nil || k.Events == nil {
		t.Fatalf("New() returned incomplete kernel: %#v", k)
	}
	if k.Resources != k.ResourceManager || k.Capabilities != k.CapabilityManager || k.Executions != k.ExecutionManager || k.Events != k.EventManager {
		t.Fatal("kernel manager aliases do not point to the same managers")
	}
}
