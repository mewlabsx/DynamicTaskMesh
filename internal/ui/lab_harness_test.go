package ui

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func writeLabManifest(t *testing.T, manifest LabManifest) string {
	t.Helper()
	directory := t.TempDir()
	path := filepath.Join(directory, "lab-manifest.json")
	data, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadLabManifestResolvesPathsAndRejectsDuplicateNodes(t *testing.T) {
	manifestPath := writeLabManifest(t, LabManifest{
		SessionID: "session-1",
		Runtimes: []RuntimeDescriptor{
			{NodeID: "node-a", Executable: "bin/dtm-runtime.exe", ConfigPath: "configs/a.yaml", ControlEndpoint: "127.0.0.1:45001"},
		},
	})
	manifest, err := LoadLabManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	base := filepath.Dir(manifestPath)
	if manifest.Runtimes[0].Executable != filepath.Join(base, "bin", "dtm-runtime.exe") || manifest.Runtimes[0].WorkingDirectory != filepath.Join(base, "configs") {
		t.Fatalf("resolved runtime = %#v", manifest.Runtimes[0])
	}

	duplicatePath := writeLabManifest(t, LabManifest{
		SessionID: "session-2",
		Runtimes: []RuntimeDescriptor{
			{NodeID: "node-a", Executable: "runtime", ConfigPath: "a.yaml", ControlEndpoint: "127.0.0.1:45001"},
			{NodeID: "node-a", Executable: "runtime", ConfigPath: "b.yaml", ControlEndpoint: "127.0.0.1:45002"},
		},
	})
	if _, err := LoadLabManifest(duplicatePath); err == nil {
		t.Fatal("duplicate node manifest unexpectedly loaded")
	}
}

func TestProcessLabHarnessRejectsUnknownRuntime(t *testing.T) {
	harness, err := NewProcessLabHarness(LabManifest{
		SessionID: "session-1",
		Runtimes:  []RuntimeDescriptor{{NodeID: "node-a", Executable: "runtime", ConfigPath: "config.yaml", ControlEndpoint: "127.0.0.1:45001"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := harness.StopRuntime(nil, "node-missing"); !errors.Is(err, ErrUnknownRuntime) {
		t.Fatalf("error = %v, want ErrUnknownRuntime", err)
	}
	views := harness.Runtimes()
	if len(views) != 1 || views[0].ProcessState != "STOPPED" {
		t.Fatalf("views = %#v", views)
	}
}
