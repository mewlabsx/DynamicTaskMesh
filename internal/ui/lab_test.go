package ui

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type testLabHarness struct {
	mu           sync.Mutex
	runtimes     []LabRuntimeView
	stopped      []string
	startEntered chan struct{}
	startRelease chan struct{}
	startOnce    sync.Once
}

func (harness *testLabHarness) SessionID() string { return "test-session" }

func (harness *testLabHarness) Runtimes() []LabRuntimeView {
	harness.mu.Lock()
	defer harness.mu.Unlock()
	return append([]LabRuntimeView(nil), harness.runtimes...)
}

func (harness *testLabHarness) StartRuntime(context.Context, string) error {
	if harness.startEntered != nil {
		harness.startOnce.Do(func() { close(harness.startEntered) })
		<-harness.startRelease
	}
	return nil
}

func (harness *testLabHarness) StopRuntime(_ context.Context, nodeID string) error {
	harness.mu.Lock()
	harness.stopped = append(harness.stopped, nodeID)
	harness.mu.Unlock()
	return nil
}

func (harness *testLabHarness) RestartRuntime(context.Context, string) error { return nil }
func (harness *testLabHarness) Reset(context.Context) error                  { return nil }

type testGreenhouseSubmitter struct {
	taskID string
}

func (submitter testGreenhouseSubmitter) SubmitGreenhouseTask(context.Context) (string, error) {
	return submitter.taskID, nil
}

func newTestLabController(t *testing.T, harness LabHarness, submitter GreenhouseSubmitter, snapshot Snapshot) *LabController {
	t.Helper()
	controller, err := NewLabController(LabControllerOptions{
		Harness:          harness,
		Snapshot:         func(context.Context) Snapshot { return snapshot },
		Submitter:        submitter,
		OperationTimeout: time.Second,
		Now:              func() time.Time { return time.Unix(10, 0).UTC() },
	})
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func TestLabControllerGreenhouseUsesSubmitter(t *testing.T) {
	harness := &testLabHarness{}
	controller := newTestLabController(t, harness, testGreenhouseSubmitter{taskID: "greenhouse-1"}, Snapshot{})

	operation, err := controller.RunGreenhouseTask(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if operation.Status != "SUCCESS" || operation.Type != "RUN_GREENHOUSE_TASK" || operation.TaskID != "greenhouse-1" {
		t.Fatalf("operation = %#v", operation)
	}
}

func TestLabControllerStopsCurrentCoordinatorFromSnapshot(t *testing.T) {
	harness := &testLabHarness{
		runtimes: []LabRuntimeView{{NodeID: "node-a"}, {NodeID: "node-b", PID: 42}},
	}
	controller := newTestLabController(t, harness, testGreenhouseSubmitter{taskID: "unused"}, Snapshot{
		Status: "READY",
		Mesh:   MeshOverview{Coordinator: "node-b"},
	})

	operation, err := controller.StopCurrentCoordinator(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if operation.Target != "node-b" {
		t.Fatalf("operation target = %q", operation.Target)
	}
	harness.mu.Lock()
	defer harness.mu.Unlock()
	if len(harness.stopped) != 1 || harness.stopped[0] != "node-b" {
		t.Fatalf("stopped = %#v", harness.stopped)
	}
}

func TestLabControllerRejectsConcurrentOperation(t *testing.T) {
	harness := &testLabHarness{startEntered: make(chan struct{}), startRelease: make(chan struct{})}
	controller := newTestLabController(t, harness, testGreenhouseSubmitter{taskID: "unused"}, Snapshot{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := controller.StartRuntime(context.Background(), "node-a")
		firstDone <- err
	}()
	<-harness.startEntered

	if _, err := controller.StartRuntime(context.Background(), "node-a"); !errors.Is(err, ErrLabOperationBusy) {
		t.Fatalf("second operation error = %v, want ErrLabOperationBusy", err)
	}
	close(harness.startRelease)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
}
