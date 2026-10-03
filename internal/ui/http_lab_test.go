package ui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

type testLabOperations struct {
	enabled       bool
	greenhouseRan bool
}

func (operations *testLabOperations) Enabled() bool { return operations.enabled }
func (operations *testLabOperations) Snapshot() LabView {
	return LabView{Enabled: operations.enabled, Mode: "LAB", ControlsEnabled: operations.enabled, SessionID: "http-test"}
}
func (operations *testLabOperations) RunGreenhouseTask(context.Context) (LabOperation, error) {
	operations.greenhouseRan = true
	return LabOperation{OperationID: "op-1", Type: "RUN_GREENHOUSE_TASK", Status: "SUCCESS", TaskID: "task-1"}, nil
}
func (operations *testLabOperations) StartRuntime(context.Context, string) (LabOperation, error) {
	return LabOperation{OperationID: "op-2", Type: "START_RUNTIME", Status: "SUCCESS"}, nil
}
func (operations *testLabOperations) StopRuntime(context.Context, string) (LabOperation, error) {
	return LabOperation{OperationID: "op-3", Type: "STOP_RUNTIME", Status: "SUCCESS"}, nil
}
func (operations *testLabOperations) RestartRuntime(context.Context, string) (LabOperation, error) {
	return LabOperation{OperationID: "op-4", Type: "RESTART_RUNTIME", Status: "SUCCESS"}, nil
}
func (operations *testLabOperations) StopCurrentCoordinator(context.Context) (LabOperation, error) {
	return LabOperation{OperationID: "op-5", Type: "STOP_CURRENT_COORDINATOR", Status: "SUCCESS"}, nil
}
func (operations *testLabOperations) Reset(context.Context) (LabOperation, error) {
	return LabOperation{OperationID: "op-6", Type: "RESET_DEMO", Status: "SUCCESS"}, nil
}

func TestHTTPGatesLabControlsAndRequiresPost(t *testing.T) {
	provider := staticSnapshotProvider(Snapshot{})
	disabled := NewHTTPHandler(provider)
	request := httptest.NewRequest(http.MethodPost, "/api/lab/tasks/greenhouse", nil)
	recorder := httptest.NewRecorder()
	disabled.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusForbidden {
		t.Fatalf("disabled status = %d, want 403", recorder.Code)
	}

	operations := &testLabOperations{enabled: true}
	enabled := NewHTTPHandler(provider, operations)
	request = httptest.NewRequest(http.MethodGet, "/api/lab/tasks/greenhouse", nil)
	recorder = httptest.NewRecorder()
	enabled.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", recorder.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "/api/lab/tasks/greenhouse", nil)
	recorder = httptest.NewRecorder()
	enabled.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !operations.greenhouseRan {
		t.Fatalf("POST status = %d, greenhouseRan = %t", recorder.Code, operations.greenhouseRan)
	}

	request = httptest.NewRequest(http.MethodPost, "/api/lab/not-a-control", nil)
	recorder = httptest.NewRecorder()
	enabled.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("unknown status = %d, want 404", recorder.Code)
	}
}
