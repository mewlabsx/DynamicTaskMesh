package ui

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type staticSnapshotProvider Snapshot

func (provider staticSnapshotProvider) Snapshot(context.Context) Snapshot { return Snapshot(provider) }

func TestHTTPHealthDashboardAndSnapshot(t *testing.T) {
	handler := NewHTTPHandler(staticSnapshotProvider(Snapshot{
		Timestamp: time.Unix(1, 0).UTC(), Status: "READY",
		Mesh: MeshOverview{Status: "READY", RuntimeCount: 3, Coordinator: "node-a"},
	}))
	server := httptest.NewServer(handler)
	defer server.Close()

	response, err := http.Get(server.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", response.StatusCode)
	}
	var health map[string]any
	if err := json.NewDecoder(response.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	if health["ui_read_only"] != true || health["runtime_depends_on_ui"] != false {
		t.Fatalf("health = %#v", health)
	}

	response, err = http.Get(server.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("dashboard status = %d", response.StatusCode)
	}
	page, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(page), "Mesh 可观测性控制台") || !strings.Contains(string(page), "观察入口") || !strings.Contains(string(page), `lang="zh-CN"`) || !strings.Contains(string(page), "/app.js") {
		t.Fatalf("dashboard content does not contain expected shell")
	}

	response, err = http.Get(server.URL + "/app.js")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	appScript, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(appScript), "最新快照") || !strings.Contains(string(appScript), "现有 API 未暴露") {
		t.Fatalf("localized app asset is not served as expected")
	}

	response, err = http.Get(server.URL + "/api/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("snapshot status = %d", response.StatusCode)
	}
	var snapshot Snapshot
	if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
		t.Fatal(err)
	}
	if snapshot.Mesh.RuntimeCount != 3 || snapshot.Mesh.Coordinator != "node-a" {
		t.Fatalf("snapshot = %#v", snapshot)
	}
}

func TestHTTPRejectsUnsupportedMethod(t *testing.T) {
	handler := NewHTTPHandler(staticSnapshotProvider(Snapshot{}))
	request := httptest.NewRequest(http.MethodPost, "/api/snapshot", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", recorder.Code)
	}
}
