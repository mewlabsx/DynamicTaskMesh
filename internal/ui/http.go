package ui

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
)

//go:embed web/index.html web/app.js web/style.css
var webAssets embed.FS

type SnapshotProvider interface {
	Snapshot(context.Context) Snapshot
}

func NewHTTPHandler(provider SnapshotProvider, controllers ...LabOperations) http.Handler {
	var labController LabOperations
	if len(controllers) > 0 {
		labController = controllers[0]
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		labEnabled := labController != nil && labController.Enabled()
		writeJSON(writer, http.StatusOK, map[string]any{
			"status":                "ok",
			"ui_read_only":          !labEnabled,
			"runtime_depends_on_ui": false,
			"mode":                  labMode(labEnabled),
			"lab_controls_enabled":  labEnabled,
		})
	})
	mux.HandleFunc("/api/snapshot", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if provider == nil {
			writeJSON(writer, http.StatusServiceUnavailable, map[string]string{"error": "snapshot provider is not configured"})
			return
		}
		snapshot := provider.Snapshot(request.Context())
		snapshot.Lab = labSnapshot(labController)
		writeJSON(writer, http.StatusOK, snapshot)
	})
	mux.HandleFunc("/api/lab/", labHandler(labController))
	mux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet || request.URL.Path != "/" {
			writer.Header().Set("Allow", http.MethodGet)
			http.NotFound(writer, request)
			return
		}
		content, err := webAssets.ReadFile("web/index.html")
		if err != nil {
			http.Error(writer, "dashboard asset is unavailable", http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = writer.Write(content)
	})
	mux.HandleFunc("/app.js", staticAsset("web/app.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("/style.css", staticAsset("web/style.css", "text/css; charset=utf-8"))
	return mux
}

func labMode(enabled bool) string {
	if enabled {
		return "LAB"
	}
	return "OBSERVE"
}

func labSnapshot(controller LabOperations) LabView {
	if controller == nil || !controller.Enabled() {
		return ObserveLabView()
	}
	return controller.Snapshot()
}

func labHandler(controller LabOperations) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if controller == nil || !controller.Enabled() {
			writeJSON(writer, http.StatusForbidden, map[string]string{
				"error": "LAB test controls are disabled; start dtm-ui with -enable-test-controls",
			})
			return
		}
		if request.Method != http.MethodPost {
			writer.Header().Set("Allow", http.MethodPost)
			writeJSON(writer, http.StatusMethodNotAllowed, map[string]string{"error": "LAB actions require POST"})
			return
		}

		path := strings.TrimPrefix(request.URL.Path, "/api/lab/")
		var operation LabOperation
		var err error
		switch {
		case path == "tasks/greenhouse":
			operation, err = controller.RunGreenhouseTask(request.Context())
		case path == "coordinator/stop":
			operation, err = controller.StopCurrentCoordinator(request.Context())
		case path == "reset":
			operation, err = controller.Reset(request.Context())
		case strings.HasPrefix(path, "runtimes/"):
			parts := strings.Split(path, "/")
			if len(parts) != 3 || strings.TrimSpace(parts[1]) == "" {
				http.NotFound(writer, request)
				return
			}
			nodeID, decodeErr := url.PathUnescape(parts[1])
			if decodeErr != nil || strings.TrimSpace(nodeID) == "" || strings.Contains(nodeID, "/") {
				http.NotFound(writer, request)
				return
			}
			switch parts[2] {
			case "start":
				operation, err = controller.StartRuntime(request.Context(), nodeID)
			case "stop":
				operation, err = controller.StopRuntime(request.Context(), nodeID)
			case "restart":
				operation, err = controller.RestartRuntime(request.Context(), nodeID)
			default:
				http.NotFound(writer, request)
				return
			}
		default:
			http.NotFound(writer, request)
			return
		}
		if err != nil {
			writeJSON(writer, labErrorStatus(err), map[string]any{
				"error":     err.Error(),
				"operation": operation,
			})
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"operation": operation})
	}
}

func labErrorStatus(err error) int {
	switch {
	case errors.Is(err, ErrUnknownRuntime):
		return http.StatusNotFound
	case errors.Is(err, ErrLabOperationBusy), errors.Is(err, ErrRuntimeAlreadyRunning), errors.Is(err, ErrRuntimeAlreadyStopped):
		return http.StatusConflict
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	default:
		return http.StatusBadGateway
	}
}

func staticAsset(name, contentType string) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		content, err := webAssets.ReadFile(name)
		if err != nil {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", contentType)
		_, _ = writer.Write(content)
	}
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
