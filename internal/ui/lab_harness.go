package ui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type LabManifest struct {
	SessionID string              `json:"session_id"`
	Runtimes  []RuntimeDescriptor `json:"runtimes"`
}

type RuntimeDescriptor struct {
	NodeID           string `json:"node_id"`
	Executable       string `json:"executable"`
	ConfigPath       string `json:"config_path"`
	WorkingDirectory string `json:"working_directory"`
	ControlEndpoint  string `json:"control_endpoint"`
	PID              int    `json:"pid,omitempty"`
	StdoutPath       string `json:"stdout_path,omitempty"`
	StderrPath       string `json:"stderr_path,omitempty"`
}

func LoadLabManifest(path string) (LabManifest, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return LabManifest{}, errors.New("lab manifest path is required")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return LabManifest{}, fmt.Errorf("read lab manifest: %w", err)
	}
	data = bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})
	var manifest LabManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return LabManifest{}, fmt.Errorf("decode lab manifest: %w", err)
	}
	base := filepath.Dir(path)
	manifest.SessionID = strings.TrimSpace(manifest.SessionID)
	if manifest.SessionID == "" {
		return LabManifest{}, errors.New("lab manifest session_id is required")
	}
	if len(manifest.Runtimes) == 0 {
		return LabManifest{}, errors.New("lab manifest contains no runtimes")
	}
	seen := make(map[string]struct{}, len(manifest.Runtimes))
	for index := range manifest.Runtimes {
		runtime := &manifest.Runtimes[index]
		runtime.NodeID = strings.TrimSpace(runtime.NodeID)
		runtime.Executable = resolveManifestPath(base, runtime.Executable)
		runtime.ConfigPath = resolveManifestPath(base, runtime.ConfigPath)
		runtime.WorkingDirectory = resolveManifestPath(base, runtime.WorkingDirectory)
		if runtime.WorkingDirectory == "" {
			runtime.WorkingDirectory = filepath.Dir(runtime.ConfigPath)
		}
		runtime.ControlEndpoint = strings.TrimSpace(runtime.ControlEndpoint)
		runtime.StdoutPath = resolveManifestPath(base, runtime.StdoutPath)
		runtime.StderrPath = resolveManifestPath(base, runtime.StderrPath)
		if runtime.NodeID == "" || runtime.Executable == "" || runtime.ConfigPath == "" || runtime.ControlEndpoint == "" {
			return LabManifest{}, fmt.Errorf("lab manifest runtime %d is incomplete", index)
		}
		if _, exists := seen[runtime.NodeID]; exists {
			return LabManifest{}, fmt.Errorf("lab manifest repeats node %q", runtime.NodeID)
		}
		seen[runtime.NodeID] = struct{}{}
	}
	return manifest, nil
}

func resolveManifestPath(base, value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if filepath.IsAbs(value) {
		return filepath.Clean(value)
	}
	return filepath.Clean(filepath.Join(base, value))
}

type managedRuntime struct {
	descriptor RuntimeDescriptor
	process    *os.Process
	command    *exec.Cmd
	running    bool
	state      string
	lastError  string
}

type ProcessLabHarness struct {
	sessionID string
	mu        sync.RWMutex
	runtimes  map[string]*managedRuntime
	gate      chan struct{}
}

func NewProcessLabHarness(manifest LabManifest) (*ProcessLabHarness, error) {
	if strings.TrimSpace(manifest.SessionID) == "" || len(manifest.Runtimes) == 0 {
		return nil, errors.New("lab manifest is incomplete")
	}
	seen := make(map[string]struct{}, len(manifest.Runtimes))
	for index, descriptor := range manifest.Runtimes {
		nodeID := strings.TrimSpace(descriptor.NodeID)
		if nodeID == "" || strings.TrimSpace(descriptor.Executable) == "" || strings.TrimSpace(descriptor.ConfigPath) == "" || strings.TrimSpace(descriptor.ControlEndpoint) == "" {
			return nil, fmt.Errorf("lab manifest runtime %d is incomplete", index)
		}
		if _, exists := seen[nodeID]; exists {
			return nil, fmt.Errorf("lab manifest repeats node %q", nodeID)
		}
		seen[nodeID] = struct{}{}
	}
	harness := &ProcessLabHarness{
		sessionID: manifest.SessionID,
		runtimes:  make(map[string]*managedRuntime, len(manifest.Runtimes)),
		gate:      make(chan struct{}, 1),
	}
	harness.gate <- struct{}{}
	for _, descriptor := range manifest.Runtimes {
		process := findProcess(descriptor.PID)
		state := "STOPPED"
		if process != nil {
			state = "RUNNING"
		} else {
			descriptor.PID = 0
		}
		harness.runtimes[descriptor.NodeID] = &managedRuntime{
			descriptor: descriptor,
			process:    process,
			running:    process != nil,
			state:      state,
		}
	}
	return harness, nil
}

func findProcess(pid int) *os.Process {
	if pid <= 0 {
		return nil
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return nil
	}
	return process
}

func (harness *ProcessLabHarness) SessionID() string { return harness.sessionID }

func (harness *ProcessLabHarness) Runtimes() []LabRuntimeView {
	harness.mu.RLock()
	defer harness.mu.RUnlock()
	result := make([]LabRuntimeView, 0, len(harness.runtimes))
	for _, runtime := range harness.runtimes {
		result = append(result, LabRuntimeView{
			NodeID: runtime.descriptor.NodeID, ControlEndpoint: runtime.descriptor.ControlEndpoint,
			ProcessState: runtime.state, PID: runtime.descriptor.PID, Error: runtime.lastError,
		})
	}
	sort.Slice(result, func(left, right int) bool { return result[left].NodeID < result[right].NodeID })
	return result
}

func (harness *ProcessLabHarness) StartRuntime(ctx context.Context, nodeID string) error {
	return harness.withOperation(ctx, func() error {
		runtime, err := harness.runtime(nodeID)
		if err != nil {
			return err
		}
		return harness.startRuntime(ctx, runtime)
	})
}

func (harness *ProcessLabHarness) StopRuntime(ctx context.Context, nodeID string) error {
	return harness.withOperation(ctx, func() error {
		runtime, err := harness.runtime(nodeID)
		if err != nil {
			return err
		}
		return harness.stopRuntime(ctx, runtime, false)
	})
}

func (harness *ProcessLabHarness) RestartRuntime(ctx context.Context, nodeID string) error {
	return harness.withOperation(ctx, func() error {
		runtime, err := harness.runtime(nodeID)
		if err != nil {
			return err
		}
		if err := harness.stopRuntime(ctx, runtime, true); err != nil {
			return err
		}
		return harness.startRuntime(ctx, runtime)
	})
}

func (harness *ProcessLabHarness) Reset(ctx context.Context) error {
	return harness.withOperation(ctx, func() error {
		runtimes := harness.sortedRuntimes()
		for _, runtime := range runtimes {
			if err := harness.stopRuntime(ctx, runtime, true); err != nil {
				return err
			}
		}
		for _, runtime := range runtimes {
			if err := harness.startRuntime(ctx, runtime); err != nil {
				return err
			}
		}
		return nil
	})
}

func (harness *ProcessLabHarness) withOperation(ctx context.Context, action func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-harness.gate:
		defer func() { harness.gate <- struct{}{} }()
		return action()
	default:
		return ErrLabOperationBusy
	}
}

func (harness *ProcessLabHarness) runtime(nodeID string) (*managedRuntime, error) {
	nodeID = strings.TrimSpace(nodeID)
	harness.mu.RLock()
	runtime := harness.runtimes[nodeID]
	harness.mu.RUnlock()
	if runtime == nil {
		return nil, fmt.Errorf("%w: %s", ErrUnknownRuntime, nodeID)
	}
	return runtime, nil
}

func (harness *ProcessLabHarness) sortedRuntimes() []*managedRuntime {
	harness.mu.RLock()
	result := make([]*managedRuntime, 0, len(harness.runtimes))
	for _, runtime := range harness.runtimes {
		result = append(result, runtime)
	}
	harness.mu.RUnlock()
	sort.Slice(result, func(left, right int) bool { return result[left].descriptor.NodeID < result[right].descriptor.NodeID })
	return result
}

func (harness *ProcessLabHarness) startRuntime(ctx context.Context, runtime *managedRuntime) error {
	harness.mu.RLock()
	if runtime.running {
		harness.mu.RUnlock()
		return ErrRuntimeAlreadyRunning
	}
	descriptor := runtime.descriptor
	harness.mu.RUnlock()
	if err := contextErr(ctx); err != nil {
		return err
	}
	command := exec.Command(descriptor.Executable, "-config", descriptor.ConfigPath)
	command.Dir = descriptor.WorkingDirectory
	stdout, stderr, err := openRuntimeLogs(descriptor)
	if err != nil {
		return err
	}
	if stdout != nil {
		command.Stdout = stdout
	}
	if stderr != nil {
		command.Stderr = stderr
	}
	if err := command.Start(); err != nil {
		closeFile(stdout)
		closeFile(stderr)
		return fmt.Errorf("start runtime %s: %w", descriptor.NodeID, err)
	}
	closeFile(stdout)
	closeFile(stderr)
	harness.mu.Lock()
	runtime.process = command.Process
	runtime.command = command
	runtime.descriptor.PID = command.Process.Pid
	runtime.running = true
	runtime.state = "RUNNING"
	runtime.lastError = ""
	harness.mu.Unlock()
	go harness.waitForChild(runtime, command)
	return nil
}

func (harness *ProcessLabHarness) waitForChild(runtime *managedRuntime, command *exec.Cmd) {
	err := command.Wait()
	harness.mu.Lock()
	defer harness.mu.Unlock()
	if runtime.command != command {
		return
	}
	runtime.running = false
	runtime.state = "STOPPED"
	runtime.command = nil
	if err != nil {
		runtime.lastError = err.Error()
	}
}

func (harness *ProcessLabHarness) stopRuntime(ctx context.Context, runtime *managedRuntime, allowStopped bool) error {
	harness.mu.Lock()
	if !runtime.running {
		harness.mu.Unlock()
		if allowStopped {
			return nil
		}
		return ErrRuntimeAlreadyStopped
	}
	process := runtime.process
	endpoint := runtime.descriptor.ControlEndpoint
	runtime.running = false
	runtime.state = "STOPPED"
	runtime.lastError = ""
	harness.mu.Unlock()
	if process == nil {
		return errors.New("runtime process handle is unavailable")
	}
	if err := process.Kill(); err != nil {
		harness.mu.Lock()
		runtime.running = true
		runtime.state = "RUNNING"
		runtime.lastError = err.Error()
		harness.mu.Unlock()
		return fmt.Errorf("stop runtime %s: %w", runtime.descriptor.NodeID, err)
	}
	return waitForEndpointClosed(ctx, endpoint)
}

func openRuntimeLogs(descriptor RuntimeDescriptor) (*os.File, *os.File, error) {
	stdout, err := openLog(descriptor.StdoutPath)
	if err != nil {
		return nil, nil, err
	}
	stderr, err := openLog(descriptor.StderrPath)
	if err != nil {
		closeFile(stdout)
		return nil, nil, err
	}
	return stdout, stderr, nil
}

func openLog(path string) (*os.File, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lab runtime log %s: %w", path, err)
	}
	return file, nil
}

func closeFile(file *os.File) {
	if file != nil {
		_ = file.Close()
	}
}

func waitForEndpointClosed(ctx context.Context, endpoint string) error {
	if strings.TrimSpace(endpoint) == "" {
		return nil
	}
	for {
		if err := contextErr(ctx); err != nil {
			return err
		}
		connection, err := net.DialTimeout("tcp", endpoint, 100*time.Millisecond)
		if err != nil {
			return nil
		}
		_ = connection.Close()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func contextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
