package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestRunRequiresConfigFlag(t *testing.T) {
	var stderr bytes.Buffer

	if code := run(context.Background(), nil, nil, &stderr); code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	if stderr.Len() == 0 {
		t.Fatal("run() stderr is empty, want flag error")
	}
}

func TestLeaseMonitorMarksAbnormallyStoppedNodeOffline(t *testing.T) {
	deps, err := composeWithLeaseTTL(30 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deps.AsyncTasks.Close()
		_ = deps.Repository.Close()
	})
	_, err = deps.RegistryAPI.RegisterNode(context.Background(), &dtmv1.RegisterNodeRequest{
		Node: &dtmv1.Node{
			Id:               "node-1",
			Capabilities:     []string{"temperature_sensor"},
			Status:           dtmv1.NodeStatus_NODE_STATUS_ONLINE,
			ExecutionAddress: "127.0.0.1:50061",
		},
		RegistrationId: "registration-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := deps.Registry.Discover("temperature_sensor"); len(got) != 1 {
		t.Fatalf("Discover() after registration = %v", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = monitorLeases(ctx, deps.RegistryAPI, 5*time.Millisecond)

	deadline := time.After(time.Second)
	for {
		if got := deps.Registry.Discover("temperature_sensor"); len(got) == 0 {
			if _, err := deps.Endpoints.Resolve("node-1"); err == nil {
				t.Fatal("expired node endpoint remains resolvable")
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("node remained online after lease TTL")
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestRunRejectsInvalidFlag(t *testing.T) {
	var stderr bytes.Buffer

	if code := run(context.Background(), []string{"-unknown"}, nil, &stderr); code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "flag provided but not defined") {
		t.Fatalf("run() stderr = %q, want flag error", stderr.String())
	}
}

func TestRunRejectsInvalidCoreConfig(t *testing.T) {
	path := writeCoreConfig(t, "server: {}\nnats: {}\n")
	var stderr bytes.Buffer

	if code := run(context.Background(), []string{"-config", path}, nil, &stderr); code != 2 {
		t.Fatalf("run() code = %d, want 2", code)
	}
	if !strings.Contains(stderr.String(), "server.address") {
		t.Fatalf("run() stderr = %q, want validation error", stderr.String())
	}
}

func TestRunServesUntilCanceled(t *testing.T) {
	path := writeCoreConfig(t, "server:\n  address: 127.0.0.1:0\n")
	command := exec.Command(os.Args[0], "-test.run=TestCoreRunProcessHelper")
	command.Env = append(os.Environ(), "DTM_CORE_RUN_HELPER=1", "DTM_CORE_RUN_CONFIG="+path)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	line := ""
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "dtm-core listening ") {
			line = scanner.Text()
			break
		}
	}
	if line == "" {
		_ = stdin.Close()
		_ = command.Wait()
		t.Fatalf("helper did not report readiness: scan=%v stderr=%q", scanner.Err(), stderr.String())
	}
	address := strings.TrimSpace(strings.TrimPrefix(line, "dtm-core listening "))
	if address == "" {
		t.Fatal("listening address is empty")
	}
	connection, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatalf("server is not accepting connections: %v", err)
	}
	connection.Close()
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("Core helper exit = %v; stderr = %q", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		t.Fatal("Core helper did not return after graceful cancellation")
	}
	if connection, err := net.DialTimeout("tcp", address, 200*time.Millisecond); err == nil {
		connection.Close()
		t.Fatal("listener still accepts connections after cancellation")
	}
	databasePath := filepath.Join(filepath.Dir(path), "dtm.db")
	if err := os.Remove(databasePath); err != nil && !os.IsNotExist(err) {
		t.Fatalf("database handle was not released after helper exit: %v", err)
	}
}

func TestCoreRunProcessHelper(t *testing.T) {
	if os.Getenv("DTM_CORE_RUN_HELPER") != "1" {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()
	if code := run(ctx, []string{"-config", os.Getenv("DTM_CORE_RUN_CONFIG")}, os.Stdout, os.Stderr); code != 0 {
		t.Fatalf("run() code = %d", code)
	}
}

func TestLeaseMonitorChannelClosureDisablesSelectAndPreservesPendingError(t *testing.T) {
	want := errors.New("lease sweep failed before shutdown")
	errorsChannel := make(chan error, 1)
	errorsChannel <- want
	close(errorsChannel)
	var stderr bytes.Buffer

	err, open := <-errorsChannel
	active := handleLeaseMonitorResult(errorsChannel, err, open, &stderr)
	if active == nil || !strings.Contains(stderr.String(), want.Error()) {
		t.Fatalf("pending lease error was not preserved: active=%v stderr=%q", active, stderr.String())
	}
	err, open = <-active
	active = handleLeaseMonitorResult(active, err, open, &stderr)
	if active != nil {
		t.Fatalf("closed lease error channel remained active: %v", active)
	}
	select {
	case <-active:
		t.Fatal("nil lease error channel unexpectedly remained selectable")
	default:
	}
}

func TestMonitorLeasesClosesChannelAfterContextCancellation(t *testing.T) {
	deps, err := composeWithLeaseTTL(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deps.AsyncTasks.Close()
		_ = deps.Repository.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	errorsChannel := monitorLeases(ctx, deps.RegistryAPI, time.Hour)
	cancel()
	closed := make(chan struct{})
	go func() {
		for range errorsChannel {
		}
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("lease monitor did not close its result channel after cancellation")
	}
}

func TestRunUsesAbsoluteDatabasePathAndCreatesMissingDirectory(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config", "core.yaml")
	databasePath := filepath.Join(root, "absolute", "missing", "core.db")
	writeCoreConfigAt(t, configPath, "server:\n  address: 127.0.0.1:0\nstorage:\n  path: "+strconv.Quote(databasePath)+"\n")

	startAndStopCore(t, configPath)

	if info, err := os.Stat(databasePath); err != nil || info.IsDir() {
		t.Fatalf("absolute database path stat = (%v, %v), want database file", info, err)
	}
}

func TestRunResolvesRelativeDatabasePathFromConfigDirectory(t *testing.T) {
	root := t.TempDir()
	configPath := filepath.Join(root, "config", "core.yaml")
	workingDirectory := filepath.Join(root, "working")
	if err := os.MkdirAll(workingDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	writeCoreConfigAt(t, configPath, "server:\n  address: 127.0.0.1:0\nstorage:\n  path: state/core.db\n")
	originalWorkingDirectory, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(workingDirectory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalWorkingDirectory); err != nil {
			t.Errorf("restore working directory: %v", err)
		}
	})

	startAndStopCore(t, configPath)

	want := filepath.Join(filepath.Dir(configPath), "state", "core.db")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("config-relative database stat error = %v", err)
	}
	unexpected := filepath.Join(workingDirectory, "state", "core.db")
	if _, err := os.Stat(unexpected); !os.IsNotExist(err) {
		t.Fatalf("working-directory database stat error = %v, want not exist", err)
	}
}

func TestRunRejectsCorruptDatabaseWithoutChangingOriginal(t *testing.T) {
	root := t.TempDir()
	databasePath := filepath.Join(root, "corrupt.db")
	original := []byte("this is deliberately not a sqlite database\x00keep-original")
	if err := os.WriteFile(databasePath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(root, "core.yaml")
	writeCoreConfigAt(t, configPath, "server:\n  address: 127.0.0.1:0\nstorage:\n  path: corrupt.db\n")
	var stdout, stderr bytes.Buffer

	if code := run(context.Background(), []string{"-config", configPath}, &stdout, &stderr); code != 13 {
		t.Fatalf("run() code = %d, want 13; stdout = %q; stderr = %q", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), strconv.Quote(databasePath)) ||
		!strings.Contains(stderr.String(), "error_class=corrupt") {
		t.Fatalf("run() stderr = %q, want stable corrupt storage diagnostic with database path", stderr.String())
	}
	got, err := os.ReadFile(databasePath)
	if err != nil {
		t.Fatalf("read corrupt database after failed start: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("corrupt database changed after failed start: got %q, want %q", got, original)
	}
}

func TestStopGRPCServerStopsBlockingUnaryRPCAfterTimeout(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	entered := make(chan struct{})
	dtmv1.RegisterNodeRegistryServiceServer(server, blockingRegistryServer{entered: entered})
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	connection, err := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("DialContext() error = %v", err)
	}
	defer connection.Close()
	rpcDone := make(chan error, 1)
	go func() {
		_, err := dtmv1.NewNodeRegistryServiceClient(connection).RegisterNode(context.Background(), &dtmv1.RegisterNodeRequest{})
		rpcDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("blocking RPC did not enter handler")
	}

	started := time.Now()
	stopGRPCServer(server, 20*time.Millisecond)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("stopGRPCServer() took %s, want bounded stop", elapsed)
	}
	select {
	case err := <-rpcDone:
		if err == nil {
			t.Fatal("blocking RPC error = nil, want termination")
		}
	case <-time.After(time.Second):
		t.Fatal("blocking RPC did not terminate")
	}
	select {
	case err := <-serveDone:
		if err != nil && err != grpc.ErrServerStopped {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not return after stopping server")
	}
}

func TestStopGRPCServerGracefullyStopsWithoutActiveRPC(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()

	stopGRPCServer(server, time.Second)
	select {
	case err := <-serveDone:
		if err != nil && err != grpc.ErrServerStopped {
			t.Fatalf("Serve() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve() did not return after graceful stop")
	}
}

func startAndStopCore(t *testing.T, configPath string) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=TestCoreRunProcessHelper")
	command.Env = append(os.Environ(), "DTM_CORE_RUN_HELPER=1", "DTM_CORE_RUN_CONFIG="+configPath)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	command.Stderr = &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	scanner := bufio.NewScanner(stdout)
	ready := false
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "dtm-core listening ") {
			ready = true
			break
		}
	}
	if !ready {
		_ = stdin.Close()
		_ = command.Wait()
		t.Fatalf("Core helper did not become ready: scan=%v stderr=%q", scanner.Err(), stderr.String())
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	waited := make(chan error, 1)
	go func() { waited <- command.Wait() }()
	select {
	case err := <-waited:
		if err != nil {
			t.Fatalf("Core helper exit=%v stderr=%q", err, stderr.String())
		}
	case <-time.After(10 * time.Second):
		_ = command.Process.Kill()
		t.Fatal("Core helper did not stop after cancellation")
	}
}

func writeCoreConfigAt(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create config directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
}

func writeCoreConfig(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "core.yaml")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

type signalBuffer struct {
	mu   sync.Mutex
	text string
	note chan struct{}
}

type blockingRegistryServer struct {
	dtmv1.UnimplementedNodeRegistryServiceServer
	entered chan<- struct{}
}

func (server blockingRegistryServer) RegisterNode(
	ctx context.Context,
	_ *dtmv1.RegisterNodeRequest,
) (*dtmv1.RegisterNodeResponse, error) {
	close(server.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}

func newSignalBuffer() *signalBuffer {
	return &signalBuffer{note: make(chan struct{}, 1)}
}

func (buffer *signalBuffer) Write(value []byte) (int, error) {
	buffer.mu.Lock()
	buffer.text += string(value)
	select {
	case buffer.note <- struct{}{}:
	default:
	}
	buffer.mu.Unlock()
	return len(value), nil
}

func (buffer *signalBuffer) waitFor(t *testing.T, prefix string) string {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		buffer.mu.Lock()
		for _, line := range strings.Split(buffer.text, "\n") {
			if strings.HasPrefix(line, prefix) {
				buffer.mu.Unlock()
				return line
			}
		}
		buffer.mu.Unlock()
		select {
		case <-buffer.note:
		case <-deadline:
			t.Fatalf("timed out waiting for output prefix %q", prefix)
		}
	}
}
