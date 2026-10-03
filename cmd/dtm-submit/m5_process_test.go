package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestM5RealProcessSequentialAndQueryProof(t *testing.T) {
	mesh := newM5ProcessMesh(t, false, []string{"temperature_sensor", "cooling_control"})
	key := "process-sequential-key"
	first := mesh.runSubmit(t, "-task-id", "task-sequential-first", "-idempotency-key", key)
	retry := mesh.runSubmit(t, "-task-id", "task-sequential-retry", "-idempotency-key", key)
	if first.exit != 0 || retry.exit != 0 || !strings.Contains(first.stdout, "deduplicated=false") ||
		!strings.Contains(retry.stdout, "deduplicated=true") || submitTaskID(first.stdout) != submitTaskID(retry.stdout) {
		t.Fatalf("sequential submissions: first=%+v retry=%+v", first, retry)
	}
	taskID := submitTaskID(first.stdout)
	get := mesh.runQuery(t, "get", "--task-id", taskID, "--output", "json")
	list := mesh.runQuery(t, "list", "--output", "json")
	executions := mesh.runQuery(t, "executions", "--task-id", taskID, "--output", "json")
	if get.exit != 0 || list.exit != 0 || executions.exit != 0 || !strings.Contains(get.stdout, "TASK_STATUS_SUCCEEDED") {
		t.Fatalf("query proof: get=%+v list=%+v executions=%+v", get, list, executions)
	}
	if jsonArrayLength(t, list.stdout, "tasks") != 1 || jsonArrayLength(t, executions.stdout, "executions") != 2 {
		t.Fatalf("query counts: list=%s executions=%s", list.stdout, executions.stdout)
	}
	mesh.assertCoreCounts(t, taskID, map[string]int{"tasks": 1, "task_events": 14, "task_steps": 2, "executions": 2, "task_submission_keys": 1})
	mesh.logScenario(t, key, taskID, first, retry, get, list, executions)
}

func TestM5RealProcessConcurrentSubmissions(t *testing.T) {
	mesh := newM5ProcessMesh(t, false, []string{"temperature_sensor", "cooling_control"})
	key := "process-concurrent-key"
	const count = 20
	start := make(chan struct{})
	results := make(chan m5CommandResult, count)
	var group sync.WaitGroup
	for index := 0; index < count; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			results <- mesh.runSubmit(t, "-async", "-task-id", fmt.Sprintf("task-concurrent-%02d", index), "-idempotency-key", key)
		}(index)
	}
	close(start)
	group.Wait()
	close(results)
	created, deduplicated := 0, 0
	unique := map[string]struct{}{}
	all := make([]m5CommandResult, 0, count)
	for result := range results {
		all = append(all, result)
		if result.exit != 0 {
			t.Fatalf("concurrent submit failed: %+v", result)
		}
		unique[submitTaskID(result.stdout)] = struct{}{}
		if strings.Contains(result.stdout, "deduplicated=false") {
			created++
		}
		if strings.Contains(result.stdout, "deduplicated=true") {
			deduplicated++
		}
	}
	if created != 1 || deduplicated != count-1 || len(unique) != 1 {
		t.Fatalf("created=%d deduplicated=%d unique_task_ids=%d", created, deduplicated, len(unique))
	}
	taskID := submitTaskID(all[0].stdout)
	mesh.waitForTaskStatus(t, taskID, "TASK_STATUS_SUCCEEDED")
	mesh.assertCoreCounts(t, taskID, map[string]int{"tasks": 1, "task_steps": 2, "executions": 2, "task_submission_keys": 1})
	t.Logf("started_at=%s finished_at=%s core_pid=%d agent_pid=%d submit_processes=%d key_hash_prefix=%s task_id=%s created=%d deduplicated=%d unique_task_ids=%d",
		all[0].started.Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano), mesh.core.command.Process.Pid, mesh.agent.command.Process.Pid,
		count, m5KeyHashPrefix(key), taskID, created, deduplicated, len(unique))
}

func TestM5RealProcessConflict(t *testing.T) {
	mesh := newM5ProcessMesh(t, false, []string{"temperature_sensor", "cooling_control"})
	key := "process-conflict-key"
	first := mesh.runSubmit(t, "-async", "-task-id", "task-conflict-first", "-target-temperature", "26", "-idempotency-key", key)
	conflict := mesh.runSubmit(t, "-async", "-task-id", "task-conflict-second", "-target-temperature", "27", "-idempotency-key", key)
	if first.exit != 0 || conflict.exit != 3 || conflict.stdout != "" || !strings.Contains(conflict.stderr, "idempotency key is already bound to a different request") {
		t.Fatalf("conflict submissions: first=%+v conflict=%+v", first, conflict)
	}
	for _, forbidden := range []string{key, "v1:", "SQLite", "SELECT", "target_temperature"} {
		if strings.Contains(conflict.stderr, forbidden) {
			t.Fatalf("conflict stderr leaked %q: %q", forbidden, conflict.stderr)
		}
	}
	taskID := submitTaskID(first.stdout)
	mesh.assertCoreCounts(t, taskID, map[string]int{"tasks": 1, "task_submission_keys": 1})
	mesh.logScenario(t, key, taskID, first, conflict)
}

func TestM5RealProcessInFlightRetry(t *testing.T) {
	mesh := newM5ProcessMesh(t, true, []string{"temperature_sensor", "cooling_control"})
	key := "process-inflight-key"
	first := mesh.runSubmit(t, "-async", "-task-id", "task-inflight-first", "-idempotency-key", key)
	if first.exit != 0 {
		t.Fatalf("first submit: %+v", first)
	}
	m5WaitForFile(t, mesh.barrierEntered)
	retry := mesh.runSubmit(t, "-async", "-task-id", "task-inflight-retry", "-idempotency-key", key)
	if retry.exit != 0 || !strings.Contains(retry.stdout, "deduplicated=true") || submitTaskID(retry.stdout) != submitTaskID(first.stdout) {
		t.Fatalf("in-flight retry: %+v", retry)
	}
	taskID := submitTaskID(first.stdout)
	executionsBefore := mesh.runQuery(t, "executions", "--task-id", taskID, "--output", "json")
	if executionsBefore.exit != 0 || jsonArrayLength(t, executionsBefore.stdout, "executions") != 1 {
		t.Fatalf("executions before release: %+v", executionsBefore)
	}
	if err := os.WriteFile(mesh.barrierRelease, []byte("release\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mesh.waitForTaskStatus(t, taskID, "TASK_STATUS_SUCCEEDED")
	mesh.assertCoreCounts(t, taskID, map[string]int{"tasks": 1, "task_steps": 2, "executions": 2, "task_submission_keys": 1})
	if got := countRows(t, mesh.agentDatabase, "executions", ""); got != 2 {
		t.Fatalf("agent handler invocation records = %d, want 2 steps", got)
	}
	mesh.logScenario(t, key, taskID, first, retry, executionsBefore)
}

func TestM5RealProcessTerminalRetries(t *testing.T) {
	t.Run("succeeded", func(t *testing.T) {
		mesh := newM5ProcessMesh(t, false, []string{"temperature_sensor", "cooling_control"})
		assertTerminalRetry(t, mesh, "terminal-success-key", "task-success", 0, "TASK_STATUS_SUCCEEDED")
	})
	t.Run("failed", func(t *testing.T) {
		mesh := newM5ProcessMesh(t, false, []string{"temperature_sensor"})
		assertTerminalRetry(t, mesh, "terminal-failure-key", "task-failure", 1, "TASK_STATUS_FAILED")
	})
}

func TestM5RealProcessResponseLostRestartBeforeWorker(t *testing.T) {
	if os.Getenv("DTM_M5_REAL_PROCESS_E2E") != "1" {
		t.Skip("set DTM_M5_REAL_PROCESS_E2E=1 to run the real M5 process test")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	temporary := t.TempDir()
	coreExecutable := filepath.Join(temporary, m5ExecutableName("dtm-core"))
	submitExecutable := filepath.Join(temporary, m5ExecutableName("dtm-submit"))
	m5Build(t, root, coreExecutable, "-tags", "dtm_test_submission_barrier", "./cmd/dtm-core")
	m5Build(t, root, submitExecutable, "./cmd/dtm-submit")

	coreAddress := m5ReserveAddress(t)
	databasePath := filepath.Join(temporary, "core.db")
	configPath := filepath.Join(temporary, "core.yaml")
	config := fmt.Sprintf("server:\n  address: %q\nlease:\n  ttl: 30s\n  sweep_interval: 100ms\nstorage:\n  driver: sqlite\n  path: %q\n  auto_migrate: true\n", coreAddress, filepath.ToSlash(databasePath))
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	acceptedFile := filepath.Join(temporary, "submission-accepted")
	releaseFile := filepath.Join(temporary, "submission-release")
	firstCore := m5Start(t, coreExecutable, []string{
		"DTM_TEST_SUBMISSION_ACCEPTED_FILE=" + acceptedFile,
		"DTM_TEST_SUBMISSION_RELEASE_FILE=" + releaseFile,
	}, "-config", configPath)
	m5WaitForTCP(t, coreAddress)

	firstSubmit := m5Start(t, submitExecutable, nil,
		"-core", coreAddress, "-async", "-task-id", "task-before-crash", "-idempotency-key", "process-retry-key")
	m5WaitForFile(t, acceptedFile)
	if err := firstCore.command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = firstCore.command.Wait()
	firstSubmitErr := firstSubmit.command.Wait()
	if firstSubmitErr == nil {
		t.Fatal("response-lost submit unexpectedly received a successful response")
	}

	secondCore := m5Start(t, coreExecutable, nil, "-config", configPath)
	t.Cleanup(func() { m5Stop(secondCore.command) })
	m5WaitForTCP(t, coreAddress)
	retry := exec.Command(submitExecutable,
		"-core", coreAddress, "-async", "-task-id", "task-after-crash", "-idempotency-key", "process-retry-key")
	retryOutput, err := retry.CombinedOutput()
	if err != nil {
		t.Fatalf("retry failed: %v\n%s\ncore stderr:\n%s", err, retryOutput, secondCore.stderr.String())
	}
	output := string(retryOutput)
	if !strings.Contains(output, "task_id=task-before-crash") || !strings.Contains(output, "deduplicated=true") {
		t.Fatalf("retry output = %q", output)
	}

	database, err := sql.Open("sqlite", "file:"+filepath.ToSlash(databasePath)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	for table, want := range map[string]int{"tasks": 1, "task_events": 1, "task_submission_keys": 1, "task_steps": 0, "executions": 0} {
		var got int
		if err := database.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Fatalf("%s count = %d, want %d", table, got, want)
		}
	}
	t.Logf("response_lost_exit=%v retry=%s task_count=1 event_count=1 execution_count=0", firstSubmitErr, strings.TrimSpace(output))
}

type m5ProcessMesh struct {
	coreAddress    string
	agentAddress   string
	databasePath   string
	agentDatabase  string
	submit         string
	query          string
	core           m5Process
	agent          m5Process
	barrierEntered string
	barrierRelease string
}

type m5CommandResult struct {
	pid      int
	started  time.Time
	finished time.Time
	exit     int
	stdout   string
	stderr   string
}

func newM5ProcessMesh(t *testing.T, barrier bool, capabilities []string) *m5ProcessMesh {
	t.Helper()
	if os.Getenv("DTM_M5_REAL_PROCESS_E2E") != "1" {
		t.Skip("set DTM_M5_REAL_PROCESS_E2E=1 to run the real M5 process test")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	temporary := t.TempDir()
	executables := map[string]string{}
	for _, name := range []string{"dtm-core", "dtm-agent", "dtm-submit", "dtm-query"} {
		executables[name] = filepath.Join(temporary, m5ExecutableName(name))
		arguments := []string{"./cmd/" + name}
		if barrier && name == "dtm-agent" {
			arguments = []string{"-tags", "dtm_test_barrier", "./cmd/" + name}
		}
		m5Build(t, root, executables[name], arguments...)
	}
	mesh := &m5ProcessMesh{
		coreAddress: m5ReserveAddress(t), agentAddress: m5ReserveAddress(t),
		databasePath: filepath.Join(temporary, "core.db"), agentDatabase: filepath.Join(temporary, "agent.db"),
		submit: executables["dtm-submit"], query: executables["dtm-query"],
		barrierEntered: filepath.Join(temporary, "handler-entered"), barrierRelease: filepath.Join(temporary, "handler-release"),
	}
	coreConfig := filepath.Join(temporary, "core.yaml")
	agentConfig := filepath.Join(temporary, "agent.yaml")
	writeM5File(t, coreConfig, fmt.Sprintf("server:\n  address: %q\nlease:\n  ttl: 30s\n  sweep_interval: 20ms\nstorage:\n  driver: sqlite\n  path: %q\n  auto_migrate: true\nretry:\n  max_attempts: 2\n  backoff: 10ms\n", mesh.coreAddress, filepath.ToSlash(mesh.databasePath)))
	quotedCapabilities := make([]string, len(capabilities))
	for index := range capabilities {
		quotedCapabilities[index] = fmt.Sprintf("%q", capabilities[index])
	}
	writeM5File(t, agentConfig, fmt.Sprintf("node:\n  id: process-agent\n  capabilities: [%s]\n  advertise_address: %q\ncore:\n  address: %q\nserver:\n  listen_address: %q\nheartbeat:\n  interval: 20ms\nstorage:\n  path: %q\n", strings.Join(quotedCapabilities, ", "), mesh.agentAddress, mesh.coreAddress, mesh.agentAddress, filepath.ToSlash(mesh.agentDatabase)))
	mesh.core = m5Start(t, executables["dtm-core"], nil, "-config", coreConfig)
	m5WaitForTCP(t, mesh.coreAddress)
	var agentEnvironment []string
	if barrier {
		agentEnvironment = []string{"DTM_TEST_BARRIER_ENTERED_FILE=" + mesh.barrierEntered, "DTM_TEST_BARRIER_RELEASE_FILE=" + mesh.barrierRelease}
	}
	mesh.agent = m5Start(t, executables["dtm-agent"], agentEnvironment, "-config", agentConfig)
	m5WaitForText(t, mesh.agent.stdout, "register success node_id=process-agent", mesh.agent.stderr)
	return mesh
}

func (mesh *m5ProcessMesh) runSubmit(t *testing.T, arguments ...string) m5CommandResult {
	t.Helper()
	return runM5Command(t, mesh.submit, append([]string{"-core", mesh.coreAddress}, arguments...)...)
}

func (mesh *m5ProcessMesh) runQuery(t *testing.T, arguments ...string) m5CommandResult {
	t.Helper()
	return runM5Command(t, mesh.query, append(arguments, "--core-address", mesh.coreAddress)...)
}

func (mesh *m5ProcessMesh) waitForTaskStatus(t *testing.T, taskID, statusText string) m5CommandResult {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var result m5CommandResult
	for time.Now().Before(deadline) {
		result = mesh.runQuery(t, "get", "--task-id", taskID, "--output", "json")
		if result.exit == 0 && strings.Contains(result.stdout, statusText) {
			return result
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("task %s did not reach %s: %+v\ncore stderr=%s\nagent stderr=%s", taskID, statusText, result, mesh.core.stderr.String(), mesh.agent.stderr.String())
	return result
}

func (mesh *m5ProcessMesh) assertCoreCounts(t *testing.T, taskID string, expected map[string]int) {
	t.Helper()
	for table, want := range expected {
		where := ""
		if table != "tasks" && table != "task_submission_keys" {
			where = "task_id = '" + taskID + "'"
		} else if table == "tasks" {
			where = "task_id = '" + taskID + "'"
		} else {
			where = "task_id = '" + taskID + "'"
		}
		if got := countRows(t, mesh.databasePath, table, where); got != want {
			t.Fatalf("%s count = %d, want %d", table, got, want)
		}
	}
}

func (mesh *m5ProcessMesh) logScenario(t *testing.T, key, taskID string, results ...m5CommandResult) {
	t.Helper()
	for _, result := range results {
		t.Logf("started_at=%s finished_at=%s core_pid=%d agent_pid=%d process_pid=%d core_address=%s agent_address=%s database_path=%s key_hash_prefix=%s task_id=%s exit_code=%d stdout=%q stderr=%q",
			result.started.Format(time.RFC3339Nano), result.finished.Format(time.RFC3339Nano), mesh.core.command.Process.Pid, mesh.agent.command.Process.Pid,
			result.pid, mesh.coreAddress, mesh.agentAddress, filepath.Base(mesh.databasePath), m5KeyHashPrefix(key), taskID, result.exit, result.stdout, result.stderr)
	}
}

func assertTerminalRetry(t *testing.T, mesh *m5ProcessMesh, key, taskID string, expectedExit int, statusText string) {
	t.Helper()
	first := mesh.runSubmit(t, "-task-id", taskID, "-idempotency-key", key)
	if first.exit != expectedExit || !strings.Contains(first.stdout, "deduplicated=false") {
		t.Fatalf("terminal first submit = %+v", first)
	}
	mesh.waitForTaskStatus(t, taskID, statusText)
	before := taskDatabaseSnapshot(t, mesh.databasePath, taskID)
	retry := mesh.runSubmit(t, "-task-id", taskID+"-retry", "-idempotency-key", key)
	after := taskDatabaseSnapshot(t, mesh.databasePath, taskID)
	if retry.exit != expectedExit || !strings.Contains(retry.stdout, "deduplicated=true") || submitTaskID(retry.stdout) != taskID || before != after {
		t.Fatalf("terminal retry: first=%+v retry=%+v before=%+v after=%+v", first, retry, before, after)
	}
	mesh.logScenario(t, key, taskID, first, retry)
}

type m5TaskSnapshot struct{ version, events, steps, executions int }

func taskDatabaseSnapshot(t *testing.T, path, taskID string) m5TaskSnapshot {
	t.Helper()
	database := openM5Database(t, path)
	defer database.Close()
	var snapshot m5TaskSnapshot
	if err := database.QueryRow("SELECT version FROM tasks WHERE task_id = ?", taskID).Scan(&snapshot.version); err != nil {
		t.Fatal(err)
	}
	for table, destination := range map[string]*int{"task_events": &snapshot.events, "task_steps": &snapshot.steps, "executions": &snapshot.executions} {
		if err := database.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE task_id = ?", taskID).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	return snapshot
}

func runM5Command(t *testing.T, executable string, arguments ...string) m5CommandResult {
	t.Helper()
	stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
	command := exec.Command(executable, arguments...)
	command.Stdout, command.Stderr = stdout, stderr
	result := m5CommandResult{started: time.Now().UTC()}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	result.pid = command.Process.Pid
	err := command.Wait()
	result.finished, result.stdout, result.stderr = time.Now().UTC(), stdout.String(), stderr.String()
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			result.exit = exitError.ExitCode()
		} else {
			result.exit = -1
		}
	}
	return result
}

func submitTaskID(output string) string {
	for _, field := range strings.Fields(output) {
		if strings.HasPrefix(field, "task_id=") {
			return strings.TrimPrefix(field, "task_id=")
		}
	}
	return ""
}

func jsonArrayLength(t *testing.T, output, field string) int {
	t.Helper()
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatal(err)
	}
	var items []json.RawMessage
	if err := json.Unmarshal(decoded[field], &items); err != nil {
		t.Fatal(err)
	}
	return len(items)
}

func countRows(t *testing.T, path, table, where string) int {
	t.Helper()
	database := openM5Database(t, path)
	defer database.Close()
	query := "SELECT COUNT(*) FROM " + table
	if where != "" {
		query += " WHERE " + where
	}
	var count int
	if err := database.QueryRow(query).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func openM5Database(t *testing.T, path string) *sql.DB {
	t.Helper()
	database, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	return database
}

func writeM5File(t *testing.T, path, contents string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}

func m5WaitForText(t *testing.T, output *bytes.Buffer, needle string, diagnostic *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(output.String(), needle) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q\nstdout=%s\nstderr=%s", needle, output.String(), diagnostic.String())
}

func m5KeyHashPrefix(key string) string {
	digest := sha256.Sum256([]byte(key))
	return hex.EncodeToString(digest[:])[:12]
}

type m5Process struct {
	command *exec.Cmd
	stdout  *bytes.Buffer
	stderr  *bytes.Buffer
}

func m5Start(t *testing.T, executable string, environment []string, arguments ...string) m5Process {
	t.Helper()
	process := m5Process{command: exec.Command(executable, arguments...), stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}}
	process.command.Env = append(os.Environ(), environment...)
	process.command.Stdout, process.command.Stderr = process.stdout, process.stderr
	if err := process.command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m5Stop(process.command) })
	return process
}

func m5Stop(command *exec.Cmd) {
	if command == nil || command.Process == nil || command.ProcessState != nil {
		return
	}
	_ = command.Process.Kill()
	_, _ = command.Process.Wait()
}

func m5Build(t *testing.T, root, output string, arguments ...string) {
	t.Helper()
	args := append([]string{"build", "-o", output}, arguments...)
	command := exec.Command("go", args...)
	command.Dir = root
	if combined, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", output, err, combined)
	}
}

func m5ReserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func m5WaitForTCP(t *testing.T, address string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		connection, err := net.DialTimeout("tcp", address, 100*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", address)
}

func m5WaitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func m5ExecutableName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}
