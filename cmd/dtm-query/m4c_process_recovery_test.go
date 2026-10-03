package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/platform/sqlite"
	storageport "dtm/internal/storage"

	"google.golang.org/grpc"
)

func TestM4CRealCoreKillRestartRecovery(t *testing.T) {
	if os.Getenv("DTM_M4C_REAL_PROCESS_E2E") != "1" {
		t.Skip("set DTM_M4C_REAL_PROCESS_E2E=1 to run real Core kill/restart recovery")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	temporary := t.TempDir()
	coreExecutable := filepath.Join(temporary, executableName("dtm-core"))
	queryExecutable := filepath.Join(temporary, executableName("dtm-query"))
	for _, build := range []struct{ output, packagePath string }{{coreExecutable, "./cmd/dtm-core"}, {queryExecutable, "./cmd/dtm-query"}} {
		command := exec.Command("go", "build", "-o", build.output, build.packagePath)
		command.Dir = root
		if output, buildErr := command.CombinedOutput(); buildErr != nil {
			t.Fatalf("build %s: %v\n%s", build.packagePath, buildErr, output)
		}
	}
	databasePath := filepath.Join(temporary, "core.db")
	seedM4CInterruptedTask(t, databasePath, "task-m4c-process-idempotent", model.IdempotencyIdempotent)
	seedM4CInterruptedTask(t, databasePath, "task-m4c-process-non-idempotent", model.IdempotencyNonIdempotent)

	agent := &controlledProcessAgent{calls: make(map[string]int)}
	agentListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	agentServer := grpc.NewServer()
	dtmv1.RegisterAgentExecutionServiceServer(agentServer, agent)
	go func() { _ = agentServer.Serve(agentListener) }()
	t.Cleanup(func() { agentServer.Stop(); _ = agentListener.Close() })

	coreAddress := reserveAddress(t)
	configPath := filepath.Join(temporary, "core.yaml")
	configuration := fmt.Sprintf("server:\n  address: %q\nlease:\n  ttl: 30s\n  sweep_interval: 1s\nstorage:\n  driver: sqlite\n  path: %q\n  auto_migrate: true\nretry:\n  max_attempts: 1\n  backoff: 1ms\n", coreAddress, filepath.ToSlash(databasePath))
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	startCore := func() (*exec.Cmd, *bytes.Buffer, *bytes.Buffer, *grpc.ClientConn) {
		stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
		command := exec.Command(coreExecutable, "-config", configPath)
		command.Stdout, command.Stderr = stdout, stderr
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		return command, stdout, stderr, waitForCore(t, coreAddress)
	}
	firstCore, firstStdout, firstStderr, firstConnection := startCore()
	firstPID := firstCore.Process.Pid
	t.Cleanup(func() { _ = firstCore.Process.Kill(); _ = firstCore.Wait(); _ = firstConnection.Close() })
	firstClient := dtmv1.NewCoreServiceClient(firstConnection)
	waitForM4CTaskState(t, firstClient, "task-m4c-process-idempotent", dtmv1.TaskStatus_TASK_STATUS_RETRYING, firstStdout, firstStderr)
	waitForM4CTaskState(t, firstClient, "task-m4c-process-non-idempotent", dtmv1.TaskStatus_TASK_STATUS_FAILED, firstStdout, firstStderr)

	beforeRegister := runProcess(t, queryExecutable, "executions", "--core-address", coreAddress, "--task-id", "task-m4c-process-idempotent", "--output", "json")
	if beforeRegister.exit != 0 || executionCount(t, beforeRegister.stdout) != 1 {
		t.Fatalf("pre-registration executions = %+v", beforeRegister)
	}
	registry := dtmv1.NewNodeRegistryServiceClient(firstConnection)
	registered, err := registry.RegisterNode(context.Background(), &dtmv1.RegisterNodeRequest{
		Node:           &dtmv1.Node{Id: "process-agent", Capabilities: []string{"temperature_sensor"}, Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, ExecutionAddress: agentListener.Addr().String()},
		RegistrationId: "registration-after-restart",
	})
	if err != nil || !registered.GetAccepted() {
		t.Fatalf("register Agent: %#v, %v", registered, err)
	}
	waitForM4CTaskState(t, firstClient, "task-m4c-process-idempotent", dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED, firstStdout, firstStderr)
	idempotentExecutions := runProcess(t, queryExecutable, "executions", "--core-address", coreAddress, "--task-id", "task-m4c-process-idempotent", "--output", "json")
	nonIdempotentExecutions := runProcess(t, queryExecutable, "executions", "--core-address", coreAddress, "--task-id", "task-m4c-process-non-idempotent", "--output", "json")
	if idempotentExecutions.exit != 0 || executionCount(t, idempotentExecutions.stdout) != 2 || nonIdempotentExecutions.exit != 0 || executionCount(t, nonIdempotentExecutions.stdout) != 1 {
		t.Fatalf("recovery histories: idempotent=%+v non-idempotent=%+v", idempotentExecutions, nonIdempotentExecutions)
	}
	agent.mu.Lock()
	idempotentCalls := agent.calls["task-m4c-process-idempotent/step-1"]
	nonIdempotentCalls := agent.calls["task-m4c-process-non-idempotent/step-1"]
	agent.mu.Unlock()
	if idempotentCalls != 1 || nonIdempotentCalls != 0 {
		t.Fatalf("handler calls idempotent=%d non-idempotent=%d", idempotentCalls, nonIdempotentCalls)
	}
	page := runProcess(t, queryExecutable, "list", "--core-address", coreAddress, "--limit", "1", "--output", "json")
	pageToken := nextPageToken(t, page.stdout)
	if page.exit != 0 || pageToken == "" {
		t.Fatalf("pre-restart page = %+v token=%q", page, pageToken)
	}

	if err := firstCore.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	firstWaitErr := firstCore.Wait()
	_ = firstConnection.Close()
	secondCore, secondStdout, secondStderr, secondConnection := startCore()
	secondPID := secondCore.Process.Pid
	t.Cleanup(func() { _ = secondCore.Process.Kill(); _ = secondCore.Wait(); _ = secondConnection.Close() })
	afterRestart := runProcess(t, queryExecutable, "get", "--core-address", coreAddress, "--task-id", "task-m4c-process-idempotent", "--output", "json")
	afterRestartExecutions := runProcess(t, queryExecutable, "executions", "--core-address", coreAddress, "--task-id", "task-m4c-process-idempotent", "--output", "json")
	pageAfterRestart := runProcess(t, queryExecutable, "list", "--core-address", coreAddress, "--limit", "1", "--page-token", pageToken, "--output", "json")
	if afterRestart.exit != 0 || afterRestartExecutions.exit != 0 || executionCount(t, afterRestartExecutions.stdout) != 2 || pageAfterRestart.exit != 0 {
		t.Fatalf("post-restart queries: get=%+v executions=%+v page=%+v", afterRestart, afterRestartExecutions, pageAfterRestart)
	}

	common := fmt.Sprintf("started_at=%s\nfinished_at=%s\nCore PID before=%d\nCore PID after=%d\ndatabase path=%s\nold execution_id=task-m4c-process-idempotent-execution-1\nold attempt=1\nnew attempt=2\nagent registration_id before=<stale-or-absent>\nagent registration_id after=registration-after-restart\nkill_wait_error=%v\nfirst_stdout:\n%s\nfirst_stderr:\n%s\nsecond_stdout:\n%s\nsecond_stderr:\n%s\n", time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano), firstPID, secondPID, databasePath, firstWaitErr, firstStdout.String(), firstStderr.String(), secondStdout.String(), secondStderr.String())
	writeM4CProcessEvidence(t, "core-restart-idempotent.txt", common+beforeRegister.logText()+idempotentExecutions.logText())
	writeM4CProcessEvidence(t, "core-restart-non-idempotent.txt", common+nonIdempotentExecutions.logText()+fmt.Sprintf("handler_calls=%d\n", nonIdempotentCalls))
	writeM4CProcessEvidence(t, "core-restart-query-continuity.txt", common+afterRestart.logText()+afterRestartExecutions.logText())
	writeM4CProcessEvidence(t, "core-restart-stale-node.txt", common+beforeRegister.logText())
	writeM4CProcessEvidence(t, "core-restart-reregister.txt", common+idempotentExecutions.logText())
	writeM4CProcessEvidence(t, "core-restart-double-restart.txt", common+afterRestartExecutions.logText())
	writeM4CProcessEvidence(t, "core-restart-page-token.txt", common+page.logText()+pageAfterRestart.logText())
}

func seedM4CInterruptedTask(t *testing.T, databasePath string, taskID model.TaskID, mode model.IdempotencyMode) {
	t.Helper()
	repository, err := sqlite.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	now := time.Now().UTC()
	stepID, nodeID := model.StepID("step-1"), model.NodeID("process-agent")
	if err := repository.CreateTask(context.Background(), storageport.Task{ID: taskID, Intent: "read_temperature", Requirements: []model.Capability{"temperature_sensor"}, State: lifecycle.StateRunning, CreatedAt: now, UpdatedAt: now, StartedAt: &now, Version: 1, Steps: []storageport.TaskStep{{ID: stepID, TaskID: taskID, Sequence: 0, Capability: "temperature_sensor", IdempotencyMode: mode, Input: map[string]string{"operation": "read_temperature"}, State: lifecycle.StepStateDispatched, AssignedNodeID: &nodeID, MaxAttempts: 2, CreatedAt: now, UpdatedAt: now, Version: 1}}}); err != nil {
		t.Fatal(err)
	}
	executionID := model.ExecutionID(string(taskID) + "-execution-1")
	if _, _, err := repository.StartExecution(context.Background(), storageport.StartExecutionRequest{ExecutionID: executionID, TaskID: taskID, StepID: stepID, AttemptNo: 1, NodeID: nodeID, Request: map[string]string{"operation": "read_temperature"}, ExpectedStepState: lifecycle.StepStateDispatched, ExpectedStepVersion: 1, StartedAt: now, Event: storageport.TaskEvent{TaskID: taskID, StepID: &stepID, Type: "execution_started", FromState: string(lifecycle.StepStateDispatched), ToState: string(lifecycle.StepStateRunning), CreatedAt: now}}); err != nil {
		t.Fatal(err)
	}
}

func waitForM4CTaskState(t *testing.T, client dtmv1.CoreServiceClient, taskID string, want dtmv1.TaskStatus, diagnostics ...*bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var last dtmv1.TaskStatus
	for time.Now().Before(deadline) {
		response, err := client.GetTask(context.Background(), &dtmv1.GetTaskRequest{TaskId: taskID})
		if err == nil && response.GetTask() != nil {
			last = response.GetTask().GetStatus()
			if last == want {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	var detail strings.Builder
	for index, diagnostic := range diagnostics {
		fmt.Fprintf(&detail, "\ndiagnostic[%d]:\n%s", index, diagnostic.String())
	}
	t.Fatalf("task %s state=%s, want=%s%s", taskID, last, want, detail.String())
}

func executionCount(t *testing.T, output string) int {
	t.Helper()
	var decoded struct {
		Executions []json.RawMessage `json:"executions"`
	}
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatalf("decode executions: %v\n%s", err, output)
	}
	return len(decoded.Executions)
}

func nextPageToken(t *testing.T, output string) string {
	t.Helper()
	var decoded struct {
		NextPageToken string `json:"next_page_token"`
	}
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatalf("decode page: %v\n%s", err, output)
	}
	return decoded.NextPageToken
}

func writeM4CProcessEvidence(t *testing.T, name, content string) {
	t.Helper()
	directory := strings.TrimSpace(os.Getenv("DTM_M4C_EVIDENCE_DIR"))
	if directory == "" {
		return
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(directory, name), []byte(strings.ReplaceAll(content, "\r\n", "\n")), 0o644); err != nil {
		t.Fatal(err)
	}
}
