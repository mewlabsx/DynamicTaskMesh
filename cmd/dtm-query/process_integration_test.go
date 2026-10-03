package main

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
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

type controlledProcessAgent struct {
	dtmv1.UnimplementedAgentExecutionServiceServer
	mu    sync.Mutex
	calls map[string]int
}

func (agent *controlledProcessAgent) ExecuteStep(_ context.Context, request *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error) {
	key := request.GetTaskId() + "/" + request.GetStep().GetStepId()
	agent.mu.Lock()
	agent.calls[key]++
	call := agent.calls[key]
	agent.mu.Unlock()
	if request.GetTaskId() == "task-retry" && request.GetStep().GetCapability() == "temperature_sensor" && call == 1 {
		return nil, status.Error(codes.Unavailable, "controlled transient failure")
	}
	if request.GetTaskId() == "task-failed" {
		return &dtmv1.ExecuteStepResponse{Result: &dtmv1.StepResult{
			StepId: request.GetStep().GetStepId(), NodeId: request.GetStep().GetNodeId(),
			Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_FAILED, Error: "controlled terminal failure",
		}}, nil
	}
	output, _ := structpb.NewStruct(map[string]any{"controlled": true, "attempt": request.GetAttempt()})
	return &dtmv1.ExecuteStepResponse{Result: &dtmv1.StepResult{
		StepId: request.GetStep().GetStepId(), NodeId: request.GetStep().GetNodeId(),
		Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED, Output: output,
	}}, nil
}

type processResult struct {
	command string
	stdout  string
	stderr  string
	exit    int
}

func runProcess(t *testing.T, executable string, args ...string) processResult {
	t.Helper()
	command := exec.Command(executable, args...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	exitCode := 0
	if err != nil {
		var exitError *exec.ExitError
		if !errors.As(err, &exitError) {
			t.Fatalf("run %s: %v", executable, err)
		}
		exitCode = exitError.ExitCode()
	}
	return processResult{command: strings.Join(append([]string{executable}, args...), " "), stdout: stdout.String(), stderr: stderr.String(), exit: exitCode}
}

func (result processResult) logText() string {
	return fmt.Sprintf("command=%s\nexit_code=%d\nstdout:\n%s\nstderr:\n%s\n", result.command, result.exit, result.stdout, result.stderr)
}

func writeProcessEvidence(t *testing.T, name, content string) {
	t.Helper()
	directory := strings.TrimSpace(os.Getenv("DTM_M4B_EVIDENCE_DIR"))
	if directory == "" {
		return
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	content = strings.ReplaceAll(content, "\r\n", "\n")
	if err := os.WriteFile(filepath.Join(directory, name), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func executableName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}

func reserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func waitForCore(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	client := dtmv1.NewCoreServiceClient(connection)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, rpcErr := client.GetTask(ctx, &dtmv1.GetTaskRequest{TaskId: "readiness-probe"})
		cancel()
		if status.Code(rpcErr) == codes.NotFound {
			return connection
		}
		time.Sleep(25 * time.Millisecond)
	}
	_ = connection.Close()
	t.Fatalf("Core %s did not become ready", address)
	return nil
}

func waitForTaskStatus(t *testing.T, client dtmv1.CoreServiceClient, taskID string, want dtmv1.TaskStatus) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.GetTaskStatus(context.Background(), &dtmv1.GetTaskStatusRequest{TaskId: taskID})
		if err == nil && response.GetStatus() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("task %s did not reach %s", taskID, want)
}

func TestRealProcessEndToEndScenarios(t *testing.T) {
	if os.Getenv("DTM_M4B_REAL_PROCESS_E2E") != "1" {
		t.Skip("set DTM_M4B_REAL_PROCESS_E2E=1 to run real process evidence test")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	temporary := t.TempDir()
	coreExecutable := filepath.Join(temporary, executableName("dtm-core"))
	queryExecutable := filepath.Join(temporary, executableName("dtm-query"))
	submitExecutable := filepath.Join(temporary, executableName("dtm-submit"))
	for _, build := range []struct{ output, packagePath string }{
		{coreExecutable, "./cmd/dtm-core"}, {queryExecutable, "./cmd/dtm-query"}, {submitExecutable, "./cmd/dtm-submit"},
	} {
		command := exec.Command("go", "build", "-o", build.output, build.packagePath)
		command.Dir = root
		if output, buildErr := command.CombinedOutput(); buildErr != nil {
			t.Fatalf("build %s: %v\n%s", build.packagePath, buildErr, output)
		}
	}

	agentListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	agentServer := grpc.NewServer()
	dtmv1.RegisterAgentExecutionServiceServer(agentServer, &controlledProcessAgent{calls: make(map[string]int)})
	go func() { _ = agentServer.Serve(agentListener) }()
	t.Cleanup(func() { agentServer.Stop(); _ = agentListener.Close() })

	coreAddress := reserveAddress(t)
	databasePath := filepath.ToSlash(filepath.Join(temporary, "core.db"))
	configPath := filepath.Join(temporary, "core.yaml")
	configuration := fmt.Sprintf("server:\n  address: %q\nlease:\n  ttl: 30s\n  sweep_interval: 1s\nstorage:\n  driver: sqlite\n  path: %q\n  auto_migrate: true\nretry:\n  max_attempts: 2\n  backoff: 1ms\n", coreAddress, databasePath)
	if err := os.WriteFile(configPath, []byte(configuration), 0o600); err != nil {
		t.Fatal(err)
	}
	coreContext, stopCore := context.WithCancel(context.Background())
	coreCommand := exec.CommandContext(coreContext, coreExecutable, "-config", configPath)
	var coreStdout, coreStderr bytes.Buffer
	coreCommand.Stdout, coreCommand.Stderr = &coreStdout, &coreStderr
	if err := coreCommand.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCore()
		_ = coreCommand.Wait()
	})
	connection := waitForCore(t, coreAddress)
	t.Cleanup(func() { _ = connection.Close() })
	coreClient := dtmv1.NewCoreServiceClient(connection)
	registryClient := dtmv1.NewNodeRegistryServiceClient(connection)
	registered, err := registryClient.RegisterNode(context.Background(), &dtmv1.RegisterNodeRequest{
		Node:           &dtmv1.Node{Id: "controlled-agent", Capabilities: []string{"temperature_sensor", "cooling_control"}, Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, ExecutionAddress: agentListener.Addr().String()},
		RegistrationId: "controlled-agent-registration",
	})
	if err != nil || !registered.GetAccepted() {
		t.Fatalf("register controlled Agent: response=%#v error=%v", registered, err)
	}

	success := runProcess(t, submitExecutable, "-core", coreAddress, "-task-id", "task-success")
	if success.exit != 0 || !strings.Contains(success.stdout, "status=succeeded") {
		t.Fatalf("success submit: %+v", success)
	}
	successQuery := runProcess(t, queryExecutable, "get", "--core-address", coreAddress, "--task-id", "task-success", "--output", "json")
	var successJSON map[string]any
	if successQuery.exit != 0 || json.Unmarshal([]byte(successQuery.stdout), &successJSON) != nil || successJSON["task"].(map[string]any)["status"] != "TASK_STATUS_SUCCEEDED" {
		t.Fatalf("success query: %+v", successQuery)
	}
	writeProcessEvidence(t, "cli-smoke-success.txt", success.logText()+successQuery.logText())

	retry := runProcess(t, submitExecutable, "-core", coreAddress, "-task-id", "task-retry")
	retryQuery := runProcess(t, queryExecutable, "executions", "--core-address", coreAddress, "--task-id", "task-retry", "--output", "json")
	var retryJSON map[string]any
	if retry.exit != 0 || retryQuery.exit != 0 || json.Unmarshal([]byte(retryQuery.stdout), &retryJSON) != nil {
		t.Fatalf("retry scenario: submit=%+v query=%+v", retry, retryQuery)
	}
	retryExecutions := retryJSON["executions"].([]any)
	if len(retryExecutions) != 3 || retryExecutions[0].(map[string]any)["attempt_number"].(float64) != 1 || retryExecutions[1].(map[string]any)["attempt_number"].(float64) != 2 {
		t.Fatalf("retry executions=%#v", retryExecutions)
	}
	writeProcessEvidence(t, "cli-smoke-retry.txt", retry.logText()+retryQuery.logText())

	failed := runProcess(t, submitExecutable, "-core", coreAddress, "-task-id", "task-failed")
	failedQuery := runProcess(t, queryExecutable, "get", "--core-address", coreAddress, "--task-id", "task-failed", "--output", "json")
	var failedJSON map[string]any
	if failed.exit == 0 || failedQuery.exit != 0 || json.Unmarshal([]byte(failedQuery.stdout), &failedJSON) != nil || failedJSON["task"].(map[string]any)["status"] != "TASK_STATUS_FAILED" {
		t.Fatalf("failed scenario: submit=%+v query=%+v", failed, failedQuery)
	}
	writeProcessEvidence(t, "cli-smoke-failed-task.txt", failed.logText()+failedQuery.logText())

	if _, err := coreClient.SubmitTask(context.Background(), &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{Id: "task-empty-executions", Intent: "unsupported_intent"}, Async: true}); err != nil {
		t.Fatal(err)
	}
	waitForTaskStatus(t, coreClient, "task-empty-executions", dtmv1.TaskStatus_TASK_STATUS_FAILED)
	empty := runProcess(t, queryExecutable, "executions", "--core-address", coreAddress, "--task-id", "task-empty-executions", "--output", "json")
	var emptyJSON map[string]any
	if empty.exit != 0 || json.Unmarshal([]byte(empty.stdout), &emptyJSON) != nil || len(emptyJSON["executions"].([]any)) != 0 {
		t.Fatalf("empty executions: %+v", empty)
	}
	writeProcessEvidence(t, "cli-smoke-empty-executions.txt", empty.logText())

	missingGet := runProcess(t, queryExecutable, "get", "--core-address", coreAddress, "--task-id", "missing-task")
	missingExecutions := runProcess(t, queryExecutable, "executions", "--core-address", coreAddress, "--task-id", "missing-task")
	if missingGet.exit != exitNotFound || missingExecutions.exit != exitNotFound || missingGet.stdout != "" || missingExecutions.stdout != "" {
		t.Fatalf("not found: get=%+v executions=%+v", missingGet, missingExecutions)
	}
	writeProcessEvidence(t, "cli-smoke-not-found.txt", missingGet.logText()+missingExecutions.logText())

	for index := 1; index <= 5; index++ {
		id := fmt.Sprintf("task-page-%d", index)
		if _, err := coreClient.SubmitTask(context.Background(), &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{Id: id, Intent: "unsupported_intent"}, Async: true}); err != nil {
			t.Fatal(err)
		}
	}
	all := runProcess(t, queryExecutable, "list", "--core-address", coreAddress, "--limit", "200", "--output", "json")
	var allJSON map[string]any
	if all.exit != 0 || json.Unmarshal([]byte(all.stdout), &allJSON) != nil {
		t.Fatalf("all tasks: %+v", all)
	}
	wantIDs := make([]string, 0)
	for _, item := range allJSON["tasks"].([]any) {
		wantIDs = append(wantIDs, item.(map[string]any)["task_id"].(string))
	}
	var pageLog strings.Builder
	var gotIDs []string
	token := ""
	pages := 0
	for {
		args := []string{"list", "--core-address", coreAddress, "--limit", "2", "--output", "json"}
		if token != "" {
			args = append(args, "--page-token", token)
		}
		page := runProcess(t, queryExecutable, args...)
		pageLog.WriteString(page.logText())
		var decoded map[string]any
		if page.exit != 0 || json.Unmarshal([]byte(page.stdout), &decoded) != nil {
			t.Fatalf("page query: %+v", page)
		}
		pages++
		for _, item := range decoded["tasks"].([]any) {
			gotIDs = append(gotIDs, item.(map[string]any)["task_id"].(string))
		}
		token = decoded["next_page_token"].(string)
		if token == "" {
			break
		}
	}
	if pages < 3 || strings.Join(gotIDs, "\n") != strings.Join(wantIDs, "\n") {
		t.Fatalf("real pagination pages=%d got=%v want=%v", pages, gotIDs, wantIDs)
	}
	writeProcessEvidence(t, "cli-smoke-pagination.txt", fmt.Sprintf("pages=%d\ntotal=%d\n", pages, len(gotIDs))+pageLog.String())

	filter := runProcess(t, queryExecutable, "list", "--core-address", coreAddress, "--status", "FAILED", "--output", "json")
	invalidToken := runProcess(t, queryExecutable, "list", "--core-address", coreAddress, "--page-token", "invalid-token")
	if filter.exit != 0 || invalidToken.exit != exitArguments || invalidToken.stdout != "" || strings.Contains(invalidToken.stderr, "invalid-token") {
		t.Fatalf("filter/errors: filter=%+v invalid=%+v", filter, invalidToken)
	}
	writeProcessEvidence(t, "cli-smoke-filters.txt", filter.logText())
	writeProcessEvidence(t, "cli-smoke-json.txt", successQuery.logText()+retryQuery.logText()+empty.logText()+all.logText())
	writeProcessEvidence(t, "cli-smoke-errors.txt", invalidToken.logText()+missingGet.logText()+missingExecutions.logText())
	t.Logf("real process evidence: success=%s retry=%s failed=%s empty=%s pages=%d total=%d", "task-success", "task-retry", "task-failed", "task-empty-executions", pages, len(gotIDs))
}
