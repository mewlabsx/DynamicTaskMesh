package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/model"
	"dtm/internal/platform/sqlite"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type m4crExecution struct {
	ExecutionID string `json:"execution_id"`
	RequestID   string `json:"request_id"`
	StepID      string `json:"step_id"`
	Attempt     int    `json:"attempt_number"`
	Status      string `json:"status"`
	FailureCode string `json:"failure_code"`
}

func TestM4CRRealCoreKillWhileAgentHandlerRunning(t *testing.T) {
	if os.Getenv("DTM_M4CR_REAL_PROCESS_E2E") != "1" {
		t.Skip("set DTM_M4CR_REAL_PROCESS_E2E=1 to run real Core kill-while-running recovery")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	temporary := t.TempDir()
	executables := map[string]string{}
	for _, name := range []string{"dtm-core", "dtm-agent", "dtm-submit", "dtm-query"} {
		executables[name] = filepath.Join(temporary, executableName(name))
		arguments := []string{"build", "-o", executables[name]}
		if name == "dtm-agent" {
			arguments = append(arguments, "-tags", "dtm_test_barrier")
		}
		arguments = append(arguments, "./cmd/"+name)
		command := exec.Command("go", arguments...)
		command.Dir = root
		if output, buildErr := command.CombinedOutput(); buildErr != nil {
			t.Fatalf("build %s: %v\n%s", name, buildErr, output)
		}
	}

	coreAddress, agentAddress := reserveAddress(t), reserveAddress(t)
	databasePath := filepath.Join(temporary, "core.db")
	coreConfig := filepath.Join(temporary, "core.yaml")
	agentOneConfig := filepath.Join(temporary, "agent-one.yaml")
	agentTwoConfig := filepath.Join(temporary, "agent-two.yaml")
	writeM4CRFile(t, coreConfig, fmt.Sprintf("server:\n  address: %q\nlease:\n  ttl: 30s\n  sweep_interval: 20ms\nstorage:\n  driver: sqlite\n  path: %q\n  auto_migrate: true\nretry:\n  max_attempts: 2\n  backoff: 10ms\n", coreAddress, filepath.ToSlash(databasePath)))
	writeM4CRFile(t, agentOneConfig, m4crAgentConfig(coreAddress, agentAddress, filepath.Join(temporary, "agent-one.db")))
	writeM4CRFile(t, agentTwoConfig, m4crAgentConfig(coreAddress, agentAddress, filepath.Join(temporary, "agent-two.db")))

	startProcess := func(executable string, environment []string, arguments ...string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer) {
		stdout, stderr := &bytes.Buffer{}, &bytes.Buffer{}
		command := exec.Command(executable, arguments...)
		command.Env = append(os.Environ(), environment...)
		command.Stdout, command.Stderr = stdout, stderr
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		return command, stdout, stderr
	}
	startCore := func() (*exec.Cmd, *bytes.Buffer, *bytes.Buffer) {
		return startProcess(executables["dtm-core"], nil, "-config", coreConfig)
	}

	firstCore, firstCoreOut, firstCoreErr := startCore()
	firstConnection := waitForCore(t, coreAddress)
	firstCorePID := firstCore.Process.Pid
	barrierEntered, barrierRelease := filepath.Join(temporary, "handler-entered"), filepath.Join(temporary, "handler-release")
	firstAgent, firstAgentOut, firstAgentErr := startProcess(executables["dtm-agent"], []string{
		"DTM_TEST_BARRIER_ENTERED_FILE=" + barrierEntered,
		"DTM_TEST_BARRIER_RELEASE_FILE=" + barrierRelease,
	}, "-config", agentOneConfig)
	firstAgentPID := firstAgent.Process.Pid
	t.Cleanup(func() {
		_ = firstAgent.Process.Kill()
		_, _ = firstAgent.Process.Wait()
	})
	waitForText(t, firstAgentOut, "register success node_id=process-agent", firstAgentErr)

	taskID := "task-m4cr-kill-running"
	submit := runProcess(t, executables["dtm-submit"], "-core", coreAddress, "-task-id", taskID, "-target-temperature", "26", "-async")
	if submit.exit != 0 || !strings.Contains(submit.stdout, "task_id="+taskID) {
		t.Fatalf("submit failed: %+v", submit)
	}
	waitForFile(t, barrierEntered)
	beforeKill := waitForM4CRExecutions(t, executables["dtm-query"], coreAddress, taskID, 1)
	oldExecution := findM4CRExecution(t, beforeKill.stdout, "step-1", 1)
	if oldExecution.Status != "EXECUTION_STATUS_STARTED" {
		t.Fatalf("old execution is not persisted STARTED: %#v", oldExecution)
	}
	oldRegistration := readM4CRRegistration(t, databasePath)

	killTimestamp := time.Now().UTC()
	if err := firstCore.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	firstCoreWait := firstCore.Wait()
	firstCoreExit := processExitCode(firstCoreWait)
	_ = firstConnection.Close()
	if err := firstAgent.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	firstAgentWait := firstAgent.Wait()

	secondCore, secondCoreOut, secondCoreErr := startCore()
	secondConnection := waitForCore(t, coreAddress)
	secondCorePID := secondCore.Process.Pid
	t.Cleanup(func() {
		_ = secondCore.Process.Kill()
		_, _ = secondCore.Process.Wait()
		_ = secondConnection.Close()
	})
	registry := dtmv1.NewNodeRegistryServiceClient(secondConnection)
	_, oldHeartbeatErr := registry.Heartbeat(context.Background(), &dtmv1.HeartbeatRequest{NodeId: "process-agent", RegistrationId: oldRegistration})
	if code := status.Code(oldHeartbeatErr); code != codes.FailedPrecondition && code != codes.NotFound {
		t.Fatalf("old registration heartbeat code=%s error=%v", code, oldHeartbeatErr)
	}
	preRegister := waitForM4CRExecutions(t, executables["dtm-query"], coreAddress, taskID, 1)
	time.Sleep(150 * time.Millisecond)
	stillPreRegister := runProcess(t, executables["dtm-query"], "executions", "--core-address", coreAddress, "--task-id", taskID, "--output", "json")
	if stillPreRegister.exit != 0 || executionCount(t, stillPreRegister.stdout) != 1 {
		t.Fatalf("attempt+1 created before Agent re-registration: %+v", stillPreRegister)
	}

	secondAgent, secondAgentOut, secondAgentErr := startProcess(executables["dtm-agent"], nil, "-config", agentTwoConfig)
	t.Cleanup(func() {
		_ = secondAgent.Process.Kill()
		_, _ = secondAgent.Process.Wait()
	})
	waitForText(t, secondAgentOut, "register success node_id=process-agent", secondAgentErr)
	secondClient := dtmv1.NewCoreServiceClient(secondConnection)
	waitForM4CTaskState(t, secondClient, taskID, dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED, secondCoreOut, secondCoreErr)
	finalTask := runProcess(t, executables["dtm-query"], "get", "--core-address", coreAddress, "--task-id", taskID, "--output", "json")
	finalExecutions := waitForM4CRExecutions(t, executables["dtm-query"], coreAddress, taskID, 3)
	newExecution := findM4CRExecution(t, finalExecutions.stdout, "step-1", 2)
	oldExecution = findM4CRExecution(t, finalExecutions.stdout, "step-1", 1)
	if oldExecution.FailureCode != "CORE_RESTART_INTERRUPTED" || newExecution.Status != "EXECUTION_STATUS_SUCCEEDED" ||
		oldExecution.ExecutionID == newExecution.ExecutionID || oldExecution.RequestID == newExecution.RequestID {
		t.Fatalf("recovered execution identities: old=%#v new=%#v", oldExecution, newExecution)
	}
	newRegistration := readM4CRRegistration(t, databasePath)
	if newRegistration == oldRegistration {
		t.Fatalf("registration did not rotate: %q", newRegistration)
	}
	if finalTask.exit != 0 || !strings.Contains(finalTask.stdout, "TASK_STATUS_SUCCEEDED") || !strings.Contains(finalTask.stdout, "STEP_STATUS_SUCCEEDED") {
		t.Fatalf("final task query: %+v", finalTask)
	}

	evidence := fmt.Sprintf("kill_timestamp=%s\ncore_pid_before=%d\ncore_pid_after=%d\ncore_kill_exit_code=%d\ncore_kill_wait_error=%v\nagent_pid_before=%d\nagent_kill_wait_error=%v\ntask_id=%s\nold_execution_id=%s\nold_request_id=%s\nold_attempt=%d\nold_failure_code=%s\nnew_execution_id=%s\nnew_request_id=%s\nnew_attempt=%d\nold_registration_id=%s\nnew_registration_id=%s\nold_registration_heartbeat_code=%s\nfinal_task_status=SUCCEEDED\nfinal_step_status=SUCCEEDED\n\nsubmit:\n%s\nbefore_kill:\n%s\nbefore_reregister:\n%s\nafter_recovery:\n%s\nfinal_task:\n%s\nfirst_core_stdout:\n%s\nfirst_core_stderr:\n%s\nsecond_core_stdout:\n%s\nsecond_core_stderr:\n%s\nfirst_agent_stdout:\n%s\nfirst_agent_stderr:\n%s\nsecond_agent_stdout:\n%s\nsecond_agent_stderr:\n%s\n",
		killTimestamp.Format(time.RFC3339Nano), firstCorePID, secondCorePID, firstCoreExit, firstCoreWait,
		firstAgentPID, firstAgentWait, taskID,
		oldExecution.ExecutionID, oldExecution.RequestID, oldExecution.Attempt, oldExecution.FailureCode,
		newExecution.ExecutionID, newExecution.RequestID, newExecution.Attempt,
		oldRegistration, newRegistration, status.Code(oldHeartbeatErr),
		submit.logText(), beforeKill.logText(), preRegister.logText(), finalExecutions.logText(), finalTask.logText(),
		firstCoreOut.String(), firstCoreErr.String(), secondCoreOut.String(), secondCoreErr.String(),
		firstAgentOut.String(), firstAgentErr.String(), secondAgentOut.String(), secondAgentErr.String())
	writeM4CProcessEvidence(t, "core-kill-running-execution.txt", evidence)
}

func m4crAgentConfig(coreAddress, agentAddress, databasePath string) string {
	return fmt.Sprintf("node:\n  id: process-agent\n  capabilities: [temperature_sensor, cooling_control]\n  advertise_address: %q\ncore:\n  address: %q\nserver:\n  listen_address: %q\nheartbeat:\n  interval: 20ms\nstorage:\n  path: %q\n", agentAddress, coreAddress, agentAddress, filepath.ToSlash(databasePath))
}

func writeM4CRFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func waitForFile(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for file %s", path)
}

func waitForText(t *testing.T, output *bytes.Buffer, needle string, diagnostic *bytes.Buffer) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(output.String(), needle) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %q\nstdout:\n%s\nstderr:\n%s", needle, output.String(), diagnostic.String())
}

func waitForM4CRExecutions(t *testing.T, executable, coreAddress, taskID string, count int) processResult {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var result processResult
	for time.Now().Before(deadline) {
		result = runProcess(t, executable, "executions", "--core-address", coreAddress, "--task-id", taskID, "--output", "json")
		if result.exit == 0 && executionCount(t, result.stdout) == count {
			return result
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("execution count did not reach %d: %+v", count, result)
	return result
}

func findM4CRExecution(t *testing.T, output, stepID string, attempt int) m4crExecution {
	t.Helper()
	var decoded struct {
		Executions []m4crExecution `json:"executions"`
	}
	if err := json.Unmarshal([]byte(output), &decoded); err != nil {
		t.Fatal(err)
	}
	for _, item := range decoded.Executions {
		if item.StepID == stepID && item.Attempt == attempt {
			return item
		}
	}
	t.Fatalf("execution %s attempt %d not found in %s", stepID, attempt, output)
	return m4crExecution{}
}

func readM4CRRegistration(t *testing.T, databasePath string) string {
	t.Helper()
	repository, err := sqlite.Open(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	record, err := repository.GetNode(context.Background(), model.NodeID("process-agent"))
	if err != nil {
		t.Fatal(err)
	}
	return record.RegistrationID
}

func processExitCode(err error) int {
	if err == nil {
		return 0
	}
	if exitError, ok := err.(*exec.ExitError); ok {
		return exitError.ExitCode()
	}
	return -1
}
