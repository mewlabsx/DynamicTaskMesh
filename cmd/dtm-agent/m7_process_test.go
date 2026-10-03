package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestM7RealProcessAgentCorruptStoragePreventsRegistration(t *testing.T) {
	if os.Getenv("DTM_M7_REAL_PROCESS_E2E") != "1" {
		t.Skip("set DTM_M7_REAL_PROCESS_E2E=1")
	}
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	executable := filepath.Join(root, "dtm-agent")
	if runtime.GOOS == "windows" {
		executable += ".exe"
	}
	build := exec.Command("go", "build", "-o", executable, "./cmd/dtm-agent")
	build.Dir = repositoryRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build agent: %v\n%s", err, output)
	}
	databasePath := filepath.Join(root, "agent-corrupt.db")
	original := []byte("not sqlite; preserve agent forensic bytes")
	if err := os.WriteFile(databasePath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	beforeInfo, _ := os.Stat(databasePath)
	before := m7AgentHash(t, databasePath)
	agentAddress := m7AgentReserveAddress(t)
	configPath := filepath.Join(root, "agent.yaml")
	config := fmt.Sprintf("node:\n  id: m7-real-agent\n  capabilities: [temperature_sensor]\ncore:\n  address: 127.0.0.1:1\nserver:\n  address: %s\n  advertised_address: %s\nstorage:\n  path: %q\n", agentAddress, agentAddress, filepath.ToSlash(databasePath))
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	command := exec.Command(executable, "-config", configPath)
	command.Stdout, command.Stderr = &stdout, &stderr
	started := time.Now().UTC()
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	pid := command.Process.Pid
	err = command.Wait()
	finished := time.Now().UTC()
	exitCode := 0
	if exitErr, ok := err.(*exec.ExitError); ok {
		exitCode = exitErr.ExitCode()
	} else if err != nil {
		t.Fatal(err)
	}
	afterInfo, _ := os.Stat(databasePath)
	after := m7AgentHash(t, databasePath)
	if exitCode != 13 || before != after || beforeInfo.Size() != afterInfo.Size() || stdout.Len() != 0 || !strings.Contains(stderr.String(), "error_class=corrupt") {
		t.Fatalf("exit=%d size=%d/%d hash=%s/%s stdout=%q stderr=%q", exitCode, beforeInfo.Size(), afterInfo.Size(), before, after, stdout.String(), stderr.String())
	}
	if connection, err := net.DialTimeout("tcp", agentAddress, 100*time.Millisecond); err == nil {
		_ = connection.Close()
		t.Fatal("agent listened despite corrupt storage")
	}
	t.Logf("started_at=%s finished_at=%s program=dtm-agent pid=%d command=%q exit_code=%d database_path=%s database_size_before=%d database_size_after=%d database_sha256_before=%s database_sha256_after=%s stdout=%q stderr=%q registered=false capability_published=false", started.Format(time.RFC3339Nano), finished.Format(time.RFC3339Nano), pid, strings.Join(command.Args, " "), exitCode, filepath.Base(databasePath), beforeInfo.Size(), afterInfo.Size(), before, after, stdout.String(), stderr.String())
}

func m7AgentHash(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(contents)
	return hex.EncodeToString(sum[:])
}

func m7AgentReserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}
