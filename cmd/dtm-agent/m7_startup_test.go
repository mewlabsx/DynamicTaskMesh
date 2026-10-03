package main

import (
	"bytes"
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestM7AgentStorageFailurePreventsListenAndRegistration(t *testing.T) {
	root := t.TempDir()
	databasePath := filepath.Join(root, "corrupt-agent.db")
	original := []byte("not sqlite agent evidence")
	if err := os.WriteFile(databasePath, original, 0o600); err != nil {
		t.Fatal(err)
	}
	address := reserveAddress(t)
	configPath := writeAgentConfig(t, "node:\n  id: m7-agent\n  capabilities: [temperature_sensor]\ncore:\n  address: 127.0.0.1:1\nserver:\n  address: "+address+"\n  advertised_address: "+address+"\nstorage:\n  path: "+strconv.Quote(databasePath)+"\n")
	var stdout, stderr bytes.Buffer
	if code := run(context.Background(), []string{"-config", configPath}, &stdout, &stderr); code != 13 {
		t.Fatalf("run() code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if stdout.Len() != 0 || !strings.Contains(stderr.String(), "event=storage_startup_failed") || !strings.Contains(stderr.String(), "error_class=corrupt") {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	connection, err := netDialM7(address)
	if err == nil {
		_ = connection.Close()
		t.Fatal("agent listened despite storage startup failure")
	}
	got, err := os.ReadFile(databasePath)
	if err != nil || !bytes.Equal(got, original) {
		t.Fatalf("agent database changed: got=%q err=%v", got, err)
	}
}

func netDialM7(address string) (interface{ Close() error }, error) {
	return net.DialTimeout("tcp", address, 100*time.Millisecond)
}
