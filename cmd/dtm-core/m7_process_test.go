package main

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
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

	sqliteplatform "dtm/internal/platform/sqlite"

	_ "modernc.org/sqlite"
)

func TestM7RealProcessCorePathFailure(t *testing.T) {
	m7RequireRealProcess(t)
	root, executable := m7BuildExecutable(t, "dtm-core")
	blocking := filepath.Join(root, "blocking-file")
	if err := os.WriteFile(blocking, []byte("block"), 0o600); err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(blocking, "core.db")
	config := m7WriteCoreConfig(t, root, databasePath, m7ReserveAddress(t), "WAL", 80*time.Millisecond)
	result := m7RunCommand(t, executable, "-config", config)
	if result.exit != 10 || !strings.Contains(result.stderr, "error_class=path") || strings.Contains(result.stdout, "listening") {
		t.Fatalf("path result=%+v", result)
	}
	if _, err := os.Stat(databasePath); !os.IsNotExist(err) {
		t.Fatalf("alternative database was created: %v", err)
	}
	m7LogProcess(t, "dtm-core", databasePath, result, "busy_timeout=80ms")
}

func TestM7RealProcessCoreDatabaseLockedAndRecovery(t *testing.T) {
	m7RequireRealProcess(t)
	root, executable := m7BuildExecutable(t, "dtm-core")
	databasePath := filepath.Join(root, "locked.db")
	database, err := sqliteplatform.OpenWithOptions(sqliteplatform.Options{Path: databasePath, AutoMigrate: true, JournalMode: "DELETE", BusyTimeout: 80 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	_ = database.Close()
	lock := m7OpenRaw(t, databasePath)
	if _, err := lock.Exec(`PRAGMA journal_mode=DELETE`); err != nil {
		t.Fatal(err)
	}
	if _, err := lock.Exec(`BEGIN EXCLUSIVE`); err != nil {
		t.Fatal(err)
	}
	lockStarted := time.Now().UTC()
	address := m7ReserveAddress(t)
	config := m7WriteCoreConfig(t, root, databasePath, address, "DELETE", 80*time.Millisecond)
	result := m7RunCommand(t, executable, "-config", config)
	if result.exit != 11 || !strings.Contains(result.stderr, "error_class=unavailable") || result.finished.Sub(result.started) > 2*time.Second {
		t.Fatalf("locked result=%+v", result)
	}
	if _, err := lock.Exec(`ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	lockReleased := time.Now().UTC()
	_ = lock.Close()
	process, output := m7StartCore(t, executable, config)
	m7StopProcess(process)
	t.Logf("started_at=%s finished_at=%s program=dtm-core pid=%d command=%q exit_code=11 database_path=%s busy_timeout=80ms lock_started_at=%s lock_released_at=%s retry_start=PASS stdout=%q stderr=%q", result.started.Format(time.RFC3339Nano), result.finished.Format(time.RFC3339Nano), result.pid, result.command, filepath.Base(databasePath), lockStarted.Format(time.RFC3339Nano), lockReleased.Format(time.RFC3339Nano), output, result.stderr)
}

func TestM7RealProcessCoreCorruptPreservesDatabase(t *testing.T) {
	m7RequireRealProcess(t)
	root, executable := m7BuildExecutable(t, "dtm-core")
	databasePath := filepath.Join(root, "corrupt.db")
	if err := os.WriteFile(databasePath, []byte("not sqlite; preserve forensic bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	beforeInfo, _ := os.Stat(databasePath)
	before := m7FileHash(t, databasePath)
	config := m7WriteCoreConfig(t, root, databasePath, m7ReserveAddress(t), "WAL", 80*time.Millisecond)
	result := m7RunCommand(t, executable, "-config", config)
	afterInfo, _ := os.Stat(databasePath)
	after := m7FileHash(t, databasePath)
	if result.exit != 13 || before != after || beforeInfo.Size() != afterInfo.Size() {
		t.Fatalf("corrupt result=%+v size=%d/%d hash=%s/%s", result, beforeInfo.Size(), afterInfo.Size(), before, after)
	}
	t.Logf("started_at=%s finished_at=%s program=dtm-core pid=%d command=%q exit_code=%d database_path=%s database_size_before=%d database_size_after=%d database_sha256_before=%s database_sha256_after=%s stdout=%q stderr=%q", result.started.Format(time.RFC3339Nano), result.finished.Format(time.RFC3339Nano), result.pid, result.command, result.exit, filepath.Base(databasePath), beforeInfo.Size(), afterInfo.Size(), before, after, result.stdout, result.stderr)
}

func TestM7RealProcessCoreMigrationMismatch(t *testing.T) {
	m7RequireRealProcess(t)
	root, executable := m7BuildExecutable(t, "dtm-core")
	databasePath := filepath.Join(root, "core.db")
	prepareM7CoreChecksumMismatch(t, root)
	config := m7WriteCoreConfig(t, root, databasePath, m7ReserveAddress(t), "WAL", 80*time.Millisecond)
	result := m7RunCommand(t, executable, "-config", config)
	if result.exit != 12 || !strings.Contains(result.stderr, "error_class=schema_mismatch") {
		t.Fatalf("migration result=%+v", result)
	}
	m7LogProcess(t, "dtm-core", databasePath, result, "migration_mismatch=checksum")
}

func TestM7RealProcessRuntimeLockRejectsSubmissionAndRecovers(t *testing.T) {
	m7RequireRealProcess(t)
	root, coreExecutable := m7BuildExecutable(t, "dtm-core")
	_, submitExecutable := m7BuildExecutableAt(t, root, "dtm-submit")
	_, queryExecutable := m7BuildExecutableAt(t, root, "dtm-query")
	databasePath := filepath.Join(root, "runtime.db")
	address := m7ReserveAddress(t)
	config := m7WriteCoreConfig(t, root, databasePath, address, "DELETE", 80*time.Millisecond)
	core, coreOutput := m7StartCore(t, coreExecutable, config)
	defer m7StopProcess(core)
	lock := m7OpenRaw(t, databasePath)
	if _, err := lock.Exec(`BEGIN EXCLUSIVE`); err != nil {
		t.Fatal(err)
	}
	lockStarted := time.Now().UTC()
	failed := m7RunCommand(t, submitExecutable, "-core", address, "-async", "-task-id", "m7-runtime", "-idempotency-key", "m7-runtime-key")
	if failed.exit != 4 || failed.stdout != "" {
		t.Fatalf("locked submit=%+v", failed)
	}
	for _, forbidden := range []string{"SQLite", "SELECT", "INSERT", databasePath, "m7-runtime-key"} {
		if strings.Contains(failed.stderr, forbidden) {
			t.Fatalf("locked submit leaked %q: %q", forbidden, failed.stderr)
		}
	}
	countsBefore := m7RuntimeCounts(t, lock)
	if countsBefore != [6]int{} {
		t.Fatalf("locked submission left half state: %v", countsBefore)
	}
	if _, err := lock.Exec(`ROLLBACK`); err != nil {
		t.Fatal(err)
	}
	lockReleased := time.Now().UTC()
	_ = lock.Close()
	succeeded := m7RunCommand(t, submitExecutable, "-core", address, "-async", "-task-id", "m7-runtime", "-idempotency-key", "m7-runtime-key")
	if succeeded.exit != 0 {
		t.Fatalf("submit after release=%+v core=%s", succeeded, coreOutput)
	}
	query := m7RunCommand(t, queryExecutable, "list", "--core-address", address, "--output", "json")
	if query.exit != 0 || !strings.Contains(query.stdout, "m7-runtime") {
		t.Fatalf("query after recovery=%+v", query)
	}
	db := m7OpenRaw(t, databasePath)
	countsAfter := m7RuntimeCounts(t, db)
	_ = db.Close()
	t.Logf("started_at=%s finished_at=%s program=dtm-submit pid=%d command=%q exit_code=%d database_path=%s busy_timeout=80ms lock_started_at=%s lock_released_at=%s task_count=%d task_event_count=%d submission_key_count=%d node_count=%d step_count=%d execution_count=%d retry_exit=%d query_pid=%d stdout=%q stderr=%q", failed.started.Format(time.RFC3339Nano), query.finished.Format(time.RFC3339Nano), failed.pid, failed.command, failed.exit, filepath.Base(databasePath), lockStarted.Format(time.RFC3339Nano), lockReleased.Format(time.RFC3339Nano), countsAfter[0], countsAfter[1], countsAfter[2], countsAfter[3], countsAfter[4], countsAfter[5], succeeded.exit, query.pid, failed.stdout, failed.stderr)
}

type m7CommandResult struct {
	pid                     int
	exit                    int
	started, finished       time.Time
	command, stdout, stderr string
}

func m7RequireRealProcess(t *testing.T) {
	t.Helper()
	if os.Getenv("DTM_M7_REAL_PROCESS_E2E") != "1" {
		t.Skip("set DTM_M7_REAL_PROCESS_E2E=1")
	}
}

func m7BuildExecutable(t *testing.T, name string) (string, string) {
	t.Helper()
	root := t.TempDir()
	return m7BuildExecutableAt(t, root, name)
}

func m7BuildExecutableAt(t *testing.T, outputRoot, name string) (string, string) {
	t.Helper()
	repositoryRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(outputRoot, name)
	if runtime.GOOS == "windows" {
		executable += ".exe"
	}
	command := exec.Command("go", "build", "-o", executable, "./cmd/"+name)
	command.Dir = repositoryRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build %s: %v\n%s", name, err, output)
	}
	return outputRoot, executable
}

func m7WriteCoreConfig(t *testing.T, root, databasePath, address, journal string, timeout time.Duration) string {
	t.Helper()
	path := filepath.Join(root, "core-"+strings.ToLower(journal)+".yaml")
	contents := fmt.Sprintf("server:\n  address: %q\nstorage:\n  path: %q\n  journal_mode: %q\n  busy_timeout: %q\n  auto_migrate: true\n", address, filepath.ToSlash(databasePath), journal, timeout.String())
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func m7ReserveAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func m7RunCommand(t *testing.T, executable string, arguments ...string) m7CommandResult {
	t.Helper()
	var stdout, stderr bytes.Buffer
	command := exec.Command(executable, arguments...)
	command.Stdout, command.Stderr = &stdout, &stderr
	result := m7CommandResult{started: time.Now().UTC(), command: strings.Join(append([]string{executable}, arguments...), " ")}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	result.pid = command.Process.Pid
	err := command.Wait()
	result.finished, result.stdout, result.stderr = time.Now().UTC(), stdout.String(), stderr.String()
	if err == nil {
		return result
	}
	if exitErr, ok := err.(*exec.ExitError); ok {
		result.exit = exitErr.ExitCode()
		return result
	}
	t.Fatal(err)
	return result
}

func m7StartCore(t *testing.T, executable, config string) (*exec.Cmd, string) {
	t.Helper()
	stdout := newSignalBuffer()
	var stderr bytes.Buffer
	command := exec.Command(executable, "-config", config)
	command.Stdout, command.Stderr = stdout, &stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	line := stdout.waitFor(t, "dtm-core listening ")
	return command, line
}

func m7StopProcess(command *exec.Cmd) {
	if command == nil || command.Process == nil {
		return
	}
	_ = command.Process.Kill()
	_ = command.Wait()
}

func m7OpenRaw(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?mode=rw")
	if err != nil {
		t.Fatal(err)
	}
	return db
}

func m7FileHash(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(contents)
	return hex.EncodeToString(sum[:])
}

func m7RuntimeCounts(t *testing.T, db *sql.DB) [6]int {
	t.Helper()
	var result [6]int
	for index, table := range []string{"tasks", "task_events", "task_submission_keys", "nodes", "task_steps", "executions"} {
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&result[index]); err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func m7LogProcess(t *testing.T, program, databasePath string, result m7CommandResult, detail string) {
	t.Helper()
	info, _ := os.Stat(databasePath)
	size := int64(0)
	hash := "missing"
	if info != nil {
		size = info.Size()
		hash = m7FileHash(t, databasePath)
	}
	t.Logf("started_at=%s finished_at=%s program=%s pid=%d command=%q exit_code=%d database_path=%s database_size_before=%d database_size_after=%d database_sha256_before=%s database_sha256_after=%s %s stdout=%q stderr=%q", result.started.Format(time.RFC3339Nano), result.finished.Format(time.RFC3339Nano), program, result.pid, result.command, result.exit, filepath.Base(databasePath), size, size, hash, hash, detail, result.stdout, result.stderr)
}
