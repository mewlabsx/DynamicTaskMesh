package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"dtm/internal/lifecycle"
	"dtm/internal/model"
	storageport "dtm/internal/storage"
)

func TestCreateTaskSubmissionDeduplicatesAndRejectsConflictingFingerprint(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	first := submissionRequest("task-first", "key-1", "v1:aaa")
	created, err := repository.CreateTaskSubmission(ctx, first)
	if err != nil || !created.Created || created.Deduplicated {
		t.Fatalf("first submission = (%#v, %v)", created, err)
	}

	retry := submissionRequest("task-retry", "key-1", "v1:aaa")
	deduplicated, err := repository.CreateTaskSubmission(ctx, retry)
	if err != nil {
		t.Fatal(err)
	}
	if !deduplicated.Deduplicated || deduplicated.Created || deduplicated.Task.ID != first.Task.ID {
		t.Fatalf("retry result = %#v", deduplicated)
	}
	if _, err := repository.CreateTaskSubmission(ctx, submissionRequest("task-conflict", "key-1", "v1:bbb")); !errors.Is(err, storageport.ErrIdempotencyConflict) {
		t.Fatalf("conflicting fingerprint error = %v", err)
	}
	assertTableCount(t, repository, "tasks", 1)
	assertTableCount(t, repository, "task_submission_keys", 1)
	assertTableCount(t, repository, "task_events", 1)
	if _, err := repository.GetTask(ctx, retry.Task.ID); !errors.Is(err, storageport.ErrNotFound) {
		t.Fatalf("rolled-back retry task error = %v", err)
	}
}

func TestCreateTaskSubmissionEmptyKeyKeepsCompatibility(t *testing.T) {
	repository := newRepository(t)
	for _, id := range []model.TaskID{"task-one", "task-two"} {
		result, err := repository.CreateTaskSubmission(context.Background(), submissionRequest(id, "", ""))
		if err != nil || !result.Created || result.Deduplicated {
			t.Fatalf("submission %q = (%#v, %v)", id, result, err)
		}
	}
	assertTableCount(t, repository, "tasks", 2)
	assertTableCount(t, repository, "task_submission_keys", 0)
	assertTableCount(t, repository, "task_events", 2)
}

func TestCreateTaskSubmissionConcurrentSameKeyCreatesExactlyOneTask(t *testing.T) {
	repository := newRepository(t)
	const count = 50
	start := make(chan struct{})
	results := make(chan storageport.CreateTaskSubmissionResult, count)
	errorsCh := make(chan error, count)
	var group sync.WaitGroup
	for index := 0; index < count; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			result, err := repository.CreateTaskSubmission(context.Background(), submissionRequest(
				model.TaskID(fmt.Sprintf("task-%02d", index)), "shared-key", "v1:same",
			))
			results <- result
			errorsCh <- err
		}(index)
	}
	close(start)
	group.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	created, deduplicated := 0, 0
	var taskID model.TaskID
	for result := range results {
		if result.Created {
			created++
		}
		if result.Deduplicated {
			deduplicated++
		}
		if taskID == "" {
			taskID = result.Task.ID
		} else if result.Task.ID != taskID {
			t.Fatalf("task ID = %q, want %q", result.Task.ID, taskID)
		}
	}
	if created != 1 || deduplicated != count-1 {
		t.Fatalf("created=%d deduplicated=%d", created, deduplicated)
	}
	assertTableCount(t, repository, "tasks", 1)
	assertTableCount(t, repository, "task_events", 1)
}

func TestCreateTaskSubmissionConcurrentConflictingRequestsBindOneFingerprint(t *testing.T) {
	repository := newRepository(t)
	const count = 40
	start := make(chan struct{})
	errorsCh := make(chan error, count)
	var group sync.WaitGroup
	for index := 0; index < count; index++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			fingerprint := "v1:a"
			if index%2 == 1 {
				fingerprint = "v1:b"
			}
			_, err := repository.CreateTaskSubmission(context.Background(), submissionRequest(
				model.TaskID(fmt.Sprintf("conflict-%02d", index)), "contended-key", fingerprint,
			))
			errorsCh <- err
		}(index)
	}
	close(start)
	group.Wait()
	close(errorsCh)
	conflicts := 0
	for err := range errorsCh {
		if errors.Is(err, storageport.ErrIdempotencyConflict) {
			conflicts++
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if conflicts != count/2 {
		t.Fatalf("conflicts = %d, want %d", conflicts, count/2)
	}
	assertTableCount(t, repository, "tasks", 1)
	assertTableCount(t, repository, "task_events", 1)
}

func TestCreateTaskSubmissionSurvivesRepositoryRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	first, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	created, err := first.CreateTaskSubmission(context.Background(), submissionRequest("task-before-restart", "restart-key", "v1:same"))
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	retry, err := second.CreateTaskSubmission(context.Background(), submissionRequest("task-after-restart", "restart-key", "v1:same"))
	if err != nil {
		t.Fatal(err)
	}
	if !retry.Deduplicated || retry.Task.ID != created.Task.ID {
		t.Fatalf("restart retry = %#v, first = %#v", retry, created)
	}
}

func TestCreateTaskSubmissionRollsBackTaskEventAndKeyOnInjectedKeyFailure(t *testing.T) {
	repository := newRepository(t)
	_, err := repository.db.Exec(`
CREATE TRIGGER fail_submission_key
BEFORE INSERT ON task_submission_keys
WHEN NEW.idempotency_key = 'trigger-failure'
BEGIN
	SELECT RAISE(ABORT, 'injected key insert failure');
END`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repository.CreateTaskSubmission(context.Background(), submissionRequest("task-rollback", "trigger-failure", "v1:same"))
	if err == nil {
		t.Fatal("injected key failure unexpectedly succeeded")
	}
	assertTableCount(t, repository, "tasks", 0)
	assertTableCount(t, repository, "task_events", 0)
	assertTableCount(t, repository, "task_submission_keys", 0)
}

func TestM7DatabaseFullSubmissionFailsAtomicallyAndRecovers(t *testing.T) {
	repository := newRepository(t)
	if _, err := repository.db.Exec(`VACUUM`); err != nil {
		t.Fatal(err)
	}
	var pages int
	if err := repository.db.QueryRow(`PRAGMA page_count`).Scan(&pages); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.db.Exec(fmt.Sprintf(`PRAGMA max_page_count=%d`, pages)); err != nil {
		t.Fatal(err)
	}
	request := submissionRequest("task-full", "full-key", "v1:full")
	request.Task.Constraints = map[string]string{"large": strings.Repeat("x", 1024*1024)}
	_, err := repository.CreateTaskSubmission(context.Background(), request)
	if !errors.Is(err, storageport.ErrStorageFull) {
		t.Fatalf("CreateTaskSubmission() error = %v, want storage full", err)
	}
	assertTableCount(t, repository, "tasks", 0)
	assertTableCount(t, repository, "task_events", 0)
	assertTableCount(t, repository, "task_submission_keys", 0)
	if _, err := repository.db.Exec(fmt.Sprintf(`PRAGMA max_page_count=%d`, pages+4096)); err != nil {
		t.Fatal(err)
	}
	created, err := repository.CreateTaskSubmission(context.Background(), request)
	if err != nil || !created.Created {
		t.Fatalf("submission after capacity recovery = %#v, %v", created, err)
	}
}

func TestCreateTaskSubmissionCorruptBindingFailsClosed(t *testing.T) {
	repository := newRepository(t)
	if _, err := repository.db.Exec("PRAGMA foreign_keys = OFF"); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.db.Exec(`
INSERT INTO task_submission_keys(idempotency_key, request_fingerprint, task_id, created_at)
VALUES ('corrupt-key', 'v1:same', 'missing-task', 1)`); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatal(err)
	}
	_, err := repository.CreateTaskSubmission(context.Background(), submissionRequest("candidate-task", "corrupt-key", "v1:same"))
	if !errors.Is(err, storageport.ErrSubmissionBindingCorrupt) {
		t.Fatalf("corrupt binding error = %v, want ErrSubmissionBindingCorrupt", err)
	}
	if strings.Contains(err.Error(), "corrupt-key") || strings.Contains(err.Error(), "missing-task") {
		t.Fatalf("corrupt binding error leaked binding data: %v", err)
	}
	assertTableCount(t, repository, "tasks", 0)
	assertTableCount(t, repository, "task_events", 0)
	assertTableCount(t, repository, "task_submission_keys", 1)
}

func submissionRequest(taskID model.TaskID, key, fingerprint string) storageport.CreateTaskSubmissionRequest {
	now := testTime(0)
	return storageport.CreateTaskSubmissionRequest{
		Task: baseTask(taskID, now),
		Event: storageport.TaskEvent{
			TaskID: taskID, Type: "task_accepted", ToState: string(lifecycle.StateCreated), CreatedAt: now,
		},
		IdempotencyKey: key, RequestFingerprint: fingerprint,
	}
}

func assertTableCount(t *testing.T, repository *Repository, table string, want int) {
	t.Helper()
	var got int
	if err := repository.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", table, got, want)
	}
}
