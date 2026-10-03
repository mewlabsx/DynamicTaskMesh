package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/application"
	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/platform/sqlite"
	"dtm/internal/storage"
	"dtm/internal/task"
	"dtm/internal/transport/grpcapi"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type integrationSubmitter struct{}

func (integrationSubmitter) Submit(context.Context, task.Task) (application.Outcome, error) {
	return application.Outcome{}, nil
}

type integrationHarness struct {
	factory queryClientFactory
}

func newIntegrationHarness(t *testing.T, seed func(*sqlite.Repository)) integrationHarness {
	t.Helper()
	repository, err := sqlite.Open(filepath.Join(t.TempDir(), "dtm.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	seed(repository)
	queries, err := application.NewTaskQueryService(repository)
	if err != nil {
		t.Fatal(err)
	}
	core, err := grpcapi.NewCoreServer(integrationSubmitter{}, grpcapi.WithTaskQueryService(queries))
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	dtmv1.RegisterCoreServiceServer(server, core)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	return integrationHarness{factory: func(ctx context.Context, _ string) (queryClient, connectionCloser, error) {
		connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		if err != nil {
			return nil, nil, err
		}
		return dtmv1.NewCoreServiceClient(connection), connection, nil
	}}
}

func seedQueryTask(t *testing.T, repository *sqlite.Repository, id string, state lifecycle.State, created time.Time) {
	t.Helper()
	record := storage.Task{ID: model.TaskID(id), Intent: "cool_environment", State: state, CreatedAt: created, UpdatedAt: created, Version: 1}
	switch state {
	case lifecycle.StateRunning:
		started := created
		record.StartedAt = &started
	case lifecycle.StateFailed:
		completed := created.Add(time.Second)
		record.CompletedAt = &completed
		record.UpdatedAt = completed
		record.FailureCode = "controlled_failure"
		record.FailureMessage = "controlled test failure"
	case lifecycle.StateSuccess:
		started, completed := created, created.Add(time.Second)
		record.StartedAt, record.CompletedAt, record.UpdatedAt = &started, &completed, completed
		result := execution.StepResult{StepID: "step-1", NodeID: "node-1", Status: execution.StatusSucceeded}
		nodeID := model.NodeID("node-1")
		record.Steps = []storage.TaskStep{{ID: "step-1", TaskID: record.ID, Sequence: 1, Capability: "cooling_control", State: lifecycle.StepStateSuccess, AssignedNodeID: &nodeID, AttemptCount: 1, MaxAttempts: 1, Result: &result, CreatedAt: created, UpdatedAt: completed, StartedAt: &started, CompletedAt: &completed, Version: 1}}
	}
	if err := repository.CreateTask(context.Background(), record); err != nil {
		t.Fatalf("seed task %s: %v", id, err)
	}
}

func decodeJSONObject(t *testing.T, output []byte) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(output, &decoded); err != nil {
		t.Fatalf("decode JSON %q: %v", string(output), err)
	}
	return decoded
}

func taskIDsFromJSON(t *testing.T, decoded map[string]any) []string {
	t.Helper()
	items, ok := decoded["tasks"].([]any)
	if !ok {
		t.Fatalf("tasks shape = %#v", decoded["tasks"])
	}
	ids := make([]string, 0, len(items))
	for _, item := range items {
		object, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("task shape = %#v", item)
		}
		id, ok := object["task_id"].(string)
		if !ok || id == "" {
			t.Fatalf("task_id shape = %#v", object["task_id"])
		}
		if _, ok := object["status"].(string); !ok {
			t.Fatalf("status shape = %#v", object["status"])
		}
		ids = append(ids, id)
	}
	return ids
}

func TestIntegrationPaginationTraversesAllPagesInStableOrder(t *testing.T) {
	base := time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC)
	harness := newIntegrationHarness(t, func(repository *sqlite.Repository) {
		for _, item := range []struct {
			id string
			at time.Time
		}{
			{id: "task-a", at: base},
			{id: "task-b", at: base.Add(time.Minute)},
			{id: "task-c", at: base.Add(2 * time.Minute)},
			{id: "task-d", at: base.Add(2 * time.Minute)},
			{id: "task-e", at: base.Add(3 * time.Minute)},
			{id: "task-f", at: base.Add(4 * time.Minute)},
		} {
			seedQueryTask(t, repository, item.id, lifecycle.StateCreated, item.at)
		}
	})
	want := []string{"task-f", "task-e", "task-d", "task-c", "task-b", "task-a"}
	var got []string
	token := ""
	pageCount := 0
	for {
		args := []string{"list", "--core-address", "localhost:1", "--limit", "2", "--output", "json"}
		if token != "" {
			args = append(args, "--page-token", token)
		}
		var stdout, stderr bytes.Buffer
		if code := runWith(context.Background(), args, &stdout, &stderr, harness.factory); code != 0 {
			t.Fatalf("page %d code=%d stdout=%q stderr=%q", pageCount+1, code, stdout.String(), stderr.String())
		}
		decoded := decodeJSONObject(t, stdout.Bytes())
		pageIDs := taskIDsFromJSON(t, decoded)
		pageCount++
		if len(pageIDs) != 2 {
			t.Fatalf("page %d size=%d ids=%v", pageCount, len(pageIDs), pageIDs)
		}
		got = append(got, pageIDs...)
		next, ok := decoded["next_page_token"].(string)
		if !ok {
			t.Fatalf("page %d next_page_token shape=%#v", pageCount, decoded["next_page_token"])
		}
		if len(got) < len(want) && next == "" {
			t.Fatalf("page %d missing continuation token", pageCount)
		}
		if len(got) == len(want) && next != "" {
			t.Fatalf("last page token=%q, want empty", next)
		}
		if next == "" {
			break
		}
		// The next invocation receives the exact opaque string emitted by the server.
		token = next
	}
	if pageCount != 3 || !reflect.DeepEqual(got, want) {
		t.Fatalf("pages=%d ids=%v want=%v", pageCount, got, want)
	}
	seen := make(map[string]bool, len(got))
	for _, id := range got {
		if seen[id] {
			t.Fatalf("duplicate task %s in %v", id, got)
		}
		seen[id] = true
	}

	var textOut bytes.Buffer
	if code := runWith(context.Background(), []string{"list", "--core-address", "localhost:1", "--limit", "2"}, &textOut, new(bytes.Buffer), harness.factory); code != 0 {
		t.Fatalf("text page code=%d", code)
	}
	marker := "Next Page Token:\n"
	position := strings.Index(textOut.String(), marker)
	if position < 0 || strings.TrimSpace(textOut.String()[position+len(marker):]) == "" {
		t.Fatalf("text token missing or truncated: %q", textOut.String())
	}

	var invalidOut, invalidErr bytes.Buffer
	code := runWith(context.Background(), []string{"list", "--core-address", "localhost:1", "--page-token", "not-a-valid-token"}, &invalidOut, &invalidErr, harness.factory)
	if code != exitArguments || invalidOut.Len() != 0 || invalidErr.String() != "dtm-query: the query arguments were rejected\n" {
		t.Fatalf("invalid token code=%d stdout=%q stderr=%q", code, invalidOut.String(), invalidErr.String())
	}
	t.Logf("pagination evidence: pages=%d page_size=2 total=%d order=%v", pageCount, len(got), got)
}

func TestIntegrationStatusFiltersReachSQLiteThroughGRPC(t *testing.T) {
	base := time.Date(2026, 8, 2, 10, 0, 0, 0, time.UTC)
	harness := newIntegrationHarness(t, func(repository *sqlite.Repository) {
		seedQueryTask(t, repository, "task-created", lifecycle.StateCreated, base)
		seedQueryTask(t, repository, "task-running", lifecycle.StateRunning, base.Add(time.Minute))
		seedQueryTask(t, repository, "task-succeeded", lifecycle.StateSuccess, base.Add(2*time.Minute))
		seedQueryTask(t, repository, "task-failed", lifecycle.StateFailed, base.Add(3*time.Minute))
	})
	for _, test := range []struct {
		status string
		want   []string
	}{
		{status: "CREATED", want: []string{"task-created"}},
		{status: "running", want: []string{"task-running"}},
		{status: "SuCcEeDeD", want: []string{"task-succeeded"}},
		{status: "TASK_STATUS_FAILED", want: []string{"task-failed"}},
		{status: "CANCELLED", want: []string{}},
	} {
		var stdout, stderr bytes.Buffer
		code := runWith(context.Background(), []string{"list", "--core-address", "localhost:1", "--status", test.status, "--output", "json"}, &stdout, &stderr, harness.factory)
		if code != 0 || stderr.Len() != 0 {
			t.Fatalf("status=%s code=%d stderr=%q", test.status, code, stderr.String())
		}
		got := taskIDsFromJSON(t, decodeJSONObject(t, stdout.Bytes()))
		if !reflect.DeepEqual(got, test.want) {
			t.Fatalf("status=%s got=%v want=%v", test.status, got, test.want)
		}
	}
	for _, invalid := range []string{"UNSPECIFIED", "TASK_STATUS_UNSPECIFIED", "unknown"} {
		called := false
		code := runWith(context.Background(), []string{"list", "--core-address", "localhost:1", "--status", invalid}, new(bytes.Buffer), new(bytes.Buffer), func(context.Context, string) (queryClient, connectionCloser, error) {
			called = true
			return nil, nil, nil
		})
		if code != exitArguments || called {
			t.Fatalf("invalid status=%s code=%d dialed=%v", invalid, code, called)
		}
	}
}

func TestIntegrationTimeFiltersUseExclusiveBoundaries(t *testing.T) {
	base := time.Date(2026, 8, 2, 11, 0, 0, 0, time.UTC)
	harness := newIntegrationHarness(t, func(repository *sqlite.Repository) {
		seedQueryTask(t, repository, "task-a", lifecycle.StateCreated, base)
		seedQueryTask(t, repository, "task-b", lifecycle.StateCreated, base.Add(time.Minute))
		seedQueryTask(t, repository, "task-c", lifecycle.StateCreated, base.Add(2*time.Minute))
	})
	query := func(args ...string) []string {
		full := append([]string{"list", "--core-address", "localhost:1", "--output", "json"}, args...)
		var stdout, stderr bytes.Buffer
		if code := runWith(context.Background(), full, &stdout, &stderr, harness.factory); code != 0 {
			t.Fatalf("args=%v code=%d stderr=%q", args, code, stderr.String())
		}
		return taskIDsFromJSON(t, decodeJSONObject(t, stdout.Bytes()))
	}
	boundary := base.Add(time.Minute).Format(time.RFC3339Nano)
	tests := []struct {
		args []string
		want []string
	}{
		{args: []string{"--created-after", boundary}, want: []string{"task-c"}},
		{args: []string{"--created-before", boundary}, want: []string{"task-a"}},
		{args: []string{"--created-after", base.Format(time.RFC3339Nano), "--created-before", base.Add(2 * time.Minute).Format(time.RFC3339Nano)}, want: []string{"task-b"}},
		{args: []string{"--created-after", boundary, "--created-before", boundary}, want: []string{}},
		{args: []string{"--created-after", base.Add(3 * time.Minute).Format(time.RFC3339Nano)}, want: []string{}},
	}
	for _, test := range tests {
		if got := query(test.args...); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("args=%v got=%v want=%v", test.args, got, test.want)
		}
	}
	for _, flagName := range []string{"--created-after", "--created-before"} {
		var stdout, stderr bytes.Buffer
		code := runWith(context.Background(), []string{"list", "--core-address", "localhost:1", flagName, "9999-12-31T23:59:59Z"}, &stdout, &stderr, harness.factory)
		if code != exitArguments || stdout.Len() != 0 || stderr.String() != "dtm-query: the query arguments were rejected\n" {
			t.Fatalf("overflow %s code=%d stdout=%q stderr=%q", flagName, code, stdout.String(), stderr.String())
		}
	}
}

func TestIntegrationJSONShapesForGetListAndExecutions(t *testing.T) {
	base := time.Date(2026, 8, 2, 12, 0, 0, 0, time.UTC)
	harness := newIntegrationHarness(t, func(repository *sqlite.Repository) {
		seedQueryTask(t, repository, "task-empty", lifecycle.StateCreated, base)
		seedRetryExecutions(t, repository, base.Add(time.Minute))
	})
	runJSON := func(args ...string) map[string]any {
		var stdout, stderr bytes.Buffer
		args = append(args, "--core-address", "localhost:1", "--output", "json")
		if code := runWith(context.Background(), args, &stdout, &stderr, harness.factory); code != 0 || stderr.Len() != 0 {
			t.Fatalf("args=%v code=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
		}
		return decodeJSONObject(t, stdout.Bytes())
	}
	get := runJSON("get", "--task-id", "task-empty")
	taskObject, ok := get["task"].(map[string]any)
	if !ok || taskObject["task_id"] != "task-empty" || taskObject["status"] != "TASK_STATUS_CREATED" {
		t.Fatalf("get shape=%#v", get)
	}
	if _, ok := taskObject["version"].(string); !ok {
		// protojson intentionally emits int64 as a JSON string.
		t.Fatalf("get version shape=%#v", taskObject["version"])
	}
	if steps, ok := taskObject["steps"].([]any); !ok || len(steps) != 0 {
		t.Fatalf("get steps shape=%#v", taskObject["steps"])
	}

	list := runJSON("list", "--limit", "1")
	if tasks, ok := list["tasks"].([]any); !ok || len(tasks) != 1 {
		t.Fatalf("list tasks shape=%#v", list["tasks"])
	}
	if token, ok := list["next_page_token"].(string); !ok || token == "" {
		t.Fatalf("list token shape=%#v", list["next_page_token"])
	}
	emptyList := runJSON("list", "--status", "CANCELLED")
	if tasks, ok := emptyList["tasks"].([]any); !ok || len(tasks) != 0 || emptyList["next_page_token"] != "" {
		t.Fatalf("empty list shape=%#v", emptyList)
	}

	executions := runJSON("executions", "--task-id", "task-retry")
	items, ok := executions["executions"].([]any)
	if !ok || len(items) != 2 {
		t.Fatalf("executions shape=%#v", executions["executions"])
	}
	for index, item := range items {
		object, ok := item.(map[string]any)
		if !ok || object["execution_id"] == "" || object["task_id"] != "task-retry" || object["step_id"] != "step-retry" {
			t.Fatalf("execution %d shape=%#v", index, item)
		}
		if _, ok := object["attempt_number"].(float64); !ok {
			t.Fatalf("execution %d attempt shape=%#v", index, object["attempt_number"])
		}
		statusName, ok := object["status"].(string)
		if !ok || !strings.HasPrefix(statusName, "EXECUTION_STATUS_") {
			t.Fatalf("execution %d status=%#v", index, object["status"])
		}
	}
	emptyExecutions := runJSON("executions", "--task-id", "task-empty")
	if items, ok := emptyExecutions["executions"].([]any); !ok || len(items) != 0 {
		t.Fatalf("empty executions shape=%#v", emptyExecutions)
	}
}

func TestIntegrationCLIQueriesRealSQLiteThroughGRPC(t *testing.T) {
	repository, err := sqlite.Open(filepath.Join(t.TempDir(), "dtm.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = repository.Close() })
	base := time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC)
	for index := 0; index < 5; index++ {
		id := model.TaskID("task-" + string(rune('a'+index)))
		record := storage.Task{ID: id, Intent: "cool_environment", State: lifecycle.StateCreated, CreatedAt: base.Add(time.Duration(index) * time.Minute), UpdatedAt: base.Add(time.Duration(index) * time.Minute), Version: 1}
		if err := repository.CreateTask(context.Background(), record); err != nil {
			t.Fatal(err)
		}
	}
	seedRetryExecutions(t, repository, base.Add(10*time.Minute))
	queries, err := application.NewTaskQueryService(repository)
	if err != nil {
		t.Fatal(err)
	}
	core, err := grpcapi.NewCoreServer(integrationSubmitter{}, grpcapi.WithTaskQueryService(queries))
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	dtmv1.RegisterCoreServiceServer(server, core)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })
	factory := func(ctx context.Context, _ string) (queryClient, connectionCloser, error) {
		connection, err := grpc.NewClient("passthrough:///bufnet", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		if err != nil {
			return nil, nil, err
		}
		return dtmv1.NewCoreServiceClient(connection), connection, nil
	}

	var first bytes.Buffer
	if code := runWith(context.Background(), []string{"list", "--core-address", "localhost:1", "--limit", "2"}, &first, new(bytes.Buffer), factory); code != 0 {
		t.Fatalf("first page code=%d output=%q", code, first.String())
	}
	tokenMarker := "Next Page Token:\n"
	position := strings.Index(first.String(), tokenMarker)
	if position < 0 {
		t.Fatalf("first page has no token: %q", first.String())
	}
	token := strings.TrimSpace(first.String()[position+len(tokenMarker):])
	var second bytes.Buffer
	if code := runWith(context.Background(), []string{"list", "--core-address", "localhost:1", "--limit", "2", "--page-token", token, "--output", "json"}, &second, new(bytes.Buffer), factory); code != 0 || !strings.Contains(second.String(), "task-") {
		t.Fatalf("second page code=%d output=%q", code, second.String())
	}
	if strings.Contains(first.String(), "task-c") && strings.Contains(second.String(), "task-c") {
		t.Fatalf("pages overlap: first=%q second=%q", first.String(), second.String())
	}

	var get bytes.Buffer
	if code := runWith(context.Background(), []string{"get", "--core-address", "localhost:1", "--task-id", "task-c"}, &get, new(bytes.Buffer), factory); code != 0 || !strings.Contains(get.String(), "task-c") {
		t.Fatalf("get code=%d output=%q", code, get.String())
	}
	var executions bytes.Buffer
	if code := runWith(context.Background(), []string{"executions", "--core-address", "localhost:1", "--task-id", "task-c"}, &executions, new(bytes.Buffer), factory); code != 0 || executions.String() != "No executions found for task task-c.\n" {
		t.Fatalf("executions code=%d output=%q", code, executions.String())
	}
	executions.Reset()
	if code := runWith(context.Background(), []string{"executions", "--core-address", "localhost:1", "--task-id", "task-retry", "--output", "json"}, &executions, new(bytes.Buffer), factory); code != 0 || !strings.Contains(executions.String(), "exec-1") || !strings.Contains(executions.String(), "exec-2") || strings.Index(executions.String(), "exec-1") > strings.Index(executions.String(), "exec-2") {
		t.Fatalf("retry executions code=%d output=%q", code, executions.String())
	}
	var filtered bytes.Buffer
	if code := runWith(context.Background(), []string{"list", "--core-address", "localhost:1", "--created-after", "2026-08-02T09:01:30Z", "--created-before", "2026-08-02T09:03:30Z", "--output", "json"}, &filtered, new(bytes.Buffer), factory); code != 0 || !strings.Contains(filtered.String(), "task-c") || !strings.Contains(filtered.String(), "task-d") || strings.Contains(filtered.String(), "task-b") {
		t.Fatalf("filtered code=%d output=%q", code, filtered.String())
	}
	var missingOut, missingErr bytes.Buffer
	if code := runWith(context.Background(), []string{"get", "--core-address", "localhost:1", "--task-id", "missing"}, &missingOut, &missingErr, factory); code != exitNotFound || missingOut.Len() != 0 {
		t.Fatalf("missing code=%d stdout=%q stderr=%q", code, missingOut.String(), missingErr.String())
	}
}

func seedRetryExecutions(t *testing.T, repository *sqlite.Repository, at time.Time) {
	t.Helper()
	ctx := context.Background()
	taskID, stepID := model.TaskID("task-retry"), model.StepID("step-retry")
	if err := repository.CreateTask(ctx, storage.Task{ID: taskID, Intent: "cool_environment", State: lifecycle.StateCreated, CreatedAt: at, UpdatedAt: at, Version: 1}); err != nil {
		t.Fatal(err)
	}
	event := func(kind, from, to string, when time.Time) storage.TaskEvent {
		id := stepID
		return storage.TaskEvent{TaskID: taskID, StepID: &id, Type: kind, FromState: from, ToState: to, CreatedAt: when}
	}
	step := storage.TaskStep{ID: stepID, TaskID: taskID, Sequence: 1, Capability: "cooling_control", Input: map[string]string{"target": "26"}, State: lifecycle.StepStateCreated, MaxAttempts: 3, CreatedAt: at, UpdatedAt: at, Version: 1}
	if _, err := repository.RecordTaskPlan(ctx, taskID, lifecycle.StateCreated, 1, []storage.TaskStep{step}, storage.TaskEvent{TaskID: taskID, Type: "planned", FromState: string(lifecycle.StateCreated), ToState: string(lifecycle.StatePlanning), CreatedAt: at.Add(time.Second)}); err != nil {
		t.Fatal(err)
	}
	stepVersion, err := repository.RecordStepAssignment(ctx, taskID, stepID, lifecycle.StepStateCreated, 1, "node-a", event("assigned", string(lifecycle.StepStateCreated), string(lifecycle.StepStateMapped), at.Add(2*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	stepVersion, err = repository.UpdateStepState(ctx, taskID, stepID, lifecycle.StepStateMapped, stepVersion, lifecycle.StepStateDispatched, nil, event("dispatched", string(lifecycle.StepStateMapped), string(lifecycle.StepStateDispatched), at.Add(3*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	first, stepVersion, err := repository.StartExecution(ctx, storage.StartExecutionRequest{ExecutionID: "exec-1", TaskID: taskID, StepID: stepID, AttemptNo: 1, NodeID: "node-a", Request: map[string]string{"target": "26"}, ExpectedStepState: lifecycle.StepStateDispatched, ExpectedStepVersion: stepVersion, StartedAt: at.Add(4 * time.Second), Event: event("started", string(lifecycle.StepStateDispatched), string(lifecycle.StepStateRunning), at.Add(4*time.Second))})
	if err != nil {
		t.Fatal(err)
	}
	failed := execution.StepResult{StepID: stepID, NodeID: "node-a", Status: execution.StatusFailed, Error: "temporary failure"}
	_, stepVersion, err = repository.CompleteExecution(ctx, storage.CompleteExecutionRequest{ExecutionID: first.ID, ExpectedExecutionState: storage.ExecutionStateStarted, ExpectedExecutionVersion: first.Version, ExpectedStepState: lifecycle.StepStateRunning, ExpectedStepVersion: stepVersion, NewExecutionState: storage.ExecutionStateFailed, NewStepState: lifecycle.StepStateFailed, Result: &failed, FailureCode: "temporary", FailureMessage: "temporary failure", CompletedAt: at.Add(5 * time.Second), Event: event("failed", string(lifecycle.StepStateRunning), string(lifecycle.StepStateFailed), at.Add(5*time.Second))})
	if err != nil {
		t.Fatal(err)
	}
	stepVersion, err = repository.UpdateStepState(ctx, taskID, stepID, lifecycle.StepStateFailed, stepVersion, lifecycle.StepStateRetrying, nil, event("retrying", string(lifecycle.StepStateFailed), string(lifecycle.StepStateRetrying), at.Add(6*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	stepVersion, err = repository.UpdateStepState(ctx, taskID, stepID, lifecycle.StepStateRetrying, stepVersion, lifecycle.StepStateDispatched, nil, event("redispatched", string(lifecycle.StepStateRetrying), string(lifecycle.StepStateDispatched), at.Add(7*time.Second)))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := repository.StartExecution(ctx, storage.StartExecutionRequest{ExecutionID: "exec-2", TaskID: taskID, StepID: stepID, AttemptNo: 2, NodeID: "node-a", Request: map[string]string{"target": "26"}, ExpectedStepState: lifecycle.StepStateDispatched, ExpectedStepVersion: stepVersion, StartedAt: at.Add(8 * time.Second), Event: event("started", string(lifecycle.StepStateDispatched), string(lifecycle.StepStateRunning), at.Add(8*time.Second))}); err != nil {
		t.Fatal(err)
	}
}
