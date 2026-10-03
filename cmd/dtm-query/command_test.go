package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type fakeQueryClient struct {
	getRequest        *dtmv1.GetTaskRequest
	listRequest       *dtmv1.ListTasksRequest
	executionRequest  *dtmv1.GetTaskExecutionsRequest
	getResponse       *dtmv1.GetTaskResponse
	listResponse      *dtmv1.ListTasksResponse
	executionResponse *dtmv1.GetTaskExecutionsResponse
	err               error
	waitForContext    bool
}

func (client *fakeQueryClient) GetTask(ctx context.Context, request *dtmv1.GetTaskRequest, _ ...grpc.CallOption) (*dtmv1.GetTaskResponse, error) {
	client.getRequest = request
	if client.waitForContext {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return client.getResponse, client.err
}
func (client *fakeQueryClient) ListTasks(ctx context.Context, request *dtmv1.ListTasksRequest, _ ...grpc.CallOption) (*dtmv1.ListTasksResponse, error) {
	client.listRequest = request
	return client.listResponse, client.err
}
func (client *fakeQueryClient) GetTaskExecutions(ctx context.Context, request *dtmv1.GetTaskExecutionsRequest, _ ...grpc.CallOption) (*dtmv1.GetTaskExecutionsResponse, error) {
	client.executionRequest = request
	return client.executionResponse, client.err
}

type fakeConnection struct{ closed bool }

func (connection *fakeConnection) Close() error { connection.closed = true; return nil }

type failingWriter struct {
	writes int
	failAt int
}

func (writer *failingWriter) Write(data []byte) (int, error) {
	writer.writes++
	if writer.writes >= writer.failAt {
		return 0, errors.New("injected writer failure with secret path C:\\private")
	}
	return len(data), nil
}

func TestFlagsParseListFiltersAndPreservePageToken(t *testing.T) {
	options, err := parseCommand([]string{"list", "--core-address", "127.0.0.1:50051", "--status", " RuNnInG ", "--created-after", "2026-08-02T09:00:00.123456789Z", "--created-before", "2026-08-03T00:00:00+08:00", "--limit", "20", "--page-token", " opaque token ", "--output", "JSON", "--timeout", "3s"}, new(bytes.Buffer))
	if err != nil {
		t.Fatal(err)
	}
	if options.status != dtmv1.TaskStatus_TASK_STATUS_RUNNING || options.limit != 20 || options.pageToken != " opaque token " || options.output != "json" || options.timeout != 3*time.Second {
		t.Fatalf("options = %#v", options)
	}
	if got := options.createdAfter.AsTime().Format(time.RFC3339Nano); got != "2026-08-02T09:00:00.123456789Z" {
		t.Fatalf("after = %s", got)
	}
	if got := options.createdBefore.AsTime().Location(); got != time.UTC {
		t.Fatalf("before location = %v", got)
	}
}

func TestFlagsUseConfigAndAllowExplicitAddressOverride(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.yaml")
	if err := os.WriteFile(path, []byte("server:\n  address: 127.0.0.1:50051\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	options, err := parseCommand([]string{"list", "--config", path}, new(bytes.Buffer))
	if err != nil || options.coreAddress != "127.0.0.1:50051" {
		t.Fatalf("options=%#v err=%v", options, err)
	}
	options, err = parseCommand([]string{"list", "--config", path, "--core-address", "localhost:6000"}, new(bytes.Buffer))
	if err != nil || options.coreAddress != "localhost:6000" {
		t.Fatalf("override=%#v err=%v", options, err)
	}
}

func TestFlagsRejectInvalidInputsBeforeDial(t *testing.T) {
	missingConfig := filepath.Join(t.TempDir(), "missing.yaml")
	tests := [][]string{
		{}, {"watch"}, {"get", "--core-address", "127.0.0.1:1"}, {"get", "--task-id", " ", "--core-address", "127.0.0.1:1"},
		{"get", "--task-id", "x", "extra", "--core-address", "127.0.0.1:1"}, {"list", "--output", "xml", "--core-address", "127.0.0.1:1"},
		{"get", "--task-id", "x", "--status", "running", "--core-address", "127.0.0.1:1"}, {"list", "--task-id", "x", "--core-address", "127.0.0.1:1"},
		{"list", "--timeout", "0", "--core-address", "127.0.0.1:1"}, {"list", "--timeout", "-1s", "--core-address", "127.0.0.1:1"},
		{"list", "--status", "unspecified", "--core-address", "127.0.0.1:1"}, {"list", "--status", "unknown", "--core-address", "127.0.0.1:1"},
		{"list", "--created-after", "today", "--core-address", "127.0.0.1:1"}, {"list", "--created-after", "2026-08-03T00:00:00Z", "--created-before", "2026-08-02T00:00:00Z", "--core-address", "127.0.0.1:1"},
		{"list", "--limit", "-1", "--core-address", "127.0.0.1:1"}, {"list", "--limit", "201", "--core-address", "127.0.0.1:1"},
		{"executions", "--core-address", "127.0.0.1:1"}, {"list", "--config", missingConfig}, {"list", "--core-address", "bad"},
	}
	for _, args := range tests {
		called := false
		code := runWith(context.Background(), args, new(bytes.Buffer), new(bytes.Buffer), func(context.Context, string) (queryClient, connectionCloser, error) {
			called = true
			return nil, nil, errors.New("unexpected")
		})
		if code != exitArguments || called {
			t.Fatalf("args=%v code=%d called=%v", args, code, called)
		}
	}
}

func TestCommandGetTextAndJSONHaveCleanStdout(t *testing.T) {
	now := timestamppb.New(time.Date(2026, 8, 2, 9, 0, 0, 123, time.UTC))
	response := &dtmv1.GetTaskResponse{Task: &dtmv1.TaskDetails{TaskId: "task-1", Intent: "制冷", Status: dtmv1.TaskStatus_TASK_STATUS_RUNNING, CreatedAt: now, UpdatedAt: now, Version: 2, Steps: []*dtmv1.TaskStepDetails{{StepId: "step-1", Sequence: 1, Capability: "temperature.read", Status: dtmv1.StepStatus_STEP_STATUS_RETRYING, AttemptCount: 1, MaxAttempts: 3}}}}
	for _, output := range []string{"text", "json"} {
		client := &fakeQueryClient{getResponse: response}
		connection := &fakeConnection{}
		var stdout, stderr bytes.Buffer
		code := runWith(context.Background(), []string{"get", "--task-id", " task-1 ", "--core-address", "127.0.0.1:1", "--output", output}, &stdout, &stderr, func(context.Context, string) (queryClient, connectionCloser, error) { return client, connection, nil })
		if code != 0 || stderr.Len() != 0 || !connection.closed || client.getRequest.GetTaskId() != "task-1" {
			t.Fatalf("output=%s code=%d stdout=%q stderr=%q", output, code, stdout.String(), stderr.String())
		}
		if output == "json" {
			var decoded map[string]any
			if err := json.Unmarshal(stdout.Bytes(), &decoded); err != nil {
				t.Fatalf("json=%q err=%v", stdout.String(), err)
			}
		} else if !strings.Contains(stdout.String(), "RETRYING") || !strings.Contains(stdout.String(), "制冷") {
			t.Fatalf("text=%q", stdout.String())
		}
	}
}

func TestOutputHandlesEmptyListsAndStableToken(t *testing.T) {
	var output bytes.Buffer
	if err := renderTaskList(&output, &dtmv1.ListTasksResponse{}); err != nil || output.String() != "No tasks found.\n" {
		t.Fatalf("empty list=%q err=%v", output.String(), err)
	}
	output.Reset()
	response := &dtmv1.ListTasksResponse{Tasks: []*dtmv1.TaskSummary{{TaskId: "task-1", Intent: "intent", Status: dtmv1.TaskStatus_TASK_STATUS_CREATED}}, NextPageToken: "untruncated-token=="}
	if err := renderTaskList(&output, response); err != nil || !strings.Contains(output.String(), "untruncated-token==") {
		t.Fatalf("list=%q err=%v", output.String(), err)
	}
	output.Reset()
	if err := renderExecutions(&output, "task-1", nil); err != nil || output.String() != "No executions found for task task-1.\n" {
		t.Fatalf("executions=%q err=%v", output.String(), err)
	}
}

func TestOutputPropagatesWriterErrorsAndStopsWriting(t *testing.T) {
	now := timestamppb.New(time.Date(2026, 8, 2, 9, 0, 0, 0, time.UTC))
	task := &dtmv1.TaskDetails{TaskId: "task-1", Intent: "cool_environment", Status: dtmv1.TaskStatus_TASK_STATUS_RUNNING, CreatedAt: now, UpdatedAt: now, Steps: []*dtmv1.TaskStepDetails{{StepId: "step-1", Status: dtmv1.StepStatus_STEP_STATUS_RUNNING}}}
	list := &dtmv1.ListTasksResponse{Tasks: []*dtmv1.TaskSummary{{TaskId: "task-1", Status: dtmv1.TaskStatus_TASK_STATUS_RUNNING, CreatedAt: now, UpdatedAt: now}}, NextPageToken: "opaque-token"}
	executions := []*dtmv1.TaskExecution{{ExecutionId: "exec-1", TaskId: "task-1", StepId: "step-1", Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_STARTED}}
	tests := []struct {
		name   string
		render func(io.Writer) error
	}{
		{name: "task", render: func(writer io.Writer) error { return renderTask(writer, task) }},
		{name: "list", render: func(writer io.Writer) error { return renderTaskList(writer, list) }},
		{name: "executions", render: func(writer io.Writer) error { return renderExecutions(writer, "task-1", executions) }},
		{name: "json", render: func(writer io.Writer) error {
			return renderResponse(writer, commandOptions{output: "json"}, &dtmv1.GetTaskResponse{Task: task})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer := &failingWriter{failAt: 1}
			if err := test.render(writer); err == nil {
				t.Fatal("render error = nil")
			}
			if writer.writes != 1 {
				t.Fatalf("writes = %d, want 1", writer.writes)
			}
		})
	}
}

func TestRunMapsWriterErrorToSanitizedInternalFailure(t *testing.T) {
	client := &fakeQueryClient{getResponse: &dtmv1.GetTaskResponse{Task: &dtmv1.TaskDetails{TaskId: "task-1"}}}
	stdout := &failingWriter{failAt: 1}
	var stderr bytes.Buffer
	code := runWith(context.Background(), []string{"get", "--task-id", "task-1", "--core-address", "127.0.0.1:1"}, stdout, &stderr, func(context.Context, string) (queryClient, connectionCloser, error) {
		return client, &fakeConnection{}, nil
	})
	if code != exitInternal || stdout.writes != 1 || stderr.String() != "dtm-query: could not format query response\n" || strings.Contains(stderr.String(), "secret") {
		t.Fatalf("code=%d writes=%d stderr=%q", code, stdout.writes, stderr.String())
	}
}

func TestErrorMappingUsesStableExitCodesAndSanitizedStderr(t *testing.T) {
	tests := []struct {
		code    codes.Code
		exit    int
		message string
	}{{codes.InvalidArgument, 2, "arguments"}, {codes.NotFound, 3, "not found"}, {codes.Unavailable, 4, "unavailable"}, {codes.DeadlineExceeded, 5, "timed out"}, {codes.Canceled, 6, "canceled"}, {codes.Internal, 1, "failed"}, {codes.Unknown, 1, "failed"}}
	for _, test := range tests {
		client := &fakeQueryClient{getResponse: &dtmv1.GetTaskResponse{}, err: status.Error(test.code, "secret SQL /absolute/path")}
		var stdout, stderr bytes.Buffer
		exit := runWith(context.Background(), []string{"get", "--task-id", "task", "--core-address", "127.0.0.1:1"}, &stdout, &stderr, func(context.Context, string) (queryClient, connectionCloser, error) {
			return client, &fakeConnection{}, nil
		})
		if exit != test.exit || stdout.Len() != 0 || !strings.Contains(stderr.String(), test.message) || strings.Contains(stderr.String(), "secret") {
			t.Fatalf("code=%v exit=%d stdout=%q stderr=%q", test.code, exit, stdout.String(), stderr.String())
		}
	}
}

func TestTimeoutAndCanceledContextAreDistinct(t *testing.T) {
	client := &fakeQueryClient{waitForContext: true}
	var stderr bytes.Buffer
	code := runWith(context.Background(), []string{"get", "--task-id", "task", "--core-address", "127.0.0.1:1", "--timeout", "1ms"}, new(bytes.Buffer), &stderr, func(context.Context, string) (queryClient, connectionCloser, error) {
		return client, &fakeConnection{}, nil
	})
	if code != exitTimeout {
		t.Fatalf("timeout code=%d stderr=%q", code, stderr.String())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stderr.Reset()
	code = runWith(ctx, []string{"get", "--task-id", "task", "--core-address", "127.0.0.1:1"}, new(bytes.Buffer), &stderr, func(context.Context, string) (queryClient, connectionCloser, error) {
		return client, &fakeConnection{}, nil
	})
	if code != exitCanceled {
		t.Fatalf("cancel code=%d stderr=%q", code, stderr.String())
	}
}

func TestLimitMatchesSharedServerContract(t *testing.T) {
	options, err := parseCommand([]string{"list", "--core-address", "localhost:1", "--limit", "200"}, new(bytes.Buffer))
	if err != nil || options.limit != storage.MaxTaskPageLimit {
		t.Fatalf("options=%#v err=%v", options, err)
	}
}
