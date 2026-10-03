package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
)

type recordingClient struct {
	request        *dtmv1.SubmitTaskRequest
	response       *dtmv1.SubmitTaskResponse
	err            error
	statusRequest  *dtmv1.GetTaskStatusRequest
	statusResponse *dtmv1.GetTaskStatusResponse
	statusErr      error
}

func (client *recordingClient) SubmitTask(ctx context.Context, request *dtmv1.SubmitTaskRequest, _ ...grpc.CallOption) (*dtmv1.SubmitTaskResponse, error) {
	client.request = request
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return client.response, client.err
}

func (client *recordingClient) GetTaskStatus(
	ctx context.Context,
	request *dtmv1.GetTaskStatusRequest,
	_ ...grpc.CallOption,
) (*dtmv1.GetTaskStatusResponse, error) {
	client.statusRequest = request
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return client.statusResponse, client.statusErr
}

type recordingConnection struct{ closed bool }

func (connection *recordingConnection) Close() error { connection.closed = true; return nil }

func TestRunSubmitsDefaultTask(t *testing.T) {
	client := &recordingClient{response: &dtmv1.SubmitTaskResponse{TaskId: "task-1", Status: dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED}}
	connection := &recordingConnection{}
	var stdout, stderr bytes.Buffer

	exitCode := runWith(context.Background(), []string{"-core", "core:50051"}, &stdout, &stderr, func(context.Context, string) (SubmitClient, connectionCloser, error) {
		return client, connection, nil
	}, func() string { return "task-generated" })

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %s", exitCode, stderr.String())
	}
	if !connection.closed {
		t.Fatal("connection was not closed")
	}
	if got := client.request.GetTask().GetId(); got != "task-generated" {
		t.Fatalf("task ID = %q", got)
	}
	if got := client.request.GetTask().GetIntent(); got != "cool_environment" {
		t.Fatalf("intent = %q", got)
	}
	if got := client.request.GetTask().GetConstraints(); len(got) != 1 || got["target_temperature"] != "26" {
		t.Fatalf("constraints = %#v", got)
	}
	if got := client.request.GetTask().GetRequirements(); len(got) != 0 {
		t.Fatalf("requirements = %#v", got)
	}
	if !strings.Contains(stdout.String(), "task_id=task-1 status=succeeded") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunNormalizesExplicitTaskAndTarget(t *testing.T) {
	client := &recordingClient{response: &dtmv1.SubmitTaskResponse{TaskId: "chosen", Status: dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED}}
	connection := &recordingConnection{}

	exitCode := runWith(context.Background(), []string{"-core", "core", "-task-id", " chosen ", "-target-temperature", " 26.500 "}, new(bytes.Buffer), new(bytes.Buffer), func(context.Context, string) (SubmitClient, connectionCloser, error) {
		return client, connection, nil
	}, func() string { return "unexpected" })

	if exitCode != 0 {
		t.Fatalf("exit code = %d", exitCode)
	}
	if got := client.request.GetTask().GetId(); got != "chosen" {
		t.Fatalf("task ID = %q", got)
	}
	if got := client.request.GetTask().GetConstraints()["target_temperature"]; got != "26.5" {
		t.Fatalf("target temperature = %q", got)
	}
}

func TestRunSubmitsAsyncTaskAndAcceptsCreatedStatus(t *testing.T) {
	client := &recordingClient{response: &dtmv1.SubmitTaskResponse{
		TaskId: "task-1",
		Status: dtmv1.TaskStatus_TASK_STATUS_CREATED,
	}}
	connection := &recordingConnection{}
	var stdout, stderr bytes.Buffer

	exitCode := runWith(
		context.Background(),
		[]string{"-core", "core", "-task-id", "task-1", "-async"},
		&stdout,
		&stderr,
		func(context.Context, string) (SubmitClient, connectionCloser, error) {
			return client, connection, nil
		},
		func() string { return "unexpected" },
	)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}
	if !client.request.GetAsync() {
		t.Fatal("SubmitTaskRequest.async = false, want true")
	}
	if !strings.Contains(stdout.String(), "task_id=task-1 status=created") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunPassesIdempotencyKeyAndPrintsDeduplication(t *testing.T) {
	client := &recordingClient{response: &dtmv1.SubmitTaskResponse{
		TaskId: "original-task", Status: dtmv1.TaskStatus_TASK_STATUS_RUNNING, Deduplicated: true,
	}}
	var stdout bytes.Buffer
	exitCode := runWith(context.Background(), []string{"-core", "core", "-async", "-idempotency-key", "retry:1"}, &stdout, new(bytes.Buffer), func(context.Context, string) (SubmitClient, connectionCloser, error) {
		return client, &recordingConnection{}, nil
	}, func() string { return "new-task" })
	if exitCode != 0 {
		t.Fatalf("exit code = %d", exitCode)
	}
	if client.request.GetIdempotencyKey() != "retry:1" {
		t.Fatalf("idempotency key = %q", client.request.GetIdempotencyKey())
	}
	if !strings.Contains(stdout.String(), "task_id=original-task status=running deduplicated=true") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunQueriesPersistedTaskStatus(t *testing.T) {
	client := &recordingClient{statusResponse: &dtmv1.GetTaskStatusResponse{
		TaskId: "task-1",
		Status: dtmv1.TaskStatus_TASK_STATUS_RUNNING,
	}}
	connection := &recordingConnection{}
	var stdout, stderr bytes.Buffer

	exitCode := runWith(
		context.Background(),
		[]string{"-core", "core", "-status", " task-1 "},
		&stdout,
		&stderr,
		func(context.Context, string) (SubmitClient, connectionCloser, error) {
			return client, connection, nil
		},
		func() string { return "unexpected" },
	)

	if exitCode != 0 {
		t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
	}
	if client.request != nil || client.statusRequest.GetTaskId() != "task-1" {
		t.Fatalf("requests = submit %#v, status %#v", client.request, client.statusRequest)
	}
	if !strings.Contains(stdout.String(), "task_id=task-1 status=running") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}

func TestRunRejectsInvalidArguments(t *testing.T) {
	for _, args := range [][]string{{}, {"-core", "core", "-target-temperature", "NaN"}, {"-core", "core", "-target-temperature", "Inf"}} {
		var stderr bytes.Buffer
		if got := runWith(context.Background(), args, new(bytes.Buffer), &stderr, nil, nil); got != 2 {
			t.Fatalf("runWith(%v) exit code = %d, stderr = %q", args, got, stderr.String())
		}
	}
}

func TestRunReportsFailedResponseAndClosesConnection(t *testing.T) {
	output, err := structpb.NewStruct(map[string]any{"cooling_started": true})
	if err != nil {
		t.Fatal(err)
	}
	client := &recordingClient{response: &dtmv1.SubmitTaskResponse{TaskId: "task-1", Status: dtmv1.TaskStatus_TASK_STATUS_FAILED, Error: "no cooling node", Results: []*dtmv1.StepResult{{StepId: "cool", NodeId: "node-2", Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_FAILED, Output: output, Error: "handler failed"}}}}
	connection := &recordingConnection{}
	var stdout bytes.Buffer

	if got := runWith(context.Background(), []string{"-core", "core"}, &stdout, new(bytes.Buffer), func(context.Context, string) (SubmitClient, connectionCloser, error) { return client, connection, nil }, func() string { return "task-1" }); got != 1 {
		t.Fatalf("exit code = %d", got)
	}
	if !connection.closed {
		t.Fatal("connection was not closed")
	}
	for _, want := range []string{"status=failed", "error=no cooling node", "step_id=cool", "node_id=node-2", "output={\"cooling_started\":true}", "error=handler failed"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout %q does not contain %q", stdout.String(), want)
		}
	}
}

func TestRunClassifiesFactoryAndRPCFailures(t *testing.T) {
	var stderr bytes.Buffer
	if got := runWith(context.Background(), []string{"-core", "core"}, new(bytes.Buffer), &stderr, func(context.Context, string) (SubmitClient, connectionCloser, error) {
		return nil, nil, errors.New("dial failed")
	}, func() string { return "task" }); got != 4 {
		t.Fatalf("factory error exit code = %d", got)
	}
	connection := &recordingConnection{}
	if got := runWith(context.Background(), []string{"-core", "core"}, new(bytes.Buffer), new(bytes.Buffer), func(context.Context, string) (SubmitClient, connectionCloser, error) {
		return &recordingClient{err: errors.New("rpc failed")}, connection, nil
	}, func() string { return "task" }); got != 5 {
		t.Fatalf("RPC error exit code = %d", got)
	}
	if !connection.closed {
		t.Fatal("connection was not closed after RPC failure")
	}
}

func TestRunPassesContextToFactoryAndClient(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	connection := &recordingConnection{}
	var factoryContext context.Context
	var stderr bytes.Buffer

	exitCode := runWith(ctx, []string{"-core", "core"}, new(bytes.Buffer), &stderr, func(got context.Context, _ string) (SubmitClient, connectionCloser, error) {
		factoryContext = got
		return &recordingClient{}, connection, nil
	}, func() string { return "task" })

	if exitCode != 4 {
		t.Fatalf("exit code = %d", exitCode)
	}
	if factoryContext != ctx {
		t.Fatal("factory did not receive the caller context")
	}
	if !connection.closed {
		t.Fatal("connection was not closed")
	}
	if !strings.Contains(stderr.String(), context.Canceled.Error()) {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestSubmitErrorExitCodeContract(t *testing.T) {
	for _, test := range []struct {
		code codes.Code
		want int
	}{
		{codes.InvalidArgument, 2},
		{codes.AlreadyExists, 3},
		{codes.Unavailable, 4},
		{codes.DeadlineExceeded, 4},
		{codes.Canceled, 4},
		{codes.Internal, 5},
		{codes.Unknown, 5},
	} {
		if got := submitErrorExitCode(status.Error(test.code, "failure")); got != test.want {
			t.Fatalf("code %v exit = %d, want %d", test.code, got, test.want)
		}
	}
}

func TestRunReturnsInternalExitCodeWhenOutputWriterFails(t *testing.T) {
	for _, test := range []struct {
		name   string
		args   []string
		client *recordingClient
	}{
		{
			name: "first creation", args: []string{"-core", "core", "-async"},
			client: &recordingClient{response: &dtmv1.SubmitTaskResponse{TaskId: "task-1", Status: dtmv1.TaskStatus_TASK_STATUS_CREATED}},
		},
		{
			name: "deduplicated", args: []string{"-core", "core", "-async", "-idempotency-key", "key-1"},
			client: &recordingClient{response: &dtmv1.SubmitTaskResponse{TaskId: "task-1", Status: dtmv1.TaskStatus_TASK_STATUS_RUNNING, Deduplicated: true}},
		},
		{
			name: "status query", args: []string{"-core", "core", "-status", "task-1"},
			client: &recordingClient{statusResponse: &dtmv1.GetTaskStatusResponse{TaskId: "task-1", Status: dtmv1.TaskStatus_TASK_STATUS_RUNNING}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var stderr bytes.Buffer
			exitCode := runWith(context.Background(), test.args, failingWriter{}, &stderr, func(context.Context, string) (SubmitClient, connectionCloser, error) {
				return test.client, &recordingConnection{}, nil
			}, func() string { return "task-generated" })
			if exitCode != 5 {
				t.Fatalf("exit code = %d, stderr = %q", exitCode, stderr.String())
			}
			if stderr.String() != "failed to write command output\n" || strings.Contains(stderr.String(), "writer-secret-detail") {
				t.Fatalf("stderr = %q", stderr.String())
			}
		})
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("writer-secret-detail")
}

func TestDefaultTaskIDIsUniqueForConcurrentCallsAtSameTime(t *testing.T) {
	var sequence atomic.Uint64
	newID := defaultTaskID(func() time.Time { return time.Unix(0, 123) }, &sequence)
	const count = 1000
	ids := make(chan string, count)
	var workers sync.WaitGroup
	for range count {
		workers.Add(1)
		go func() {
			defer workers.Done()
			ids <- newID()
		}()
	}
	workers.Wait()
	close(ids)

	seen := make(map[string]struct{}, count)
	for id := range ids {
		if !strings.HasPrefix(id, "task-123-") {
			t.Fatalf("task ID = %q", id)
		}
		if _, duplicate := seen[id]; duplicate {
			t.Fatalf("duplicate task ID %q", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("unique task IDs = %d, want %d", len(seen), count)
	}
}

type bufconnCoreServer struct {
	dtmv1.UnimplementedCoreServiceServer
}

func (bufconnCoreServer) SubmitTask(_ context.Context, request *dtmv1.SubmitTaskRequest) (*dtmv1.SubmitTaskResponse, error) {
	return &dtmv1.SubmitTaskResponse{TaskId: request.GetTask().GetId(), Status: dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED}, nil
}

func (bufconnCoreServer) GetTaskStatus(
	_ context.Context,
	request *dtmv1.GetTaskStatusRequest,
) (*dtmv1.GetTaskStatusResponse, error) {
	return &dtmv1.GetTaskStatusResponse{
		TaskId: request.GetTaskId(),
		Status: dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED,
	}, nil
}

func TestRunWorksWithGeneratedClientOverBufconn(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	dtmv1.RegisterCoreServiceServer(server, bufconnCoreServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })

	var stdout, stderr bytes.Buffer
	exitCode := runWith(context.Background(), []string{"-core", "bufnet"}, &stdout, &stderr, func(ctx context.Context, target string) (SubmitClient, connectionCloser, error) {
		connection, err := grpc.NewClient("passthrough:///"+target, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		if err != nil {
			return nil, nil, err
		}
		return dtmv1.NewCoreServiceClient(connection), connection, nil
	}, func() string { return "task-bufconn" })
	if exitCode != 0 {
		t.Fatalf("exit code = %d, stdout = %q, stderr = %q", exitCode, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "task_id=task-bufconn status=succeeded") {
		t.Fatalf("stdout = %q", stdout.String())
	}
}
