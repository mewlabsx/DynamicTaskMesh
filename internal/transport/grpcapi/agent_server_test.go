package grpcapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/agent"
	"dtm/internal/agentexecution"
	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
	sqliteplatform "dtm/internal/platform/sqlite"
	storageport "dtm/internal/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type stepHandlerFunc func(context.Context, mapper.MappedStep) (execution.StepResult, error)

func (function stepHandlerFunc) Execute(ctx context.Context, step mapper.MappedStep) (execution.StepResult, error) {
	return function(ctx, step)
}

type m7FailingExecutionRepository struct{ err error }

func (repository m7FailingExecutionRepository) Find(context.Context, string) (agentexecution.Record, error) {
	return agentexecution.Record{}, repository.err
}
func (repository m7FailingExecutionRepository) Start(context.Context, string, agentexecution.Fingerprint) error {
	return repository.err
}
func (repository m7FailingExecutionRepository) Complete(context.Context, string, agentexecution.Fingerprint, execution.StepResult, string) error {
	return repository.err
}
func (m7FailingExecutionRepository) Close() error { return nil }

func TestM7AgentStorageErrorsAreStableAndRedacted(t *testing.T) {
	for _, test := range []struct {
		name    string
		failure error
		code    codes.Code
		message string
	}{
		{"busy", fmt.Errorf("%w: SQLite SELECT C:\\agent-secret.db key-secret", storageport.ErrStorageUnavailable), codes.Unavailable, "storage is temporarily unavailable"},
		{"full", fmt.Errorf("%w: SQLite INSERT C:\\agent-secret.db key-secret", storageport.ErrStorageFull), codes.Internal, "persistent storage operation failed"},
		{"integrity", fmt.Errorf("%w: invalid fingerprint {invalid C:\\agent-secret.db key-secret", storageport.ErrStorageIntegrity), codes.Internal, "persistent storage operation failed"},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, err := NewAgentExecutionServer(stepHandlerFunc(func(context.Context, mapper.MappedStep) (execution.StepResult, error) {
				t.Fatal("handler called despite storage failure")
				return execution.StepResult{}, nil
			}), WithExecutionRepository(m7FailingExecutionRepository{err: test.failure}))
			if err != nil {
				t.Fatal(err)
			}
			_, err = server.ExecuteStep(context.Background(), validExecuteStepRequest())
			if status.Code(err) != test.code || status.Convert(err).Message() != test.message {
				t.Fatalf("ExecuteStep() error = %v", err)
			}
			for _, forbidden := range []string{"SQLite", "SELECT", "INSERT", "agent-secret.db", "key-secret"} {
				if strings.Contains(err.Error(), forbidden) {
					t.Fatalf("client error leaked %q: %v", forbidden, err)
				}
			}
		})
	}
}

func TestM7RAgentExecutionIntegrityLogUsesOnlyHashPrefix(t *testing.T) {
	const fullKey = "m7r-full-secret-agent-key"
	var logs bytes.Buffer
	server, err := NewAgentExecutionServer(stepHandlerFunc(func(context.Context, mapper.MappedStep) (execution.StepResult, error) {
		t.Fatal("handler called despite integrity failure")
		return execution.StepResult{}, nil
	}), WithExecutionRepository(m7FailingExecutionRepository{err: fmt.Errorf("%w: {invalid fingerprint %s", storageport.ErrStorageIntegrity, fullKey)}), WithAgentExecutionLogger(log.New(&logs, "", 0)))
	if err != nil {
		t.Fatal(err)
	}
	request := validExecuteStepRequest()
	request.IdempotencyKey = fullKey
	_, err = server.ExecuteStep(context.Background(), request)
	if status.Code(err) != codes.Internal || status.Convert(err).Message() != "persistent storage operation failed" {
		t.Fatalf("ExecuteStep() error = %v", err)
	}
	text := logs.String()
	for _, required := range []string{"operation=execute_step", "record_type=agent_execution", "error_class=storage_integrity", "key_hash_prefix=" + agentExecutionKeyHashPrefix(fullKey)} {
		if !strings.Contains(text, required) {
			t.Fatalf("log missing %q: %s", required, text)
		}
	}
	for _, forbidden := range []string{fullKey, "{invalid", "fingerprint", "SQLite", "SQL", "agent-secret.db"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("log leaked %q: %s", forbidden, text)
		}
	}
}

func TestNewAgentExecutionServerRejectsNilAndTypedNilHandler(t *testing.T) {
	var typedNil stepHandlerFunc

	for _, handler := range []StepHandler{nil, typedNil} {
		if _, err := NewAgentExecutionServer(handler); !errors.Is(err, ErrInvalidStepHandler) {
			t.Fatalf("NewAgentExecutionServer() error = %v, want %v", err, ErrInvalidStepHandler)
		}
	}
}

func TestAgentExecutionSucceedsThroughGRPC(t *testing.T) {
	var received mapper.MappedStep
	handler := stepHandlerFunc(func(_ context.Context, step mapper.MappedStep) (execution.StepResult, error) {
		received = step
		return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, map[string]any{
			"temperature": float64(30),
		}, "")
	})
	client := newAgentExecutionClient(t, handler)

	response, err := client.ExecuteStep(context.Background(), validExecuteStepRequest())

	if err != nil {
		t.Fatalf("ExecuteStep() error = %v", err)
	}
	if received.ID != "step-1" || received.NodeID != "node-1" || received.Capability != "temperature_sensor" {
		t.Fatalf("handler received step = %#v", received)
	}
	if got := received.Inputs["operation"]; got != "read_temperature" {
		t.Fatalf("handler input operation = %q", got)
	}
	result := response.GetResult()
	if result.GetStepId() != "step-1" || result.GetNodeId() != "node-1" ||
		result.GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED {
		t.Fatalf("ExecuteStep() result = %#v", result)
	}
	if got := result.GetOutput().GetFields()["temperature"].GetNumberValue(); got != 30 {
		t.Fatalf("temperature = %v, want 30", got)
	}
}

func TestAgentExecutionReplaysCompletedIdempotentRequest(t *testing.T) {
	calls := 0
	handler := stepHandlerFunc(func(_ context.Context, step mapper.MappedStep) (execution.StepResult, error) {
		calls++
		return execution.NewStepResult(
			step.ID,
			step.NodeID,
			execution.StatusSucceeded,
			map[string]any{"call": float64(calls)},
			"",
		)
	})
	client := newAgentExecutionClient(t, handler)
	request := validExecuteStepRequest()
	request.IdempotencyKey = "task-1:step-1"
	request.Attempt = 1

	first, err := client.ExecuteStep(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	request.Attempt = 2
	second, err := client.ExecuteStep(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || first.GetReplayed() || !second.GetReplayed() {
		t.Fatalf("calls = %d, first.replayed = %v, second.replayed = %v", calls, first.GetReplayed(), second.GetReplayed())
	}
	if second.GetResult().GetOutput().AsMap()["call"] != float64(1) {
		t.Fatalf("replayed output = %#v", second.GetResult().GetOutput())
	}
}

func TestAgentExecutionReplaysCompletedRequestAfterAgentRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.db")
	calls := 0
	handler := stepHandlerFunc(func(_ context.Context, step mapper.MappedStep) (execution.StepResult, error) {
		calls++
		return execution.NewStepResult(
			step.ID,
			step.NodeID,
			execution.StatusSucceeded,
			map[string]any{"call": float64(calls)},
			"",
		)
	})
	request := validExecuteStepRequest()
	request.IdempotencyKey = "persistent-key"

	firstRepository, err := sqliteplatform.OpenExecutionRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	firstServer, err := NewAgentExecutionServer(
		handler,
		WithExecutionRepository(firstRepository),
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := firstServer.ExecuteStep(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if err := firstRepository.Close(); err != nil {
		t.Fatal(err)
	}

	secondRepository, err := sqliteplatform.OpenExecutionRepository(path)
	if err != nil {
		t.Fatal(err)
	}
	defer secondRepository.Close()
	secondServer, err := NewAgentExecutionServer(
		handler,
		WithExecutionRepository(secondRepository),
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := secondServer.ExecuteStep(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || first.GetReplayed() || !second.GetReplayed() {
		t.Fatalf(
			"calls=%d first.replayed=%v second.replayed=%v",
			calls,
			first.GetReplayed(),
			second.GetReplayed(),
		)
	}
	if second.GetResult().GetOutput().AsMap()["call"] != float64(1) {
		t.Fatalf("replayed output = %#v", second.GetResult().GetOutput())
	}
}

func TestAgentExecutionPreservesPersistentConflictAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.db")
	handler := stepHandlerFunc(func(_ context.Context, step mapper.MappedStep) (execution.StepResult, error) {
		return execution.NewStepResult(
			step.ID,
			step.NodeID,
			execution.StatusSucceeded,
			nil,
			"",
		)
	})
	request := validExecuteStepRequest()
	request.IdempotencyKey = "persistent-key"
	firstRepository, _ := sqliteplatform.OpenExecutionRepository(path)
	firstServer, _ := NewAgentExecutionServer(
		handler,
		WithExecutionRepository(firstRepository),
	)
	if _, err := firstServer.ExecuteStep(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := firstRepository.Close(); err != nil {
		t.Fatal(err)
	}

	secondRepository, _ := sqliteplatform.OpenExecutionRepository(path)
	defer secondRepository.Close()
	secondServer, _ := NewAgentExecutionServer(
		handler,
		WithExecutionRepository(secondRepository),
	)
	conflict := validExecuteStepRequest()
	conflict.IdempotencyKey = "persistent-key"
	conflict.Step.Inputs["operation"] = "different"
	if _, err := secondServer.ExecuteStep(context.Background(), conflict); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("persistent conflict code = %v, error = %v", status.Code(err), err)
	}
}

func TestAgentExecutionRetriesInterruptedCancellationAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.db")
	calls := 0
	firstRepository, _ := sqliteplatform.OpenExecutionRepository(path)
	defer firstRepository.Close()
	firstServer, _ := NewAgentExecutionServer(
		stepHandlerFunc(func(ctx context.Context, step mapper.MappedStep) (execution.StepResult, error) {
			calls++
			result, err := execution.NewStepResult(
				step.ID,
				step.NodeID,
				execution.StatusFailed,
				nil,
				ctx.Err().Error(),
			)
			if err != nil {
				return execution.StepResult{}, err
			}
			return result, ctx.Err()
		}),
		WithExecutionRepository(firstRepository),
	)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := firstServer.ExecuteStep(ctx, validExecuteStepRequest()); status.Code(err) != codes.Canceled {
		t.Fatalf("canceled execution code = %v, error = %v", status.Code(err), err)
	}
	if err := firstRepository.Close(); err != nil {
		t.Fatal(err)
	}

	secondRepository, _ := sqliteplatform.OpenExecutionRepository(path)
	defer secondRepository.Close()
	secondServer, _ := NewAgentExecutionServer(
		stepHandlerFunc(func(_ context.Context, step mapper.MappedStep) (execution.StepResult, error) {
			calls++
			return execution.NewStepResult(
				step.ID,
				step.NodeID,
				execution.StatusSucceeded,
				nil,
				"",
			)
		}),
		WithExecutionRepository(secondRepository),
	)
	response, err := secondServer.ExecuteStep(context.Background(), validExecuteStepRequest())
	if err != nil {
		t.Fatal(err)
	}
	if calls != 2 || response.GetReplayed() {
		t.Fatalf("calls=%d replayed=%v, want interrupted execution rerun", calls, response.GetReplayed())
	}
}

func TestAgentExecutionCoalescesConcurrentIdempotentRequests(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	handler := stepHandlerFunc(func(_ context.Context, step mapper.MappedStep) (execution.StepResult, error) {
		calls++
		close(entered)
		<-release
		return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, nil, "")
	})
	client := newAgentExecutionClient(t, handler)
	execute := func(attempt uint32) <-chan *dtmv1.ExecuteStepResponse {
		result := make(chan *dtmv1.ExecuteStepResponse, 1)
		go func() {
			request := validExecuteStepRequest()
			request.IdempotencyKey = "coalesced-key"
			request.Attempt = attempt
			response, err := client.ExecuteStep(context.Background(), request)
			if err != nil {
				t.Errorf("ExecuteStep() error = %v", err)
			}
			result <- response
		}()
		return result
	}
	first := execute(1)
	<-entered
	second := execute(2)
	close(release)
	firstResponse := <-first
	secondResponse := <-second
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}
	if firstResponse.GetReplayed() == secondResponse.GetReplayed() {
		t.Fatalf("replayed flags = %v, %v, want one original and one replay", firstResponse.GetReplayed(), secondResponse.GetReplayed())
	}
}

func TestAgentExecutionRejectsIdempotencyKeyPayloadConflict(t *testing.T) {
	calls := 0
	client := newAgentExecutionClient(t, stepHandlerFunc(func(_ context.Context, step mapper.MappedStep) (execution.StepResult, error) {
		calls++
		return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, nil, "")
	}))
	first := validExecuteStepRequest()
	first.IdempotencyKey = "shared-key"
	if _, err := client.ExecuteStep(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	conflict := validExecuteStepRequest()
	conflict.IdempotencyKey = "shared-key"
	conflict.Step.Inputs["operation"] = "different"
	if _, err := client.ExecuteStep(context.Background(), conflict); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("conflicting ExecuteStep() code = %v, error = %v", status.Code(err), err)
	}
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}
}

func TestAgentExecutionRejectsInvalidRequestsThroughGRPC(t *testing.T) {
	calls := 0
	client := newAgentExecutionClient(t, stepHandlerFunc(func(context.Context, mapper.MappedStep) (execution.StepResult, error) {
		calls++
		return execution.StepResult{}, nil
	}))

	tests := []struct {
		name    string
		request *dtmv1.ExecuteStepRequest
	}{
		{name: "nil request"},
		{name: "nil step", request: &dtmv1.ExecuteStepRequest{TaskId: "task-1"}},
		{name: "blank task ID", request: executeStepRequest(" ", "step-1", "temperature_sensor", "node-1", nil)},
		{name: "blank step ID", request: executeStepRequest("task-1", " ", "temperature_sensor", "node-1", nil)},
		{name: "blank capability", request: executeStepRequest("task-1", "step-1", " ", "node-1", nil)},
		{name: "blank node ID", request: executeStepRequest("task-1", "step-1", "temperature_sensor", " ", nil)},
		{name: "blank input key", request: executeStepRequest("task-1", "step-1", "temperature_sensor", "node-1", map[string]string{" ": "value"})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			response, err := client.ExecuteStep(context.Background(), tt.request)
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("ExecuteStep() code = %v, want InvalidArgument (response %#v, error %v)", status.Code(err), response, err)
			}
		})
	}
	if calls != 0 {
		t.Fatalf("handler calls = %d, want 0", calls)
	}
}

func TestAgentExecutionUnknownCapabilityIsFailedPrecondition(t *testing.T) {
	router, err := agent.NewRouter(&agentTestHandler{capability: "temperature_sensor"})
	if err != nil {
		t.Fatal(err)
	}
	client := newAgentExecutionClient(t, router)
	request := executeStepRequest("task-1", "step-1", "cooling_control", "node-1", nil)

	response, err := client.ExecuteStep(context.Background(), request)

	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("ExecuteStep() code = %v, want FailedPrecondition (error %v)", status.Code(err), err)
	}
	if response != nil {
		t.Fatalf("ExecuteStep() response = %#v, want nil", response)
	}
}

func TestAgentExecutionHandlerFailureIsNormalResponse(t *testing.T) {
	handlerErr := errors.New("fan unavailable")
	router, err := agent.NewRouter(&agentTestHandler{
		capability: "cooling_control",
		output:     map[string]any{"attempted": true},
		err:        handlerErr,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := newAgentExecutionClient(t, router)
	request := executeStepRequest("task-1", "step-2", "cooling_control", "node-2", map[string]string{"target_temperature": "26"})

	response, err := client.ExecuteStep(context.Background(), request)

	if err != nil {
		t.Fatalf("ExecuteStep() error = %v, want OK response", err)
	}
	result := response.GetResult()
	if result.GetStatus() != dtmv1.ExecutionStatus_EXECUTION_STATUS_FAILED ||
		result.GetStepId() != "step-2" ||
		result.GetNodeId() != "node-2" ||
		result.GetError() != handlerErr.Error() {
		t.Fatalf("ExecuteStep() result = %#v", result)
	}
	if !result.GetOutput().GetFields()["attempted"].GetBoolValue() {
		t.Fatalf("ExecuteStep() output = %#v", result.GetOutput())
	}
}

func TestAgentExecutionRejectsMalformedHandlerResults(t *testing.T) {
	tests := []struct {
		name   string
		result execution.StepResult
	}{
		{
			name: "successful result with error",
			result: execution.StepResult{
				StepID: "step-1",
				NodeID: "node-1",
				Status: execution.StatusSucceeded,
				Error:  "unexpected error",
			},
		},
		{
			name: "failed result without error",
			result: execution.StepResult{
				StepID: "step-1",
				NodeID: "node-1",
				Status: execution.StatusFailed,
			},
		},
		{
			name: "identity mismatch",
			result: execution.StepResult{
				StepID: "different-step",
				NodeID: "different-node",
				Status: execution.StatusSucceeded,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := newAgentExecutionClient(t, stepHandlerFunc(func(context.Context, mapper.MappedStep) (execution.StepResult, error) {
				return tt.result, nil
			}))

			response, err := client.ExecuteStep(context.Background(), validExecuteStepRequest())

			if status.Code(err) != codes.Internal {
				t.Fatalf("ExecuteStep() code = %v, want Internal (response %#v, error %v)", status.Code(err), response, err)
			}
			if response != nil {
				t.Fatalf("ExecuteStep() response = %#v, want nil", response)
			}
		})
	}
}

func TestAgentExecutionPreservesCanceledContext(t *testing.T) {
	entered := make(chan struct{})
	client := newAgentExecutionClient(t, stepHandlerFunc(func(ctx context.Context, step mapper.MappedStep) (execution.StepResult, error) {
		close(entered)
		<-ctx.Done()
		return execution.NewStepResult(step.ID, step.NodeID, execution.StatusFailed, nil, ctx.Err().Error())
	}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := client.ExecuteStep(ctx, validExecuteStepRequest())
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler was not entered before timeout")
	}
	cancel()

	select {
	case err := <-result:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("ExecuteStep() code = %v, want Canceled (error %v)", status.Code(err), err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ExecuteStep() did not return after cancellation")
	}
}

func TestAgentExecutionPreservesDeadline(t *testing.T) {
	client := newAgentExecutionClient(t, stepHandlerFunc(func(ctx context.Context, step mapper.MappedStep) (execution.StepResult, error) {
		<-ctx.Done()
		return execution.NewStepResult(step.ID, step.NodeID, execution.StatusFailed, nil, ctx.Err().Error())
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	_, err := client.ExecuteStep(ctx, validExecuteStepRequest())

	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("ExecuteStep() code = %v, want DeadlineExceeded (error %v)", status.Code(err), err)
	}
}

type agentTestHandler struct {
	capability model.Capability
	output     map[string]any
	err        error
}

func (handler *agentTestHandler) Capability() model.Capability {
	return handler.capability
}

func (handler *agentTestHandler) Execute(context.Context, map[string]string) (map[string]any, error) {
	return handler.output, handler.err
}

func newAgentExecutionClient(t *testing.T, handler StepHandler) dtmv1.AgentExecutionServiceClient {
	t.Helper()
	server, err := NewAgentExecutionServer(handler)
	if err != nil {
		t.Fatal(err)
	}

	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	dtmv1.RegisterAgentExecutionServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	t.Cleanup(grpcServer.Stop)
	t.Cleanup(func() { _ = listener.Close() })

	connection, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return dtmv1.NewAgentExecutionServiceClient(connection)
}

func validExecuteStepRequest() *dtmv1.ExecuteStepRequest {
	return executeStepRequest(
		" task-1 ",
		" step-1 ",
		" temperature_sensor ",
		" node-1 ",
		map[string]string{"operation": "read_temperature"},
	)
}

func executeStepRequest(taskID, stepID, capability, nodeID string, inputs map[string]string) *dtmv1.ExecuteStepRequest {
	return &dtmv1.ExecuteStepRequest{
		TaskId: taskID,
		Step: &dtmv1.MappedStep{
			StepId:     stepID,
			Capability: capability,
			NodeId:     nodeID,
			Inputs:     inputs,
		},
	}
}
