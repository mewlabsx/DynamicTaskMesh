package grpcapi

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/runtime"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestNewRemoteExecutorRejectsNilDependencies(t *testing.T) {
	var typedNilResolver *remoteResolver
	var typedNilDialer Dialer

	tests := []struct {
		name     string
		resolver Resolver
		dialer   Dialer
		want     error
	}{
		{name: "nil resolver", dialer: func(context.Context, string) (*grpc.ClientConn, error) { return nil, nil }, want: ErrInvalidResolver},
		{name: "typed nil resolver", resolver: typedNilResolver, dialer: func(context.Context, string) (*grpc.ClientConn, error) { return nil, nil }, want: ErrInvalidResolver},
		{name: "nil dialer", resolver: remoteResolverFunc(func(model.NodeID) (string, error) { return "", nil }), want: ErrInvalidDialer},
		{name: "typed nil dialer", resolver: remoteResolverFunc(func(model.NodeID) (string, error) { return "", nil }), dialer: typedNilDialer, want: ErrInvalidDialer},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewRemoteExecutor(tt.resolver, tt.dialer)
			if !errors.Is(err, tt.want) {
				t.Fatalf("NewRemoteExecutor() error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestExecuteTreatsMissingEndpointAsRetryable(t *testing.T) {
	resolverErr := errors.New("endpoint not found")
	executor, err := NewRemoteExecutor(
		remoteResolverFunc(func(model.NodeID) (string, error) {
			return "", resolverErr
		}),
		func(context.Context, string) (*grpc.ClientConn, error) {
			t.Fatal("dialer must not run without an endpoint")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = executor.Execute(
		context.Background(),
		"task-1",
		mapper.MappedStep{
			ID: "step-1", Capability: "temperature_sensor", NodeID: "node-missing",
		},
	)
	if !errors.Is(err, runtime.ErrRetryableExecution) ||
		!errors.Is(err, ErrRemoteExecution) ||
		!errors.Is(err, resolverErr) {
		t.Fatalf("Execute() error = %v", err)
	}
}

func TestRemoteExecutorExecutesMappedStepThroughGRPC(t *testing.T) {
	var gotRequest *dtmv1.ExecuteStepRequest
	var gotMetadata metadata.MD
	service := agentExecutionServiceFunc(func(ctx context.Context, request *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error) {
		gotRequest = request
		gotMetadata, _ = metadata.FromIncomingContext(ctx)
		output, err := structpb.NewStruct(map[string]any{"temperature": float64(30)})
		if err != nil {
			t.Fatal(err)
		}
		return &dtmv1.ExecuteStepResponse{Result: &dtmv1.StepResult{
			StepId: request.GetStep().GetStepId(),
			NodeId: request.GetStep().GetNodeId(),
			Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED,
			Output: output,
		}}, nil
	})
	listener, stop := startRemoteAgentServer(t, service)
	defer stop()

	var dialContext context.Context
	var dialAddress string
	var connection *grpc.ClientConn
	dialer := func(ctx context.Context, address string) (*grpc.ClientConn, error) {
		dialContext = ctx
		dialAddress = address
		var err error
		connection, err = grpc.DialContext(
			ctx,
			"passthrough:///bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return listener.Dial()
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		return connection, err
	}
	resolver := remoteResolverFunc(func(id model.NodeID) (string, error) {
		if id != "sensor-node" {
			t.Fatalf("Resolve() node = %q, want sensor-node", id)
		}
		return "agent.internal:7001", nil
	})
	executor, err := NewRemoteExecutor(resolver, dialer)
	if err != nil {
		t.Fatal(err)
	}
	parent := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("trace-id", "trace-123"))
	step := mapper.MappedStep{
		ID:         "read-temperature",
		Capability: "temperature_sensor",
		NodeID:     "sensor-node",
		Inputs:     map[string]string{"operation": "read_temperature"},
	}

	result, err := executor.Execute(parent, "task-1", step)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if dialContext != parent {
		t.Fatal("Dialer did not receive the caller context")
	}
	if dialAddress != "agent.internal:7001" {
		t.Fatalf("Dialer address = %q", dialAddress)
	}
	if gotRequest.GetTaskId() != "task-1" {
		t.Fatalf("request task_id = %q, want task-1", gotRequest.GetTaskId())
	}
	if gotRequest.GetIdempotencyKey() != runtime.IdempotencyKey("task-1", step.ID) ||
		gotRequest.GetAttempt() != 1 {
		t.Fatalf("execution contract = key %q attempt %d", gotRequest.GetIdempotencyKey(), gotRequest.GetAttempt())
	}
	if got := gotRequest.GetStep(); got.GetStepId() != string(step.ID) ||
		got.GetCapability() != string(step.Capability) ||
		got.GetNodeId() != string(step.NodeID) ||
		got.GetInputs()["operation"] != "read_temperature" {
		t.Fatalf("ExecuteStep request = %#v", got)
	}
	if values := gotMetadata.Get("trace-id"); len(values) != 1 || values[0] != "trace-123" {
		t.Fatalf("incoming metadata trace-id = %v", values)
	}
	if result.StepID != step.ID || result.NodeID != step.NodeID ||
		result.Status != execution.StatusSucceeded || result.Output["temperature"] != float64(30) {
		t.Fatalf("Execute() result = %#v", result)
	}
	if connection.GetState() != connectivity.Shutdown {
		t.Fatalf("connection state after Execute = %v, want Shutdown", connection.GetState())
	}
}

func TestRemoteExecutorReturnsFailedAgentResponseWithoutGoError(t *testing.T) {
	service := agentExecutionServiceFunc(func(_ context.Context, request *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error) {
		return &dtmv1.ExecuteStepResponse{Result: &dtmv1.StepResult{
			StepId: request.GetStep().GetStepId(),
			NodeId: request.GetStep().GetNodeId(),
			Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_FAILED,
			Error:  "handler refused target",
		}}, nil
	})
	executor := newBufconnRemoteExecutor(t, service)

	result, err := executor.Execute(context.Background(), "task-1", validRemoteStep())
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}
	if result.Status != execution.StatusFailed || result.Error != "handler refused target" {
		t.Fatalf("Execute() result = %#v", result)
	}
}

func TestRemoteExecutorPreservesUnknownEndpoint(t *testing.T) {
	executor, err := NewRemoteExecutor(
		remoteResolverFunc(func(model.NodeID) (string, error) {
			return "", errors.Join(ErrEndpointNotFound, errors.New("cooling-node"))
		}),
		func(context.Context, string) (*grpc.ClientConn, error) {
			t.Fatal("Dialer must not be called")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	_, err = executor.Execute(context.Background(), "task-1", validRemoteStep())
	if !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("Execute() error = %v, want ErrEndpointNotFound", err)
	}
}

func TestRemoteExecutorWrapsDialAndUnavailableErrors(t *testing.T) {
	t.Run("dial", func(t *testing.T) {
		dialFailure := errors.New("dial refused")
		executor, err := NewRemoteExecutor(
			remoteResolverFunc(func(model.NodeID) (string, error) { return "agent:7001", nil }),
			func(context.Context, string) (*grpc.ClientConn, error) { return nil, dialFailure },
		)
		if err != nil {
			t.Fatal(err)
		}

		_, err = executor.Execute(context.Background(), "task-1", validRemoteStep())
		if !errors.Is(err, ErrRemoteExecution) ||
			!errors.Is(err, runtime.ErrRetryableExecution) ||
			!errors.Is(err, dialFailure) {
			t.Fatalf("Execute() error = %v, want wrapped dial error", err)
		}
	})

	t.Run("dial returns connection and error", func(t *testing.T) {
		listener, stop := startRemoteAgentServer(t, agentExecutionServiceFunc(func(context.Context, *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error) {
			return nil, errors.New("must not execute")
		}))
		defer stop()
		dialFailure := errors.New("dial completed with warning")
		var connection *grpc.ClientConn
		executor, err := NewRemoteExecutor(
			remoteResolverFunc(func(model.NodeID) (string, error) { return "agent:7001", nil }),
			func(ctx context.Context, _ string) (*grpc.ClientConn, error) {
				var dialErr error
				connection, dialErr = grpc.DialContext(
					ctx,
					"passthrough:///bufnet",
					grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
						return listener.Dial()
					}),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
				if dialErr != nil {
					return connection, dialErr
				}
				return connection, dialFailure
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if connection != nil {
				_ = connection.Close()
			}
		})

		_, err = executor.Execute(context.Background(), "task-1", validRemoteStep())
		if !errors.Is(err, ErrRemoteExecution) ||
			!errors.Is(err, runtime.ErrRetryableExecution) ||
			!errors.Is(err, dialFailure) {
			t.Fatalf("Execute() error = %v, want wrapped dial error", err)
		}
		if connection.GetState() != connectivity.Shutdown {
			t.Fatalf("connection state after dial error = %v, want Shutdown", connection.GetState())
		}
	})

	t.Run("unavailable", func(t *testing.T) {
		executor := newBufconnRemoteExecutor(t, agentExecutionServiceFunc(func(context.Context, *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error) {
			return nil, status.Error(codes.Unavailable, "agent stopping")
		}))

		_, err := executor.Execute(context.Background(), "task-1", validRemoteStep())
		if !errors.Is(err, ErrRemoteExecution) ||
			!errors.Is(err, runtime.ErrRetryableExecution) ||
			!strings.Contains(err.Error(), "agent stopping") {
			t.Fatalf("Execute() error = %v, want ErrRemoteExecution and server message", err)
		}
	})
}

func TestRemoteExecutorPreservesCanceledAndDeadlineContexts(t *testing.T) {
	service := agentExecutionServiceFunc(func(ctx context.Context, _ *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error) {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	})

	t.Run("canceled", func(t *testing.T) {
		executor := newBufconnRemoteExecutor(t, service)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := executor.Execute(ctx, "task-1", validRemoteStep())
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context.Canceled", err)
		}
	})

	t.Run("deadline", func(t *testing.T) {
		executor := newBufconnRemoteExecutor(t, service)
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer cancel()
		_, err := executor.Execute(ctx, "task-1", validRemoteStep())
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Execute() error = %v, want context.DeadlineExceeded", err)
		}
	})
}

func TestRemoteExecutorTreatsRemoteContextStatusesAsExecutionFailures(t *testing.T) {
	tests := []struct {
		name       string
		remoteCode codes.Code
		contextErr error
	}{
		{name: "remote canceled", remoteCode: codes.Canceled, contextErr: context.Canceled},
		{name: "remote deadline exceeded", remoteCode: codes.DeadlineExceeded, contextErr: context.DeadlineExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			executor := newBufconnRemoteExecutor(t, agentExecutionServiceFunc(func(context.Context, *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error) {
				return nil, status.Error(tt.remoteCode, "remote agent stopped execution")
			}))

			_, err := executor.Execute(ctx, "task-1", validRemoteStep())
			if !errors.Is(err, ErrRemoteExecution) {
				t.Fatalf("Execute() error = %v, want ErrRemoteExecution", err)
			}
			if errors.Is(err, tt.contextErr) {
				t.Fatalf("Execute() error = %v, must not propagate %v while caller context is active", err, tt.contextErr)
			}
			if ctx.Err() != nil {
				t.Fatalf("caller context error = %v, want active context", ctx.Err())
			}
		})
	}
}

func TestRemoteExecutorRejectsEmptyAndMalformedResponses(t *testing.T) {
	tests := []struct {
		name     string
		response *dtmv1.ExecuteStepResponse
	}{
		{name: "nil result", response: &dtmv1.ExecuteStepResponse{}},
		{name: "unspecified status", response: &dtmv1.ExecuteStepResponse{Result: &dtmv1.StepResult{
			StepId: "cool",
			NodeId: "cooling-node",
		}}},
		{name: "wrong identity", response: &dtmv1.ExecuteStepResponse{Result: &dtmv1.StepResult{
			StepId: "another-step",
			NodeId: "cooling-node",
			Status: dtmv1.ExecutionStatus_EXECUTION_STATUS_SUCCEEDED,
		}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := newBufconnRemoteExecutor(t, agentExecutionServiceFunc(func(context.Context, *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error) {
				return tt.response, nil
			}))
			_, err := executor.Execute(context.Background(), "task-1", validRemoteStep())
			if !errors.Is(err, ErrRemoteExecution) {
				t.Fatalf("Execute() error = %v, want ErrRemoteExecution", err)
			}
		})
	}
}

type remoteResolver struct{}

func (*remoteResolver) Resolve(model.NodeID) (string, error) { return "", nil }

type remoteResolverFunc func(model.NodeID) (string, error)

func (function remoteResolverFunc) Resolve(id model.NodeID) (string, error) {
	return function(id)
}

type agentExecutionServiceFunc func(context.Context, *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error)

func (function agentExecutionServiceFunc) ExecuteStep(ctx context.Context, request *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error) {
	return function(ctx, request)
}

func (agentExecutionServiceFunc) mustEmbedUnimplementedAgentExecutionServiceServer() {}

type testAgentExecutionServer struct {
	dtmv1.UnimplementedAgentExecutionServiceServer
	execute agentExecutionServiceFunc
}

func (server testAgentExecutionServer) ExecuteStep(ctx context.Context, request *dtmv1.ExecuteStepRequest) (*dtmv1.ExecuteStepResponse, error) {
	return server.execute(ctx, request)
}

func startRemoteAgentServer(t *testing.T, service agentExecutionServiceFunc) (*bufconn.Listener, func()) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	dtmv1.RegisterAgentExecutionServiceServer(server, testAgentExecutionServer{execute: service})
	go func() {
		_ = server.Serve(listener)
	}()
	return listener, func() {
		server.Stop()
		_ = listener.Close()
	}
}

func newBufconnRemoteExecutor(t *testing.T, service agentExecutionServiceFunc) *RemoteExecutor {
	t.Helper()
	listener, stop := startRemoteAgentServer(t, service)
	t.Cleanup(stop)
	executor, err := NewRemoteExecutor(
		remoteResolverFunc(func(model.NodeID) (string, error) { return "bufnet", nil }),
		func(ctx context.Context, _ string) (*grpc.ClientConn, error) {
			return grpc.DialContext(
				ctx,
				"passthrough:///bufnet",
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
					return listener.Dial()
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	return executor
}

func validRemoteStep() mapper.MappedStep {
	return mapper.MappedStep{
		ID:         "cool",
		Capability: "cooling_control",
		NodeID:     "cooling-node",
		Inputs:     map[string]string{"target_temperature": "26"},
	}
}
