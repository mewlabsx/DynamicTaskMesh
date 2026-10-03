package invocation

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/transport/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type nativeInvocationHandler struct {
	mu       sync.Mutex
	calls    int
	lastStep mapper.MappedStep
	output   map[string]any
	err      error
	block    bool
}

func (handler *nativeInvocationHandler) Execute(ctx context.Context, step mapper.MappedStep) (execution.StepResult, error) {
	handler.mu.Lock()
	handler.calls++
	handler.lastStep = step
	block := handler.block
	err := handler.err
	output := handler.output
	handler.mu.Unlock()
	if block {
		<-ctx.Done()
		result, resultErr := execution.NewStepResult(step.ID, step.NodeID, execution.StatusFailed, nil, ctx.Err().Error())
		return result, errors.Join(resultErr, ctx.Err())
	}
	if err != nil {
		result, resultErr := execution.NewStepResult(step.ID, step.NodeID, execution.StatusFailed, nil, err.Error())
		return result, errors.Join(resultErr, err)
	}
	result, resultErr := execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, output, "")
	return result, resultErr
}

func nativeTestRequest(t *testing.T, adapter *LegacyCapabilityAdapter, ref model.ResourceRef, id InvocationID, key string, payload []byte) (InvocationRequest, ResolvedInvocationTarget) {
	t.Helper()
	if payload == nil {
		var err error
		payload, err = adapter.EncodeRequest(mapper.MappedStep{
			ID: "step-1", Capability: "temperature_sensor", NodeID: ref.OwnerNodeID,
			ResourceRef: ref, Inputs: map[string]string{"operation": "read_temperature"},
		}, "read_temperature")
		if err != nil {
			t.Fatal(err)
		}
	}
	request := InvocationRequest{
		InvocationID: id, Target: ref, Operation: "read_temperature", Payload: payload,
		Metadata: InvocationMetadata{TaskID: "task-1", StepID: "step-1", Attempt: 1, IdempotencyKey: key},
	}
	target := ResolvedInvocationTarget{ResourceRef: ref, Operation: request.Operation, Endpoint: ResourceEndpoint{TransportID: TransportGRPC, Address: "bufnet"}}
	return request, target
}

func nativeBufconnTransport(t *testing.T, handler *nativeInvocationHandler, validator ...grpcapi.ExecutionFenceValidator) (*NativeGrpcInvocationTransport, *grpcapi.AgentExecutionServer) {
	t.Helper()
	serverOptions := []grpcapi.AgentExecutionServerOption{
		grpcapi.WithInvocationAdapter(NewLegacyCapabilityAdapter()),
	}
	if len(validator) > 0 {
		serverOptions = append(serverOptions, grpcapi.WithExecutionFenceValidator(validator[0]))
	}
	agentServer, err := grpcapi.NewAgentExecutionServer(handler, serverOptions...)
	if err != nil {
		t.Fatal(err)
	}
	listener := bufconn.Listen(1024 * 1024)
	grpcServer := grpc.NewServer()
	dtmv1.RegisterResourceInvocationServiceServer(grpcServer, agentServer)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(func() { grpcServer.Stop(); _ = listener.Close() })
	var mu sync.Mutex
	dials := 0
	dialer := func(ctx context.Context, _ string) (*grpc.ClientConn, error) {
		mu.Lock()
		dials++
		mu.Unlock()
		return grpc.DialContext(ctx, "passthrough:///bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	transport, err := NewNativeGrpcInvocationTransport(dialer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Close() })
	return transport, agentServer
}

func TestNativeGrpcInvocationTransportPreservesEnvelopeAndReusesConnection(t *testing.T) {
	handler := &nativeInvocationHandler{output: map[string]any{"temperature": float64(30)}}
	transport, _ := nativeBufconnTransport(t, handler)
	adapter := NewLegacyCapabilityAdapter()
	ref := testResourceRef(t)
	firstID, err := NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	first, target := nativeTestRequest(t, adapter, ref, firstID, "native-key-1", nil)
	result, err := transport.Invoke(context.Background(), target, first)
	if err != nil {
		t.Fatal(err)
	}
	if result.Metadata.TransportProfile != TransportProfileNative || result.Metadata.ExecutionFenceValidation != FenceValidationUnconfirmed ||
		result.Metadata.ExecutionFenceMode != ExecutionFenceModeNone || result.Metadata.AcceptedTargetConfirmation != FenceValidationUnconfirmed {
		t.Fatalf("native result metadata = %+v", result.Metadata)
	}
	handler.mu.Lock()
	step := handler.lastStep
	handler.mu.Unlock()
	if step.ResourceRef != ref || step.NodeID != ref.OwnerNodeID || step.Capability != "temperature_sensor" || step.Inputs["operation"] != "read_temperature" {
		t.Fatalf("native mapped step = %+v, want full target and adapter inputs", step)
	}
	secondID, err := NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	second, secondTarget := nativeTestRequest(t, adapter, ref, secondID, "native-key-2", first.Payload)
	if _, err := transport.Invoke(context.Background(), secondTarget, second); err != nil {
		t.Fatal(err)
	}
	handler.mu.Lock()
	calls := handler.calls
	handler.mu.Unlock()
	if calls != 2 {
		t.Fatalf("native handler calls = %d, want 2", calls)
	}
	if err := transport.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = transport.Invoke(context.Background(), target, first)
	if !isInvocationCode(err, ErrorCodeTransportFailure) {
		t.Fatalf("invoke after Close() error = %v, want transport failure", err)
	}
}

func TestNativeGrpcInvocationMapsFenceAndExecutionErrors(t *testing.T) {
	ref := testResourceRef(t)
	validator, err := grpcapi.NewStaticExecutionFenceValidator(ref)
	if err != nil {
		t.Fatal(err)
	}
	transport, _ := nativeBufconnTransport(t, &nativeInvocationHandler{output: map[string]any{}}, validator)
	adapter := NewLegacyCapabilityAdapter()
	requestID, err := NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	request, target := nativeTestRequest(t, adapter, ref, requestID, "fence-key", nil)
	stale := ref
	stale.ResourceGeneration++
	request.Target = stale
	target.ResourceRef = stale
	_, invokeErr := transport.Invoke(context.Background(), target, request)
	var typed *InvocationError
	if !errors.As(invokeErr, &typed) || typed.Code != ErrorCodeResourceStale || typed.DispatchState != DispatchStateNotDispatched {
		t.Fatalf("stale invocation error = %v, want RESOURCE_STALE/not_dispatched", invokeErr)
	}
	wrongOwner := ref
	wrongOwner.OwnerNodeID = "node-2"
	requestID, err = NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	request, target = nativeTestRequest(t, adapter, wrongOwner, requestID, "owner-key", nil)
	_, invokeErr = transport.Invoke(context.Background(), target, request)
	if !errors.As(invokeErr, &typed) || typed.Code != ErrorCodeFenceRejected || typed.DispatchState != DispatchStateNotDispatched {
		t.Fatalf("wrong-owner invocation error = %v, want FENCE_REJECTED/not_dispatched", invokeErr)
	}

	notFoundValidator, err := grpcapi.NewStaticExecutionFenceValidator()
	if err != nil {
		t.Fatal(err)
	}
	notFoundTransport, _ := nativeBufconnTransport(t, &nativeInvocationHandler{output: map[string]any{}}, notFoundValidator)
	requestID, err = NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	request, target = nativeTestRequest(t, adapter, ref, requestID, "not-found-key", nil)
	_, invokeErr = notFoundTransport.Invoke(context.Background(), target, request)
	if !errors.As(invokeErr, &typed) || typed.Code != ErrorCodeResourceNotFound || typed.DispatchState != DispatchStateNotDispatched {
		t.Fatalf("not-found invocation error = %v, want RESOURCE_NOT_FOUND/not_dispatched", invokeErr)
	}

	invalid := request
	invalid.InvocationID = InvocationID{}
	_, invokeErr = notFoundTransport.Invoke(context.Background(), target, invalid)
	if !errors.As(invokeErr, &typed) || typed.Code != ErrorCodeInvalidRequest || typed.DispatchState != DispatchStateNotDispatched {
		t.Fatalf("invalid invocation error = %v, want INVALID_REQUEST/not_dispatched", invokeErr)
	}

	conflictHandler := &nativeInvocationHandler{output: map[string]any{"temperature": float64(30)}}
	conflictTransport, _ := nativeBufconnTransport(t, conflictHandler)
	requestID, err = NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	request, target = nativeTestRequest(t, adapter, ref, requestID, "conflict-key", nil)
	if _, invokeErr = conflictTransport.Invoke(context.Background(), target, request); invokeErr != nil {
		t.Fatal(invokeErr)
	}
	changedPayload, err := adapter.EncodeRequest(mapper.MappedStep{
		ID: "step-1", Capability: "temperature_sensor", NodeID: ref.OwnerNodeID, ResourceRef: ref,
		Inputs: map[string]string{"operation": "read_temperature", "target": "27"},
	}, "read_temperature")
	if err != nil {
		t.Fatal(err)
	}
	conflicting := request
	conflicting.InvocationID, err = NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	conflicting.Payload = changedPayload
	_, invokeErr = conflictTransport.Invoke(context.Background(), target, conflicting)
	if !errors.As(invokeErr, &typed) || typed.Code != ErrorCodeIdempotencyConflict || typed.DispatchState != DispatchStateRemoteOutcomeReceived {
		t.Fatalf("idempotency conflict = %v, want IDEMPOTENCY_CONFLICT/remote_outcome_received", invokeErr)
	}

	failing := &nativeInvocationHandler{err: errors.New("native handler refused")}
	failingTransport, _ := nativeBufconnTransport(t, failing)
	requestID, err = NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	request, target = nativeTestRequest(t, adapter, ref, requestID, "failure-key", nil)
	_, invokeErr = failingTransport.Invoke(context.Background(), target, request)
	if !errors.As(invokeErr, &typed) || typed.Code != ErrorCodeExecutionFailure || typed.DispatchState != DispatchStateRemoteOutcomeReceived {
		t.Fatalf("execution failure = %v, want typed remote outcome", invokeErr)
	}

	timeoutHandler := &nativeInvocationHandler{block: true}
	timeoutTransport, _ := nativeBufconnTransport(t, timeoutHandler)
	requestID, err = NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	request, target = nativeTestRequest(t, adapter, ref, requestID, "timeout-key", nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, invokeErr = timeoutTransport.Invoke(ctx, target, request)
	if !errors.As(invokeErr, &typed) || typed.Code != ErrorCodeTimeout || !typed.OutcomeUnknown || typed.DispatchState != DispatchStateMayHaveDispatched {
		t.Fatalf("timeout = %v, want TIMEOUT/may_have_dispatched/unknown", invokeErr)
	}
}
