package invocation

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/agent"
	"dtm/internal/demo"
	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
	meshruntime "dtm/internal/runtime"
	"dtm/internal/transport/grpcapi"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestInvocationIDIsNonZeroLowercaseHexAndUnique(t *testing.T) {
	seen := make(map[string]struct{}, 64)
	for range 64 {
		id, err := NewInvocationID()
		if err != nil {
			t.Fatal(err)
		}
		value := id.String()
		if len(value) != 32 || value != strings.ToLower(value) {
			t.Fatalf("id = %q, want 32 lowercase hex characters", value)
		}
		if _, exists := seen[value]; exists {
			t.Fatalf("duplicate invocation id %q", value)
		}
		seen[value] = struct{}{}
		parsed, err := ParseInvocationID(value)
		if err != nil || parsed != id {
			t.Fatalf("ParseInvocationID(%q) = %v, %v; want %v", value, parsed, err, id)
		}
	}
	if _, err := ParseInvocationID(strings.Repeat("0", 32)); err == nil {
		t.Fatal("ParseInvocationID() accepted a zero id")
	}
}

func TestInvocationRequestAndResultValidation(t *testing.T) {
	ref := testResourceRef(t)
	id, err := NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	request := InvocationRequest{
		InvocationID: id, Target: ref, Operation: "read_temperature", Payload: []byte("{}"),
		Metadata: InvocationMetadata{TaskID: "task-1", StepID: "step-1", Attempt: 1, IdempotencyKey: "key-1"},
	}
	if err := request.Validate(); err != nil {
		t.Fatalf("valid request error = %v", err)
	}
	invalid := request
	invalid.Metadata.Attempt = 0
	if err := invalid.Validate(); err == nil {
		t.Fatal("request with zero attempt validated")
	}
	result := InvocationResult{InvocationID: id, Payload: []byte("{}"), Metadata: ExecutionMetadata{
		ExecutionFenceValidation:   FenceValidationUnconfirmed,
		AcceptedTargetConfirmation: FenceValidationUnconfirmed,
	}}
	if err := result.Validate(); err != nil {
		t.Fatalf("success-only result error = %v", err)
	}
	authorityResult := result
	authorityResult.Metadata.ExecutionFenceMode = ExecutionFenceModeAuthorityConfirmed
	if err := authorityResult.Validate(); err == nil {
		t.Fatal("result accepted authority-confirmed execution fence without an Authority receipt")
	}
	accepted := ref
	result.Metadata.AcceptedTarget = &accepted
	if err := result.Validate(); err == nil {
		t.Fatal("result accepted an execution target while confirmation is unconfirmed")
	}
	for _, code := range []ErrorCode{
		RESOURCE_NOT_FOUND, RESOURCE_STALE, FENCE_REJECTED, UNSUPPORTED_OPERATION,
		INVALID_REQUEST, TRANSPORT_FAILURE, TIMEOUT, CANCELED, EXECUTION_FAILURE,
		PROTOCOL_ERROR, IDEMPOTENCY_CONFLICT,
	} {
		if strings.TrimSpace(string(code)) == "" {
			t.Fatalf("empty error code")
		}
	}
}

func TestAuthoritativeResolverDoubleValidatesExactFence(t *testing.T) {
	ref := testResourceRef(t)
	view := testResourceView(t, ref)
	resolver, err := NewAuthoritativeTargetResolver(&sequenceResourceLookup{views: []resourcedirectory.ResourceRecordView{view, view}}, staticEndpointLookup{address: "127.0.0.1:9000"})
	if err != nil {
		t.Fatal(err)
	}
	targets, err := resolver.Resolve(context.Background(), ref, "read_temperature")
	if err != nil || len(targets) != 1 {
		t.Fatalf("Resolve() targets=%#v error=%v", targets, err)
	}
	if targets[0].ResourceRef != ref || targets[0].Endpoint.Address != "127.0.0.1:9000" {
		t.Fatalf("resolved target=%+v, want exact ref and endpoint", targets[0])
	}
	if targets[0].Endpoint.Address == string(ref.OwnerNodeID) {
		t.Fatal("endpoint was used as ResourceRef identity")
	}

	stale := ref
	stale.ResourceGeneration++
	_, err = resolver.Resolve(context.Background(), stale, "read_temperature")
	if !isInvocationCode(err, ErrorCodeResourceStale) {
		t.Fatalf("stale generation error=%v, want RESOURCE_STALE", err)
	}
	_, err = resolver.Resolve(context.Background(), ref, "set_target_temperature")
	if !isInvocationCode(err, ErrorCodeUnsupportedOperation) {
		t.Fatalf("unsupported operation error=%v, want UNSUPPORTED_OPERATION", err)
	}

	changed := testResourceRef(t)
	changed.ResourceGeneration++
	changedView := testResourceView(t, changed)
	_, err = NewAuthoritativeTargetResolver(&sequenceResourceLookup{views: []resourcedirectory.ResourceRecordView{view, changedView}}, staticEndpointLookup{address: "127.0.0.1:9000"})
	if err != nil {
		t.Fatal(err)
	}
	resolver, _ = NewAuthoritativeTargetResolver(&sequenceResourceLookup{views: []resourcedirectory.ResourceRecordView{view, changedView}}, staticEndpointLookup{address: "127.0.0.1:9000"})
	_, err = resolver.Resolve(context.Background(), ref, "read_temperature")
	if !isInvocationCode(err, ErrorCodeResourceStale) {
		t.Fatalf("second-validation stale error=%v, want RESOURCE_STALE", err)
	}
}

func TestLegacyCapabilityAdapterRoundTrip(t *testing.T) {
	adapter := NewLegacyCapabilityAdapter()
	step := mapper.MappedStep{ID: "step-1", Capability: "temperature_sensor", Inputs: map[string]string{"operation": "read_temperature"}}
	payload, err := adapter.EncodeRequest(step, "read_temperature")
	if err != nil {
		t.Fatal(err)
	}
	capability, inputs, err := adapter.DecodeRequest("read_temperature", payload)
	if err != nil || capability != step.Capability || inputs["operation"] != "read_temperature" {
		t.Fatalf("request round trip capability=%q inputs=%v error=%v", capability, inputs, err)
	}
	outputPayload, err := adapter.EncodeOutput(map[string]any{"temperature": float64(30)})
	if err != nil {
		t.Fatal(err)
	}
	output, err := adapter.DecodeOutput(outputPayload)
	if err != nil || output["temperature"] != float64(30) {
		t.Fatalf("output round trip=%v error=%v", output, err)
	}
}

func TestInvocationExecutorGeneratesNewIDPerAttempt(t *testing.T) {
	ref := testResourceRef(t)
	resolver := &staticTargetResolver{target: ResolvedInvocationTarget{ResourceRef: ref, Operation: "read_temperature", Endpoint: ResourceEndpoint{TransportID: TransportGRPC, Address: "test"}}}
	adapter := NewLegacyCapabilityAdapter()
	transport := &recordingTransport{adapter: adapter}
	service, err := NewInvocationService(resolver, transport)
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewInvocationExecutor(service, adapter)
	if err != nil {
		t.Fatal(err)
	}
	step := mapper.MappedStep{ID: "step-1", Capability: "temperature_sensor", NodeID: ref.OwnerNodeID, ResourceRef: ref, Inputs: map[string]string{"operation": "read_temperature"}}
	first, err := executor.ExecuteAttempt(context.Background(), "task-1", step, meshruntime.StepAttempt{Number: 1, IdempotencyKey: "key-1"})
	if err != nil || first.Status != execution.StatusSucceeded || first.Output["temperature"] != float64(30) {
		t.Fatalf("first result=%+v error=%v", first, err)
	}
	second, err := executor.ExecuteAttempt(context.Background(), "task-1", step, meshruntime.StepAttempt{Number: 2, IdempotencyKey: "key-1"})
	if err != nil || second.Status != execution.StatusSucceeded {
		t.Fatalf("second result=%+v error=%v", second, err)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.requests) != 2 || transport.requests[0].InvocationID == transport.requests[1].InvocationID {
		t.Fatalf("invocation ids=%v, want a new id per attempt", transport.requests)
	}
	if transport.requests[0].Operation != "read_temperature" || transport.requests[0].Target != ref {
		t.Fatalf("request=%+v, want explicit operation and exact ref", transport.requests[0])
	}
}

func TestGRPCCompatibilityTransportUsesExistingAgentExecutionService(t *testing.T) {
	const bufferSize = 1024 * 1024
	listener := bufconn.Listen(bufferSize)
	server := grpc.NewServer()
	router, err := agent.NewRouter(demo.NewTemperatureHandler())
	if err != nil {
		t.Fatal(err)
	}
	agentServer, err := grpcapi.NewAgentExecutionServer(router)
	if err != nil {
		t.Fatal(err)
	}
	dtmv1.RegisterAgentExecutionServiceServer(server, agentServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })

	dialer := func(ctx context.Context, address string) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, address, grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	adapter := NewLegacyCapabilityAdapter()
	transport, err := NewGrpcCompatibilityTransport(dialer, adapter)
	if err != nil {
		t.Fatal(err)
	}
	ref := testResourceRef(t)
	step := mapper.MappedStep{ID: "step-1", Capability: "temperature_sensor", NodeID: ref.OwnerNodeID, ResourceRef: ref, Inputs: map[string]string{"operation": "read_temperature"}}
	payload, err := adapter.EncodeRequest(step, "read_temperature")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := NewInvocationID()
	result, err := transport.Invoke(context.Background(), ResolvedInvocationTarget{ResourceRef: ref, Operation: "read_temperature", Endpoint: ResourceEndpoint{TransportID: TransportGRPC, Address: "bufnet"}}, InvocationRequest{
		InvocationID: id, Target: ref, Operation: "read_temperature", Payload: payload,
		Metadata: InvocationMetadata{TaskID: "task-1", StepID: "step-1", Attempt: 1, IdempotencyKey: "key-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	output, err := adapter.DecodeOutput(result.Payload)
	if err != nil || output["temperature"] != float64(30) {
		t.Fatalf("legacy output=%v error=%v", output, err)
	}
	if result.Metadata.ExecutionFenceValidation != FenceValidationUnconfirmed || result.Metadata.ExecutionFenceMode != ExecutionFenceModeNone ||
		result.Metadata.AcceptedTargetConfirmation != FenceValidationUnconfirmed {
		t.Fatalf("legacy metadata=%+v, want unconfirmed execution fence", result.Metadata)
	}
	if !result.Metadata.Start.IsZero() {
		t.Fatalf("legacy transport populated Invocation Start=%v; service owns invocation timing", result.Metadata.Start)
	}
}

func TestGRPCCompatibilityTransportMarksDefiniteDialFailureNotDispatched(t *testing.T) {
	adapter := NewLegacyCapabilityAdapter()
	transport, err := NewGrpcCompatibilityTransport(func(context.Context, string) (*grpc.ClientConn, error) {
		return nil, errors.New("dial refused locally")
	}, adapter)
	if err != nil {
		t.Fatal(err)
	}
	ref := testResourceRef(t)
	step := mapper.MappedStep{ID: "step-1", Capability: "temperature_sensor", NodeID: ref.OwnerNodeID, ResourceRef: ref, Inputs: map[string]string{"operation": "read_temperature"}}
	payload, err := adapter.EncodeRequest(step, "read_temperature")
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	_, invokeErr := transport.Invoke(context.Background(), ResolvedInvocationTarget{
		ResourceRef: ref,
		Operation:   "read_temperature",
		Endpoint:    ResourceEndpoint{TransportID: TransportGRPC, Address: "dial-failure"},
	}, InvocationRequest{
		InvocationID: id,
		Target:       ref,
		Operation:    "read_temperature",
		Payload:      payload,
		Metadata:     InvocationMetadata{TaskID: "task-1", StepID: "step-1", Attempt: 1, IdempotencyKey: "key-1"},
	})
	var invocationErr *InvocationError
	if !errors.As(invokeErr, &invocationErr) {
		t.Fatalf("transport error=%v, want InvocationError", invokeErr)
	}
	if invocationErr.Code != ErrorCodeTransportFailure || invocationErr.DispatchState != DispatchStateNotDispatched || invocationErr.OutcomeUnknown {
		t.Fatalf("dial failure error=%+v, want definite not-dispatched", invocationErr)
	}
}

func TestGRPCCompatibilityTransportMarksLegacyRemoteFailureUnconfirmed(t *testing.T) {
	const bufferSize = 1024 * 1024
	listener := bufconn.Listen(bufferSize)
	server := grpc.NewServer()
	router, err := agent.NewRouter(&failingInvocationHandler{})
	if err != nil {
		t.Fatal(err)
	}
	agentServer, err := grpcapi.NewAgentExecutionServer(router)
	if err != nil {
		t.Fatal(err)
	}
	dtmv1.RegisterAgentExecutionServiceServer(server, agentServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); _ = listener.Close() })

	dialer := func(ctx context.Context, address string) (*grpc.ClientConn, error) {
		return grpc.DialContext(ctx, address, grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}), grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	adapter := NewLegacyCapabilityAdapter()
	transport, err := NewGrpcCompatibilityTransport(dialer, adapter)
	if err != nil {
		t.Fatal(err)
	}
	ref := testResourceRef(t)
	step := mapper.MappedStep{ID: "step-1", Capability: "temperature_sensor", NodeID: ref.OwnerNodeID, ResourceRef: ref, Inputs: map[string]string{"operation": "read_temperature"}}
	payload, err := adapter.EncodeRequest(step, "read_temperature")
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	_, invokeErr := transport.Invoke(context.Background(), ResolvedInvocationTarget{
		ResourceRef: ref,
		Operation:   "read_temperature",
		Endpoint:    ResourceEndpoint{TransportID: TransportGRPC, Address: "bufnet"},
	}, InvocationRequest{
		InvocationID: id,
		Target:       ref,
		Operation:    "read_temperature",
		Payload:      payload,
		Metadata:     InvocationMetadata{TaskID: "task-1", StepID: "step-1", Attempt: 1, IdempotencyKey: "key-1"},
	})
	var invocationErr *InvocationError
	if !errors.As(invokeErr, &invocationErr) {
		t.Fatalf("transport error=%v, want InvocationError", invokeErr)
	}
	if invocationErr.Code != ErrorCodeExecutionFailure || invocationErr.DispatchState != DispatchStateRemoteOutcomeReceived || invocationErr.OutcomeUnknown {
		t.Fatalf("legacy remote failure error=%+v, want remote outcome unconfirmed", invocationErr)
	}
}

type failingInvocationHandler struct{}

func (*failingInvocationHandler) Capability() model.Capability { return "temperature_sensor" }

func (*failingInvocationHandler) Execute(context.Context, map[string]string) (map[string]any, error) {
	return nil, errors.New("legacy handler refused invocation")
}

type sequenceResourceLookup struct {
	mu    sync.Mutex
	views []resourcedirectory.ResourceRecordView
	calls int
}

func (lookup *sequenceResourceLookup) GetByID(model.ResourceID) (resourcedirectory.ResourceRecordView, error) {
	lookup.mu.Lock()
	defer lookup.mu.Unlock()
	if len(lookup.views) == 0 {
		return resourcedirectory.ResourceRecordView{}, errors.New("no view")
	}
	index := lookup.calls
	lookup.calls++
	if index >= len(lookup.views) {
		index = len(lookup.views) - 1
	}
	return lookup.views[index], nil
}

type staticEndpointLookup struct{ address string }

func (lookup staticEndpointLookup) Resolve(model.NodeID) (string, error) { return lookup.address, nil }

type staticTargetResolver struct{ target ResolvedInvocationTarget }

func (resolver *staticTargetResolver) Resolve(_ context.Context, _ model.ResourceRef, _ model.OperationID) ([]ResolvedInvocationTarget, error) {
	return []ResolvedInvocationTarget{resolver.target}, nil
}

type recordingTransport struct {
	adapter  *LegacyCapabilityAdapter
	mu       sync.Mutex
	requests []InvocationRequest
}

func (transport *recordingTransport) Invoke(_ context.Context, _ ResolvedInvocationTarget, request InvocationRequest) (InvocationResult, error) {
	transport.mu.Lock()
	transport.requests = append(transport.requests, cloneRequest(request))
	transport.mu.Unlock()
	payload, err := transport.adapter.EncodeOutput(map[string]any{"temperature": float64(30)})
	if err != nil {
		return InvocationResult{}, err
	}
	return InvocationResult{InvocationID: request.InvocationID, Payload: payload}, nil
}

func testResourceRef(t *testing.T) model.ResourceRef {
	t.Helper()
	ref, err := model.NewResourceRef("resource-1", 1, "node-1", 1, "registration-1")
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func testResourceView(t *testing.T, ref model.ResourceRef) resourcedirectory.ResourceRecordView {
	t.Helper()
	descriptor, err := model.AdaptLegacyCapability(ref.OwnerNodeID, "temperature_sensor", ref.ResourceGeneration)
	if err != nil {
		t.Fatal(err)
	}
	descriptor.ID = ref.ResourceID
	return resourcedirectory.ResourceRecordView{Descriptor: descriptor, NodeGeneration: ref.OwnerNodeGeneration, RegistrationID: ref.RegistrationID, PublicationState: resourcedirectory.PublicationStatePublished, Eligible: true}
}

func isInvocationCode(err error, code ErrorCode) bool {
	var invocationErr *InvocationError
	return errors.As(err, &invocationErr) && invocationErr.Code == code
}
