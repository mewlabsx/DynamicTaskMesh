package invocation

import (
	"context"
	"testing"
	"time"

	"dtm/internal/model"
)

func TestInvocationFailureEvidenceUsesActualFencePhaseAndTiming(t *testing.T) {
	ref := testResourceRef(t)
	request := testInvocationRequest(t, ref)
	t0 := time.Date(2026, 8, 20, 10, 0, 0, 0, time.UTC)
	t1 := t0.Add(7 * time.Millisecond)

	tests := []struct {
		name               string
		request            InvocationRequest
		resolverError      error
		transportError     error
		wantCallerFence    string
		wantExecution      string
		wantAccepted       string
		wantEndpoint       string
		wantTransport      string
		wantErrorCode      ErrorCode
		wantOutcomeUnknown bool
		wantDispatchState  DispatchState
		wantResolverCall   int
		wantTransportCall  int
	}{
		{
			name:              "invalid request",
			request:           InvocationRequest{Target: ref, Operation: request.Operation, Metadata: request.Metadata},
			wantCallerFence:   FenceValidationNotRun,
			wantExecution:     FenceValidationNotRun,
			wantAccepted:      FenceValidationNotRun,
			wantErrorCode:     ErrorCodeInvalidRequest,
			wantResolverCall:  0,
			wantTransportCall: 0,
		},
		{
			name:              "stale resource ref",
			resolverError:     &InvocationError{Code: ErrorCodeResourceStale, Message: "stale resource ref", Source: "resolver"},
			wantCallerFence:   FenceValidationRejected,
			wantExecution:     FenceValidationNotRun,
			wantAccepted:      FenceValidationNotRun,
			wantErrorCode:     ErrorCodeResourceStale,
			wantResolverCall:  1,
			wantTransportCall: 0,
		},
		{
			name:              "unsupported operation resolver rejection",
			resolverError:     &InvocationError{Code: ErrorCodeUnsupportedOperation, Message: "operation not published", Source: "resource-directory", Classification: "operation_not_published"},
			wantCallerFence:   FenceValidationRejected,
			wantExecution:     FenceValidationNotRun,
			wantAccepted:      FenceValidationNotRun,
			wantErrorCode:     ErrorCodeUnsupportedOperation,
			wantResolverCall:  1,
			wantTransportCall: 0,
		},
		{
			name:              "missing endpoint resolver rejection",
			resolverError:     &InvocationError{Code: ErrorCodeResourceNotFound, Message: "endpoint missing", Source: "endpoint-directory", Classification: "compatibility_endpoint_missing"},
			wantCallerFence:   FenceValidationRejected,
			wantExecution:     FenceValidationNotRun,
			wantAccepted:      FenceValidationNotRun,
			wantErrorCode:     ErrorCodeResourceNotFound,
			wantResolverCall:  1,
			wantTransportCall: 0,
		},
		{
			name:              "transport failure after resolve",
			transportError:    &InvocationError{Code: ErrorCodeTransportFailure, Message: "dial failed", DispatchState: DispatchStateNotDispatched, Source: "transport"},
			wantCallerFence:   FenceValidationPassed,
			wantExecution:     FenceValidationNotRun,
			wantAccepted:      FenceValidationNotRun,
			wantEndpoint:      "127.0.0.1:9000",
			wantTransport:     TransportGRPC,
			wantErrorCode:     ErrorCodeTransportFailure,
			wantDispatchState: DispatchStateNotDispatched,
			wantResolverCall:  1,
			wantTransportCall: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := &m1R1Resolver{
				targets: []ResolvedInvocationTarget{{
					ResourceRef: ref,
					Operation:   request.Operation,
					Endpoint:    ResourceEndpoint{TransportID: TransportGRPC, Address: "127.0.0.1:9000"},
				}},
				err: test.resolverError,
			}
			transport := &m1R1Transport{err: test.transportError}
			recorder := &m1R1Observer{}
			service, err := NewInvocationService(resolver, transport, WithInvocationObserver(recorder.Observe))
			if err != nil {
				t.Fatal(err)
			}
			service.now = (&m1R1Clock{times: []time.Time{t0, t1}}).Now

			caseRequest := test.request
			if caseRequest.InvocationID.IsZero() && test.name != "invalid request" {
				caseRequest = request
			}
			_, invokeErr := service.Invoke(context.Background(), caseRequest)
			if invokeErr == nil || !isInvocationCode(invokeErr, test.wantErrorCode) {
				t.Fatalf("Invoke() error=%v, want %s", invokeErr, test.wantErrorCode)
			}
			if resolver.calls != test.wantResolverCall {
				t.Fatalf("resolver calls=%d, want %d", resolver.calls, test.wantResolverCall)
			}
			if transport.calls != test.wantTransportCall {
				t.Fatalf("transport calls=%d, want %d", transport.calls, test.wantTransportCall)
			}
			if recorder.calls != 1 {
				t.Fatalf("observer calls=%d, want 1", recorder.calls)
			}
			observation := recorder.observation
			metadata := observation.Metadata
			if metadata.CallerFenceValidation != test.wantCallerFence ||
				metadata.ExecutionFenceValidation != test.wantExecution ||
				metadata.AcceptedTargetConfirmation != test.wantAccepted {
				t.Fatalf("failure metadata=%+v", metadata)
			}
			if metadata.Status != InvocationStatusFailed || metadata.ErrorCode != test.wantErrorCode {
				t.Fatalf("failure status/error metadata=%+v", metadata)
			}
			if observation.Error == nil || observation.Error.DispatchState != test.wantDispatchState || metadata.OutcomeUnknown != test.wantOutcomeUnknown {
				t.Fatalf("failure dispatch metadata=%+v error=%+v", metadata, observation.Error)
			}
			if !metadata.Start.Equal(t0) || metadata.Duration != t1.Sub(t0) || metadata.Duration < 0 {
				t.Fatalf("failure timing start=%v duration=%v", metadata.Start, metadata.Duration)
			}
			if metadata.Endpoint != test.wantEndpoint || metadata.Transport != test.wantTransport {
				t.Fatalf("unresolved/transport metadata endpoint=%q transport=%q", metadata.Endpoint, metadata.Transport)
			}
		})
	}
}

func TestInvocationExecutionEvidenceUsesDispatchCertainty(t *testing.T) {
	ref := testResourceRef(t)
	request := testInvocationRequest(t, ref)
	tests := []struct {
		name           string
		transportError error
		result         InvocationResult
		wantExecution  string
		wantAccepted   string
		wantUnknown    bool
		wantErrorCode  ErrorCode
	}{
		{
			name: "outcome unknown transport failure",
			transportError: &InvocationError{
				Code: ErrorCodeTransportFailure, Message: "connection lost after write", OutcomeUnknown: true,
			},
			wantExecution: FenceValidationUnconfirmed,
			wantAccepted:  FenceValidationUnconfirmed,
			wantUnknown:   true,
			wantErrorCode: ErrorCodeTransportFailure,
		},
		{
			name: "definite local pre-dispatch failure",
			transportError: &InvocationError{
				Code: ErrorCodeTransportFailure, Message: "local dial configuration rejected", DispatchState: DispatchStateNotDispatched,
			},
			wantExecution: FenceValidationNotRun,
			wantAccepted:  FenceValidationNotRun,
			wantErrorCode: ErrorCodeTransportFailure,
		},
		{
			name: "legacy remote handler failure",
			transportError: &InvocationError{
				Code: ErrorCodeExecutionFailure, Message: "handler failed", DispatchState: DispatchStateRemoteOutcomeReceived,
			},
			wantExecution: FenceValidationUnconfirmed,
			wantAccepted:  FenceValidationUnconfirmed,
			wantErrorCode: ErrorCodeExecutionFailure,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := &m1R1Resolver{targets: []ResolvedInvocationTarget{{
				ResourceRef: ref,
				Operation:   request.Operation,
				Endpoint:    ResourceEndpoint{TransportID: TransportGRPC, Address: "127.0.0.1:9000"},
			}}}
			transport := &m1R1Transport{err: test.transportError, result: test.result}
			recorder := &m1R1Observer{}
			service, err := NewInvocationService(resolver, transport, WithInvocationObserver(recorder.Observe))
			if err != nil {
				t.Fatal(err)
			}
			_, invokeErr := service.Invoke(context.Background(), request)
			if invokeErr == nil || !isInvocationCode(invokeErr, test.wantErrorCode) {
				t.Fatalf("Invoke() error=%v, want %s", invokeErr, test.wantErrorCode)
			}
			if recorder.calls != 1 || recorder.observation.Error == nil {
				t.Fatalf("observation=%+v", recorder.observation)
			}
			metadata := recorder.observation.Metadata
			if metadata.CallerFenceValidation != FenceValidationPassed || metadata.ExecutionFenceValidation != test.wantExecution ||
				metadata.AcceptedTargetConfirmation != test.wantAccepted || metadata.OutcomeUnknown != test.wantUnknown {
				t.Fatalf("dispatch evidence metadata=%+v", metadata)
			}
		})
	}
}

func TestInvocationPostTransportCorrelationFailureRemainsUnconfirmed(t *testing.T) {
	ref := testResourceRef(t)
	request := testInvocationRequest(t, ref)
	wrongID, err := NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	resolver := &m1R1Resolver{targets: []ResolvedInvocationTarget{{
		ResourceRef: ref,
		Operation:   request.Operation,
		Endpoint:    ResourceEndpoint{TransportID: TransportGRPC, Address: "127.0.0.1:9000"},
	}}}
	transport := &m1R1Transport{result: InvocationResult{InvocationID: wrongID}}
	recorder := &m1R1Observer{}
	service, err := NewInvocationService(resolver, transport, WithInvocationObserver(recorder.Observe))
	if err != nil {
		t.Fatal(err)
	}
	_, invokeErr := service.Invoke(context.Background(), request)
	if invokeErr == nil || !isInvocationCode(invokeErr, ErrorCodeProtocolError) {
		t.Fatalf("Invoke() error=%v, want PROTOCOL_ERROR", invokeErr)
	}
	if recorder.observation.Metadata.ExecutionFenceValidation != FenceValidationUnconfirmed ||
		recorder.observation.Metadata.AcceptedTargetConfirmation != FenceValidationUnconfirmed {
		t.Fatalf("post-transport metadata=%+v", recorder.observation.Metadata)
	}
}

func TestInvocationServiceOwnsStartAndDurationOverTransport(t *testing.T) {
	ref := testResourceRef(t)
	request := testInvocationRequest(t, ref)
	t0 := time.Date(2026, 8, 20, 11, 0, 0, 0, time.UTC)
	t1 := t0.Add(3 * time.Millisecond)
	t2 := t0.Add(11 * time.Millisecond)
	resolver := &m1R1Resolver{targets: []ResolvedInvocationTarget{{
		ResourceRef: ref,
		Operation:   request.Operation,
		Endpoint:    ResourceEndpoint{TransportID: TransportGRPC, Address: "127.0.0.1:9000"},
	}}}
	transport := &m1R1Transport{result: InvocationResult{
		InvocationID: request.InvocationID,
		Payload:      []byte(`{"temperature":30}`),
		Metadata: ExecutionMetadata{
			Start:                      t1,
			Duration:                   time.Hour,
			Status:                     "succeeded",
			ExecutionFenceValidation:   FenceValidationUnconfirmed,
			AcceptedTargetConfirmation: FenceValidationUnconfirmed,
		},
	}}
	recorder := &m1R1Observer{}
	service, err := NewInvocationService(resolver, transport, WithInvocationObserver(recorder.Observe))
	if err != nil {
		t.Fatal(err)
	}
	service.now = (&m1R1Clock{times: []time.Time{t0, t2}}).Now

	result, err := service.Invoke(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Metadata.Start.Equal(t0) || result.Metadata.Duration != t2.Sub(t0) {
		t.Fatalf("result timing start=%v duration=%v, want start=%v duration=%v", result.Metadata.Start, result.Metadata.Duration, t0, t2.Sub(t0))
	}
	if result.Metadata.Status != InvocationStatusSuccess || result.Metadata.CallerFenceValidation != FenceValidationPassed {
		t.Fatalf("result metadata=%+v", result.Metadata)
	}
	if recorder.calls != 1 || recorder.observation.Metadata.Start != result.Metadata.Start || recorder.observation.Metadata.Duration != result.Metadata.Duration {
		t.Fatalf("observation metadata=%+v result metadata=%+v", recorder.observation.Metadata, result.Metadata)
	}
}

type m1R1Resolver struct {
	targets []ResolvedInvocationTarget
	err     error
	calls   int
}

func (resolver *m1R1Resolver) Resolve(context.Context, model.ResourceRef, model.OperationID) ([]ResolvedInvocationTarget, error) {
	resolver.calls++
	if resolver.err != nil {
		return nil, resolver.err
	}
	return resolver.targets, nil
}

type m1R1Transport struct {
	result InvocationResult
	err    error
	calls  int
}

func (transport *m1R1Transport) Invoke(context.Context, ResolvedInvocationTarget, InvocationRequest) (InvocationResult, error) {
	transport.calls++
	if transport.err != nil {
		return InvocationResult{}, transport.err
	}
	return transport.result, nil
}

type m1R1Observer struct {
	observation InvocationObservation
	calls       int
}

func (observer *m1R1Observer) Observe(_ context.Context, observation InvocationObservation) error {
	observer.observation = observation
	observer.calls++
	return nil
}

type m1R1Clock struct {
	times []time.Time
	index int
}

func (clock *m1R1Clock) Now() time.Time {
	if clock.index >= len(clock.times) {
		return clock.times[len(clock.times)-1]
	}
	value := clock.times[clock.index]
	clock.index++
	return value
}

func testInvocationRequest(t *testing.T, ref model.ResourceRef) InvocationRequest {
	t.Helper()
	id, err := NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	return InvocationRequest{
		InvocationID: id,
		Target:       ref,
		Operation:    "read_temperature",
		Payload:      []byte(`{}`),
		Metadata: InvocationMetadata{
			TaskID:         "task-r1",
			StepID:         "step-r1",
			Attempt:        1,
			IdempotencyKey: "key-r1",
		},
	}
}

var _ TargetResolver = (*m1R1Resolver)(nil)
var _ Transport = (*m1R1Transport)(nil)
var _ InvocationObserver = (*m1R1Observer)(nil).Observe
