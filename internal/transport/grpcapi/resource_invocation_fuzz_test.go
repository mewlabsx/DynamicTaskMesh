package grpcapi

import (
	"bytes"
	"context"
	"testing"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/execution"
	"dtm/internal/mapper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const maxNativeInvocationFuzzInput = 1 << 20

type fuzzInvocationHandler struct{}

func (fuzzInvocationHandler) Execute(context.Context, mapper.MappedStep) (execution.StepResult, error) {
	return execution.StepResult{Status: execution.StatusFailed, Error: "fuzz handler must not be called"}, nil
}

func FuzzNativeInvocationRequest(f *testing.F) {
	seed := &dtmv1.InvocationRequest{
		InvocationId: []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		ResourceRef: &dtmv1.ResourceRef{
			ResourceId:          "resource-1",
			ResourceGeneration:  1,
			OwnerNodeId:         "node-a",
			OwnerNodeGeneration: 1,
			RegistrationId:      "registration-1",
		},
		OperationId: "read_temperature",
		Payload:     []byte(`{"value":"opaque"}`),
		Metadata: &dtmv1.InvocationMetadata{
			TaskId: "task-1", StepId: "step-1", Attempt: 1,
			IdempotencyKey: "key-1", TimeoutMillis: 1000,
		},
	}
	encoded, err := proto.Marshal(seed)
	if err != nil {
		f.Fatalf("proto.Marshal(seed) = %v", err)
	}
	f.Add(encoded)
	f.Add([]byte{})
	f.Add([]byte{0x0a, 0x01, 0x00})

	server, err := NewAgentExecutionServer(fuzzInvocationHandler{})
	if err != nil {
		f.Fatalf("NewAgentExecutionServer() = %v", err)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxNativeInvocationFuzzInput {
			return
		}
		request := new(dtmv1.InvocationRequest)
		if err := proto.Unmarshal(data, request); err != nil {
			return
		}
		_, _, _, parseErr := nativeInvocationInputs(request)
		response, rpcErr := server.InvokeResource(context.Background(), request)
		if parseErr != nil {
			if rpcErr == nil || status.Code(rpcErr) != codes.InvalidArgument || response != nil {
				t.Fatalf("invalid request result = response:%#v rpc:%v", response, rpcErr)
			}
			return
		}
		if rpcErr != nil {
			t.Fatalf("valid request returned RPC error: %v", rpcErr)
		}
		if response == nil || response.GetError() == nil || response.GetResult() != nil {
			t.Fatalf("valid request did not fail closed with a typed error: %#v", response)
		}
		if !bytes.Equal(response.GetError().GetInvocationId(), request.GetInvocationId()) {
			t.Fatalf("error correlation ID = %x, want %x", response.GetError().GetInvocationId(), request.GetInvocationId())
		}
	})
}
