package dtmv1

import (
	"bytes"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestInvocationProtoRoundTripPreservesNativeEnvelope(t *testing.T) {
	want := &InvocationRequest{
		InvocationId: []byte{1, 2, 3, 4},
		ResourceRef: &ResourceRef{
			ResourceId:          "resource-1",
			ResourceGeneration:  7,
			OwnerNodeId:         "node-1",
			OwnerNodeGeneration: 11,
			RegistrationId:      "registration-1",
		},
		OperationId: "read_temperature",
		Payload:     []byte{0, 1, 2, 3},
		Metadata: &InvocationMetadata{
			TaskId: "task-1", StepId: "step-1", Attempt: 2,
			IdempotencyKey: "key-1", TimeoutMillis: 1250,
		},
	}
	encoded, err := proto.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got := new(InvocationRequest)
	if err := proto.Unmarshal(encoded, got); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(want, got) || !bytes.Equal(got.GetPayload(), want.GetPayload()) {
		t.Fatalf("round-trip request = %#v, want %#v", got, want)
	}

	response := &InvocationResponse{Outcome: &InvocationResponse_Error{Error: &InvocationError{
		InvocationId: []byte{1, 2, 3, 4}, Code: "EXECUTION_FAILURE", Message: "handler refused", Retryable: false,
		OutcomeUnknown: false, Source: "agent", Classification: "handler_result",
		DispatchState: "remote_outcome_received",
	}}}
	encoded, err = proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(InvocationResponse)
	if err := proto.Unmarshal(encoded, decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.GetResult() != nil || decoded.GetError() == nil || decoded.GetError().GetCode() != "EXECUTION_FAILURE" {
		t.Fatalf("round-trip oneof = %#v", decoded.GetOutcome())
	}
}
