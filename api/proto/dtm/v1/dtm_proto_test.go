package dtmv1

import (
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

func TestRegistrationOwnershipFieldsRemainAdditive(t *testing.T) {
	tests := []struct {
		message string
		field   string
		number  protoreflect.FieldNumber
	}{
		{message: "RegisterNodeRequest", field: "registration_id", number: 2},
		{message: "HeartbeatRequest", field: "registration_id", number: 2},
		{message: "UpdateNodeStatusRequest", field: "registration_id", number: 3},
	}

	messages := File_api_proto_dtm_v1_dtm_proto.Messages()
	for _, tt := range tests {
		t.Run(tt.message, func(t *testing.T) {
			message := messages.ByName(protoreflect.Name(tt.message))
			if message == nil {
				t.Fatalf("message %q not found", tt.message)
			}
			field := message.Fields().ByName(protoreflect.Name(tt.field))
			if field == nil {
				t.Fatalf("%s.%s not found", tt.message, tt.field)
			}
			if field.Number() != tt.number {
				t.Fatalf("%s.%s number = %d, want %d", tt.message, tt.field, field.Number(), tt.number)
			}
			if field.Kind() != protoreflect.StringKind {
				t.Fatalf("%s.%s kind = %s, want string", tt.message, tt.field, field.Kind())
			}
		})
	}
}

func TestAsyncTaskQueryContractIsAdditive(t *testing.T) {
	messages := File_api_proto_dtm_v1_dtm_proto.Messages()
	submit := messages.ByName("SubmitTaskRequest")
	if submit == nil {
		t.Fatal("SubmitTaskRequest not found")
	}
	async := submit.Fields().ByName("async")
	if async == nil || async.Number() != 2 || async.Kind() != protoreflect.BoolKind {
		t.Fatalf("SubmitTaskRequest.async = %#v, want bool field 2", async)
	}
	if messages.ByName("GetTaskStatusRequest") == nil ||
		messages.ByName("GetTaskStatusResponse") == nil {
		t.Fatal("GetTaskStatus messages not found")
	}
	core := File_api_proto_dtm_v1_dtm_proto.Services().ByName("CoreService")
	if core == nil || core.Methods().ByName("GetTaskStatus") == nil {
		t.Fatal("CoreService.GetTaskStatus not found")
	}
}

func TestStepExecutionIdempotencyContractIsAdditive(t *testing.T) {
	messages := File_api_proto_dtm_v1_dtm_proto.Messages()
	request := messages.ByName("ExecuteStepRequest")
	if request == nil {
		t.Fatal("ExecuteStepRequest not found")
	}
	key := request.Fields().ByName("idempotency_key")
	if key == nil || key.Number() != 3 || key.Kind() != protoreflect.StringKind {
		t.Fatalf("ExecuteStepRequest.idempotency_key = %#v", key)
	}
	attempt := request.Fields().ByName("attempt")
	if attempt == nil || attempt.Number() != 4 || attempt.Kind() != protoreflect.Uint32Kind {
		t.Fatalf("ExecuteStepRequest.attempt = %#v", attempt)
	}
	response := messages.ByName("ExecuteStepResponse")
	replayed := response.Fields().ByName("replayed")
	if replayed == nil || replayed.Number() != 2 || replayed.Kind() != protoreflect.BoolKind {
		t.Fatalf("ExecuteStepResponse.replayed = %#v", replayed)
	}
}

func TestM4TaskQueryContractIsAdditive(t *testing.T) {
	services := File_api_proto_dtm_v1_dtm_proto.Services()
	core := services.ByName("CoreService")
	for _, method := range []protoreflect.Name{"SubmitTask", "GetTaskStatus", "GetTask", "ListTasks", "GetTaskExecutions"} {
		if core == nil || core.Methods().ByName(method) == nil {
			t.Fatalf("CoreService.%s not found", method)
		}
	}
	messages := File_api_proto_dtm_v1_dtm_proto.Messages()
	list := messages.ByName("ListTasksRequest")
	for field, number := range map[protoreflect.Name]protoreflect.FieldNumber{
		"status": 1, "created_after": 2, "created_before": 3, "limit": 4, "page_token": 5,
	} {
		descriptor := list.Fields().ByName(field)
		if descriptor == nil || descriptor.Number() != number {
			t.Fatalf("ListTasksRequest.%s = %#v, want field %d", field, descriptor, number)
		}
	}
	if messages.ByName("TaskDetails") == nil || messages.ByName("TaskStepDetails") == nil ||
		messages.ByName("TaskExecution") == nil {
		t.Fatal("M4 query response messages not found")
	}
}

func TestM5TaskSubmissionIdempotencyContractIsAdditive(t *testing.T) {
	messages := File_api_proto_dtm_v1_dtm_proto.Messages()
	request := messages.ByName("SubmitTaskRequest")
	key := request.Fields().ByName("idempotency_key")
	if key == nil || key.Number() != 3 || key.Kind() != protoreflect.StringKind {
		t.Fatalf("SubmitTaskRequest.idempotency_key = %#v, want string field 3", key)
	}
	response := messages.ByName("SubmitTaskResponse")
	deduplicated := response.Fields().ByName("deduplicated")
	if deduplicated == nil || deduplicated.Number() != 5 || deduplicated.Kind() != protoreflect.BoolKind {
		t.Fatalf("SubmitTaskResponse.deduplicated = %#v, want bool field 5", deduplicated)
	}
}

func TestM4GenerationPreservesExistingNumbersAndGoPackage(t *testing.T) {
	file := File_api_proto_dtm_v1_dtm_proto
	options, ok := file.Options().(*descriptorpb.FileOptions)
	if !ok || options.GetGoPackage() != "dtm/api/proto/dtm/v1;dtmv1" {
		t.Fatalf("go_package = %#v", file.Options())
	}
	for enumName, values := range map[protoreflect.Name]map[protoreflect.Name]protoreflect.EnumNumber{
		"TaskStatus": {
			"TASK_STATUS_UNSPECIFIED": 0, "TASK_STATUS_CREATED": 1, "TASK_STATUS_PLANNED": 2,
			"TASK_STATUS_MAPPED": 3, "TASK_STATUS_RUNNING": 4, "TASK_STATUS_SUCCEEDED": 5, "TASK_STATUS_FAILED": 6,
		},
		"ExecutionStatus": {
			"EXECUTION_STATUS_UNSPECIFIED": 0, "EXECUTION_STATUS_SUCCEEDED": 1, "EXECUTION_STATUS_FAILED": 2,
		},
	} {
		enum := file.Enums().ByName(enumName)
		if enum == nil {
			t.Fatalf("enum %s not found", enumName)
		}
		for valueName, number := range values {
			value := enum.Values().ByName(valueName)
			if value == nil || value.Number() != number {
				t.Fatalf("%s.%s = %#v, want %d", enumName, valueName, value, number)
			}
		}
	}
	for messageName, fields := range map[protoreflect.Name]map[protoreflect.Name]protoreflect.FieldNumber{
		"Task":              {"id": 1, "intent": 2, "requirements": 3, "constraints": 4},
		"SubmitTaskRequest": {"task": 1, "async": 2},
		"MappedStep":        {"step_id": 1, "capability": 2, "node_id": 3, "inputs": 4},
	} {
		message := file.Messages().ByName(messageName)
		if message == nil {
			t.Fatalf("message %s not found", messageName)
		}
		for fieldName, number := range fields {
			field := message.Fields().ByName(fieldName)
			if field == nil || field.Number() != number {
				t.Fatalf("%s.%s = %#v, want field %d", messageName, fieldName, field, number)
			}
		}
	}
}
