package runtimehost

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"dtm/internal/application"
	"dtm/internal/execution"
	"dtm/internal/invocation"
	"dtm/internal/model"
	"dtm/internal/node"
	"dtm/internal/resourcedirectory"
	"dtm/internal/transport/grpcapi"
)

func TestTaskResourceEvidenceSinkLinksAuthorityFenceEndpointAndExecution(t *testing.T) {
	descriptor, err := model.AdaptLegacyCapability("sensor-1", "temperature_sensor", 7)
	if err != nil {
		t.Fatal(err)
	}
	directory := resourcedirectory.New()
	if err := directory.RestoreRecords([]resourcedirectory.ResourceRecordView{{
		Descriptor: descriptor, NodeGeneration: 3, RegistrationID: "registration-sensor-3",
		PublicationState: resourcedirectory.PublicationStatePublished,
	}}); err != nil {
		t.Fatal(err)
	}
	lifecycle, err := resourcedirectory.NewNodeLifecycleView("sensor-1", 3, "registration-sensor-3", node.StatusActive, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if err := directory.ActivateNodeLifecycle(lifecycle); err != nil {
		t.Fatal(err)
	}
	endpoints := grpcapi.NewEndpointDirectory()
	if err := endpoints.Set("sensor-1", "172.16.42.11:47101"); err != nil {
		t.Fatal(err)
	}
	ref, err := model.NewResourceRef(descriptor.ID, descriptor.Generation, "sensor-1", 3, "registration-sensor-3")
	if err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	sink := newTaskResourceEvidenceSink(&output, directory, endpoints)
	sink.now = func() time.Time { return time.Date(2026, 8, 19, 1, 2, 3, 0, time.UTC) }
	sink.Observe(application.TaskResourceEvidence{
		Stage: "attempt_succeeded", TaskID: "task-1", StepID: "step-1", Capability: "temperature_sensor",
		NodeID: "sensor-1", ResourceRef: ref, Attempt: 1, MappingValidated: true,
		ActualExecutionNode: "sensor-1", ExecutionStatus: execution.StatusSucceeded,
	})
	line := strings.TrimSpace(output.String())
	if !strings.HasPrefix(line, taskResourceEvidencePrefix) {
		t.Fatalf("evidence line = %q", line)
	}
	var envelope taskResourceEvidenceEnvelope
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, taskResourceEvidencePrefix)), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Schema != "dtm.m8.task_resource_evidence.v1" || envelope.TaskID != "task-1" ||
		envelope.ResourceRef.ResourceID != string(descriptor.ID) || envelope.ResourceRef.ResourceGeneration != 7 ||
		envelope.ResourceRef.OwnerNodeGeneration != 3 || envelope.ResourceRef.RegistrationID != "registration-sensor-3" ||
		envelope.Endpoint != "172.16.42.11:47101" || !envelope.EndpointResolved || !envelope.DirectoryMatch ||
		!envelope.Eligible || envelope.PublicationState != "published" || envelope.FenceValidation != "passed" ||
		envelope.ActualExecutionOwner != "sensor-1" || envelope.ExecutionStatus != "succeeded" {
		t.Fatalf("evidence envelope = %+v", envelope)
	}
}

func TestInvocationEvidenceSinkSerializesDecidedFailureMetadata(t *testing.T) {
	ref, err := model.NewResourceRef("resource-r1", 4, "node-r1", 2, "registration-r1")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 8, 20, 2, 3, 4, 0, time.UTC)
	var output bytes.Buffer
	sink := newTaskResourceEvidenceSink(&output, nil, nil)
	sink.now = func() time.Time { return start.Add(time.Hour) }
	id, err := invocation.NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	observation := invocation.InvocationObservation{
		Request: invocation.InvocationRequest{
			InvocationID: id,
			Target:       ref,
			Operation:    "read_temperature",
			Payload:      []byte(`{}`),
			Metadata: invocation.InvocationMetadata{
				TaskID: "task-r1", StepID: "step-r1", Attempt: 1, IdempotencyKey: "key-r1",
			},
		},
		Error: &invocation.InvocationError{Code: invocation.ErrorCodeInvalidRequest, Source: "invocation-service", Classification: "request_validation"},
		Metadata: invocation.ExecutionMetadata{
			CallerFenceValidation:      invocation.FenceValidationNotRun,
			ExecutionFenceValidation:   invocation.FenceValidationNotRun,
			ExecutionFenceMode:         invocation.ExecutionFenceModeNone,
			AcceptedTargetConfirmation: invocation.FenceValidationNotRun,
			Start:                      start,
			Duration:                   5 * time.Millisecond,
			Status:                     invocation.InvocationStatusFailed,
			ErrorCode:                  invocation.ErrorCodeInvalidRequest,
			ErrorSource:                "invocation-service",
			ErrorClassification:        "request_validation",
			RequestBytes:               2,
		},
	}
	if err := sink.ObserveInvocation(nil, observation); err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(output.String())
	var envelope invocationEvidenceEnvelope
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, invocationEvidencePrefix)), &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.Start.Equal(start) || envelope.Duration != 5*time.Millisecond || envelope.Status != invocation.InvocationStatusFailed ||
		envelope.CallerFenceValidation != invocation.FenceValidationNotRun || envelope.ExecutionFenceValidation != invocation.FenceValidationNotRun ||
		envelope.ExecutionFenceMode != invocation.ExecutionFenceModeNone || envelope.AcceptedTargetConfirmation != invocation.FenceValidationNotRun ||
		envelope.Endpoint != "" || envelope.Transport != "" ||
		envelope.ErrorCode != string(invocation.ErrorCodeInvalidRequest) {
		t.Fatalf("failure invocation evidence=%+v", envelope)
	}
}

func TestInvocationEvidenceSinkPreservesOutcomeUnknownExecutionState(t *testing.T) {
	ref, err := model.NewResourceRef("resource-unknown", 1, "node-unknown", 1, "registration-unknown")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 8, 20, 2, 4, 5, 0, time.UTC)
	var output bytes.Buffer
	sink := newTaskResourceEvidenceSink(&output, nil, nil)
	id, err := invocation.NewInvocationID()
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.ObserveInvocation(nil, invocation.InvocationObservation{
		Request: invocation.InvocationRequest{
			InvocationID: id,
			Target:       ref,
			Operation:    "read_temperature",
			Metadata: invocation.InvocationMetadata{
				TaskID: "task-unknown", StepID: "step-unknown", Attempt: 1, IdempotencyKey: "key-unknown",
			},
		},
		Error: &invocation.InvocationError{Code: invocation.ErrorCodeTransportFailure, OutcomeUnknown: true, Source: "legacy-grpc"},
		Metadata: invocation.ExecutionMetadata{
			CallerFenceValidation:      invocation.FenceValidationPassed,
			ExecutionFenceValidation:   invocation.FenceValidationUnconfirmed,
			ExecutionFenceMode:         invocation.ExecutionFenceModeNone,
			AcceptedTargetConfirmation: invocation.FenceValidationUnconfirmed,
			Start:                      start,
			Duration:                   time.Millisecond,
			Status:                     invocation.InvocationStatusFailed,
			ErrorCode:                  invocation.ErrorCodeTransportFailure,
			ErrorSource:                "legacy-grpc",
			OutcomeUnknown:             true,
		},
	}); err != nil {
		t.Fatal(err)
	}
	line := strings.TrimSpace(output.String())
	var envelope invocationEvidenceEnvelope
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, invocationEvidencePrefix)), &envelope); err != nil {
		t.Fatal(err)
	}
	if !envelope.OutcomeUnknown || envelope.ExecutionFenceValidation != invocation.FenceValidationUnconfirmed ||
		envelope.ExecutionFenceMode != invocation.ExecutionFenceModeNone ||
		envelope.AcceptedTargetConfirmation != invocation.FenceValidationUnconfirmed || envelope.CallerFenceValidation != invocation.FenceValidationPassed {
		t.Fatalf("outcome-unknown invocation evidence=%+v", envelope)
	}
}

func TestInvocationEvidenceSinkRejectsAuthorityConfirmedMode(t *testing.T) {
	var output bytes.Buffer
	sink := newTaskResourceEvidenceSink(&output, nil, nil)
	if err := sink.ObserveInvocation(nil, invocation.InvocationObservation{
		Metadata: invocation.ExecutionMetadata{ExecutionFenceMode: invocation.ExecutionFenceModeAuthorityConfirmed},
	}); err == nil {
		t.Fatal("evidence sink accepted authority-confirmed execution fence without an Authority receipt")
	}
	if output.Len() != 0 {
		t.Fatalf("evidence sink wrote an authority-confirmed record: %q", output.String())
	}
}
