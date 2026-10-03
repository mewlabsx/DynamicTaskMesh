package runtimehost

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"dtm/internal/application"
	"dtm/internal/execution"
	"dtm/internal/invocation"
	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
	"dtm/internal/transport/grpcapi"
)

const taskResourceEvidencePrefix = "task_resource_evidence "
const invocationEvidencePrefix = "invocation_evidence "

type taskResourceEvidenceEnvelope struct {
	Schema                  string                  `json:"schema"`
	Timestamp               time.Time               `json:"timestamp"`
	Stage                   string                  `json:"stage"`
	TaskID                  string                  `json:"task_id"`
	StepID                  string                  `json:"step_id"`
	Capability              string                  `json:"capability"`
	MappingSource           string                  `json:"mapping_source"`
	SelectedNodeID          string                  `json:"selected_node_id"`
	ResourceRef             taskResourceRefEnvelope `json:"resource_ref"`
	Endpoint                string                  `json:"endpoint"`
	EndpointResolved        bool                    `json:"endpoint_resolved"`
	PublicationState        string                  `json:"publication_state"`
	Eligible                bool                    `json:"eligible"`
	DirectoryMatch          bool                    `json:"directory_match"`
	FenceValidation         string                  `json:"fence_validation"`
	Attempt                 uint32                  `json:"attempt,omitempty"`
	ActualExecutionOwner    string                  `json:"actual_execution_owner,omitempty"`
	ExecutionStatus         string                  `json:"execution_status,omitempty"`
	Failure                 string                  `json:"failure,omitempty"`
	DirectoryLookupError    string                  `json:"directory_lookup_error,omitempty"`
	EndpointResolutionError string                  `json:"endpoint_resolution_error,omitempty"`
}

type taskResourceRefEnvelope struct {
	ResourceID          string `json:"resource_id"`
	ResourceGeneration  uint64 `json:"resource_generation"`
	OwnerNodeID         string `json:"owner_node_id"`
	OwnerNodeGeneration int64  `json:"owner_node_generation"`
	RegistrationID      string `json:"registration_id"`
}

type invocationEvidenceEnvelope struct {
	Schema                     string                       `json:"schema"`
	Timestamp                  time.Time                    `json:"timestamp"`
	TaskID                     string                       `json:"task_id"`
	StepID                     string                       `json:"step_id"`
	Attempt                    uint32                       `json:"attempt"`
	InvocationID               string                       `json:"invocation_id"`
	ResourceID                 string                       `json:"resource_id"`
	ResourceGeneration         uint64                       `json:"resource_generation"`
	OwnerNodeID                string                       `json:"owner_node_id"`
	OwnerNodeGeneration        int64                        `json:"owner_node_generation"`
	RegistrationID             string                       `json:"registration_id"`
	Operation                  string                       `json:"operation"`
	Transport                  string                       `json:"transport"`
	TransportProfile           string                       `json:"transport_profile"`
	Endpoint                   string                       `json:"endpoint"`
	CallerFenceValidation      string                       `json:"caller_fence_validation"`
	ExecutionFenceValidation   string                       `json:"execution_fence_validation"`
	ExecutionFenceMode         execution.ExecutionFenceMode `json:"execution_fence_mode"`
	AcceptedTargetConfirmation string                       `json:"accepted_target_confirmation"`
	Start                      time.Time                    `json:"start"`
	Duration                   time.Duration                `json:"duration"`
	Status                     string                       `json:"status"`
	ErrorCode                  string                       `json:"error_code,omitempty"`
	ErrorSource                string                       `json:"error_source,omitempty"`
	ErrorClassification        string                       `json:"error_classification,omitempty"`
	OutcomeUnknown             bool                         `json:"outcome_unknown"`
	RequestBytes               int                          `json:"request_bytes"`
	ResponseBytes              int                          `json:"response_bytes"`
}

type taskResourceEvidenceSink struct {
	mu        sync.Mutex
	writer    io.Writer
	resources *resourcedirectory.Directory
	endpoints *grpcapi.EndpointDirectory
	now       func() time.Time
}

func newTaskResourceEvidenceSink(writer io.Writer, resources *resourcedirectory.Directory, endpoints *grpcapi.EndpointDirectory) *taskResourceEvidenceSink {
	return &taskResourceEvidenceSink{writer: writer, resources: resources, endpoints: endpoints, now: time.Now}
}

func (sink *taskResourceEvidenceSink) Observe(evidence application.TaskResourceEvidence) {
	if sink == nil || sink.writer == nil || evidence.ResourceRef == (model.ResourceRef{}) {
		return
	}
	ref := evidence.ResourceRef
	envelope := taskResourceEvidenceEnvelope{
		Schema: "dtm.m8.task_resource_evidence.v1", Timestamp: sink.now().UTC(),
		Stage: evidence.Stage, TaskID: string(evidence.TaskID), StepID: string(evidence.StepID),
		Capability: string(evidence.Capability), MappingSource: "authority_resource_directory",
		SelectedNodeID: string(evidence.NodeID),
		ResourceRef: taskResourceRefEnvelope{
			ResourceID: string(ref.ResourceID), ResourceGeneration: uint64(ref.ResourceGeneration),
			OwnerNodeID: string(ref.OwnerNodeID), OwnerNodeGeneration: ref.OwnerNodeGeneration,
			RegistrationID: ref.RegistrationID,
		},
		FenceValidation: "not_run", Attempt: evidence.Attempt,
		ActualExecutionOwner: string(evidence.ActualExecutionNode), ExecutionStatus: string(evidence.ExecutionStatus),
		Failure: evidence.Failure,
	}
	if evidence.MappingValidated {
		envelope.FenceValidation = "passed"
	}
	if sink.resources != nil {
		view, err := sink.resources.GetByID(ref.ResourceID)
		if err != nil {
			envelope.DirectoryLookupError = err.Error()
		} else {
			envelope.PublicationState = string(view.PublicationState)
			envelope.Eligible = view.Eligible
			envelope.DirectoryMatch = resourceRefMatchesView(ref, view)
		}
	}
	if sink.endpoints != nil {
		endpoint, err := sink.endpoints.Resolve(ref.OwnerNodeID)
		if err != nil {
			envelope.EndpointResolutionError = err.Error()
		} else {
			envelope.Endpoint = endpoint
			envelope.EndpointResolved = true
		}
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	_, _ = fmt.Fprintf(sink.writer, "%s%s\n", taskResourceEvidencePrefix, encoded)
}

// ObserveInvocation records the transport-independent invocation boundary.
// It intentionally omits payloads and full error stacks; the legacy gRPC
// adapter cannot confirm an execution-side ResourceRef, so the evidence keeps
// that distinction explicit.
func (sink *taskResourceEvidenceSink) ObserveInvocation(_ context.Context, observation invocation.InvocationObservation) error {
	if sink == nil || sink.writer == nil {
		return nil
	}
	request := observation.Request
	metadata := observation.Metadata.Clone()
	// Result metadata is an additive compatibility fallback for direct callers
	// that predate InvocationObservation.Metadata. The service supplies
	// observation metadata for every terminal path, including failures.
	if metadata == (invocation.ExecutionMetadata{}) && observation.Result != nil {
		metadata = observation.Result.Metadata.Clone()
	}
	executionFenceMode := metadata.ExecutionFenceMode
	if executionFenceMode == "" {
		executionFenceMode = execution.ExecutionFenceModeNone
	}
	if err := executionFenceMode.Validate(); err != nil {
		return err
	}
	if executionFenceMode == execution.ExecutionFenceModeAuthorityConfirmed {
		return fmt.Errorf("authority-confirmed execution fence requires an Authority receipt")
	}
	envelope := invocationEvidenceEnvelope{
		Schema:                     "dtm.v0.6.invocation_evidence.v1",
		Timestamp:                  sink.now().UTC(),
		TaskID:                     string(request.Metadata.TaskID),
		StepID:                     string(request.Metadata.StepID),
		Attempt:                    request.Metadata.Attempt,
		InvocationID:               request.InvocationID.String(),
		ResourceID:                 string(request.Target.ResourceID),
		ResourceGeneration:         uint64(request.Target.ResourceGeneration),
		OwnerNodeID:                string(request.Target.OwnerNodeID),
		OwnerNodeGeneration:        request.Target.OwnerNodeGeneration,
		RegistrationID:             request.Target.RegistrationID,
		Operation:                  string(request.Operation),
		Transport:                  metadata.Transport,
		TransportProfile:           metadata.TransportProfile,
		Endpoint:                   metadata.Endpoint,
		CallerFenceValidation:      metadata.CallerFenceValidation,
		ExecutionFenceValidation:   metadata.ExecutionFenceValidation,
		ExecutionFenceMode:         executionFenceMode,
		AcceptedTargetConfirmation: metadata.AcceptedTargetConfirmation,
		Start:                      metadata.Start,
		Duration:                   metadata.Duration,
		Status:                     metadata.Status,
		ErrorCode:                  string(metadata.ErrorCode),
		ErrorSource:                metadata.ErrorSource,
		ErrorClassification:        metadata.ErrorClassification,
		OutcomeUnknown:             metadata.OutcomeUnknown,
		RequestBytes:               metadata.RequestBytes,
		ResponseBytes:              metadata.ResponseBytes,
	}
	if observation.Error != nil && envelope.ErrorCode == "" {
		envelope.ErrorCode = string(observation.Error.Code)
		envelope.ErrorSource = observation.Error.Source
		envelope.ErrorClassification = observation.Error.Classification
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	_, _ = fmt.Fprintf(sink.writer, "%s%s\n", invocationEvidencePrefix, encoded)
	return nil
}

func resourceRefMatchesView(ref model.ResourceRef, view resourcedirectory.ResourceRecordView) bool {
	return view.Descriptor.ID == ref.ResourceID &&
		view.Descriptor.Generation == ref.ResourceGeneration &&
		view.Descriptor.OwnerNodeID == ref.OwnerNodeID &&
		view.NodeGeneration == ref.OwnerNodeGeneration &&
		view.RegistrationID == ref.RegistrationID
}
