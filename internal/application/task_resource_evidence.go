package application

import (
	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
	meshruntime "dtm/internal/runtime"
)

// TaskResourceEvidence is an observation of the exact ResourceRef selected by
// Mapper and carried through pre-execution fence validation and execution. It
// does not introduce a second scheduling or fencing model.
type TaskResourceEvidence struct {
	Stage               string
	TaskID              model.TaskID
	StepID              model.StepID
	Capability          model.Capability
	NodeID              model.NodeID
	ResourceRef         model.ResourceRef
	Attempt             uint32
	MappingValidated    bool
	ActualExecutionNode model.NodeID
	ExecutionStatus     execution.Status
	Failure             string
}

type TaskResourceEvidenceObserver func(TaskResourceEvidence)

func selectedResourceEvidence(taskID model.TaskID, step mapper.MappedStep) TaskResourceEvidence {
	return TaskResourceEvidence{
		Stage: "selected", TaskID: taskID, StepID: step.ID,
		Capability: step.Capability, NodeID: step.NodeID, ResourceRef: step.ResourceRef,
	}
}

func progressResourceEvidence(progress meshruntime.Progress) TaskResourceEvidence {
	evidence := TaskResourceEvidence{
		Stage: string(progress.State), TaskID: progress.TaskID, StepID: progress.StepID,
		Capability: progress.Capability, NodeID: progress.NodeID, ResourceRef: progress.ResourceRef,
		Attempt: progress.Attempt, MappingValidated: progress.MappingValidated,
	}
	if progress.Result != nil {
		evidence.ActualExecutionNode = progress.Result.NodeID
		evidence.ExecutionStatus = progress.Result.Status
	}
	if progress.Failure != nil {
		evidence.Failure = progress.Failure.Error()
	}
	return evidence
}

func resourceEvidenceDetail(evidence TaskResourceEvidence) map[string]any {
	if evidence.ResourceRef == (model.ResourceRef{}) {
		return nil
	}
	validation := "not_run"
	if evidence.MappingValidated {
		validation = "passed"
	}
	detail := map[string]any{
		"evidence_schema":  "dtm.task_resource.v1",
		"stage":            evidence.Stage,
		"mapping_source":   "authority_resource_directory",
		"capability":       string(evidence.Capability),
		"node_id":          string(evidence.NodeID),
		"fence_validation": validation,
		"resource_ref": map[string]any{
			"resource_id":           string(evidence.ResourceRef.ResourceID),
			"resource_generation":   uint64(evidence.ResourceRef.ResourceGeneration),
			"owner_node_id":         string(evidence.ResourceRef.OwnerNodeID),
			"owner_node_generation": evidence.ResourceRef.OwnerNodeGeneration,
			"registration_id":       evidence.ResourceRef.RegistrationID,
		},
	}
	if evidence.Attempt > 0 {
		detail["attempt"] = evidence.Attempt
	}
	if evidence.ActualExecutionNode != "" {
		detail["actual_execution_node"] = string(evidence.ActualExecutionNode)
	}
	if evidence.ExecutionStatus != "" {
		detail["execution_status"] = string(evidence.ExecutionStatus)
	}
	if evidence.Failure != "" {
		detail["failure"] = evidence.Failure
	}
	return detail
}
