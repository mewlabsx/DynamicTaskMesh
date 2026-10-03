package ui

import (
	"sort"
	"strings"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Snapshot is the private, read-only JSON model served by dtm-ui. It is not a
// DTM protocol message and intentionally contains only evidence observable by
// the UI adapter.
type Snapshot struct {
	Timestamp              time.Time          `json:"timestamp"`
	Status                 string             `json:"status"`
	Error                  string             `json:"error,omitempty"`
	LastSuccessfulSnapshot *time.Time         `json:"last_successful_snapshot,omitempty"`
	Observer               ObserverView       `json:"observer"`
	TaskQueryError         string             `json:"task_query_error,omitempty"`
	Mesh                   MeshOverview       `json:"mesh"`
	Runtimes               []RuntimeView      `json:"runtimes"`
	Resources              []ResourceView     `json:"resources"`
	Tasks                  []TaskView         `json:"tasks"`
	Events                 []ObservationEvent `json:"events"`
	Lab                    LabView            `json:"lab"`
}

// ObserverView describes the Runtime endpoint used to obtain this snapshot.
// It is deliberately separate from MeshOverview.Coordinator: an observation
// seed is a read path, not a Core or authority selection.
type ObserverView struct {
	Endpoint  string `json:"endpoint,omitempty"`
	NodeID    string `json:"node_id,omitempty"`
	State     string `json:"state"`
	Source    string `json:"source"`
	SeedCount int    `json:"seed_count"`
}

type MeshOverview struct {
	Status           string `json:"status"`
	RuntimeCount     int    `json:"runtime_count"`
	Coordinator      string `json:"coordinator"`
	CoordinatorCount int    `json:"coordinator_count"`
	CoordinatorState string `json:"coordinator_state"`
	CoreState        string `json:"core_state"`
	AuthorityState   string `json:"authority_state"`
	IngressState     string `json:"ingress_state"`
	CoreEndpoint     string `json:"core_endpoint,omitempty"`
}

type RuntimeView struct {
	NodeID                string     `json:"node_id"`
	RuntimeInstanceID     string     `json:"runtime_instance_id"`
	ControlEndpoint       string     `json:"control_endpoint"`
	MembershipState       string     `json:"membership_state"`
	LastSeen              *time.Time `json:"last_seen,omitempty"`
	Coordinator           bool       `json:"coordinator"`
	CoreActive            bool       `json:"core_active"`
	CoreReady             bool       `json:"core_ready"`
	AuthorityReady        bool       `json:"authority_ready"`
	IngressReady          bool       `json:"ingress_ready"`
	LocalAuthorityBinding string     `json:"local_authority_binding"`
	CoreEndpoint          string     `json:"core_endpoint,omitempty"`
	Error                 string     `json:"error,omitempty"`
}

type ResourceView struct {
	ResourceID             string `json:"resource_id"`
	Kind                   string `json:"kind"`
	Capability             string `json:"capability"`
	OwnerNodeID            string `json:"owner_node_id"`
	OwnerRuntimeInstanceID string `json:"owner_runtime_instance_id"`
	Endpoint               string `json:"endpoint,omitempty"`
	Discovered             bool   `json:"discovered"`
	AuthorityBound         bool   `json:"authority_bound"`
	Published              string `json:"published"`
	Eligible               string `json:"eligible"`
	Status                 string `json:"status"`
	NodeGeneration         int64  `json:"node_generation,omitempty"`
	ResourceGeneration     uint64 `json:"resource_generation,omitempty"`
	RegistrationID         string `json:"registration_id,omitempty"`
}

type TaskView struct {
	TaskID      string     `json:"task_id"`
	TaskType    string     `json:"task_type"`
	Status      string     `json:"status"`
	CreatedAt   *time.Time `json:"created_at,omitempty"`
	UpdatedAt   *time.Time `json:"updated_at,omitempty"`
	FailureCode string     `json:"failure_code,omitempty"`
	Failure     string     `json:"failure,omitempty"`
	Steps       []StepView `json:"steps"`
}

type StepView struct {
	StepID             string         `json:"step_id"`
	Sequence           int32          `json:"sequence"`
	Capability         string         `json:"capability"`
	MappedNode         string         `json:"mapped_node"`
	Status             string         `json:"status"`
	MappingSource      string         `json:"mapping_source"`
	ResourceRefHistory string         `json:"resource_ref_history"`
	ExecutionStatus    string         `json:"execution_status,omitempty"`
	Result             map[string]any `json:"result,omitempty"`
	Failure            string         `json:"failure,omitempty"`
}

type ObservationEvent struct {
	Timestamp time.Time `json:"timestamp"`
	Type      string    `json:"type"`
	Message   string    `json:"message"`
}

func taskStatusName(value dtmv1.TaskStatus) string {
	return enumName(value.String(), "TASK_STATUS_")
}

func stepStatusName(value dtmv1.StepStatus) string {
	return enumName(value.String(), "STEP_STATUS_")
}

func executionStatusName(value dtmv1.ExecutionStatus) string {
	return enumName(value.String(), "EXECUTION_STATUS_")
}

func enumName(value, prefix string) string {
	value = strings.TrimPrefix(value, prefix)
	if value == "UNSPECIFIED" {
		return value
	}
	return value
}

func optionalTimestamp(value *timestamppb.Timestamp) *time.Time {
	if value == nil || value.CheckValid() != nil {
		return nil
	}
	converted := value.AsTime().UTC()
	return &converted
}

func sortSnapshot(snapshot *Snapshot) {
	sort.Slice(snapshot.Runtimes, func(left, right int) bool {
		if snapshot.Runtimes[left].NodeID != snapshot.Runtimes[right].NodeID {
			return snapshot.Runtimes[left].NodeID < snapshot.Runtimes[right].NodeID
		}
		return snapshot.Runtimes[left].RuntimeInstanceID < snapshot.Runtimes[right].RuntimeInstanceID
	})
	sort.Slice(snapshot.Resources, func(left, right int) bool {
		return snapshot.Resources[left].ResourceID < snapshot.Resources[right].ResourceID
	})
	sort.Slice(snapshot.Tasks, func(left, right int) bool {
		return snapshot.Tasks[left].TaskID < snapshot.Tasks[right].TaskID
	})
}

func cloneSnapshot(input Snapshot) Snapshot {
	output := input
	output.Runtimes = append([]RuntimeView(nil), input.Runtimes...)
	output.Resources = append([]ResourceView(nil), input.Resources...)
	output.Tasks = make([]TaskView, len(input.Tasks))
	for index, task := range input.Tasks {
		output.Tasks[index] = task
		output.Tasks[index].Steps = make([]StepView, len(task.Steps))
		for stepIndex, step := range task.Steps {
			output.Tasks[index].Steps[stepIndex] = step
			if step.Result != nil {
				output.Tasks[index].Steps[stepIndex].Result = make(map[string]any, len(step.Result))
				for key, value := range step.Result {
					output.Tasks[index].Steps[stepIndex].Result[key] = value
				}
			}
		}
	}
	output.Events = append([]ObservationEvent(nil), input.Events...)
	output.Lab.Runtimes = append([]LabRuntimeView(nil), input.Lab.Runtimes...)
	if input.Lab.LastOperation != nil {
		lastOperation := *input.Lab.LastOperation
		output.Lab.LastOperation = &lastOperation
	}
	if input.LastSuccessfulSnapshot != nil {
		last := *input.LastSuccessfulSnapshot
		output.LastSuccessfulSnapshot = &last
	}
	return output
}
