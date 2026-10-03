package agentexecution

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"time"

	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
)

type Status string

const (
	StatusRunning     Status = "running"
	StatusCompleted   Status = "completed"
	StatusInterrupted Status = "interrupted"
)

var (
	ErrNotFound        = errors.New("agent execution record not found")
	ErrConflict        = errors.New("idempotency key reused for a different step request")
	ErrAlreadyComplete = errors.New("agent execution is already complete")
)

type Fingerprint struct {
	TaskID        model.TaskID       `json:"task_id"`
	StepID        model.StepID       `json:"step_id"`
	Capability    model.Capability   `json:"capability"`
	NodeID        model.NodeID       `json:"node_id"`
	Inputs        map[string]string  `json:"inputs,omitempty"`
	ResourceRef   *model.ResourceRef `json:"resource_ref,omitempty"`
	OperationID   model.OperationID  `json:"operation_id,omitempty"`
	PayloadDigest string             `json:"payload_digest,omitempty"`
}

func NewFingerprint(taskID model.TaskID, step mapper.MappedStep) Fingerprint {
	return Fingerprint{
		TaskID:     taskID,
		StepID:     step.ID,
		Capability: step.Capability,
		NodeID:     step.NodeID,
		Inputs:     cloneInputs(step.Inputs),
	}
}

// NewInvocationFingerprint extends the legacy mapped-step fingerprint with
// the exact ResourceRef, explicit OperationID, and opaque payload digest used
// by the native Invocation RPC. Legacy ExecuteStep callers keep the original
// fingerprint shape for persistent compatibility.
func NewInvocationFingerprint(
	taskID model.TaskID,
	step mapper.MappedStep,
	operation model.OperationID,
	payload []byte,
) Fingerprint {
	fingerprint := NewFingerprint(taskID, step)
	ref := step.ResourceRef
	fingerprint.ResourceRef = &ref
	fingerprint.OperationID = operation
	digest := sha256.Sum256(payload)
	fingerprint.PayloadDigest = hex.EncodeToString(digest[:])
	return fingerprint
}

func (fingerprint Fingerprint) Equal(other Fingerprint) bool {
	return reflect.DeepEqual(fingerprint, other)
}

type Record struct {
	ExecutionID    string
	IdempotencyKey string
	Fingerprint    Fingerprint
	Status         Status
	Result         execution.StepResult
	ExecutionError string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type Repository interface {
	Find(context.Context, string) (Record, error)
	Start(context.Context, string, Fingerprint) error
	Complete(
		context.Context,
		string,
		Fingerprint,
		execution.StepResult,
		string,
	) error
}

type NoopRepository struct{}

func (NoopRepository) Find(context.Context, string) (Record, error) {
	return Record{}, ErrNotFound
}

func (NoopRepository) Start(context.Context, string, Fingerprint) error {
	return nil
}

func (NoopRepository) Complete(
	context.Context,
	string,
	Fingerprint,
	execution.StepResult,
	string,
) error {
	return nil
}

func cloneInputs(input map[string]string) map[string]string {
	if input == nil {
		return nil
	}
	result := make(map[string]string, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}
