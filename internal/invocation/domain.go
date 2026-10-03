package invocation

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"dtm/internal/execution"
	"dtm/internal/model"
)

// Transport identifiers are deliberately small and descriptive. They are
// selected by the Invocation Service after authoritative resolution; they are
// never part of ResourceRef.
const (
	TransportGRPC          = "grpc"
	TransportProfileLegacy = "legacy/compatibility"
	TransportProfileNative = "native/grpc"
	// ExecutionFenceMode is evidence about the execution boundary's actual
	// validation scope. Authority-confirmed is reserved for a future receipt.
	ExecutionFenceModeNone               = execution.ExecutionFenceModeNone
	ExecutionFenceModeLocalValidator     = execution.ExecutionFenceModeLocalValidator
	ExecutionFenceModeAuthorityConfirmed = execution.ExecutionFenceModeAuthorityConfirmed
	// FenceValidationNotRun means that the corresponding fence was not
	// reached. It is deliberately distinct from both a rejected fence and a
	// legacy transport's unconfirmed execution-side result.
	FenceValidationNotRun   = "not_run"
	FenceValidationPassed   = "passed/current"
	FenceValidationRejected = "rejected"
	// FenceValidationConfirmed is retained as a source-compatible alias for
	// callers of the initial M1 implementation. Evidence uses the more
	// precise passed/current value.
	FenceValidationConfirmed   = FenceValidationPassed
	FenceValidationUnconfirmed = "legacy/unconfirmed"

	InvocationStatusSuccess = "success"
	InvocationStatusFailed  = "failed"

	// MaxInvocationTimeout is the M1 compatibility bound. A zero timeout means
	// that the caller context supplies the deadline (the legacy RPC contract did
	// not carry a timeout field).
	MaxInvocationTimeout = 5 * time.Minute
)

type ExecutionFenceMode = execution.ExecutionFenceMode

func (code ErrorCode) Validate() error {
	switch code {
	case ErrorCodeResourceNotFound, ErrorCodeResourceStale, ErrorCodeFenceRejected,
		ErrorCodeUnsupportedOperation, ErrorCodeInvalidRequest, ErrorCodeTransportFailure,
		ErrorCodeTimeout, ErrorCodeCanceled, ErrorCodeExecutionFailure, ErrorCodeProtocolError,
		ErrorCodeIdempotencyConflict:
		return nil
	default:
		return fmt.Errorf("unknown invocation error code %q", code)
	}
}

// InvocationID is an opaque 128-bit identifier. It is represented as raw
// bytes in memory and as 32 lowercase hexadecimal characters in logs.
type InvocationID [16]byte

func NewInvocationID() (InvocationID, error) {
	var id InvocationID
	if _, err := rand.Read(id[:]); err != nil {
		return InvocationID{}, fmt.Errorf("generate invocation id: %w", err)
	}
	if id.IsZero() {
		// crypto/rand returning an all-zero value is extraordinarily unlikely,
		// but the non-zero invariant is explicit and cheap to enforce.
		return InvocationID{}, errors.New("generated zero invocation id")
	}
	return id, nil
}

func ParseInvocationID(value string) (InvocationID, error) {
	if len(value) != hex.EncodedLen(len(InvocationID{})) {
		return InvocationID{}, fmt.Errorf("invalid invocation id: want %d lowercase hex characters", hex.EncodedLen(len(InvocationID{})))
	}
	var id InvocationID
	decoded, err := hex.Decode(id[:], []byte(value))
	if err != nil || decoded != len(id) || strings.ToLower(value) != value {
		return InvocationID{}, errors.New("invalid invocation id encoding")
	}
	if id.IsZero() {
		return InvocationID{}, errors.New("invocation id must be non-zero")
	}
	return id, nil
}

func (id InvocationID) IsZero() bool {
	var zero InvocationID
	return id == zero
}

func (id InvocationID) Validate() error {
	if id.IsZero() {
		return errors.New("invocation id must be non-zero")
	}
	return nil
}

func (id InvocationID) Bytes() []byte {
	if id.IsZero() {
		return nil
	}
	return append([]byte(nil), id[:]...)
}

func (id InvocationID) String() string {
	if id.IsZero() {
		return ""
	}
	return hex.EncodeToString(id[:])
}

type InvocationMetadata struct {
	TaskID         model.TaskID
	StepID         model.StepID
	Attempt        uint32
	IdempotencyKey string
	Timeout        time.Duration
}

func (metadata InvocationMetadata) Validate() error {
	if strings.TrimSpace(string(metadata.TaskID)) == "" {
		return errors.New("task id is required")
	}
	if strings.TrimSpace(string(metadata.StepID)) == "" {
		return errors.New("step id is required")
	}
	if metadata.Attempt == 0 {
		return errors.New("attempt must be positive")
	}
	if strings.TrimSpace(metadata.IdempotencyKey) == "" {
		return errors.New("idempotency key is required")
	}
	if !utf8.ValidString(metadata.IdempotencyKey) || strings.TrimSpace(metadata.IdempotencyKey) != metadata.IdempotencyKey || strings.ContainsRune(metadata.IdempotencyKey, '\x00') {
		return errors.New("idempotency key is not well formed")
	}
	if metadata.Timeout < 0 || metadata.Timeout > MaxInvocationTimeout {
		return fmt.Errorf("timeout must be between 0 and %s", MaxInvocationTimeout)
	}
	return nil
}

type InvocationRequest struct {
	InvocationID InvocationID
	Target       model.ResourceRef
	Operation    model.OperationID
	Payload      []byte
	Metadata     InvocationMetadata
}

func (request InvocationRequest) Validate() error {
	if err := request.InvocationID.Validate(); err != nil {
		return fmt.Errorf("invalid invocation request: %w", err)
	}
	if err := request.Target.Validate(); err != nil {
		return fmt.Errorf("invalid invocation request target: %w", err)
	}
	if err := request.Operation.Validate(); err != nil {
		return fmt.Errorf("invalid invocation request operation: %w", err)
	}
	if err := request.Metadata.Validate(); err != nil {
		return fmt.Errorf("invalid invocation request metadata: %w", err)
	}
	return nil
}

// ExecutionMetadata contains only evidence available at the invocation
// boundary. ExecutionFenceMode describes whether the execution boundary did
// no validation or only local validation; it is not an Authority receipt.
// In particular, legacy ExecuteStep cannot return an execution-side ResourceRef
// receipt, so AcceptedTarget remains nil and its confirmation is explicitly
// unconfirmed.
type ExecutionMetadata struct {
	Transport                  string
	TransportProfile           string
	Endpoint                   string
	CallerFenceValidation      string
	ExecutionFenceValidation   string
	ExecutionFenceMode         execution.ExecutionFenceMode
	AcceptedTarget             *model.ResourceRef
	AcceptedTargetConfirmation string
	Start                      time.Time
	Duration                   time.Duration
	Status                     string
	ErrorCode                  ErrorCode
	ErrorSource                string
	ErrorClassification        string
	OutcomeUnknown             bool
	RequestBytes               int
	ResponseBytes              int
}

func (metadata ExecutionMetadata) Clone() ExecutionMetadata {
	clone := metadata
	if metadata.AcceptedTarget != nil {
		ref := *metadata.AcceptedTarget
		clone.AcceptedTarget = &ref
	}
	return clone
}

type InvocationResult struct {
	InvocationID InvocationID
	Payload      []byte
	Metadata     ExecutionMetadata
}

func (result InvocationResult) Validate() error {
	if err := result.InvocationID.Validate(); err != nil {
		return fmt.Errorf("invalid invocation result: %w", err)
	}
	if err := result.Metadata.ExecutionFenceMode.Validate(); err != nil {
		return fmt.Errorf("invalid invocation result execution fence mode: %w", err)
	}
	if result.Metadata.ExecutionFenceMode == execution.ExecutionFenceModeAuthorityConfirmed {
		return errors.New("authority-confirmed execution fence requires an Authority receipt")
	}
	if result.Metadata.AcceptedTarget != nil {
		if err := result.Metadata.AcceptedTarget.Validate(); err != nil {
			return fmt.Errorf("invalid invocation result accepted target: %w", err)
		}
		if result.Metadata.AcceptedTargetConfirmation == "" || result.Metadata.AcceptedTargetConfirmation == FenceValidationUnconfirmed {
			return errors.New("accepted target requires an explicit confirmation")
		}
	}
	if result.Metadata.Status != "" && result.Metadata.Status != InvocationStatusSuccess && result.Metadata.Status != "succeeded" {
		return fmt.Errorf("invocation result status %q is not success", result.Metadata.Status)
	}
	return nil
}

type ErrorCode string

const (
	ErrorCodeResourceNotFound     ErrorCode = "RESOURCE_NOT_FOUND"
	ErrorCodeResourceStale        ErrorCode = "RESOURCE_STALE"
	ErrorCodeFenceRejected        ErrorCode = "FENCE_REJECTED"
	ErrorCodeUnsupportedOperation ErrorCode = "UNSUPPORTED_OPERATION"
	ErrorCodeInvalidRequest       ErrorCode = "INVALID_REQUEST"
	ErrorCodeTransportFailure     ErrorCode = "TRANSPORT_FAILURE"
	ErrorCodeTimeout              ErrorCode = "TIMEOUT"
	ErrorCodeCanceled             ErrorCode = "CANCELED"
	ErrorCodeExecutionFailure     ErrorCode = "EXECUTION_FAILURE"
	ErrorCodeProtocolError        ErrorCode = "PROTOCOL_ERROR"
	ErrorCodeIdempotencyConflict  ErrorCode = "IDEMPOTENCY_CONFLICT"
)

// Uppercase aliases mirror the frozen wire taxonomy and make tests/logging
// readable without forcing callers to know the Go naming convention.
const (
	RESOURCE_NOT_FOUND    = ErrorCodeResourceNotFound
	RESOURCE_STALE        = ErrorCodeResourceStale
	FENCE_REJECTED        = ErrorCodeFenceRejected
	UNSUPPORTED_OPERATION = ErrorCodeUnsupportedOperation
	INVALID_REQUEST       = ErrorCodeInvalidRequest
	TRANSPORT_FAILURE     = ErrorCodeTransportFailure
	TIMEOUT               = ErrorCodeTimeout
	CANCELED              = ErrorCodeCanceled
	EXECUTION_FAILURE     = ErrorCodeExecutionFailure
	PROTOCOL_ERROR        = ErrorCodeProtocolError
	IDEMPOTENCY_CONFLICT  = ErrorCodeIdempotencyConflict
)

// InvocationError is failure-only. A successful invocation is represented by
// InvocationResult and never by an InvocationError carrying a success status.
type InvocationError struct {
	Code               ErrorCode
	Message            string
	Cause              error
	Retryable          bool
	OutcomeUnknown     bool
	DispatchState      DispatchState
	Source             string
	Classification     string
	ExecutionFenceMode execution.ExecutionFenceMode
}

// DispatchState is the smallest transport fact needed to avoid claiming that
// a legacy execution fence definitely did not run. Empty means that the
// transport did not provide a certainty receipt; callers must then use the
// conservative may-have-dispatched interpretation.
type DispatchState string

const (
	DispatchStateNotDispatched         DispatchState = "not_dispatched"
	DispatchStateMayHaveDispatched     DispatchState = "may_have_dispatched"
	DispatchStateRemoteOutcomeReceived DispatchState = "remote_outcome_received"
)

func (state DispatchState) Validate() error {
	if state == "" {
		return nil
	}
	switch state {
	case DispatchStateNotDispatched, DispatchStateMayHaveDispatched, DispatchStateRemoteOutcomeReceived:
		return nil
	default:
		return fmt.Errorf("unknown dispatch state %q", state)
	}
}

func (err *InvocationError) Error() string {
	if err == nil {
		return "<nil>"
	}
	message := err.Message
	if message == "" {
		message = string(err.Code)
	}
	if err.Code == "" {
		message = err.Message
	}
	if err.Source != "" {
		message = fmt.Sprintf("%s (source=%s)", message, err.Source)
	}
	return message
}

func (err *InvocationError) Validate() error {
	if err == nil {
		return errors.New("nil invocation error")
	}
	if err.DispatchState.Validate() != nil {
		return err.DispatchState.Validate()
	}
	if err.ExecutionFenceMode.Validate() != nil {
		return err.ExecutionFenceMode.Validate()
	}
	if err.ExecutionFenceMode == execution.ExecutionFenceModeAuthorityConfirmed {
		return errors.New("authority-confirmed execution fence requires an Authority receipt")
	}
	return err.Code.Validate()
}

func (err *InvocationError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Cause
}

func (err *InvocationError) Is(target error) bool {
	other, ok := target.(*InvocationError)
	return ok && err != nil && other != nil && err.Code == other.Code
}

func NewInvocationError(code ErrorCode, message string, cause error) *InvocationError {
	return &InvocationError{Code: code, Message: message, Cause: cause}
}

type ResourceEndpoint struct {
	TransportID string
	Address     string
}

func (endpoint ResourceEndpoint) Validate() error {
	if strings.TrimSpace(endpoint.TransportID) == "" {
		return errors.New("endpoint transport is required")
	}
	if strings.TrimSpace(endpoint.Address) == "" {
		return errors.New("endpoint address is required")
	}
	return nil
}

type ResolvedInvocationTarget struct {
	ResourceRef model.ResourceRef
	Operation   model.OperationID
	Endpoint    ResourceEndpoint
}

func (target ResolvedInvocationTarget) Validate() error {
	if err := target.ResourceRef.Validate(); err != nil {
		return fmt.Errorf("invalid resolved target resource ref: %w", err)
	}
	if err := target.Operation.Validate(); err != nil {
		return fmt.Errorf("invalid resolved target operation: %w", err)
	}
	if err := target.Endpoint.Validate(); err != nil {
		return fmt.Errorf("invalid resolved target endpoint: %w", err)
	}
	return nil
}

func (target ResolvedInvocationTarget) Clone() ResolvedInvocationTarget {
	return target
}

type Transport interface {
	Invoke(context.Context, ResolvedInvocationTarget, InvocationRequest) (InvocationResult, error)
}
