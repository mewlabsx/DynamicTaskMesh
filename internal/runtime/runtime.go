package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
)

var (
	ErrInvalidMappedPlan               = errors.New("invalid mapped plan")
	ErrExecutionFailed                 = errors.New("execution failed")
	ErrRetryableExecution              = errors.New("retryable execution failure")
	ErrInvalidRetryPolicy              = errors.New("invalid retry policy")
	ErrInvalidRemappingPolicy          = errors.New("invalid remapping policy")
	ErrInvalidMappingValidator         = mapper.ErrInvalidResourceMappingValidator
	ErrInvalidResourceMapping          = mapper.ErrInvalidResourceMapping
	ErrStaleResourceMapping            = mapper.ErrStaleResourceMapping
	ErrResourceMappingValidationFailed = mapper.ErrResourceMappingValidationFailed
)

type Executor interface {
	Execute(context.Context, model.TaskID, mapper.MappedStep) (execution.StepResult, error)
}

type StepAttempt struct {
	Number         uint32
	IdempotencyKey string
}

type AttemptExecutor interface {
	ExecuteAttempt(
		context.Context,
		model.TaskID,
		mapper.MappedStep,
		StepAttempt,
	) (execution.StepResult, error)
}

type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
}

var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts: 3,
	Backoff:     100 * time.Millisecond,
}

type RemappingPolicy struct {
	MaxRemap             int
	PreferSameCapability bool
}

var DefaultRemappingPolicy = RemappingPolicy{
	MaxRemap:             1,
	PreferSameCapability: true,
}

type Remapper interface {
	Remap(mapper.MappedStep, map[model.NodeID]struct{}) (mapper.MappedStep, error)
}

// MappingValidator is invoked immediately before every remote execution
// attempt. Resource-mode implementations must validate the complete mapping
// and current fence; Legacy runtimes leave this dependency unset.
type MappingValidator interface {
	Validate(mapper.MappedStep) error
}

type ProgressState string

const (
	ProgressRetrying         ProgressState = "retrying"
	ProgressDispatched       ProgressState = "dispatched"
	ProgressRunning          ProgressState = "running"
	ProgressRemapped         ProgressState = "remapped"
	ProgressAttemptStarted   ProgressState = "attempt_started"
	ProgressAttemptSucceeded ProgressState = "attempt_succeeded"
	ProgressAttemptFailed    ProgressState = "attempt_failed"
	ProgressAttemptCanceled  ProgressState = "attempt_canceled"
)

type Progress struct {
	TaskID       model.TaskID
	StepID       model.StepID
	Capability   model.Capability
	State        ProgressState
	NodeID       model.NodeID
	PreviousNode model.NodeID
	ResourceRef  model.ResourceRef
	// MappingValidated is true only after the configured MappingValidator has
	// accepted this exact ResourceRef immediately before an execution attempt.
	MappingValidated bool
	Attempt          uint32
	Remap            int
	Result           *execution.StepResult
	Failure          error
}

type Observer func(context.Context, Progress) error

type Runtime struct {
	executor         Executor
	retryPolicy      RetryPolicy
	remapper         Remapper
	remappingPolicy  RemappingPolicy
	mappingValidator MappingValidator
}

type Option func(*Runtime) error

func WithRetryPolicy(policy RetryPolicy) Option {
	return func(instance *Runtime) error {
		if policy.MaxAttempts < 1 || policy.Backoff < 0 {
			return ErrInvalidRetryPolicy
		}
		instance.retryPolicy = policy
		return nil
	}
}

func WithRemapping(remapper Remapper, policy RemappingPolicy) Option {
	return func(instance *Runtime) error {
		if remapper == nil || isNilRemapper(remapper) || policy.MaxRemap < 0 {
			return ErrInvalidRemappingPolicy
		}
		instance.remapper = remapper
		instance.remappingPolicy = policy
		return nil
	}
}

// WithMappingValidator enables Resource pre-execution validation. A nil or
// typed-nil validator is rejected at construction time.
func WithMappingValidator(validator MappingValidator) Option {
	return func(instance *Runtime) error {
		if validator == nil || isNilMappingValidator(validator) {
			return ErrInvalidMappingValidator
		}
		instance.mappingValidator = validator
		return nil
	}
}

// WithResourceMappingValidator is a descriptive alias for callers composing
// the Resource scheduling path.
func WithResourceMappingValidator(validator MappingValidator) Option {
	return WithMappingValidator(validator)
}

func New(executor Executor, options ...Option) (*Runtime, error) {
	if executor == nil || isNilExecutor(executor) {
		return nil, ErrInvalidMappedPlan
	}
	instance := &Runtime{
		executor:        executor,
		retryPolicy:     DefaultRetryPolicy,
		remappingPolicy: DefaultRemappingPolicy,
	}
	for _, option := range options {
		if option == nil {
			return nil, ErrInvalidRetryPolicy
		}
		if err := option(instance); err != nil {
			return nil, err
		}
	}
	return instance, nil
}

func (runtime *Runtime) MaxExecutionAttempts() int {
	return runtime.retryPolicy.MaxAttempts * (runtime.remappingPolicy.MaxRemap + 1)
}

func (runtime *Runtime) Execute(ctx context.Context, plan mapper.MappedPlan) (execution.Result, error) {
	return runtime.ExecuteWithObserver(ctx, plan, nil)
}

func (runtime *Runtime) ExecuteWithObserver(
	ctx context.Context,
	plan mapper.MappedPlan,
	observer Observer,
) (execution.Result, error) {
	if err := validateMappedPlan(plan); err != nil {
		return execution.Result{}, err
	}

	results := make([]execution.StepResult, 0, len(plan.Steps))
	for _, originalStep := range plan.Steps {
		step := copyMappedStep(originalStep)
		var stepResult execution.StepResult
		var err error
		excluded := make(map[model.NodeID]struct{})
		remaps := 0
		attemptNumber := originalStep.AttemptOffset
		for {
			staleRemapped := false
			for attemptOnNode := 1; attemptOnNode <= runtime.retryPolicy.MaxAttempts; attemptOnNode++ {
				if originalStep.MaxAttempts > 0 && attemptNumber >= originalStep.MaxAttempts {
					break
				}
				mappingValidated := false
				if runtime.mappingValidator != nil {
					validationErr := runtime.mappingValidator.Validate(step)
					if validationErr != nil {
						if !errors.Is(validationErr, ErrStaleResourceMapping) {
							err = validationErr
							break
						}
						if runtime.remapper == nil || remaps >= runtime.remappingPolicy.MaxRemap {
							err = validationErr
							break
						}
						previousNode := step.NodeID
						remapped, remapErr := runtime.remapper.Remap(step, excluded)
						if remapErr != nil {
							err = errors.Join(validationErr, fmt.Errorf("remap stale step %q: %w", step.ID, remapErr))
							break
						}
						remaps++
						step = copyMappedStep(remapped)
						if notifyErr := notify(ctx, observer, Progress{
							TaskID: plan.TaskID, StepID: step.ID, State: ProgressRemapped,
							Capability: step.Capability, NodeID: step.NodeID, PreviousNode: previousNode,
							ResourceRef: step.ResourceRef,
							Attempt:     attemptNumber, Remap: remaps,
						}); notifyErr != nil {
							err = notifyErr
							break
						}
						staleRemapped = true
						break
					}
					mappingValidated = true
				}
				attemptNumber++
				if notifyErr := notify(ctx, observer, Progress{
					TaskID: plan.TaskID, StepID: step.ID, State: ProgressAttemptStarted,
					Capability: step.Capability, NodeID: step.NodeID, ResourceRef: step.ResourceRef,
					MappingValidated: mappingValidated, Attempt: attemptNumber, Remap: remaps,
				}); notifyErr != nil {
					err = notifyErr
					break
				}
				idempotencyKey := IdempotencyKey(plan.TaskID, step.ID)
				if originalStep.AttemptOffset > 0 {
					idempotencyKey = fmt.Sprintf("%s:%d", idempotencyKey, attemptNumber)
				}
				stepResult, err = runtime.executeAttempt(
					ctx,
					plan.TaskID,
					step,
					StepAttempt{
						Number:         attemptNumber,
						IdempotencyKey: idempotencyKey,
					},
				)
				stepResult, err = normalizeAttemptResult(step, stepResult, err)
				completionState := ProgressAttemptSucceeded
				if err != nil {
					completionState = ProgressAttemptFailed
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						completionState = ProgressAttemptCanceled
					}
				}
				progressResult := stepResult
				if notifyErr := notify(ctx, observer, Progress{
					TaskID: plan.TaskID, StepID: step.ID, State: completionState,
					Capability: step.Capability, NodeID: step.NodeID, ResourceRef: step.ResourceRef,
					MappingValidated: mappingValidated, Attempt: attemptNumber, Remap: remaps,
					Result: &progressResult, Failure: err,
				}); notifyErr != nil {
					err = notifyErr
					break
				}
				if err == nil || !errors.Is(err, ErrRetryableExecution) {
					break
				}
				if attemptOnNode == runtime.retryPolicy.MaxAttempts {
					break
				}
				if originalStep.MaxAttempts > 0 && attemptNumber >= originalStep.MaxAttempts {
					break
				}
				if waitErr := waitForRetry(ctx, runtime.retryPolicy.Backoff); waitErr != nil {
					stepResult = execution.StepResult{}
					err = waitErr
					break
				}
				if notifyErr := notify(ctx, observer, Progress{
					TaskID: plan.TaskID, StepID: step.ID, State: ProgressRetrying,
					NodeID: step.NodeID, Attempt: attemptNumber + 1, Remap: remaps,
				}); notifyErr != nil {
					err = notifyErr
					break
				}
				if notifyErr := notify(ctx, observer, Progress{
					TaskID: plan.TaskID, StepID: step.ID, State: ProgressDispatched,
					NodeID: step.NodeID, Attempt: attemptNumber + 1, Remap: remaps,
				}); notifyErr != nil {
					err = notifyErr
					break
				}
				if notifyErr := notify(ctx, observer, Progress{
					TaskID: plan.TaskID, StepID: step.ID, State: ProgressRunning,
					NodeID: step.NodeID, Attempt: attemptNumber + 1, Remap: remaps,
				}); notifyErr != nil {
					err = notifyErr
					break
				}
			}
			if staleRemapped {
				continue
			}
			if err == nil || !errors.Is(err, ErrRetryableExecution) ||
				(originalStep.MaxAttempts > 0 && attemptNumber >= originalStep.MaxAttempts) ||
				runtime.remapper == nil || remaps >= runtime.remappingPolicy.MaxRemap {
				break
			}
			excluded[step.NodeID] = struct{}{}
			previousNode := step.NodeID
			remapped, remapErr := runtime.remapper.Remap(step, excluded)
			if remapErr != nil {
				err = errors.Join(err, fmt.Errorf("remap step %q: %w", step.ID, remapErr))
				break
			}
			remaps++
			step = copyMappedStep(remapped)
			if notifyErr := notify(ctx, observer, Progress{
				TaskID: plan.TaskID, StepID: step.ID, State: ProgressRetrying,
				NodeID: step.NodeID, PreviousNode: previousNode,
				Attempt: attemptNumber + 1, Remap: remaps,
			}); notifyErr != nil {
				err = notifyErr
				break
			}
			if notifyErr := notify(ctx, observer, Progress{
				TaskID: plan.TaskID, StepID: step.ID, State: ProgressRemapped,
				Capability: step.Capability, NodeID: step.NodeID, PreviousNode: previousNode,
				ResourceRef: step.ResourceRef,
				Attempt:     attemptNumber, Remap: remaps,
			}); notifyErr != nil {
				err = notifyErr
				break
			}
			if notifyErr := notify(ctx, observer, Progress{
				TaskID: plan.TaskID, StepID: step.ID, State: ProgressRunning,
				NodeID: step.NodeID, Attempt: attemptNumber + 1, Remap: remaps,
			}); notifyErr != nil {
				err = notifyErr
				break
			}
		}
		if err != nil {
			failed, resultErr := execution.NewStepResult(
				step.ID,
				step.NodeID,
				execution.StatusFailed,
				stepResult.Output,
				err.Error(),
			)
			if resultErr != nil {
				return execution.Result{}, fmt.Errorf("%w: %v", ErrExecutionFailed, resultErr)
			}
			results = append(results, failed)

			result, resultErr := execution.NewResult(
				plan.TaskID,
				execution.StatusFailed,
				results,
				err.Error(),
			)
			if resultErr != nil {
				return execution.Result{}, fmt.Errorf("%w: %v", ErrExecutionFailed, resultErr)
			}
			return result, fmt.Errorf("%w: %w", ErrExecutionFailed, err)
		}
		if stepResult.StepID != step.ID || stepResult.NodeID != step.NodeID {
			identityErr := fmt.Errorf(
				"executor result identity mismatch for step %q on node %q",
				step.ID,
				step.NodeID,
			)
			failed, resultErr := execution.NewStepResult(
				step.ID,
				step.NodeID,
				execution.StatusFailed,
				stepResult.Output,
				identityErr.Error(),
			)
			if resultErr != nil {
				return execution.Result{}, fmt.Errorf("%w: %v", ErrExecutionFailed, resultErr)
			}
			results = append(results, failed)

			result, resultErr := execution.NewResult(
				plan.TaskID,
				execution.StatusFailed,
				results,
				identityErr.Error(),
			)
			if resultErr != nil {
				return execution.Result{}, fmt.Errorf("%w: %v", ErrExecutionFailed, resultErr)
			}
			return result, fmt.Errorf("%w: %w", ErrExecutionFailed, identityErr)
		}
		if stepResult.Status != execution.StatusSucceeded {
			message := stepResult.Error
			if message == "" {
				message = fmt.Sprintf("executor returned non-success status %q", stepResult.Status)
			}
			statusErr := errors.New(message)
			failed, resultErr := execution.NewStepResult(
				step.ID,
				step.NodeID,
				execution.StatusFailed,
				stepResult.Output,
				message,
			)
			if resultErr != nil {
				return execution.Result{}, fmt.Errorf("%w: %v", ErrExecutionFailed, resultErr)
			}
			results = append(results, failed)

			result, resultErr := execution.NewResult(
				plan.TaskID,
				execution.StatusFailed,
				results,
				message,
			)
			if resultErr != nil {
				return execution.Result{}, fmt.Errorf("%w: %v", ErrExecutionFailed, resultErr)
			}
			return result, fmt.Errorf("%w: %w", ErrExecutionFailed, statusErr)
		}
		results = append(results, stepResult)
	}

	result, err := execution.NewResult(plan.TaskID, execution.StatusSucceeded, results, "")
	if err != nil {
		return execution.Result{}, fmt.Errorf("%w: %v", ErrExecutionFailed, err)
	}
	return result, nil
}

func notify(ctx context.Context, observer Observer, progress Progress) error {
	if observer == nil {
		return nil
	}
	if err := observer(ctx, progress); err != nil {
		return fmt.Errorf("observe runtime progress %q: %w", progress.State, err)
	}
	return nil
}

func (runtime *Runtime) executeAttempt(
	ctx context.Context,
	taskID model.TaskID,
	step mapper.MappedStep,
	attempt StepAttempt,
) (execution.StepResult, error) {
	copied := copyMappedStep(step)
	if executor, ok := runtime.executor.(AttemptExecutor); ok {
		return executor.ExecuteAttempt(ctx, taskID, copied, attempt)
	}
	return runtime.executor.Execute(ctx, taskID, copied)
}

func IdempotencyKey(taskID model.TaskID, stepID model.StepID) string {
	return fmt.Sprintf("%d:%s:%d:%s", len(taskID), taskID, len(stepID), stepID)
}

func waitForRetry(ctx context.Context, delay time.Duration) error {
	if delay == 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func copyMappedStep(step mapper.MappedStep) mapper.MappedStep {
	step.Inputs = copyInputs(step.Inputs)
	return step
}

func copyInputs(inputs map[string]string) map[string]string {
	if inputs == nil {
		return nil
	}

	result := make(map[string]string, len(inputs))
	for key, value := range inputs {
		result[key] = value
	}
	return result
}

func validateMappedPlan(plan mapper.MappedPlan) error {
	if strings.TrimSpace(string(plan.TaskID)) == "" || len(plan.Steps) == 0 {
		return ErrInvalidMappedPlan
	}

	seen := make(map[model.StepID]struct{}, len(plan.Steps))
	for _, step := range plan.Steps {
		if strings.TrimSpace(string(step.ID)) == "" ||
			strings.TrimSpace(string(step.Capability)) == "" ||
			strings.TrimSpace(string(step.NodeID)) == "" {
			return ErrInvalidMappedPlan
		}
		if _, exists := seen[step.ID]; exists {
			return ErrInvalidMappedPlan
		}
		seen[step.ID] = struct{}{}
	}
	return nil
}

func isNilExecutor(executor Executor) bool {
	value := reflect.ValueOf(executor)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilRemapper(remapper Remapper) bool {
	value := reflect.ValueOf(remapper)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func isNilMappingValidator(validator MappingValidator) bool {
	value := reflect.ValueOf(validator)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

func normalizeAttemptResult(
	step mapper.MappedStep,
	result execution.StepResult,
	err error,
) (execution.StepResult, error) {
	if err != nil {
		return result, err
	}
	if result.StepID != step.ID || result.NodeID != step.NodeID {
		return result, fmt.Errorf(
			"executor result identity mismatch for step %q on node %q",
			step.ID, step.NodeID,
		)
	}
	if result.Status != execution.StatusSucceeded {
		message := result.Error
		if message == "" {
			message = fmt.Sprintf("executor returned non-success status %q", result.Status)
		}
		return result, errors.New(message)
	}
	return result, nil
}
