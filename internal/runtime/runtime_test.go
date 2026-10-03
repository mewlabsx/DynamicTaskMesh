package runtime

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"dtm/internal/execution"
	"dtm/internal/mapper"
	"dtm/internal/model"
)

type executorFunc func(context.Context, mapper.MappedStep) (execution.StepResult, error)

func (function executorFunc) Execute(
	ctx context.Context,
	_ model.TaskID,
	step mapper.MappedStep,
) (execution.StepResult, error) {
	return function(ctx, step)
}

type taskIDExecutorFunc func(context.Context, model.TaskID, mapper.MappedStep) (execution.StepResult, error)

func (function taskIDExecutorFunc) Execute(
	ctx context.Context,
	taskID model.TaskID,
	step mapper.MappedStep,
) (execution.StepResult, error) {
	return function(ctx, taskID, step)
}

type attemptExecutorFunc func(
	context.Context,
	model.TaskID,
	mapper.MappedStep,
	StepAttempt,
) (execution.StepResult, error)

func (function attemptExecutorFunc) Execute(
	ctx context.Context,
	taskID model.TaskID,
	step mapper.MappedStep,
) (execution.StepResult, error) {
	return function(ctx, taskID, step, StepAttempt{
		Number:         1,
		IdempotencyKey: IdempotencyKey(taskID, step.ID),
	})
}

type remapperFunc func(mapper.MappedStep, map[model.NodeID]struct{}) (mapper.MappedStep, error)

func (function remapperFunc) Remap(
	step mapper.MappedStep,
	excluded map[model.NodeID]struct{},
) (mapper.MappedStep, error) {
	return function(step, excluded)
}

func (function attemptExecutorFunc) ExecuteAttempt(
	ctx context.Context,
	taskID model.TaskID,
	step mapper.MappedStep,
	attempt StepAttempt,
) (execution.StepResult, error) {
	return function(ctx, taskID, step, attempt)
}

func TestExecuteForwardsMappedPlanTaskID(t *testing.T) {
	plan := mappedPlan("task-1", validStep("step-1"))
	instance, err := New(taskIDExecutorFunc(func(
		_ context.Context,
		taskID model.TaskID,
		step mapper.MappedStep,
	) (execution.StepResult, error) {
		if taskID != plan.TaskID {
			t.Fatalf("executor task ID = %q, want %q", taskID, plan.TaskID)
		}
		return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, nil, "")
	}))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := instance.Execute(context.Background(), plan); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
}

func TestNewRejectsNilExecutor(t *testing.T) {
	var typedNil executorFunc
	for _, executor := range []Executor{nil, typedNil} {
		_, err := New(executor)
		if !errors.Is(err, ErrInvalidMappedPlan) {
			t.Fatalf("New(%v) error = %v, want ErrInvalidMappedPlan", executor, err)
		}
	}
}

func TestNewRejectsInvalidRetryPolicy(t *testing.T) {
	executor := executorFunc(func(context.Context, mapper.MappedStep) (execution.StepResult, error) {
		return execution.StepResult{}, nil
	})
	for _, policy := range []RetryPolicy{
		{MaxAttempts: 0},
		{MaxAttempts: 1, Backoff: -time.Millisecond},
	} {
		if _, err := New(executor, WithRetryPolicy(policy)); !errors.Is(err, ErrInvalidRetryPolicy) {
			t.Fatalf("New() policy %+v error = %v", policy, err)
		}
	}
}

func TestExecuteRetriesMarkedFailuresWithStableIdempotencyKey(t *testing.T) {
	plan := mappedPlan("task-1", validStep("step-1"))
	var attempts []StepAttempt
	instance, err := New(
		attemptExecutorFunc(func(
			_ context.Context,
			_ model.TaskID,
			step mapper.MappedStep,
			attempt StepAttempt,
		) (execution.StepResult, error) {
			attempts = append(attempts, attempt)
			if attempt.Number < 3 {
				return execution.StepResult{}, fmt.Errorf("%w: unavailable", ErrRetryableExecution)
			}
			return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, nil, "")
		}),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 3}),
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := instance.Execute(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != execution.StatusSucceeded || len(attempts) != 3 {
		t.Fatalf("Execute() = %#v, attempts = %#v", result, attempts)
	}
	wantKey := IdempotencyKey(plan.TaskID, plan.Steps[0].ID)
	for index, attempt := range attempts {
		if attempt.Number != uint32(index+1) || attempt.IdempotencyKey != wantKey {
			t.Fatalf("attempt[%d] = %#v, want number %d and key %q", index, attempt, index+1, wantKey)
		}
	}
}

func TestExecuteRecoveryOffsetUsesNewAttemptAndRequestIdentity(t *testing.T) {
	step := validStep("step-1")
	step.AttemptOffset = 1
	plan := mappedPlan("task-recovery", step)
	var got StepAttempt
	instance, err := New(attemptExecutorFunc(func(_ context.Context, _ model.TaskID, step mapper.MappedStep, attempt StepAttempt) (execution.StepResult, error) {
		got = attempt
		return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, nil, "")
	}), WithRetryPolicy(RetryPolicy{MaxAttempts: 1}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := instance.Execute(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	oldRequestID := IdempotencyKey(plan.TaskID, step.ID)
	if got.Number != 2 || got.IdempotencyKey == oldRequestID || got.IdempotencyKey != oldRequestID+":2" {
		t.Fatalf("recovery attempt = %#v, old request ID = %q", got, oldRequestID)
	}
}

func TestExecuteRecoveryNeverExceedsPersistedAttemptBudget(t *testing.T) {
	step := validStep("step-1")
	step.AttemptOffset = 1
	step.MaxAttempts = 2
	plan := mappedPlan("task-recovery-budget", step)
	var attempts []uint32
	instance, err := New(attemptExecutorFunc(func(_ context.Context, _ model.TaskID, _ mapper.MappedStep, attempt StepAttempt) (execution.StepResult, error) {
		attempts = append(attempts, attempt.Number)
		return execution.StepResult{}, fmt.Errorf("%w: still unavailable", ErrRetryableExecution)
	}), WithRetryPolicy(RetryPolicy{MaxAttempts: 3}))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := instance.Execute(context.Background(), plan); !errors.Is(err, ErrRetryableExecution) {
		t.Fatalf("Execute() error = %v", err)
	}
	if !reflect.DeepEqual(attempts, []uint32{2}) {
		t.Fatalf("absolute recovery attempts = %v, want [2]", attempts)
	}
}

func TestExecuteStopsAfterRetryBudgetIsExhausted(t *testing.T) {
	plan := mappedPlan("task-1", validStep("step-1"))
	calls := 0
	instance, err := New(
		attemptExecutorFunc(func(
			context.Context,
			model.TaskID,
			mapper.MappedStep,
			StepAttempt,
		) (execution.StepResult, error) {
			calls++
			return execution.StepResult{}, fmt.Errorf("%w: unavailable", ErrRetryableExecution)
		}),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 2}),
	)
	if err != nil {
		t.Fatal(err)
	}

	result, err := instance.Execute(context.Background(), plan)
	if !errors.Is(err, ErrExecutionFailed) || !errors.Is(err, ErrRetryableExecution) {
		t.Fatalf("Execute() error = %v", err)
	}
	if calls != 2 || result.Status != execution.StatusFailed {
		t.Fatalf("calls = %d, result = %#v", calls, result)
	}
}

func TestExecuteRemapsAfterRetryBudgetAndReportsProgress(t *testing.T) {
	plan := mappedPlan("task-1", mapper.MappedStep{
		ID: "step-1", Capability: "cooling", NodeID: "node-a",
	})
	var nodes []model.NodeID
	instance, err := New(
		attemptExecutorFunc(func(
			_ context.Context,
			_ model.TaskID,
			step mapper.MappedStep,
			_ StepAttempt,
		) (execution.StepResult, error) {
			nodes = append(nodes, step.NodeID)
			if step.NodeID == "node-a" {
				return execution.StepResult{}, fmt.Errorf("%w: node lost", ErrRetryableExecution)
			}
			return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, nil, "")
		}),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 2}),
		WithRemapping(remapperFunc(func(
			step mapper.MappedStep,
			excluded map[model.NodeID]struct{},
		) (mapper.MappedStep, error) {
			if _, ok := excluded["node-a"]; !ok {
				t.Fatal("failed node was not excluded")
			}
			step.NodeID = "node-b"
			return step, nil
		}), RemappingPolicy{MaxRemap: 1, PreferSameCapability: true}),
	)
	if err != nil {
		t.Fatal(err)
	}
	var progress []Progress
	result, err := instance.ExecuteWithObserver(
		context.Background(),
		plan,
		func(_ context.Context, event Progress) error {
			progress = append(progress, event)
			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(nodes, []model.NodeID{"node-a", "node-a", "node-b"}) {
		t.Fatalf("execution nodes = %v", nodes)
	}
	if result.Status != execution.StatusSucceeded ||
		result.StepResults[0].NodeID != "node-b" {
		t.Fatalf("result = %#v", result)
	}
	var states []ProgressState
	for _, event := range progress {
		states = append(states, event.State)
	}
	wantStates := []ProgressState{
		ProgressAttemptStarted, ProgressAttemptFailed,
		ProgressRetrying, ProgressDispatched, ProgressRunning,
		ProgressAttemptStarted, ProgressAttemptFailed,
		ProgressRetrying, ProgressRemapped, ProgressRunning,
		ProgressAttemptStarted, ProgressAttemptSucceeded,
	}
	if !reflect.DeepEqual(states, wantStates) {
		t.Fatalf("progress states = %v, want %v", states, wantStates)
	}
}

func TestRetryingIsNotReportedWhenNoNextAttemptExists(t *testing.T) {
	instance, err := New(
		executorFunc(func(context.Context, mapper.MappedStep) (execution.StepResult, error) {
			return execution.StepResult{}, fmt.Errorf("%w: final failure", ErrRetryableExecution)
		}),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 1}),
	)
	if err != nil {
		t.Fatal(err)
	}
	var states []ProgressState
	_, _ = instance.ExecuteWithObserver(context.Background(), mappedPlan("task-final", validStep("step-1")), func(_ context.Context, progress Progress) error {
		states = append(states, progress.State)
		return nil
	})
	for _, state := range states {
		if state == ProgressRetrying {
			t.Fatalf("final failure reported retrying: %v", states)
		}
	}
}

func TestExecuteStopsWhenRemapBudgetIsExhausted(t *testing.T) {
	plan := mappedPlan("task-1", validStep("step-1"))
	remaps := 0
	instance, err := New(
		executorFunc(func(context.Context, mapper.MappedStep) (execution.StepResult, error) {
			return execution.StepResult{}, fmt.Errorf("%w: unavailable", ErrRetryableExecution)
		}),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 1}),
		WithRemapping(remapperFunc(func(
			step mapper.MappedStep,
			_ map[model.NodeID]struct{},
		) (mapper.MappedStep, error) {
			remaps++
			step.NodeID = model.NodeID(fmt.Sprintf("node-remap-%d", remaps))
			return step, nil
		}), RemappingPolicy{MaxRemap: 1}),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, err = instance.Execute(context.Background(), plan)
	if !errors.Is(err, ErrRetryableExecution) || remaps != 1 {
		t.Fatalf("Execute() error = %v, remaps = %d", err, remaps)
	}
}

func TestExecuteCancelsRetryBackoff(t *testing.T) {
	plan := mappedPlan("task-1", validStep("step-1"))
	called := make(chan struct{})
	calls := 0
	instance, err := New(
		attemptExecutorFunc(func(
			context.Context,
			model.TaskID,
			mapper.MappedStep,
			StepAttempt,
		) (execution.StepResult, error) {
			calls++
			close(called)
			return execution.StepResult{}, fmt.Errorf("%w: unavailable", ErrRetryableExecution)
		}),
		WithRetryPolicy(RetryPolicy{MaxAttempts: 3, Backoff: time.Hour}),
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := instance.Execute(ctx, plan)
		done <- err
	}()
	<-called
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Execute() error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Execute() did not stop during retry backoff")
	}
	if calls != 1 {
		t.Fatalf("executor calls = %d, want 1", calls)
	}
}

func TestExecuteRejectsInvalidMappedPlanBeforeDispatch(t *testing.T) {
	tests := []struct {
		name string
		plan mapper.MappedPlan
	}{
		{name: "blank task ID", plan: mappedPlan(" ", validStep("step-1"))},
		{name: "no steps", plan: mapper.MappedPlan{TaskID: "task-1"}},
		{name: "blank step ID", plan: mappedPlan("task-1", validStep(" "))},
		{
			name: "duplicate step ID",
			plan: mappedPlan("task-1", validStep("step-1"), validStep("step-1")),
		},
		{
			name: "blank capability",
			plan: mappedPlan("task-1", mapper.MappedStep{ID: "step-1", Capability: " ", NodeID: "node-1"}),
		},
		{
			name: "blank node ID",
			plan: mappedPlan("task-1", mapper.MappedStep{ID: "step-1", Capability: "cooling", NodeID: " "}),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			instance, _ := New(executorFunc(func(context.Context, mapper.MappedStep) (execution.StepResult, error) {
				calls++
				return execution.StepResult{}, nil
			}))

			_, err := instance.Execute(context.Background(), test.plan)

			if !errors.Is(err, ErrInvalidMappedPlan) {
				t.Fatalf("Execute() error = %v, want ErrInvalidMappedPlan", err)
			}
			if calls != 0 {
				t.Fatalf("executor calls = %d, want 0", calls)
			}
		})
	}
}

func TestExecuteRunsStepsSequentiallyAndForwardsContext(t *testing.T) {
	type contextKey string
	ctx := context.WithValue(context.Background(), contextKey("trace"), "trace-1")
	plan := mappedPlan("task-1", validStep("step-1"), validStep("step-2"))
	var calls []model.StepID

	instance, _ := New(executorFunc(func(received context.Context, step mapper.MappedStep) (execution.StepResult, error) {
		if received != ctx {
			t.Fatal("executor received a different context")
		}
		calls = append(calls, step.ID)
		return execution.NewStepResult(
			step.ID,
			step.NodeID,
			execution.StatusSucceeded,
			map[string]any{"step": string(step.ID)},
			"",
		)
	}))

	result, err := instance.Execute(ctx, plan)
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !reflect.DeepEqual(calls, []model.StepID{"step-1", "step-2"}) {
		t.Fatalf("executor calls = %v, want [step-1 step-2]", calls)
	}
	if result.Status != execution.StatusSucceeded {
		t.Fatalf("result.Status = %q, want %q", result.Status, execution.StatusSucceeded)
	}
	if got := result.StepResults[0].Output; !reflect.DeepEqual(got, map[string]any{"step": "step-1"}) {
		t.Fatalf("first output = %v, want executor output only", got)
	}
}

func TestExecuteCopiesInputsBeforeDispatch(t *testing.T) {
	plan := mappedPlan(
		"task-1",
		mapper.MappedStep{
			ID:         "step-1",
			Capability: "temperature_sensor",
			NodeID:     "sensor-node-1",
			Inputs:     map[string]string{"operation": "read_temperature"},
		},
	)
	instance, _ := New(executorFunc(func(_ context.Context, step mapper.MappedStep) (execution.StepResult, error) {
		want := map[string]string{"operation": "read_temperature"}
		if !reflect.DeepEqual(step.Inputs, want) {
			t.Fatalf("executor inputs = %v, want %v", step.Inputs, want)
		}
		step.Inputs["operation"] = "tampered"
		return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, nil, "")
	}))

	if _, err := instance.Execute(context.Background(), plan); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got := plan.Steps[0].Inputs["operation"]; got != "read_temperature" {
		t.Fatalf("mapped plan input after execution = %q, want %q", got, "read_temperature")
	}
}

func TestExecuteStopsAndReturnsNormalizedFailure(t *testing.T) {
	executorError := errors.New("cooling actuator unavailable")
	plan := mappedPlan(
		"task-1",
		validStep("step-1"),
		validStep("step-2"),
		validStep("step-3"),
	)
	calls := 0
	instance, _ := New(executorFunc(func(_ context.Context, step mapper.MappedStep) (execution.StepResult, error) {
		calls++
		if step.ID == "step-2" {
			return execution.StepResult{
				StepID: "wrong",
				NodeID: "wrong",
				Status: "unknown",
				Output: map[string]any{"partial": true},
			}, executorError
		}
		return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, nil, "")
	}))

	result, err := instance.Execute(context.Background(), plan)

	if !errors.Is(err, ErrExecutionFailed) {
		t.Fatalf("Execute() error = %v, want ErrExecutionFailed", err)
	}
	if calls != 2 {
		t.Fatalf("executor calls = %d, want 2", calls)
	}
	if result.Status != execution.StatusFailed || result.Error != executorError.Error() {
		t.Fatalf("result = %+v, want failed result with executor error", result)
	}
	if len(result.StepResults) != 2 {
		t.Fatalf("step result count = %d, want 2", len(result.StepResults))
	}
	failed := result.StepResults[1]
	if failed.StepID != "step-2" || failed.NodeID != "node-step-2" ||
		failed.Status != execution.StatusFailed || failed.Error != executorError.Error() {
		t.Fatalf("failed step = %+v, want normalized step-2 failure", failed)
	}
	if !reflect.DeepEqual(failed.Output, map[string]any{"partial": true}) {
		t.Fatalf("failed output = %v, want partial executor output", failed.Output)
	}
}

func TestExecutePreservesExecutorErrorInChain(t *testing.T) {
	plan := mappedPlan("task-1", validStep("step-1"))
	instance, _ := New(executorFunc(func(context.Context, mapper.MappedStep) (execution.StepResult, error) {
		return execution.StepResult{}, context.Canceled
	}))

	result, err := instance.Execute(context.Background(), plan)

	if !errors.Is(err, ErrExecutionFailed) {
		t.Fatalf("Execute() error = %v, want ErrExecutionFailed", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled in error chain", err)
	}
	if result.Status != execution.StatusFailed || len(result.StepResults) != 1 {
		t.Fatalf("result = %+v, want one normalized failed step", result)
	}
	failed := result.StepResults[0]
	if failed.StepID != "step-1" || failed.NodeID != "node-step-1" ||
		failed.Status != execution.StatusFailed || failed.Error != context.Canceled.Error() {
		t.Fatalf("failed step = %+v, want normalized mapped identity and context error", failed)
	}
}

func TestExecuteRejectsSuccessfulResultForDifferentStep(t *testing.T) {
	plan := mappedPlan("task-1", validStep("step-1"))
	instance, _ := New(executorFunc(func(context.Context, mapper.MappedStep) (execution.StepResult, error) {
		return execution.NewStepResult("wrong-step", "wrong-node", execution.StatusSucceeded, nil, "")
	}))

	result, err := instance.Execute(context.Background(), plan)

	if !errors.Is(err, ErrExecutionFailed) {
		t.Fatalf("Execute() error = %v, want ErrExecutionFailed", err)
	}
	if result.Status != execution.StatusFailed || len(result.StepResults) != 1 {
		t.Fatalf("result = %+v, want one normalized failed step", result)
	}
	failed := result.StepResults[0]
	if failed.StepID != "step-1" || failed.NodeID != "node-step-1" ||
		failed.Status != execution.StatusFailed || failed.Error == "" {
		t.Fatalf("failed step = %+v, want normalized mapped identity failure", failed)
	}
}

func TestExecuteStopsOnNonSuccessfulResultWithoutGoError(t *testing.T) {
	tests := []struct {
		name       string
		first      execution.StepResult
		wantError  string
		wantOutput map[string]any
	}{
		{
			name: "reported failure",
			first: execution.StepResult{
				StepID: "step-1",
				NodeID: "node-step-1",
				Status: execution.StatusFailed,
				Output: map[string]any{"partial": true},
				Error:  "executor reported failure",
			},
			wantError:  "executor reported failure",
			wantOutput: map[string]any{"partial": true},
		},
		{
			name: "unknown status",
			first: execution.StepResult{
				StepID: "step-1",
				NodeID: "node-step-1",
				Status: "pending",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			plan := mappedPlan("task-1", validStep("step-1"), validStep("step-2"))
			calls := 0
			instance, _ := New(executorFunc(func(_ context.Context, step mapper.MappedStep) (execution.StepResult, error) {
				calls++
				if calls == 1 {
					return test.first, nil
				}
				return execution.NewStepResult(step.ID, step.NodeID, execution.StatusSucceeded, nil, "")
			}))

			result, err := instance.Execute(context.Background(), plan)

			if !errors.Is(err, ErrExecutionFailed) {
				t.Fatalf("Execute() error = %v, want ErrExecutionFailed", err)
			}
			if calls != 1 {
				t.Fatalf("executor calls = %d, want 1", calls)
			}
			if result.Status != execution.StatusFailed || len(result.StepResults) != 1 {
				t.Fatalf("result = %+v, want one failed step", result)
			}
			failed := result.StepResults[0]
			if failed.StepID != "step-1" || failed.NodeID != "node-step-1" ||
				failed.Status != execution.StatusFailed || failed.Error == "" {
				t.Fatalf("failed step = %+v, want normalized mapped failure", failed)
			}
			if test.wantError != "" && failed.Error != test.wantError {
				t.Fatalf("failed.Error = %q, want %q", failed.Error, test.wantError)
			}
			if !reflect.DeepEqual(failed.Output, test.wantOutput) {
				t.Fatalf("failed.Output = %v, want %v", failed.Output, test.wantOutput)
			}
		})
	}
}

func mappedPlan(taskID model.TaskID, steps ...mapper.MappedStep) mapper.MappedPlan {
	return mapper.MappedPlan{TaskID: taskID, Steps: steps}
}

func validStep(id model.StepID) mapper.MappedStep {
	return mapper.MappedStep{
		ID:         id,
		Capability: "capability-" + model.Capability(id),
		NodeID:     "node-" + model.NodeID(id),
	}
}
