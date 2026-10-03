package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"testing"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/application"
	"dtm/internal/execution"
	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/task"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestValidateIdempotencyKeyContract(t *testing.T) {
	for _, valid := range []string{"", "a", "A-Z_0.3:retry", strings.Repeat("x", 128)} {
		if err := validateIdempotencyKey(valid); err != nil {
			t.Fatalf("valid key %q: %v", valid, err)
		}
	}
	for _, invalid := range []string{" ", " leading", "trailing ", "slash/no", "中文", strings.Repeat("x", 129)} {
		if err := validateIdempotencyKey(invalid); !errors.Is(err, errInvalidIdempotencyKey) {
			t.Fatalf("invalid key %q error = %v", invalid, err)
		}
	}
}

func TestSubmissionFingerprintIgnoresTransportIdentityAndMode(t *testing.T) {
	first := &dtmv1.SubmitTaskRequest{
		Task:  &dtmv1.Task{Id: "task-a", Intent: "cool", Requirements: []string{"sensor"}, Constraints: map[string]string{"target": "26"}},
		Async: true, IdempotencyKey: "key-a",
	}
	retry := &dtmv1.SubmitTaskRequest{
		Task:  &dtmv1.Task{Id: "task-b", Intent: "cool", Requirements: []string{"sensor"}, Constraints: map[string]string{"target": "26"}},
		Async: false, IdempotencyKey: "key-b",
	}
	firstTask, err := taskFromProto(first)
	if err != nil {
		t.Fatal(err)
	}
	retryTask, err := taskFromProto(retry)
	if err != nil {
		t.Fatal(err)
	}
	firstFingerprint, err := submissionFingerprint(firstTask)
	if err != nil {
		t.Fatal(err)
	}
	retryFingerprint, err := submissionFingerprint(retryTask)
	if err != nil {
		t.Fatal(err)
	}
	if firstFingerprint != retryFingerprint || !strings.HasPrefix(firstFingerprint, "v1:") || len(firstFingerprint) != 67 {
		t.Fatalf("fingerprints = %q and %q", firstFingerprint, retryFingerprint)
	}
	if retryFingerprint != "v1:1ae621e985a1cda236e4e1779044fb9257eecd79ecdc48108582e60e57906451" {
		t.Fatalf("normalized v1 compatibility fingerprint = %q", retryFingerprint)
	}
	retry.Task.Constraints["target"] = "27"
	retryTask, err = taskFromProto(retry)
	if err != nil {
		t.Fatal(err)
	}
	different, err := submissionFingerprint(retryTask)
	if err != nil {
		t.Fatal(err)
	}
	if different == firstFingerprint {
		t.Fatal("business-semantic change did not change fingerprint")
	}
}

func TestSubmissionFingerprintUsesNormalizedBusinessSemantics(t *testing.T) {
	first := &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{
		Id: " task-a ", Intent: " cool ", Requirements: []string{" sensor "},
		Constraints: map[string]string{" target ": " 26 "},
	}, Async: true, IdempotencyKey: "key-a"}
	second := &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{
		Id: "task-b", Intent: "cool", Requirements: []string{"sensor"},
		Constraints: map[string]string{"target": "26"},
	}, IdempotencyKey: "key-b"}
	firstTask, err := taskFromProto(first)
	if err != nil {
		t.Fatal(err)
	}
	secondTask, err := taskFromProto(second)
	if err != nil {
		t.Fatal(err)
	}
	firstFingerprint, err := submissionFingerprint(firstTask)
	if err != nil {
		t.Fatal(err)
	}
	secondFingerprint, err := submissionFingerprint(secondTask)
	if err != nil {
		t.Fatal(err)
	}
	if firstFingerprint != secondFingerprint {
		t.Fatalf("normalized-equivalent fingerprints differ: %q != %q", firstFingerprint, secondFingerprint)
	}
}

func TestCoreSubmitTaskIdempotencyResponseAndConflict(t *testing.T) {
	submitter := &idempotentSubmitterStub{}
	server, err := NewCoreServer(submitter)
	if err != nil {
		t.Fatal(err)
	}
	request := func(id, key, intent string) *dtmv1.SubmitTaskRequest {
		return &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{Id: id, Intent: intent}, IdempotencyKey: key}
	}
	first, err := server.SubmitTask(context.Background(), request("task-first", "key-1", "cool"))
	if err != nil {
		t.Fatal(err)
	}
	retry, err := server.SubmitTask(context.Background(), request("task-retry", "key-1", "cool"))
	if err != nil {
		t.Fatal(err)
	}
	if first.GetDeduplicated() || !retry.GetDeduplicated() || retry.GetTaskId() != first.GetTaskId() {
		t.Fatalf("first=%#v retry=%#v", first, retry)
	}
	_, err = server.SubmitTask(context.Background(), request("task-conflict", "key-1", "heat"))
	if status.Code(err) != codes.AlreadyExists || status.Convert(err).Message() != application.ErrIdempotencyConflict.Error() {
		t.Fatalf("conflict = %v", err)
	}
	_, err = server.SubmitTask(context.Background(), request("task-invalid", " bad", "cool"))
	if status.Code(err) != codes.InvalidArgument || status.Convert(err).Message() != errInvalidIdempotencyKey.Error() {
		t.Fatalf("invalid key = %v", err)
	}
}

func TestCoreSubmitTaskNormalizedEquivalentRequestDeduplicates(t *testing.T) {
	server, err := NewCoreServer(&idempotentSubmitterStub{})
	if err != nil {
		t.Fatal(err)
	}
	first, err := server.SubmitTask(context.Background(), &dtmv1.SubmitTaskRequest{
		Task:           &dtmv1.Task{Id: " task-first ", Intent: " cool ", Requirements: []string{" sensor "}, Constraints: map[string]string{" target ": " 26 "}},
		IdempotencyKey: "normalized-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := server.SubmitTask(context.Background(), &dtmv1.SubmitTaskRequest{
		Task:           &dtmv1.Task{Id: "task-retry", Intent: "cool", Requirements: []string{"sensor"}, Constraints: map[string]string{"target": "26"}},
		IdempotencyKey: "normalized-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	if retry.GetTaskId() != first.GetTaskId() || !retry.GetDeduplicated() {
		t.Fatalf("first=%#v retry=%#v", first, retry)
	}
}

func TestSubmitTaskInternalErrorsAreRedacted(t *testing.T) {
	secretKey := "sensitive-key-123"
	secretDetails := `SQLite query failed at C:\private\state.db for request_fingerprint=v1:secret SQL=SELECT key=` + secretKey
	for _, test := range []struct {
		name    string
		cause   error
		code    codes.Code
		message string
		class   string
	}{
		{"corrupt binding", fmt.Errorf("%w: %s", application.ErrSubmissionBindingCorrupt, secretDetails), codes.Internal, submissionPersistenceMessage, "internal"},
		{"repository failure", fmt.Errorf("%w: %s", application.ErrSubmissionPersistence, secretDetails), codes.Internal, submissionPersistenceMessage, "internal"},
		{"storage unavailable", fmt.Errorf("%w: %s", application.ErrSubmissionUnavailable, secretDetails), codes.Unavailable, submissionUnavailableMessage, "unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var logs strings.Builder
			server, err := NewCoreServer(&submissionErrorStub{err: test.cause}, WithCoreLogger(log.New(&logs, "", 0)))
			if err != nil {
				t.Fatal(err)
			}
			_, err = server.SubmitTask(context.Background(), &dtmv1.SubmitTaskRequest{
				Task: &dtmv1.Task{Id: "task-1", Intent: "cool"}, IdempotencyKey: secretKey,
			})
			if status.Code(err) != test.code || status.Convert(err).Message() != test.message {
				t.Fatalf("client error = %v", err)
			}
			clientText := err.Error()
			for _, forbidden := range []string{secretKey, "SQLite", "SQL", "state.db", "request_fingerprint", "load task for submission key"} {
				if strings.Contains(clientText, forbidden) {
					t.Fatalf("client error leaked %q: %q", forbidden, clientText)
				}
			}
			logText := logs.String()
			if !strings.Contains(logText, "key_hash_prefix="+idempotencyKeyHashPrefix(secretKey)) || !strings.Contains(logText, "error_class="+test.class) {
				t.Fatalf("log = %q", logText)
			}
			if strings.Contains(logText, secretKey) || strings.Contains(logText, secretDetails) {
				t.Fatalf("log leaked sensitive details: %q", logText)
			}
		})
	}
}

func TestCoreSubmitTaskResponseLostAfterAcceptanceReturnsOriginalOnRetry(t *testing.T) {
	async := &responseLostAsyncStub{}
	server, err := NewCoreServer(&idempotentSubmitterStub{}, WithAsyncTaskService(async))
	if err != nil {
		t.Fatal(err)
	}
	first := &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{Id: "task-first", Intent: "cool"}, Async: true, IdempotencyKey: "response-loss-key"}
	if _, err := server.SubmitTask(context.Background(), first); status.Code(err) != codes.Canceled {
		t.Fatalf("lost response error = %v", err)
	}
	retry := &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{Id: "task-retry", Intent: "cool"}, Async: true, IdempotencyKey: "response-loss-key"}
	response, err := server.SubmitTask(context.Background(), retry)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetTaskId() != "task-first" || !response.GetDeduplicated() {
		t.Fatalf("retry response = %#v", response)
	}
}

type idempotentSubmitterStub struct {
	key         string
	fingerprint string
	taskID      model.TaskID
}

type submissionErrorStub struct{ err error }

func (stub *submissionErrorStub) Submit(context.Context, task.Task) (application.Outcome, error) {
	return application.Outcome{}, stub.err
}

func (stub *submissionErrorStub) SubmitSubmission(context.Context, task.Task, string, string) (application.Outcome, bool, error) {
	return application.Outcome{}, false, stub.err
}

type responseLostAsyncStub struct {
	record      application.TaskRecord
	fingerprint string
}

func (stub *responseLostAsyncStub) Submit(context.Context, task.Task) (application.TaskRecord, error) {
	return application.TaskRecord{}, errors.New("unexpected non-idempotent submission")
}

func (stub *responseLostAsyncStub) SubmitSubmission(
	_ context.Context,
	input task.Task,
	_ string,
	fingerprint string,
) (application.TaskRecord, bool, error) {
	if stub.record.Task.ID == "" {
		stub.record = application.TaskRecord{Task: input, State: lifecycle.StateCreated}
		stub.fingerprint = fingerprint
		return application.TaskRecord{}, false, context.Canceled
	}
	if stub.fingerprint != fingerprint {
		return application.TaskRecord{}, false, application.ErrIdempotencyConflict
	}
	return stub.record, true, nil
}

func (stub *responseLostAsyncStub) Find(context.Context, model.TaskID) (application.TaskRecord, error) {
	return stub.record, nil
}

func (stub *idempotentSubmitterStub) Submit(_ context.Context, input task.Task) (application.Outcome, error) {
	return successfulOutcome(input.ID), nil
}

func (stub *idempotentSubmitterStub) SubmitSubmission(
	_ context.Context,
	input task.Task,
	key string,
	fingerprint string,
) (application.Outcome, bool, error) {
	if stub.key == "" {
		stub.key, stub.fingerprint, stub.taskID = key, fingerprint, input.ID
		return successfulOutcome(input.ID), false, nil
	}
	if stub.key != key || stub.fingerprint != fingerprint {
		return application.Outcome{}, false, application.ErrIdempotencyConflict
	}
	return successfulOutcome(stub.taskID), true, nil
}

func successfulOutcome(taskID model.TaskID) application.Outcome {
	return application.Outcome{
		TaskID: taskID,
		State:  lifecycle.StateSucceeded,
		Execution: &execution.Result{
			TaskID: taskID, Status: execution.StatusSucceeded,
			StepResults: []execution.StepResult{{
				StepID: "step-1", NodeID: "node-1", Status: execution.StatusSucceeded, Output: map[string]any{"ok": true},
			}},
		},
	}
}
