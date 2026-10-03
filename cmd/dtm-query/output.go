package main

import (
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var jsonOutput = protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true, UseEnumNumbers: false, Indent: "  "}

type checkedWriter struct {
	writer io.Writer
	err    error
}

func (writer *checkedWriter) Write(data []byte) (int, error) {
	if writer.err != nil {
		return 0, writer.err
	}
	written, err := writer.writer.Write(data)
	if err == nil && written != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		writer.err = err
	}
	return written, err
}

func (writer *checkedWriter) Err() error { return writer.err }

func renderResponse(writer io.Writer, options commandOptions, response any) error {
	if options.output == "json" {
		message, ok := response.(proto.Message)
		if !ok {
			return fmt.Errorf("response is not a protobuf message")
		}
		encoded, err := jsonOutput.Marshal(message)
		if err != nil {
			return err
		}
		checked := &checkedWriter{writer: writer}
		_, _ = fmt.Fprintf(checked, "%s\n", encoded)
		return checked.Err()
	}
	switch typed := response.(type) {
	case *dtmv1.GetTaskResponse:
		return renderTask(writer, typed.GetTask())
	case *dtmv1.ListTasksResponse:
		return renderTaskList(writer, typed)
	case *dtmv1.GetTaskExecutionsResponse:
		return renderExecutions(writer, options.taskID, typed.GetExecutions())
	default:
		return fmt.Errorf("unsupported response type %T", response)
	}
}

func renderTask(writer io.Writer, task *dtmv1.TaskDetails) error {
	if task == nil {
		return fmt.Errorf("task is missing")
	}
	checked := &checkedWriter{writer: writer}
	fmt.Fprintln(checked, "Task")
	fmt.Fprintf(checked, "  ID: %s\n", task.GetTaskId())
	fmt.Fprintf(checked, "  Intent: %s\n", task.GetIntent())
	fmt.Fprintf(checked, "  Status: %s\n", enumName(task.GetStatus().String(), "TASK_STATUS_"))
	fmt.Fprintf(checked, "  Version: %d\n", task.GetVersion())
	fmt.Fprintf(checked, "  Created At: %s\n", formatTimestamp(task.GetCreatedAt()))
	fmt.Fprintf(checked, "  Updated At: %s\n", formatTimestamp(task.GetUpdatedAt()))
	fmt.Fprintf(checked, "  Started At: %s\n", formatTimestamp(task.GetStartedAt()))
	fmt.Fprintf(checked, "  Completed At: %s\n", formatTimestamp(task.GetCompletedAt()))
	fmt.Fprintf(checked, "  Failure Code: %s\n", emptyDash(task.GetFailureCode()))
	fmt.Fprintf(checked, "  Failure Message: %s\n", emptyDash(task.GetFailureMessage()))
	fmt.Fprintln(checked)
	if len(task.GetSteps()) == 0 {
		fmt.Fprintln(checked, "No steps found.")
		return checked.Err()
	}
	fmt.Fprintln(checked, "Steps")
	table := tabwriter.NewWriter(checked, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "SEQ\tSTEP ID\tCAPABILITY\tNODE\tSTATUS\tATTEMPTS")
	for _, step := range task.GetSteps() {
		fmt.Fprintf(table, "%d\t%s\t%s\t%s\t%s\t%d/%d\n", step.GetSequence(), step.GetStepId(), step.GetCapability(), emptyDash(step.GetAssignedNodeId()), enumName(step.GetStatus().String(), "STEP_STATUS_"), step.GetAttemptCount(), step.GetMaxAttempts())
	}
	if err := table.Flush(); err != nil {
		return err
	}
	return checked.Err()
}

func renderTaskList(writer io.Writer, response *dtmv1.ListTasksResponse) error {
	checked := &checkedWriter{writer: writer}
	if len(response.GetTasks()) == 0 {
		fmt.Fprintln(checked, "No tasks found.")
		return checked.Err()
	}
	table := tabwriter.NewWriter(checked, 0, 4, 2, ' ', 0)
	fmt.Fprintln(table, "TASK ID\tSTATUS\tINTENT\tCREATED AT\tUPDATED AT")
	for _, task := range response.GetTasks() {
		fmt.Fprintf(table, "%s\t%s\t%s\t%s\t%s\n", task.GetTaskId(), enumName(task.GetStatus().String(), "TASK_STATUS_"), task.GetIntent(), formatTimestamp(task.GetCreatedAt()), formatTimestamp(task.GetUpdatedAt()))
	}
	if err := table.Flush(); err != nil {
		return err
	}
	if response.GetNextPageToken() != "" {
		fmt.Fprintf(checked, "\nNext Page Token:\n%s\n", response.GetNextPageToken())
	}
	return checked.Err()
}

func renderExecutions(writer io.Writer, taskID string, executions []*dtmv1.TaskExecution) error {
	checked := &checkedWriter{writer: writer}
	if len(executions) == 0 {
		fmt.Fprintf(checked, "No executions found for task %s.\n", taskID)
		return checked.Err()
	}
	for index, execution := range executions {
		if index > 0 {
			fmt.Fprintln(checked)
		}
		fmt.Fprintf(checked, "Execution %d\n", index+1)
		fmt.Fprintf(checked, "  ID: %s\n", execution.GetExecutionId())
		fmt.Fprintf(checked, "  Task ID: %s\n", execution.GetTaskId())
		fmt.Fprintf(checked, "  Step ID: %s\n", execution.GetStepId())
		fmt.Fprintf(checked, "  Node ID: %s\n", emptyDash(execution.GetNodeId()))
		fmt.Fprintf(checked, "  Attempt: %d\n", execution.GetAttemptNumber())
		fmt.Fprintf(checked, "  Request ID: %s\n", emptyDash(execution.GetRequestId()))
		fmt.Fprintf(checked, "  Status: %s\n", enumName(execution.GetStatus().String(), "EXECUTION_STATUS_"))
		fmt.Fprintf(checked, "  Started At: %s\n", formatTimestamp(execution.GetStartedAt()))
		fmt.Fprintf(checked, "  Completed At: %s\n", formatTimestamp(execution.GetCompletedAt()))
		fmt.Fprintf(checked, "  Updated At: %s\n", formatTimestamp(execution.GetUpdatedAt()))
		fmt.Fprintf(checked, "  Failure Code: %s\n", emptyDash(execution.GetFailureCode()))
		fmt.Fprintf(checked, "  Failure Message: %s\n", emptyDash(execution.GetFailureMessage()))
		fmt.Fprintf(checked, "  Request: %s\n", formatStruct(execution.GetRequest()))
		fmt.Fprintf(checked, "  Response: %s\n", formatStruct(execution.GetResponse()))
	}
	return checked.Err()
}

func formatTimestamp(value *timestamppb.Timestamp) string {
	if value == nil {
		return "-"
	}
	return value.AsTime().UTC().Format(time.RFC3339Nano)
}

func formatStruct(value *structpb.Struct) string {
	if value == nil {
		return "-"
	}
	encoded, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(value)
	if err != nil {
		return "-"
	}
	return string(encoded)
}

func emptyDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func enumName(value, prefix string) string { return strings.TrimPrefix(value, prefix) }
