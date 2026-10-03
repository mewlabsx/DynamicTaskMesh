package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type SubmitClient interface {
	SubmitTask(context.Context, *dtmv1.SubmitTaskRequest, ...grpc.CallOption) (*dtmv1.SubmitTaskResponse, error)
	GetTaskStatus(context.Context, *dtmv1.GetTaskStatusRequest, ...grpc.CallOption) (*dtmv1.GetTaskStatusResponse, error)
}

type runtimeStatusClient interface {
	GetRuntimeStatus(context.Context, *dtmv1.GetRuntimeStatusRequest, ...grpc.CallOption) (*dtmv1.GetRuntimeStatusResponse, error)
}

type connectionCloser interface {
	Close() error
}

type submitClientFactory func(context.Context, string) (SubmitClient, connectionCloser, error)
type runtimeStatusClientFactory func(context.Context, string) (runtimeStatusClient, connectionCloser, error)

var taskIDSequence atomic.Uint64

const runtimeBootstrapTimeout = 2 * time.Second

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runWith(ctx, args, stdout, stderr, newProductionClient, defaultTaskID(time.Now, &taskIDSequence))
}

func defaultTaskID(now func() time.Time, sequence *atomic.Uint64) func() string {
	return func() string {
		return fmt.Sprintf("task-%d-%d", now().UnixNano(), sequence.Add(1))
	}
}

func runWith(
	ctx context.Context,
	args []string,
	stdout, stderr io.Writer,
	factory submitClientFactory,
	newTaskID func() string,
) int {
	flags := flag.NewFlagSet("dtm-submit", flag.ContinueOnError)
	flags.SetOutput(stderr)
	coreAddress := flags.String("core", "", "Core gRPC address")
	runtimeAddress := flags.String("runtime", "", "Runtime bootstrap gRPC address")
	targetTemperature := flags.String("target-temperature", "26", "target temperature")
	taskID := flags.String("task-id", "", "optional task ID")
	async := flags.Bool("async", false, "accept the task and execute it asynchronously")
	idempotencyKey := flags.String("idempotency-key", "", "optional submission idempotency key")
	statusTaskID := flags.String("status", "", "query a task ID instead of submitting")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*coreAddress) == "" && strings.TrimSpace(*runtimeAddress) == "" {
		fmt.Fprintln(stderr, "missing required -core or -runtime flag")
		flags.Usage()
		return 2
	}
	if strings.TrimSpace(*coreAddress) != "" && strings.TrimSpace(*runtimeAddress) != "" {
		fmt.Fprintln(stderr, "-core and -runtime are mutually exclusive")
		return 2
	}
	resolvedCoreAddress := strings.TrimSpace(*coreAddress)
	if strings.TrimSpace(*runtimeAddress) != "" {
		var err error
		resolvedCoreAddress, err = resolveRuntimeCoreAddress(ctx, strings.TrimSpace(*runtimeAddress), newProductionRuntimeStatusClient)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return submitErrorExitCode(err)
		}
	}
	queryID := strings.TrimSpace(*statusTaskID)
	var target, id string
	if queryID == "" {
		var err error
		target, err = normalizeTargetTemperature(*targetTemperature)
		if err != nil {
			fmt.Fprintf(stderr, "invalid -target-temperature: %v\n", err)
			return 2
		}
		id = strings.TrimSpace(*taskID)
		if id == "" {
			id = newTaskID()
		}
	}
	client, connection, err := factory(ctx, resolvedCoreAddress)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 4
	}
	if connection != nil {
		defer func() { _ = connection.Close() }()
	}

	if queryID != "" {
		response, err := client.GetTaskStatus(ctx, &dtmv1.GetTaskStatusRequest{TaskId: queryID})
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		if err := writeTaskStatus(stdout, response.GetTaskId(), response.GetStatus(), response.GetError(), response.GetResults()); err != nil {
			fmt.Fprintln(stderr, "failed to write command output")
			return 5
		}
		if response.GetStatus() == dtmv1.TaskStatus_TASK_STATUS_FAILED {
			return 1
		}
		return 0
	}

	response, err := client.SubmitTask(ctx, &dtmv1.SubmitTaskRequest{Task: &dtmv1.Task{
		Id:          id,
		Intent:      "cool_environment",
		Constraints: map[string]string{"target_temperature": target},
	}, Async: *async, IdempotencyKey: *idempotencyKey})
	if err != nil {
		fmt.Fprintln(stderr, err)
		return submitErrorExitCode(err)
	}
	if err := writeResponse(stdout, response); err != nil {
		fmt.Fprintln(stderr, "failed to write command output")
		return 5
	}
	if response.GetStatus() == dtmv1.TaskStatus_TASK_STATUS_FAILED {
		return 1
	}
	if !*async && !response.GetDeduplicated() && response.GetStatus() != dtmv1.TaskStatus_TASK_STATUS_SUCCEEDED {
		return 1
	}
	return 0
}

func resolveRuntimeCoreAddress(ctx context.Context, bootstrapAddress string, factory runtimeStatusClientFactory) (string, error) {
	bootstrapAddress = strings.TrimSpace(bootstrapAddress)
	if bootstrapAddress == "" || factory == nil {
		return "", status.Error(codes.InvalidArgument, "runtime bootstrap address is required")
	}
	dialCtx, cancelDial := runtimeBootstrapContext(ctx)
	client, connection, err := factory(dialCtx, bootstrapAddress)
	cancelDial()
	if err != nil {
		return "", runtimeBootstrapError("bootstrap dial", err)
	}
	if client == nil {
		if connection != nil {
			_ = connection.Close()
		}
		return "", runtimeBootstrapError("bootstrap dial", errors.New("runtime status client is nil"))
	}
	defer func() { _ = connection.Close() }()
	statusCtx, cancelStatus := runtimeBootstrapContext(ctx)
	first, err := client.GetRuntimeStatus(statusCtx, &dtmv1.GetRuntimeStatusRequest{})
	cancelStatus()
	if err != nil {
		return "", runtimeBootstrapError("bootstrap status RPC", err)
	}
	if first == nil {
		return "", runtimeBootstrapError("bootstrap status RPC", errors.New("runtime status response is nil"))
	}
	if first.GetReadiness() == dtmv1.RuntimeReadiness_RUNTIME_READINESS_READY && strings.TrimSpace(first.GetCoreAddress()) != "" {
		return strings.TrimSpace(first.GetCoreAddress()), nil
	}
	if first.GetReadiness() != dtmv1.RuntimeReadiness_RUNTIME_READINESS_NOT_COORDINATOR || first.GetCoordinator() == nil {
		return "", runtimeReadinessError(first.GetReadiness())
	}
	coordinatorAddress := strings.TrimSpace(first.GetCoordinator().GetAdvertisedControlEndpoint())
	if coordinatorAddress == "" || coordinatorAddress == bootstrapAddress {
		return "", status.Error(codes.Unavailable, "runtime bootstrap redirect stopped: coordinator changed or is unavailable")
	}
	redirectDialCtx, cancelRedirectDial := runtimeBootstrapContext(ctx)
	secondClient, secondConnection, err := factory(redirectDialCtx, coordinatorAddress)
	cancelRedirectDial()
	if err != nil {
		return "", runtimeBootstrapError("coordinator redirect dial", err)
	}
	if secondClient == nil {
		if secondConnection != nil {
			_ = secondConnection.Close()
		}
		return "", runtimeBootstrapError("coordinator redirect dial", errors.New("redirect runtime status client is nil"))
	}
	if secondConnection != nil {
		defer func() { _ = secondConnection.Close() }()
	}
	redirectStatusCtx, cancelRedirectStatus := runtimeBootstrapContext(ctx)
	second, err := secondClient.GetRuntimeStatus(redirectStatusCtx, &dtmv1.GetRuntimeStatusRequest{})
	cancelRedirectStatus()
	if err != nil {
		return "", runtimeBootstrapError("coordinator redirect status RPC", err)
	}
	if second == nil {
		return "", runtimeBootstrapError("coordinator redirect status RPC", errors.New("redirect runtime status response is nil"))
	}
	if second.GetReadiness() != dtmv1.RuntimeReadiness_RUNTIME_READINESS_READY || strings.TrimSpace(second.GetCoreAddress()) == "" {
		return "", status.Error(codes.Unavailable, "runtime bootstrap redirect stopped: coordinator changed or is not ready")
	}
	if second.GetCoordinator() != nil && (second.GetCoordinator().GetNodeId() != first.GetCoordinator().GetNodeId() || second.GetCoordinator().GetRuntimeInstanceId() != first.GetCoordinator().GetRuntimeInstanceId()) {
		return "", status.Error(codes.Unavailable, "runtime bootstrap redirect stopped: coordinator changed")
	}
	return strings.TrimSpace(second.GetCoreAddress()), nil
}

func runtimeBootstrapContext(parent context.Context) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	return context.WithTimeout(parent, runtimeBootstrapTimeout)
}

func runtimeBootstrapError(stage string, err error) error {
	if err == nil {
		err = errors.New("unknown runtime bootstrap error")
	}
	code := status.Code(err)
	if code == codes.OK || code == codes.Unknown {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			code = codes.DeadlineExceeded
		case errors.Is(err, context.Canceled):
			code = codes.Canceled
		default:
			code = codes.Unavailable
		}
	}
	return status.Errorf(code, "runtime bootstrap %s: %v", stage, err)
}

func runtimeReadinessError(readiness dtmv1.RuntimeReadiness) error {
	return status.Errorf(codes.Unavailable, "runtime bootstrap status=%s", runtimeReadinessName(readiness))
}

func runtimeReadinessName(readiness dtmv1.RuntimeReadiness) string {
	return strings.TrimPrefix(strings.ToUpper(readiness.String()), "RUNTIME_READINESS_")
}

func normalizeTargetTemperature(raw string) (string, error) {
	target, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil || math.IsNaN(target) || math.IsInf(target, 0) {
		return "", fmt.Errorf("must be a finite number")
	}
	return strconv.FormatFloat(target, 'f', -1, 64), nil
}

func newProductionClient(_ context.Context, address string) (SubmitClient, connectionCloser, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return dtmv1.NewCoreServiceClient(connection), connection, nil
}

func newProductionRuntimeStatusClient(ctx context.Context, address string) (runtimeStatusClient, connectionCloser, error) {
	dialCtx, cancel := runtimeBootstrapContext(ctx)
	defer cancel()
	connection, err := grpc.DialContext(dialCtx, address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return dtmv1.NewRuntimeControlServiceClient(connection), connection, nil
}

func writeResponse(writer io.Writer, response *dtmv1.SubmitTaskResponse) error {
	if _, err := fmt.Fprintf(writer, "task_id=%s status=%s deduplicated=%t error=%s\n", response.GetTaskId(), taskStatusName(response.GetStatus()), response.GetDeduplicated(), response.GetError()); err != nil {
		return err
	}
	return writeStepResults(writer, response.GetResults())
}

func writeTaskStatus(
	writer io.Writer,
	taskID string,
	status dtmv1.TaskStatus,
	message string,
	results []*dtmv1.StepResult,
) error {
	if _, err := fmt.Fprintf(writer, "task_id=%s status=%s error=%s\n", taskID, taskStatusName(status), message); err != nil {
		return err
	}
	return writeStepResults(writer, results)
}

func writeStepResults(writer io.Writer, results []*dtmv1.StepResult) error {
	for _, result := range results {
		output := "null"
		if result.GetOutput() != nil {
			if encoded, err := (protojson.MarshalOptions{}).Marshal(result.GetOutput()); err == nil {
				output = string(encoded)
			}
		}
		if _, err := fmt.Fprintf(writer, "step_id=%s node_id=%s status=%s output=%s error=%s\n", result.GetStepId(), result.GetNodeId(), executionStatusName(result.GetStatus()), output, result.GetError()); err != nil {
			return err
		}
	}
	return nil
}

func submitErrorExitCode(err error) int {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return 4
	}
	switch status.Code(err) {
	case codes.InvalidArgument:
		return 2
	case codes.AlreadyExists:
		return 3
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled:
		return 4
	default:
		return 5
	}
}

func taskStatusName(status dtmv1.TaskStatus) string {
	return strings.TrimPrefix(strings.ToLower(status.String()), "task_status_")
}

func executionStatusName(status dtmv1.ExecutionStatus) string {
	return strings.TrimPrefix(strings.ToLower(status.String()), "execution_status_")
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
