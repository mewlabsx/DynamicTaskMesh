package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/config"
	"dtm/internal/storage"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	exitSuccess     = 0
	exitInternal    = 1
	exitArguments   = 2
	exitNotFound    = 3
	exitUnavailable = 4
	exitTimeout     = 5
	exitCanceled    = 6
	defaultTimeout  = 10 * time.Second
)

type queryClient interface {
	GetTask(context.Context, *dtmv1.GetTaskRequest, ...grpc.CallOption) (*dtmv1.GetTaskResponse, error)
	ListTasks(context.Context, *dtmv1.ListTasksRequest, ...grpc.CallOption) (*dtmv1.ListTasksResponse, error)
	GetTaskExecutions(context.Context, *dtmv1.GetTaskExecutionsRequest, ...grpc.CallOption) (*dtmv1.GetTaskExecutionsResponse, error)
}

type connectionCloser interface{ Close() error }
type queryClientFactory func(context.Context, string) (queryClient, connectionCloser, error)

type commandOptions struct {
	command       string
	configPath    string
	coreAddress   string
	timeout       time.Duration
	output        string
	taskID        string
	status        dtmv1.TaskStatus
	createdAfter  *timestamppb.Timestamp
	createdBefore *timestamppb.Timestamp
	limit         int
	pageToken     string
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runWith(ctx, args, stdout, stderr, newProductionQueryClient)
}

func runWith(ctx context.Context, args []string, stdout, stderr io.Writer, factory queryClientFactory) int {
	options, err := parseCommand(args, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "dtm-query: %v\n", err)
		return exitArguments
	}
	client, connection, err := factory(ctx, options.coreAddress)
	if err != nil {
		fmt.Fprintln(stderr, "dtm-query: Core is unavailable")
		return exitUnavailable
	}
	defer func() { _ = connection.Close() }()

	rpcCtx, cancel := context.WithTimeout(ctx, options.timeout)
	defer cancel()
	var response any
	switch options.command {
	case "get":
		response, err = client.GetTask(rpcCtx, &dtmv1.GetTaskRequest{TaskId: options.taskID})
	case "list":
		response, err = client.ListTasks(rpcCtx, &dtmv1.ListTasksRequest{
			Status:        options.status,
			CreatedAfter:  options.createdAfter,
			CreatedBefore: options.createdBefore,
			Limit:         int32(options.limit),
			PageToken:     options.pageToken,
		})
	case "executions":
		response, err = client.GetTaskExecutions(rpcCtx, &dtmv1.GetTaskExecutionsRequest{TaskId: options.taskID})
	}
	if err != nil {
		return reportRPCError(stderr, err, options.timeout)
	}
	if err := renderResponse(stdout, options, response); err != nil {
		fmt.Fprintln(stderr, "dtm-query: could not format query response")
		return exitInternal
	}
	return exitSuccess
}

func parseCommand(args []string, stderr io.Writer) (commandOptions, error) {
	if len(args) == 0 {
		return commandOptions{}, errors.New("a subcommand is required: get, list, or executions")
	}
	command := strings.ToLower(strings.TrimSpace(args[0]))
	if command != "get" && command != "list" && command != "executions" {
		return commandOptions{}, fmt.Errorf("unknown subcommand %q; expected get, list, or executions", args[0])
	}
	options := commandOptions{command: command, timeout: defaultTimeout, output: "text"}
	flags := flag.NewFlagSet("dtm-query "+command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.configPath, "config", "", "Core configuration file")
	flags.StringVar(&options.coreAddress, "core-address", "", "Core gRPC address")
	flags.DurationVar(&options.timeout, "timeout", defaultTimeout, "RPC timeout")
	flags.StringVar(&options.output, "output", "text", "output format: text or json")
	flags.StringVar(&options.taskID, "task-id", "", "task ID")
	var status, createdAfter, createdBefore string
	flags.StringVar(&status, "status", "", "task status filter")
	flags.StringVar(&createdAfter, "created-after", "", "exclusive RFC3339 creation lower bound")
	flags.StringVar(&createdBefore, "created-before", "", "exclusive RFC3339 creation upper bound")
	flags.IntVar(&options.limit, "limit", 0, "maximum tasks to return")
	flags.StringVar(&options.pageToken, "page-token", "", "opaque page token")
	if err := flags.Parse(args[1:]); err != nil {
		return commandOptions{}, err
	}
	if flags.NArg() != 0 {
		return commandOptions{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	commonFlags := map[string]bool{"config": true, "core-address": true, "timeout": true, "output": true}
	commandFlags := map[string]map[string]bool{
		"get":        {"task-id": true},
		"executions": {"task-id": true},
		"list":       {"status": true, "created-after": true, "created-before": true, "limit": true, "page-token": true},
	}
	var unsupported string
	flags.Visit(func(current *flag.Flag) {
		if !commonFlags[current.Name] && !commandFlags[command][current.Name] && unsupported == "" {
			unsupported = current.Name
		}
	})
	if unsupported != "" {
		return commandOptions{}, fmt.Errorf("--%s is not supported by the %s subcommand", unsupported, command)
	}
	options.output = strings.ToLower(strings.TrimSpace(options.output))
	if options.output != "text" && options.output != "json" {
		return commandOptions{}, fmt.Errorf("--output must be text or json")
	}
	if options.timeout <= 0 {
		return commandOptions{}, fmt.Errorf("--timeout must be greater than zero")
	}
	options.taskID = strings.TrimSpace(options.taskID)
	if (command == "get" || command == "executions") && options.taskID == "" {
		return commandOptions{}, fmt.Errorf("--task-id is required for %s", command)
	}
	if command == "list" {
		var err error
		options.status, err = parseTaskStatus(status)
		if err != nil {
			return commandOptions{}, err
		}
		options.createdAfter, err = parseTimestampFlag("--created-after", createdAfter)
		if err != nil {
			return commandOptions{}, err
		}
		options.createdBefore, err = parseTimestampFlag("--created-before", createdBefore)
		if err != nil {
			return commandOptions{}, err
		}
		if options.createdAfter != nil && options.createdBefore != nil && options.createdAfter.AsTime().After(options.createdBefore.AsTime()) {
			return commandOptions{}, fmt.Errorf("--created-after must not be later than --created-before")
		}
		if options.limit < 0 || options.limit > storage.MaxTaskPageLimit {
			return commandOptions{}, fmt.Errorf("--limit must be between 0 and %d", storage.MaxTaskPageLimit)
		}
		if strings.TrimSpace(options.pageToken) == "" {
			options.pageToken = ""
		}
	}
	address, err := resolveCoreAddress(options.configPath, options.coreAddress)
	if err != nil {
		return commandOptions{}, err
	}
	options.coreAddress = address
	return options, nil
}

func parseTaskStatus(raw string) (dtmv1.TaskStatus, error) {
	value := strings.ToUpper(strings.TrimSpace(raw))
	if value == "" {
		return dtmv1.TaskStatus_TASK_STATUS_UNSPECIFIED, nil
	}
	key := value
	if !strings.HasPrefix(key, "TASK_STATUS_") {
		key = "TASK_STATUS_" + key
	}
	number, ok := dtmv1.TaskStatus_value[key]
	if !ok || number == int32(dtmv1.TaskStatus_TASK_STATUS_UNSPECIFIED) {
		allowed := make([]string, 0, len(dtmv1.TaskStatus_value)-1)
		for enumName, enumValue := range dtmv1.TaskStatus_value {
			if enumValue != 0 {
				allowed = append(allowed, strings.ToLower(strings.TrimPrefix(enumName, "TASK_STATUS_")))
			}
		}
		slicesSort(allowed)
		return 0, fmt.Errorf("invalid --status %q; allowed values: %s", raw, strings.Join(allowed, ", "))
	}
	return dtmv1.TaskStatus(number), nil
}

func slicesSort(values []string) {
	for index := 1; index < len(values); index++ {
		for current := index; current > 0 && values[current] < values[current-1]; current-- {
			values[current], values[current-1] = values[current-1], values[current]
		}
	}
}

func parseTimestampFlag(name, raw string) (*timestamppb.Timestamp, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return nil, fmt.Errorf("%s must be RFC3339 or RFC3339Nano", name)
	}
	timestamp := timestamppb.New(parsed.UTC())
	if err := timestamp.CheckValid(); err != nil {
		return nil, fmt.Errorf("%s is outside the supported timestamp range", name)
	}
	return timestamp, nil
}

func resolveCoreAddress(configPath, explicit string) (string, error) {
	address := ""
	if strings.TrimSpace(configPath) != "" {
		loaded, err := config.LoadCore(strings.TrimSpace(configPath))
		if err != nil {
			return "", fmt.Errorf("load --config: %w", err)
		}
		address = loaded.Server.Address
	}
	if strings.TrimSpace(explicit) != "" {
		address = strings.TrimSpace(explicit)
	}
	if address == "" {
		return "", fmt.Errorf("--core-address or --config is required")
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.TrimSpace(host) == "" || strings.TrimSpace(port) == "" {
		return "", fmt.Errorf("Core address must be in host:port form")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return "", fmt.Errorf("Core address port must be between 1 and 65535")
	}
	return address, nil
}

func newProductionQueryClient(_ context.Context, address string) (queryClient, connectionCloser, error) {
	connection, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return dtmv1.NewCoreServiceClient(connection), connection, nil
}

func reportRPCError(stderr io.Writer, err error, timeout time.Duration) int {
	code := status.Code(err)
	switch code {
	case codes.InvalidArgument:
		fmt.Fprintln(stderr, "dtm-query: the query arguments were rejected")
		return exitArguments
	case codes.NotFound:
		fmt.Fprintln(stderr, "dtm-query: task was not found")
		return exitNotFound
	case codes.Unavailable:
		fmt.Fprintln(stderr, "dtm-query: Core is unavailable")
		return exitUnavailable
	case codes.DeadlineExceeded:
		fmt.Fprintf(stderr, "dtm-query: query timed out after %s\n", timeout)
		return exitTimeout
	case codes.Canceled:
		fmt.Fprintln(stderr, "dtm-query: query was canceled")
		return exitCanceled
	default:
		fmt.Fprintln(stderr, "dtm-query: query failed")
		return exitInternal
	}
}
