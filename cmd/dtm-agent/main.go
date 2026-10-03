package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/authoritybinding"
	"dtm/internal/config"
	storageport "dtm/internal/storage"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const agentStopTimeout = 2 * time.Second

type rpcErrorDisposition int

const (
	rpcErrorContinue rpcErrorDisposition = iota
	rpcErrorRetry
	rpcErrorReregister
	rpcErrorTerminate
	rpcErrorShutdown
)

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("dtm-agent", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to the agent YAML configuration")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "missing required -config flag")
		flags.Usage()
		return 2
	}
	agentConfig, err := config.LoadAgent(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	if agentConfig.Mode != config.ModeStatic {
		fmt.Fprintln(stderr, "dtm-agent requires static mode with core.address")
		return 2
	}
	deps, err := compose(agentConfig)
	if err != nil {
		if code := storageport.WriteStartupFailure(stderr, "dtm-agent", agentConfig.Storage.Path, err); code != 1 {
			return code
		}
		fmt.Fprintln(stderr, "agent startup failed")
		return 1
	}
	defer deps.Close()
	listener, err := net.Listen("tcp", agentConfig.Server.ListenAddress)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	status := deps.ExecutionRepository.StorageStatus()
	fmt.Fprintf(stdout, "event=storage_ready migration_set=%s schema_version=%d journal_mode=%s synchronous=%s foreign_keys=on quick_check=ok foreign_key_check=ok\n", status.MigrationSet, status.SchemaVersion, status.JournalMode, status.Synchronous)
	fmt.Fprintf(
		stdout,
		"agent starting node_id=%s capabilities=%s listen_address=%s advertise_address=%s\n",
		deps.Node.ID(),
		strings.Join(agentConfig.Node.Capabilities, ","),
		agentConfig.Server.ListenAddress,
		agentConfig.Node.AdvertiseAddress,
	)
	if err := deps.Start(listener); err != nil {
		_ = listener.Close()
		fmt.Fprintln(stderr, err)
		return 1
	}

	session, err := authoritybinding.New(authoritybinding.Options{
		NodeID: string(deps.Node.ID()), Capabilities: agentConfig.Node.Capabilities,
		ExecutionAddress: agentConfig.Node.AdvertiseAddress, HeartbeatInterval: agentConfig.Heartbeat.Interval.Duration,
		CallTimeout: agentStopTimeout,
		Dial: func(dialCtx context.Context, address string) (authoritybinding.Client, io.Closer, error) {
			connection, dialErr := grpc.DialContext(dialCtx, address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			if dialErr != nil {
				return nil, nil, dialErr
			}
			return dtmv1.NewNodeRegistryServiceClient(connection), connection, nil
		},
	})
	if err != nil {
		_ = deps.Close()
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := session.Start(ctx, agentConfig.Core.Address); err != nil {
		_ = session.Close()
		_ = deps.Close()
		fmt.Fprintln(stderr, err)
		return 1
	}
	registrationID := session.RegistrationID()
	fmt.Fprintf(
		stdout,
		"register success node_id=%s capabilities=%s advertise_address=%s core_address=%s registration_id=%s\n",
		deps.Node.ID(),
		strings.Join(agentConfig.Node.Capabilities, ","),
		agentConfig.Node.AdvertiseAddress,
		agentConfig.Core.Address,
		registrationID,
	)

	select {
	case <-ctx.Done():
		offlineErr := session.Close()
		_ = deps.Close()
		if offlineErr != nil {
			fmt.Fprintln(stderr, offlineErr)
			return 1
		}
		return 0
	case heartbeatErr := <-session.Errors():
		offlineErr := session.Close()
		_ = deps.Close()
		if heartbeatErr != nil {
			fmt.Fprintln(stderr, heartbeatErr)
		}
		if offlineErr != nil {
			fmt.Fprintln(stderr, offlineErr)
		}
		if heartbeatErr != nil || offlineErr != nil {
			return 1
		}
		return 0
	case serveErr := <-deps.Errors():
		cleanupErr := session.Close()
		if !isExpectedServeError(serveErr) {
			fmt.Fprintln(stderr, serveErr)
			return 1
		}
		if cleanupErr != nil {
			fmt.Fprintln(stderr, cleanupErr)
			return 1
		}
		return 0
	}
}

func runHeartbeatLoop(
	ctx context.Context,
	client dtmv1.NodeRegistryServiceClient,
	nodeID, registrationID string,
	interval time.Duration,
	registrationRequest *dtmv1.RegisterNodeRequest,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case sentAt := <-ticker.C:
			heartbeatCtx, cancel := context.WithTimeout(ctx, agentStopTimeout)
			_, err := client.Heartbeat(heartbeatCtx, &dtmv1.HeartbeatRequest{
				NodeId:           nodeID,
				RegistrationId:   registrationID,
				SentAtUnixMillis: sentAt.UnixMilli(),
			})
			cancel()
			switch classifyHeartbeatError(ctx, err) {
			case rpcErrorContinue, rpcErrorRetry:
				continue
			case rpcErrorReregister:
				registerCtx, cancelRegister := context.WithTimeout(ctx, agentStopTimeout)
				_, registerErr := client.RegisterNode(registerCtx, registrationRequest)
				cancelRegister()
				switch classifyRegistrationError(ctx, registerErr) {
				case rpcErrorContinue:
					continue
				case rpcErrorRetry:
					// Core may still be starting. The next heartbeat retries the
					// same registration without changing ownership identity.
					continue
				case rpcErrorShutdown:
					return nil
				case rpcErrorTerminate, rpcErrorReregister:
					return fmt.Errorf("re-registration rejected: %w", registerErr)
				}
			case rpcErrorShutdown:
				return nil
			case rpcErrorTerminate:
				return fmt.Errorf("heartbeat rejected: %w", err)
			}
		}
	}
}

func classifyRegistrationError(ctx context.Context, err error) rpcErrorDisposition {
	return classifyRPCError(ctx, err, rpcErrorTerminate)
}

func classifyHeartbeatError(ctx context.Context, err error) rpcErrorDisposition {
	return classifyRPCError(ctx, err, rpcErrorReregister)
}

func classifyRPCError(ctx context.Context, err error, notFound rpcErrorDisposition) rpcErrorDisposition {
	if err == nil {
		return rpcErrorContinue
	}
	if ctx.Err() != nil {
		return rpcErrorShutdown
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return rpcErrorRetry
	case codes.NotFound:
		return notFound
	default:
		return rpcErrorTerminate
	}
}

func newRegistrationID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate registration ID: %w", err)
	}
	return hex.EncodeToString(value), nil
}

func bestEffortOffline(client dtmv1.NodeRegistryServiceClient, nodeID, registrationID string) error {
	ctx, cancel := context.WithTimeout(context.Background(), agentStopTimeout)
	defer cancel()
	_, err := client.UpdateNodeStatus(ctx, &dtmv1.UpdateNodeStatusRequest{
		NodeId:         nodeID,
		Status:         dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		RegistrationId: registrationID,
	})
	switch status.Code(err) {
	case codes.OK, codes.NotFound, codes.FailedPrecondition:
		// NotFound means the session is already gone. FailedPrecondition means
		// another registration owns the node; cleanup must not disturb it.
		return nil
	default:
		return err
	}
}

func shutdownAgent(server *grpc.Server, listener net.Listener) {
	_ = listener.Close()
	stopGRPCServer(server, agentStopTimeout)
}

func stopGRPCServer(server *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() { server.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(timeout):
		server.Stop()
		<-done
	}
}

func isExpectedServeError(err error) bool {
	return err == nil || err == grpc.ErrServerStopped || errors.Is(err, net.ErrClosed)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
