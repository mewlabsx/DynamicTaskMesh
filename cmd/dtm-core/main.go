package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"dtm/internal/config"
	storageport "dtm/internal/storage"
	"dtm/internal/transport/grpcapi"
	"google.golang.org/grpc"
)

const gracefulStopTimeout = 5 * time.Second

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("dtm-core", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "path to the core YAML configuration")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *configPath == "" {
		fmt.Fprintln(stderr, "missing required -config flag")
		flags.Usage()
		return 2
	}
	coreConfig, err := config.LoadCore(*configPath)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 2
	}
	deps, err := composeWithConfigAndLogger(ctx, coreConfig, log.New(stdout, "", 0))
	if err != nil {
		if code := storageport.WriteStartupFailure(stderr, "dtm-core", coreConfig.Storage.Path, err); code != 1 {
			return code
		}
		fmt.Fprintln(stderr, "core startup failed")
		return 1
	}
	defer deps.Close()
	status := deps.Repository.StorageStatus()
	fmt.Fprintf(stdout, "event=storage_ready migration_set=%s schema_version=%d journal_mode=%s synchronous=%s foreign_keys=on quick_check=ok foreign_key_check=ok\n", status.MigrationSet, status.SchemaVersion, status.JournalMode, status.Synchronous)
	listener, err := net.Listen("tcp", coreConfig.Server.Address)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	if err := deps.Activate(ctx, listener); err != nil {
		_ = listener.Close()
		fmt.Fprintln(stderr, err)
		return 1
	}
	fmt.Fprintf(stdout, "dtm-core listening %s\n", listener.Addr())
	for {
		select {
		case <-ctx.Done():
			if err := deps.Close(); err != nil {
				fmt.Fprintln(stderr, err)
				return 1
			}
			return 0
		case err := <-deps.Errors():
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintln(stderr, err)
			}
		case err := <-deps.ServeErrors():
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintln(stderr, err)
				return 1
			}
			return 0
		}
	}
}

func handleLeaseMonitorResult(channel <-chan error, err error, open bool, stderr io.Writer) <-chan error {
	if !open {
		return nil
	}
	if err != nil {
		fmt.Fprintln(stderr, err)
	}
	return channel
}

func monitorLeases(
	ctx context.Context,
	registry *grpcapi.RegistryServer,
	interval time.Duration,
) <-chan error {
	errors := make(chan error, 1)
	go func() {
		defer close(errors)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				if _, err := registry.SweepExpired(now); err != nil {
					select {
					case errors <- err:
					default:
					}
				}
			}
		}
	}()
	return errors
}

func stopGRPCServer(server *grpc.Server, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
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
