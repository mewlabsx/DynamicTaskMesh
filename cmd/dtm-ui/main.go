package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"dtm/internal/config"
	"dtm/internal/mesh/discovery"
	"dtm/internal/ui"
)

const gracefulShutdownTimeout = 5 * time.Second

type endpointListFlag []string

func (flag *endpointListFlag) String() string {
	return strings.Join(*flag, ",")
}

func (flag *endpointListFlag) Set(value string) error {
	for _, endpoint := range strings.Split(value, ",") {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			return errors.New("runtime seed endpoint cannot be empty")
		}
		*flag = append(*flag, endpoint)
	}
	return nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("dtm-ui", flag.ContinueOnError)
	flags.SetOutput(stderr)
	runtimeEndpoint := flags.String("runtime", "", "Runtime control endpoint used for bootstrap")
	var runtimeSeeds endpointListFlag
	flags.Var(&runtimeSeeds, "runtime-seed", "additional Runtime control endpoint used for bootstrap fallback; repeatable or comma-separated")
	httpAddress := flags.String("http", "127.0.0.1:46080", "HTTP listen address")
	discoveryGroup := flags.String("discovery-group", config.DefaultMulticastGroup, "optional Runtime discovery multicast group")
	discoveryPort := flags.Int("discovery-port", config.DefaultMulticastPort, "optional Runtime discovery multicast port")
	discoveryInterface := flags.String("discovery-interface", "", "optional local IPv4 address for multicast discovery")
	queryTimeout := flags.Duration("query-timeout", 1500*time.Millisecond, "per Runtime/Core query timeout")
	enableTestControls := flags.Bool("enable-test-controls", false, "enable isolated LAB MODE test controls")
	labManifest := flags.String("lab-manifest", "", "JSON manifest owned by the isolated LAB harness")
	labTimeout := flags.Duration("lab-timeout", 10*time.Second, "timeout for each LAB operation")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if strings.TrimSpace(*runtimeEndpoint) == "" && len(runtimeSeeds) == 0 {
		fmt.Fprintln(stderr, "missing Runtime bootstrap endpoint; provide -runtime or -runtime-seed")
		return 2
	}
	if *enableTestControls && *labManifest == "" {
		fmt.Fprintln(stderr, "-enable-test-controls requires -lab-manifest")
		return 2
	}
	if !*enableTestControls && *labManifest != "" {
		fmt.Fprintln(stderr, "-lab-manifest requires -enable-test-controls")
		return 2
	}

	var collector *ui.DiscoveryCollector
	transport, err := discovery.NewUDPTransport(discovery.MulticastConfig{Group: *discoveryGroup, Port: *discoveryPort, Interface: *discoveryInterface})
	if err != nil {
		// Runtime bootstrap remains useful without multicast. The UI will show
		// the bootstrap Runtime and Coordinator, and report missing members.
		fmt.Fprintf(stderr, "runtime discovery unavailable: %v\n", err)
	} else {
		collector = ui.NewDiscoveryCollector(transport, 5*time.Second, 10*time.Second)
		go collector.Run(ctx)
	}

	var members ui.MemberSource
	if collector != nil {
		members = collector.Snapshot
	}
	observer, err := ui.NewObserver(ui.ObserverOptions{
		BootstrapEndpoint:  *runtimeEndpoint,
		BootstrapEndpoints: []string(runtimeSeeds),
		Members:            members,
		QueryTimeout:       *queryTimeout,
	})
	if err != nil {
		if transport != nil {
			_ = transport.Close()
		}
		fmt.Fprintln(stderr, err)
		return 1
	}
	var labController *ui.LabController
	if *enableTestControls {
		manifest, manifestErr := ui.LoadLabManifest(*labManifest)
		if manifestErr != nil {
			if transport != nil {
				_ = transport.Close()
			}
			fmt.Fprintf(stderr, "load LAB manifest: %v\n", manifestErr)
			return 1
		}
		harness, harnessErr := ui.NewProcessLabHarness(manifest)
		if harnessErr != nil {
			if transport != nil {
				_ = transport.Close()
			}
			fmt.Fprintf(stderr, "create LAB harness: %v\n", harnessErr)
			return 1
		}
		labController, err = ui.NewLabController(ui.LabControllerOptions{
			Harness:          harness,
			Snapshot:         observer.Snapshot,
			Submitter:        observer,
			OperationTimeout: *labTimeout,
		})
		if err != nil {
			if transport != nil {
				_ = transport.Close()
			}
			fmt.Fprintf(stderr, "create LAB controller: %v\n", err)
			return 1
		}
		fmt.Fprintln(stderr, "WARNING: LAB test controls are enabled; use only in an isolated development/test environment.")
	}
	handler := ui.NewHTTPHandler(observer)
	if labController != nil {
		handler = ui.NewHTTPHandler(observer, labController)
	}

	server := &http.Server{
		Addr:              *httpAddress,
		Handler:           handler,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      5 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	serveErrors := make(chan error, 1)
	go func() {
		serveErrors <- server.ListenAndServe()
	}()
	bootstrapLabel := strings.TrimSpace(*runtimeEndpoint)
	if bootstrapLabel == "" && len(runtimeSeeds) > 0 {
		bootstrapLabel = runtimeSeeds[0]
	}
	seedCount := len(runtimeSeeds)
	if strings.TrimSpace(*runtimeEndpoint) != "" {
		seedCount++
	}
	fmt.Fprintf(stdout, "dtm-ui ready http=%s runtime_bootstrap=%s runtime_seed_count=%d discovery=%s:%d lab_mode=%t lab_controls_enabled=%t ui_read_only=%t\n", *httpAddress, bootstrapLabel, seedCount, *discoveryGroup, *discoveryPort, labController != nil, labController != nil, labController == nil)

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), gracefulShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintln(stderr, err)
			if transport != nil {
				_ = transport.Close()
			}
			return 1
		}
		if transport != nil {
			_ = transport.Close()
		}
		return 0
	case err := <-serveErrors:
		if transport != nil {
			_ = transport.Close()
		}
		if errors.Is(err, http.ErrServerClosed) {
			return 0
		}
		fmt.Fprintln(stderr, err)
		return 1
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
