package main

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type runtimeStatusStub struct {
	response *dtmv1.GetRuntimeStatusResponse
	err      error
	call     func(context.Context) (*dtmv1.GetRuntimeStatusResponse, error)
}

func (stub runtimeStatusStub) GetRuntimeStatus(ctx context.Context, _ *dtmv1.GetRuntimeStatusRequest, _ ...grpc.CallOption) (*dtmv1.GetRuntimeStatusResponse, error) {
	if stub.call != nil {
		return stub.call(ctx)
	}
	return stub.response, stub.err
}

type runtimeStatusConnection struct{}

func (runtimeStatusConnection) Close() error { return nil }

func TestResolveRuntimeCoreAddressUsesReadyBootstrap(t *testing.T) {
	var addresses []string
	got, err := resolveRuntimeCoreAddress(context.Background(), "runtime-a:4100", func(_ context.Context, address string) (runtimeStatusClient, connectionCloser, error) {
		addresses = append(addresses, address)
		return runtimeStatusStub{response: &dtmv1.GetRuntimeStatusResponse{
			Readiness:   dtmv1.RuntimeReadiness_RUNTIME_READINESS_READY,
			CoreAddress: "core-a:4200",
		}}, runtimeStatusConnection{}, nil
	})
	if err != nil || got != "core-a:4200" {
		t.Fatalf("resolveRuntimeCoreAddress() = %q, %v", got, err)
	}
	if len(addresses) != 1 || addresses[0] != "runtime-a:4100" {
		t.Fatalf("status addresses = %#v", addresses)
	}
}

func TestResolveRuntimeCoreAddressRedirectsExactlyOnce(t *testing.T) {
	var addresses []string
	responses := map[string]*dtmv1.GetRuntimeStatusResponse{
		"runtime-b:4100": {
			Readiness:   dtmv1.RuntimeReadiness_RUNTIME_READINESS_NOT_COORDINATOR,
			Coordinator: &dtmv1.RuntimeIdentity{NodeId: "node-a", RuntimeInstanceId: "session-a", AdvertisedControlEndpoint: "runtime-a:4100"},
		},
		"runtime-a:4100": {
			Readiness: dtmv1.RuntimeReadiness_RUNTIME_READINESS_READY, CoreAddress: "core-a:4200",
			Coordinator: &dtmv1.RuntimeIdentity{NodeId: "node-a", RuntimeInstanceId: "session-a", AdvertisedControlEndpoint: "runtime-a:4100"},
		},
	}
	got, err := resolveRuntimeCoreAddress(context.Background(), "runtime-b:4100", func(_ context.Context, address string) (runtimeStatusClient, connectionCloser, error) {
		addresses = append(addresses, address)
		response, ok := responses[address]
		if !ok {
			return nil, nil, errors.New("unexpected address")
		}
		return runtimeStatusStub{response: response}, runtimeStatusConnection{}, nil
	})
	if err != nil || got != "core-a:4200" {
		t.Fatalf("resolveRuntimeCoreAddress() = %q, %v", got, err)
	}
	if len(addresses) != 2 || addresses[0] != "runtime-b:4100" || addresses[1] != "runtime-a:4100" {
		t.Fatalf("status addresses = %#v", addresses)
	}
}

func TestResolveRuntimeCoreAddressStopsAfterRedirectIfCoordinatorChanges(t *testing.T) {
	responses := []*dtmv1.GetRuntimeStatusResponse{
		{
			Readiness:   dtmv1.RuntimeReadiness_RUNTIME_READINESS_NOT_COORDINATOR,
			Coordinator: &dtmv1.RuntimeIdentity{NodeId: "node-a", RuntimeInstanceId: "session-a", AdvertisedControlEndpoint: "runtime-a:4100"},
		},
		{
			Readiness:   dtmv1.RuntimeReadiness_RUNTIME_READINESS_NOT_COORDINATOR,
			Coordinator: &dtmv1.RuntimeIdentity{NodeId: "node-c", RuntimeInstanceId: "session-c", AdvertisedControlEndpoint: "runtime-c:4100"},
		},
	}
	index := 0
	_, err := resolveRuntimeCoreAddress(context.Background(), "runtime-b:4100", func(_ context.Context, _ string) (runtimeStatusClient, connectionCloser, error) {
		response := responses[index]
		index++
		return runtimeStatusStub{response: response}, runtimeStatusConnection{}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "redirect stopped") {
		t.Fatalf("resolveRuntimeCoreAddress() error = %v", err)
	}
}

func TestResolveRuntimeCoreAddressBoundsBootstrapDial(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := resolveRuntimeCoreAddress(parent, "unreachable-runtime:4100", func(ctx context.Context, _ string) (runtimeStatusClient, connectionCloser, error) {
		assertRuntimeBootstrapDeadline(t, ctx)
		<-ctx.Done()
		return nil, nil, ctx.Err()
	})
	if status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "bootstrap dial") {
		t.Fatalf("bounded bootstrap dial error = %v", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("bootstrap dial was not bounded: %s", elapsed)
	}
}

func TestResolveRuntimeCoreAddressBoundsBootstrapStatusRPC(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err := resolveRuntimeCoreAddress(parent, "runtime-a:4100", func(_ context.Context, _ string) (runtimeStatusClient, connectionCloser, error) {
		return runtimeStatusStub{call: func(ctx context.Context) (*dtmv1.GetRuntimeStatusResponse, error) {
			assertRuntimeBootstrapDeadline(t, ctx)
			<-ctx.Done()
			return nil, ctx.Err()
		}}, runtimeStatusConnection{}, nil
	})
	if status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "bootstrap status RPC") {
		t.Fatalf("bounded bootstrap status error = %v", err)
	}
}

func TestResolveRuntimeCoreAddressBoundsRedirectDial(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var calls int
	_, err := resolveRuntimeCoreAddress(parent, "runtime-b:4100", func(ctx context.Context, address string) (runtimeStatusClient, connectionCloser, error) {
		calls++
		if calls == 1 {
			return runtimeStatusStub{response: &dtmv1.GetRuntimeStatusResponse{
				Readiness:   dtmv1.RuntimeReadiness_RUNTIME_READINESS_NOT_COORDINATOR,
				Coordinator: &dtmv1.RuntimeIdentity{NodeId: "node-a", RuntimeInstanceId: "session-a", AdvertisedControlEndpoint: "runtime-a:4100"},
			}}, runtimeStatusConnection{}, nil
		}
		if address != "runtime-a:4100" {
			t.Fatalf("redirect address = %q", address)
		}
		assertRuntimeBootstrapDeadline(t, ctx)
		<-ctx.Done()
		return nil, nil, ctx.Err()
	})
	if status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "coordinator redirect dial") {
		t.Fatalf("bounded redirect dial error = %v", err)
	}
}

func TestResolveRuntimeCoreAddressBoundsRedirectStatusRPC(t *testing.T) {
	parent, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var calls int
	_, err := resolveRuntimeCoreAddress(parent, "runtime-b:4100", func(ctx context.Context, _ string) (runtimeStatusClient, connectionCloser, error) {
		calls++
		if calls == 1 {
			return runtimeStatusStub{response: &dtmv1.GetRuntimeStatusResponse{
				Readiness:   dtmv1.RuntimeReadiness_RUNTIME_READINESS_NOT_COORDINATOR,
				Coordinator: &dtmv1.RuntimeIdentity{NodeId: "node-a", RuntimeInstanceId: "session-a", AdvertisedControlEndpoint: "runtime-a:4100"},
			}}, runtimeStatusConnection{}, nil
		}
		return runtimeStatusStub{call: func(ctx context.Context) (*dtmv1.GetRuntimeStatusResponse, error) {
			assertRuntimeBootstrapDeadline(t, ctx)
			<-ctx.Done()
			return nil, ctx.Err()
		}}, runtimeStatusConnection{}, nil
	})
	if status.Code(err) != codes.DeadlineExceeded || !strings.Contains(err.Error(), "coordinator redirect status RPC") {
		t.Fatalf("bounded redirect status error = %v", err)
	}
}

func assertRuntimeBootstrapDeadline(t *testing.T, ctx context.Context) {
	t.Helper()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("runtime bootstrap context has no deadline")
	}
	if time.Until(deadline) > runtimeBootstrapTimeout+100*time.Millisecond {
		t.Fatalf("runtime bootstrap deadline is too far away: %s", time.Until(deadline))
	}
}
