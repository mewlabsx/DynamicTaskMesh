package resourcesync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dtm/internal/mesh/protocol"
	"dtm/internal/mesh/resourceview"
)

type fakeClient struct {
	owners  sync.Map
	started chan protocol.Identity
	release chan struct{}
	err     error
	active  atomic.Int32
	maximum atomic.Int32
}

func (client *fakeClient) add(owner protocol.Identity) {
	client.owners.Store(owner.ControlEndpoint, owner)
}

func (client *fakeClient) GetResourceAdvertisement(ctx context.Context, endpoint string) (resourceview.Advertisement, error) {
	value, ok := client.owners.Load(endpoint)
	if !ok {
		return resourceview.Advertisement{}, fmt.Errorf("unknown endpoint %s", endpoint)
	}
	owner := value.(protocol.Identity)
	current := client.active.Add(1)
	defer client.active.Add(-1)
	for {
		maximum := client.maximum.Load()
		if current <= maximum || client.maximum.CompareAndSwap(maximum, current) {
			break
		}
	}
	client.started <- owner
	select {
	case <-ctx.Done():
		return resourceview.Advertisement{}, ctx.Err()
	case <-client.release:
	}
	if client.err != nil {
		return resourceview.Advertisement{}, client.err
	}
	return resourceview.Advertisement{Owner: owner}, nil
}

func TestSlowRequestsAreBoundedNonBlockingAndDuplicateSuppressed(t *testing.T) {
	client := &fakeClient{started: make(chan protocol.Identity, 128), release: make(chan struct{})}
	var observed atomic.Int32
	worker, err := New(client, func(resourceview.Advertisement) error { observed.Add(1); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	owners := make([]protocol.Identity, 100)
	for i := range owners {
		owners[i] = identity(i)
		client.add(owners[i])
	}
	start := time.Now()
	accepted := 0
	for _, owner := range owners {
		if worker.Request(owner) {
			accepted++
		}
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("Request backpressured caller")
	}
	if accepted == len(owners) || accepted > queueCapacity+MaxInFlight {
		t.Fatalf("bounded admission accepted=%d", accepted)
	}
	if worker.Request(owners[0]) {
		t.Fatal("duplicate active session accepted")
	}
	deadline := time.Now().Add(time.Second)
	for client.maximum.Load() < MaxInFlight && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := client.maximum.Load(); got != MaxInFlight {
		t.Fatalf("maximum=%d", got)
	}
	close(client.release)
	deadline = time.Now().Add(time.Second)
	for observed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if observed.Load() == 0 {
		t.Fatal("accepted request was not observed")
	}
}

func TestHangingRequestCancelledOnClose(t *testing.T) {
	client := &fakeClient{started: make(chan protocol.Identity, 1), release: make(chan struct{})}
	owner := identity(1)
	client.add(owner)
	worker, _ := New(client, func(resourceview.Advertisement) error { return nil })
	_ = worker.Start(context.Background())
	worker.Request(owner)
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("request did not start")
	}
	done := make(chan struct{})
	go func() { _ = worker.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel hanging RPC")
	}
}

func TestFailureIsObservableAndFutureSyncAllowed(t *testing.T) {
	failure := errors.New("pull failed")
	client := &fakeClient{started: make(chan protocol.Identity, 2), release: make(chan struct{}, 2), err: failure}
	owner := identity(1)
	client.add(owner)
	client.release <- struct{}{}
	worker, _ := New(client, func(resourceview.Advertisement) error { return nil })
	_ = worker.Start(context.Background())
	defer worker.Close()
	if !worker.Request(owner) {
		t.Fatal("request rejected")
	}
	select {
	case err := <-worker.Errors():
		if !errors.Is(err, failure) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("failure not observable")
	}
	deadline := time.Now().Add(time.Second)
	for !worker.Request(owner) && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if time.Now().After(deadline) {
		t.Fatal("failed session remained suppressed")
	}
}

func TestConcurrentRequestCloseRace(t *testing.T) {
	client := &fakeClient{started: make(chan protocol.Identity, 1000), release: make(chan struct{})}
	worker, _ := New(client, func(resourceview.Advertisement) error { return nil })
	_ = worker.Start(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		owner := identity(i)
		client.add(owner)
		wg.Add(1)
		go func() { defer wg.Done(); worker.Request(owner) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); _ = worker.Close() }()
	wg.Wait()
}

func identity(index int) protocol.Identity {
	return protocol.Identity{
		MeshNamespace: "mesh", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion,
		NodeID: fmt.Sprintf("node-%d", index), RuntimeInstance: fmt.Sprintf("session-%d", index),
		ControlEndpoint: fmt.Sprintf("127.0.0.1:%d", 46000+index),
	}
}
