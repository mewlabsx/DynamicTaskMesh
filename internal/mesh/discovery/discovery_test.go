package discovery

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"dtm/internal/mesh/handshake"
	"dtm/internal/mesh/protocol"
)

type recordingHandshaker struct {
	mu        sync.Mutex
	calls     int
	active    int
	maxActive int
	result    handshake.Result
	wait      <-chan struct{}
}

func (value *recordingHandshaker) Handshake(ctx context.Context, _ string) (handshake.Result, error) {
	value.mu.Lock()
	value.calls++
	value.active++
	if value.active > value.maxActive {
		value.maxActive = value.active
	}
	result := value.result
	value.mu.Unlock()
	defer func() { value.mu.Lock(); value.active--; value.mu.Unlock() }()
	if value.wait != nil {
		select {
		case <-value.wait:
		case <-ctx.Done():
			return handshake.Result{}, ctx.Err()
		}
	}
	return result, nil
}

func (value *recordingHandshaker) maximum() int {
	value.mu.Lock()
	defer value.mu.Unlock()
	return value.maxActive
}
func (value *recordingHandshaker) activeCount() int {
	value.mu.Lock()
	defer value.mu.Unlock()
	return value.active
}
func (value *recordingHandshaker) setResult(result handshake.Result) {
	value.mu.Lock()
	value.result = result
	value.mu.Unlock()
}
func (value *recordingHandshaker) count() int {
	value.mu.Lock()
	defer value.mu.Unlock()
	return value.calls
}

func TestMemoryTransportPublishReceiveAndCancellation(t *testing.T) {
	network := NewMemoryNetwork()
	a, b := network.NewTransport(), network.NewTransport()
	defer a.Close()
	defer b.Close()
	if err := a.Publish(context.Background(), []byte("presence")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-b.Announcements():
		if string(got) != "presence" {
			t.Fatal(string(got))
		}
	case <-time.After(time.Second):
		t.Fatal("no announcement")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := a.Publish(ctx, []byte("x")); !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish=%v", err)
	}
}

func TestDiscoveryFiltersAndDeduplicates(t *testing.T) {
	network := NewMemoryNetwork()
	receiver, sender := network.NewTransport(), network.NewTransport()
	handshaker := &recordingHandshaker{result: handshake.Result{Identity: testIdentity("node-b", "session-b")}}
	service, err := New(testIdentity("node-a", "session-a"), receiver, handshaker, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := service.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	values := []protocol.Identity{testIdentity("node-a", "session-a"), withIdentity(testIdentity("node-b", "session-b"), func(v *protocol.Identity) { v.MeshNamespace = "other" }), withIdentity(testIdentity("node-b", "session-b"), func(v *protocol.Identity) { v.ProtocolMajor = 2 }), testIdentity("node-b", "session-b"), testIdentity("node-b", "session-b")}
	for _, value := range values {
		encoded, _ := protocol.Encode(value)
		if err := sender.Publish(ctx, encoded); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case result := <-service.Results():
		if result.Identity.NodeID != "node-b" {
			t.Fatal(result)
		}
	case <-time.After(time.Second):
		t.Fatal("no result")
	}
	time.Sleep(20 * time.Millisecond)
	if calls := handshaker.count(); calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
}

func TestConfirmedRefreshUpperBound(t *testing.T) {
	bound, err := ConfirmedRefreshUpperBound(time.Second, 2*time.Second)
	if err != nil || bound != 5*time.Second {
		t.Fatalf("ConfirmedRefreshUpperBound() = %v, %v", bound, err)
	}
	for _, input := range [][2]time.Duration{{0, time.Second}, {time.Second, 0}, {time.Duration(1<<63 - 1), time.Second}} {
		if _, err := ConfirmedRefreshUpperBound(input[0], input[1]); !errors.Is(err, ErrInvalidRefreshTiming) {
			t.Fatalf("ConfirmedRefreshUpperBound(%v, %v) = %v", input[0], input[1], err)
		}
	}
}

func TestDiscoveryRejectsMalformedAndShutsDown(t *testing.T) {
	network := NewMemoryNetwork()
	receiver, sender := network.NewTransport(), network.NewTransport()
	service, _ := New(testIdentity("node-a", "session-a"), receiver, &recordingHandshaker{}, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	service.Start(ctx)
	sender.Publish(ctx, []byte("bad"))
	select {
	case err := <-service.Errors():
		if !errors.Is(err, protocol.ErrInvalidPresence) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("no error")
	}
	cancel()
	done := make(chan struct{})
	go func() { service.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked")
	}
}

func TestDiscoveryLifecycle(t *testing.T) {
	t.Run("CloseBeforeStart", func(t *testing.T) {
		service := newTestService(t, &recordingHandshaker{})
		if err := service.Close(); err != nil {
			t.Fatal(err)
		}
		if err := service.Start(context.Background()); !errors.Is(err, ErrDiscoveryClosed) {
			t.Fatalf("Start() = %v", err)
		}
	})
	t.Run("StartThenClose", func(t *testing.T) {
		service := newTestService(t, &recordingHandshaker{})
		if err := service.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := service.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("DoubleClose", func(t *testing.T) {
		service := newTestService(t, &recordingHandshaker{})
		if err := service.Close(); err != nil {
			t.Fatal(err)
		}
		if err := service.Close(); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("DoubleStart", func(t *testing.T) {
		service := newTestService(t, &recordingHandshaker{})
		defer service.Close()
		if err := service.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := service.Start(context.Background()); !errors.Is(err, ErrAlreadyStarted) {
			t.Fatalf("Start() = %v", err)
		}
	})
	t.Run("StartFailureThenClose", func(t *testing.T) {
		service := newTestService(t, &recordingHandshaker{})
		if err := service.Start(nil); !errors.Is(err, ErrInvalidStart) {
			t.Fatalf("Start(nil) = %v", err)
		}
		if err := service.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestDiscoveryConcurrentStartClose(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		service := newTestService(t, &recordingHandshaker{})
		start := make(chan struct{})
		results := make(chan error, 2)
		go func() { <-start; results <- service.Start(context.Background()) }()
		go func() { <-start; results <- service.Close() }()
		close(start)
		first, second := <-results, <-results
		if first != nil && !errors.Is(first, ErrDiscoveryClosed) {
			t.Fatalf("iteration %d: %v", iteration, first)
		}
		if second != nil && !errors.Is(second, ErrDiscoveryClosed) {
			t.Fatalf("iteration %d: %v", iteration, second)
		}
		if err := service.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

type channelTransport struct {
	values chan []byte
	closed chan struct{}
	once   sync.Once
}

func newChannelTransport(size int) *channelTransport {
	return &channelTransport{values: make(chan []byte, size), closed: make(chan struct{})}
}
func (transport *channelTransport) Announcements() <-chan []byte          { return transport.values }
func (transport *channelTransport) Publish(context.Context, []byte) error { return nil }
func (transport *channelTransport) Close() error {
	transport.once.Do(func() { close(transport.closed) })
	return nil
}

func TestDiscoveryBoundsGlobalHandshakeConcurrency(t *testing.T) {
	transport := newChannelTransport(1024)
	release := make(chan struct{})
	handshaker := &recordingHandshaker{wait: release}
	service, err := New(testIdentity("node-local", "session-local"), transport, handshaker, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 512; index++ {
		peer := testIdentity(fmt.Sprintf("node-%d", index), fmt.Sprintf("session-%d", index))
		encoded, _ := protocol.Encode(peer)
		transport.values <- encoded
	}
	deadline := time.After(time.Second)
	for handshaker.count() < maxHandshakeInFlight {
		select {
		case <-deadline:
			t.Fatalf("calls=%d", handshaker.count())
		default:
			runtime.Gosched()
		}
	}
	time.Sleep(20 * time.Millisecond)
	if maximum := handshaker.maximum(); maximum > maxHandshakeInFlight {
		t.Fatalf("maximum=%d", maximum)
	}
	duplicate := testIdentity("node-0", "session-0")
	encoded, _ := protocol.Encode(duplicate)
	for index := 0; index < 100; index++ {
		transport.values <- encoded
	}
	if calls := handshaker.count(); calls != maxHandshakeInFlight {
		t.Fatalf("calls=%d", calls)
	}
	close(release)
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryRetriesPresenceDroppedAtHandshakeCapacity(t *testing.T) {
	transport := newChannelTransport(maxHandshakeInFlight + 4)
	release := make(chan struct{})
	handshaker := &recordingHandshaker{wait: release}
	service, err := New(testIdentity("node-local", "session-local"), transport, handshaker, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	for index := 0; index < maxHandshakeInFlight; index++ {
		peer := testIdentity(fmt.Sprintf("node-%d", index), fmt.Sprintf("session-%d", index))
		encoded, _ := protocol.Encode(peer)
		transport.values <- encoded
	}
	deadline := time.After(time.Second)
	for handshaker.count() < maxHandshakeInFlight {
		select {
		case <-deadline:
			t.Fatalf("calls=%d", handshaker.count())
		default:
			runtime.Gosched()
		}
	}
	dropped := testIdentity("node-dropped", "session-dropped")
	droppedEncoded, _ := protocol.Encode(dropped)
	transport.values <- droppedEncoded
	time.Sleep(20 * time.Millisecond)
	if calls := handshaker.count(); calls != maxHandshakeInFlight {
		t.Fatalf("dropped attempt calls=%d", calls)
	}
	close(release)
	deadline = time.After(time.Second)
	for handshaker.activeCount() != 0 {
		select {
		case <-deadline:
			t.Fatal("handshakes did not drain")
		default:
			runtime.Gosched()
		}
	}
	handshaker.setResult(handshake.Result{Identity: dropped})
	transport.values <- droppedEncoded
	select {
	case result := <-service.Results():
		if !result.Identity.SameSession(dropped) {
			t.Fatalf("result=%+v", result)
		}
	case <-time.After(time.Second):
		t.Fatal("repeated dropped presence was not retried")
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDiscoveryCorrelatesPresenceAndHandshakeIdentity(t *testing.T) {
	tests := []struct {
		name       string
		confirmed  protocol.Identity
		wantResult bool
	}{
		{"match", testIdentity("node-a", "session-a"), true},
		{"node mismatch", testIdentity("node-b", "session-a"), false},
		{"session mismatch", testIdentity("node-a", "session-b"), false},
		{"both mismatch", testIdentity("node-b", "session-b"), false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := newChannelTransport(4)
			handshaker := &recordingHandshaker{result: handshake.Result{Identity: test.confirmed}}
			service, _ := New(testIdentity("local", "local-session"), transport, handshaker, time.Hour)
			service.Start(context.Background())
			defer service.Close()
			advertised := testIdentity("node-a", "session-a")
			encoded, _ := protocol.Encode(advertised)
			transport.values <- encoded
			if test.wantResult {
				select {
				case <-service.Results():
				case <-time.After(time.Second):
					t.Fatal("no result")
				}
			} else {
				select {
				case err := <-service.Errors():
					if !errors.Is(err, ErrIdentityMismatch) {
						t.Fatalf("error=%v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("no mismatch error")
				}
				select {
				case result := <-service.Results():
					t.Fatalf("unexpected result=%+v", result)
				default:
				}
			}
		})
	}
}

func newTestService(t *testing.T, handshaker Handshaker) *Service {
	t.Helper()
	network := NewMemoryNetwork()
	service, err := New(testIdentity("node-a", "session-a"), network.NewTransport(), handshaker, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return service
}

func testIdentity(node, session string) protocol.Identity {
	return protocol.Identity{MeshNamespace: "mesh-a", ProtocolMajor: 1, DTMVersion: protocol.DTMVersion, NodeID: node, RuntimeInstance: session, ControlEndpoint: "127.0.0.1:45893"}
}
func withIdentity(value protocol.Identity, change func(*protocol.Identity)) protocol.Identity {
	change(&value)
	return value
}
