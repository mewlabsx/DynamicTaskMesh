package resourcesync

import (
	"context"
	"errors"
	"sync"

	"dtm/internal/mesh/protocol"
	"dtm/internal/mesh/resourceview"
)

const MaxInFlight = 4
const queueCapacity = 32

var ErrClosed = errors.New("mesh resource sync is closed")

type Client interface {
	GetResourceAdvertisement(context.Context, string) (resourceview.Advertisement, error)
}

type Request struct {
	Owner protocol.Identity
}

type Sync struct {
	client   Client
	observe  func(resourceview.Advertisement) error
	requests chan Request
	errors   chan error
	mu       sync.Mutex
	active   map[string]struct{}
	started  bool
	closed   bool
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func New(client Client, observe func(resourceview.Advertisement) error) (*Sync, error) {
	if client == nil || observe == nil {
		return nil, errors.New("invalid mesh resource sync configuration")
	}
	return &Sync{
		client: client, observe: observe,
		requests: make(chan Request, queueCapacity), errors: make(chan error, queueCapacity),
		active: map[string]struct{}{},
	}, nil
}

func (worker *Sync) Start(parent context.Context) error {
	worker.mu.Lock()
	defer worker.mu.Unlock()
	if worker.closed {
		return ErrClosed
	}
	if worker.started {
		return nil
	}
	ctx, cancel := context.WithCancel(parent)
	worker.cancel = cancel
	worker.started = true
	worker.wg.Add(MaxInFlight)
	for i := 0; i < MaxInFlight; i++ {
		go worker.run(ctx)
	}
	return nil
}

// Request is non-blocking. Duplicate queued/in-flight sessions and capacity
// saturation are dropped; a later successful Handshake can request again.
func (worker *Sync) Request(owner protocol.Identity) bool {
	key := sessionKey(owner)
	worker.mu.Lock()
	if worker.closed || !worker.started {
		worker.mu.Unlock()
		return false
	}
	if _, exists := worker.active[key]; exists {
		worker.mu.Unlock()
		return false
	}
	worker.active[key] = struct{}{}
	select {
	case worker.requests <- Request{Owner: owner}:
		worker.mu.Unlock()
		return true
	default:
		delete(worker.active, key)
		worker.mu.Unlock()
		return false
	}
}

func (worker *Sync) run(ctx context.Context) {
	defer worker.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case request := <-worker.requests:
			advertisement, err := worker.client.GetResourceAdvertisement(ctx, request.Owner.ControlEndpoint)
			if err == nil && !advertisement.Owner.SameSession(request.Owner) {
				err = errors.New("resource advertisement owner session mismatch")
			}
			if err == nil {
				err = worker.observe(advertisement)
			}
			if err != nil && ctx.Err() == nil {
				select {
				case worker.errors <- err:
				default:
				}
			}
			worker.mu.Lock()
			delete(worker.active, sessionKey(request.Owner))
			worker.mu.Unlock()
		}
	}
}

func (worker *Sync) Errors() <-chan error { return worker.errors }
func (worker *Sync) Close() error {
	worker.mu.Lock()
	if worker.closed {
		worker.mu.Unlock()
		return nil
	}
	worker.closed = true
	cancel := worker.cancel
	worker.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	worker.wg.Wait()
	close(worker.errors)
	return nil
}
func sessionKey(owner protocol.Identity) string {
	return owner.NodeID + "\x00" + owner.RuntimeInstance
}
