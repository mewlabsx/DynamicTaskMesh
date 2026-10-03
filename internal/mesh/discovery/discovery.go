package discovery

import (
	"context"
	"errors"
	"sync"
	"time"

	"dtm/internal/mesh/handshake"
	"dtm/internal/mesh/protocol"
)

type Handshaker interface {
	Handshake(context.Context, string) (handshake.Result, error)
}

const maxHandshakeInFlight = 32

const retryIntervals = 2

var (
	ErrAlreadyStarted       = errors.New("peer discovery already started")
	ErrDiscoveryClosed      = errors.New("peer discovery is closed")
	ErrInvalidStart         = errors.New("peer discovery start context is required")
	ErrIdentityMismatch     = errors.New("presence and handshake identities do not match")
	ErrInvalidRefreshTiming = errors.New("invalid discovery confirmed refresh timing")
)

type lifecycleState uint8

const (
	stateNew lifecycleState = iota
	stateStarted
	stateClosed
)

type IdentityMismatchError struct {
	Advertised protocol.Identity
	Confirmed  protocol.Identity
}

func (mismatch *IdentityMismatchError) Error() string {
	return "presence session identity does not match direct handshake response"
}

func (*IdentityMismatchError) Unwrap() error { return ErrIdentityMismatch }

type Service struct {
	local                protocol.Identity
	transport            Transport
	handshaker           Handshaker
	interval, retryAfter time.Duration
	results              chan handshake.Result
	errors               chan error
	cancel               context.CancelFunc
	mu                   sync.Mutex
	state                lifecycleState
	attempts             map[string]time.Time
	inFlight             map[string]struct{}
	handshakeSlots       chan struct{}
	close                sync.Once
	active               sync.WaitGroup
}

func New(local protocol.Identity, transport Transport, handshaker Handshaker, interval time.Duration) (*Service, error) {
	if err := local.Validate(); err != nil || transport == nil || handshaker == nil || interval <= 0 {
		return nil, errors.New("invalid peer discovery configuration")
	}
	return &Service{local: local, transport: transport, handshaker: handshaker, interval: interval, retryAfter: interval * retryIntervals, results: make(chan handshake.Result, 32), errors: make(chan error, 32), attempts: make(map[string]time.Time), inFlight: make(map[string]struct{}), handshakeSlots: make(chan struct{}, maxHandshakeInFlight)}, nil
}

// ConfirmedRefreshUpperBound is the conservative normal-case interval between
// successful Membership refreshes when a handshake slot is available:
// retry suppression + the next periodic Presence opportunity + handshake
// completion budget.
func ConfirmedRefreshUpperBound(interval, handshakeBudget time.Duration) (time.Duration, error) {
	if interval <= 0 || handshakeBudget <= 0 || interval > (time.Duration(1<<63-1)-handshakeBudget)/(retryIntervals+1) {
		return 0, ErrInvalidRefreshTiming
	}
	return (retryIntervals+1)*interval + handshakeBudget, nil
}

func (service *Service) Start(ctx context.Context) error {
	if ctx == nil {
		return ErrInvalidStart
	}
	service.mu.Lock()
	switch service.state {
	case stateStarted:
		service.mu.Unlock()
		return ErrAlreadyStarted
	case stateClosed:
		service.mu.Unlock()
		return ErrDiscoveryClosed
	}
	encoded, err := protocol.Encode(service.local)
	if err != nil {
		service.mu.Unlock()
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	service.cancel = cancel
	service.state = stateStarted
	service.active.Add(2)
	go service.publishLoop(runCtx, encoded)
	go service.receiveLoop(runCtx)
	service.mu.Unlock()
	return nil
}

func (service *Service) publishLoop(ctx context.Context, encoded []byte) {
	defer service.active.Done()
	ticker := time.NewTicker(service.interval)
	defer ticker.Stop()
	service.publish(ctx, encoded)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			service.publish(ctx, encoded)
		}
	}
}
func (service *Service) publish(ctx context.Context, encoded []byte) {
	if err := service.transport.Publish(ctx, encoded); err != nil && ctx.Err() == nil {
		service.report(err)
	}
}
func (service *Service) receiveLoop(ctx context.Context) {
	defer service.active.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case value, ok := <-service.transport.Announcements():
			if !ok {
				return
			}
			service.observe(ctx, value)
		}
	}
}

func (service *Service) observe(ctx context.Context, value []byte) {
	peer, err := protocol.Decode(value)
	if err != nil {
		service.report(err)
		return
	}
	if service.local.SameSession(peer) || peer.MeshNamespace != service.local.MeshNamespace || peer.ProtocolMajor != service.local.ProtocolMajor {
		return
	}
	key := peer.NodeID + "\x00" + peer.RuntimeInstance
	now := time.Now()
	service.mu.Lock()
	if service.state != stateStarted {
		service.mu.Unlock()
		return
	}
	if _, exists := service.inFlight[key]; exists || !service.attempts[key].IsZero() && now.Sub(service.attempts[key]) < service.retryAfter {
		service.mu.Unlock()
		return
	}
	select {
	case service.handshakeSlots <- struct{}{}:
	default:
		service.mu.Unlock()
		return
	}
	if len(service.attempts) >= 256 {
		var oldestKey string
		var oldest time.Time
		for candidate, at := range service.attempts {
			if oldest.IsZero() || at.Before(oldest) {
				oldestKey, oldest = candidate, at
			}
		}
		delete(service.attempts, oldestKey)
	}
	service.inFlight[key] = struct{}{}
	service.active.Add(1)
	service.mu.Unlock()
	go func() {
		defer service.active.Done()
		defer func() { <-service.handshakeSlots }()
		result, callErr := service.handshaker.Handshake(ctx, peer.ControlEndpoint)
		service.mu.Lock()
		delete(service.inFlight, key)
		service.attempts[key] = time.Now()
		service.mu.Unlock()
		if callErr != nil {
			if ctx.Err() == nil {
				service.report(callErr)
			}
			return
		}
		if result.Identity.NodeID != peer.NodeID || result.Identity.RuntimeInstance != peer.RuntimeInstance {
			service.report(&IdentityMismatchError{Advertised: peer, Confirmed: result.Identity})
			return
		}
		select {
		case service.results <- result:
		case <-ctx.Done():
		}
	}()
}
func (service *Service) report(err error) {
	select {
	case service.errors <- err:
	default:
	}
}
func (service *Service) Results() <-chan handshake.Result { return service.results }
func (service *Service) Errors() <-chan error             { return service.errors }
func (service *Service) Close() error {
	var result error
	service.close.Do(func() {
		service.mu.Lock()
		service.state = stateClosed
		cancel := service.cancel
		service.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		result = service.transport.Close()
		service.active.Wait()
		close(service.results)
		close(service.errors)
	})
	return result
}
