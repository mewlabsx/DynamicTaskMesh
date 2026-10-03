package authoritybinding

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	ErrInvalidOptions = errors.New("invalid authority binding options")
	ErrActive         = errors.New("authority binding session is already active")
	ErrClosed         = errors.New("authority binding session is closed")
)

const defaultCallTimeout = 2 * time.Second

// Client is the existing v0.4 NodeRegistry wire boundary. Keeping the
// interface here lets Runtime and static Agent share the same lifecycle
// adapter without introducing a second registration protocol.
type Client interface {
	RegisterNode(context.Context, *dtmv1.RegisterNodeRequest, ...grpc.CallOption) (*dtmv1.RegisterNodeResponse, error)
	Heartbeat(context.Context, *dtmv1.HeartbeatRequest, ...grpc.CallOption) (*dtmv1.HeartbeatResponse, error)
	UpdateNodeStatus(context.Context, *dtmv1.UpdateNodeStatusRequest, ...grpc.CallOption) (*dtmv1.UpdateNodeStatusResponse, error)
}

type DialFunc func(context.Context, string) (Client, io.Closer, error)
type RegistrationIDFunc func() (string, error)

type Options struct {
	NodeID            string
	Capabilities      []string
	ExecutionAddress  string
	HeartbeatInterval time.Duration
	CallTimeout       time.Duration
	Dial              DialFunc
	NewRegistrationID RegistrationIDFunc
}

type Session struct {
	options Options

	mu           sync.RWMutex
	client       Client
	connection   io.Closer
	registration *dtmv1.RegisterNodeRequest
	target       string
	ready        bool
	started      bool
	starting     bool
	closed       bool
	cancel       context.CancelFunc
	done         chan struct{}
	errors       chan error
	closeOnce    sync.Once
}

func New(options Options) (*Session, error) {
	if strings.TrimSpace(options.NodeID) == "" || strings.TrimSpace(options.ExecutionAddress) == "" || options.HeartbeatInterval <= 0 {
		return nil, ErrInvalidOptions
	}
	if options.CallTimeout <= 0 {
		options.CallTimeout = defaultCallTimeout
	}
	if options.Dial == nil {
		options.Dial = productionDial
	}
	if options.NewRegistrationID == nil {
		options.NewRegistrationID = NewRegistrationID
	}
	capabilities := make([]string, 0, len(options.Capabilities))
	seen := make(map[string]struct{}, len(options.Capabilities))
	for _, capability := range options.Capabilities {
		capability = strings.TrimSpace(capability)
		if capability == "" {
			return nil, ErrInvalidOptions
		}
		if _, exists := seen[capability]; exists {
			continue
		}
		seen[capability] = struct{}{}
		capabilities = append(capabilities, capability)
	}
	options.Capabilities = capabilities
	return &Session{options: options, errors: make(chan error, 4)}, nil
}

func productionDial(ctx context.Context, address string) (Client, io.Closer, error) {
	connection, err := grpc.DialContext(ctx, address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, nil, err
	}
	return dtmv1.NewNodeRegistryServiceClient(connection), connection, nil
}

func (session *Session) Start(parent context.Context, target string) error {
	if parent == nil {
		parent = context.Background()
	}
	target = strings.TrimSpace(target)
	if target == "" {
		return ErrInvalidOptions
	}

	session.mu.Lock()
	if session.closed {
		session.mu.Unlock()
		return ErrClosed
	}
	if session.started || session.starting {
		session.mu.Unlock()
		return ErrActive
	}
	session.starting = true
	session.mu.Unlock()

	callCtx, cancel := context.WithTimeout(parent, session.options.CallTimeout)
	client, connection, err := session.options.Dial(callCtx, target)
	if err == nil && client == nil {
		err = errors.New("authority dial returned nil client")
	}
	var registration *dtmv1.RegisterNodeRequest
	if err == nil {
		registrationID, idErr := session.options.NewRegistrationID()
		if idErr != nil {
			err = idErr
		} else if strings.TrimSpace(registrationID) == "" {
			err = errors.New("authority registration ID is empty")
		} else {
			registration = &dtmv1.RegisterNodeRequest{
				Node: &dtmv1.Node{
					Id: session.options.NodeID, Capabilities: append([]string(nil), session.options.Capabilities...),
					Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, ExecutionAddress: session.options.ExecutionAddress,
				},
				RegistrationId: registrationID,
			}
			_, err = client.RegisterNode(callCtx, registration)
			if err == nil {
				session.mu.Lock()
				if session.closed {
					session.starting = false
					session.mu.Unlock()
					_ = connection.Close()
					cancel()
					return ErrClosed
				}
				runCtx, runCancel := context.WithCancel(parent)
				session.client = client
				session.connection = connection
				session.registration = registration
				session.target = target
				session.ready = true
				session.started = true
				session.starting = false
				session.cancel = runCancel
				session.done = make(chan struct{})
				done := session.done
				session.mu.Unlock()
				cancel()
				go session.run(runCtx, done)
				return nil
			}
		}
	}
	cancel()
	if client != nil && registration != nil {
		_ = bestEffortOffline(client, registration.GetNode().GetId(), registration.GetRegistrationId(), session.options.CallTimeout)
	}
	if connection != nil {
		_ = connection.Close()
	}
	session.mu.Lock()
	session.starting = false
	session.mu.Unlock()
	return fmt.Errorf("authority registration at %s: %w", target, err)
}

func (session *Session) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(session.options.HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			session.setReady(false)
			return
		case sentAt := <-ticker.C:
			if err := session.heartbeat(ctx, sentAt); err != nil {
				session.setReady(false)
				select {
				case session.errors <- err:
				default:
				}
				return
			}
		}
	}
}

func (session *Session) heartbeat(parent context.Context, sentAt time.Time) error {
	session.mu.RLock()
	client := session.client
	request := session.registration
	session.mu.RUnlock()
	if client == nil || request == nil {
		return ErrClosed
	}
	callCtx, cancel := context.WithTimeout(parent, session.options.CallTimeout)
	_, err := client.Heartbeat(callCtx, &dtmv1.HeartbeatRequest{
		NodeId: request.GetNode().GetId(), RegistrationId: request.GetRegistrationId(), SentAtUnixMillis: sentAt.UnixMilli(),
	})
	cancel()
	if err == nil {
		return nil
	}
	if parent.Err() != nil {
		return nil
	}
	switch classify(err) {
	case retryHeartbeat:
		return nil
	case reregisterHeartbeat:
		registerCtx, cancelRegister := context.WithTimeout(parent, session.options.CallTimeout)
		_, registerErr := client.RegisterNode(registerCtx, request)
		cancelRegister()
		if parent.Err() != nil {
			return nil
		}
		if registerErr == nil || classify(registerErr) == retryHeartbeat {
			return nil
		}
		return fmt.Errorf("re-registration rejected: %w", registerErr)
	default:
		return fmt.Errorf("heartbeat rejected: %w", err)
	}
}

type heartbeatDisposition uint8

const (
	terminateHeartbeat heartbeatDisposition = iota
	retryHeartbeat
	reregisterHeartbeat
)

func classify(err error) heartbeatDisposition {
	if err == nil {
		return retryHeartbeat
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return retryHeartbeat
	case codes.NotFound:
		return reregisterHeartbeat
	default:
		return terminateHeartbeat
	}
}

func (session *Session) setReady(ready bool) {
	session.mu.Lock()
	session.ready = ready
	session.mu.Unlock()
}

func (session *Session) Ready() bool {
	if session == nil {
		return false
	}
	session.mu.RLock()
	defer session.mu.RUnlock()
	return session.ready && !session.closed
}

func (session *Session) Errors() <-chan error {
	if session == nil {
		return nil
	}
	return session.errors
}

func (session *Session) RegistrationID() string {
	if session == nil {
		return ""
	}
	session.mu.RLock()
	defer session.mu.RUnlock()
	if session.registration == nil {
		return ""
	}
	return session.registration.GetRegistrationId()
}

func (session *Session) Target() string {
	if session == nil {
		return ""
	}
	session.mu.RLock()
	defer session.mu.RUnlock()
	return session.target
}

func (session *Session) Close() error {
	if session == nil {
		return nil
	}
	var result error
	session.closeOnce.Do(func() {
		session.mu.Lock()
		session.closed = true
		session.ready = false
		cancel := session.cancel
		client := session.client
		connection := session.connection
		request := session.registration
		done := session.done
		session.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if done != nil {
			<-done
		}
		if client != nil && request != nil {
			if err := bestEffortOffline(client, request.GetNode().GetId(), request.GetRegistrationId(), session.options.CallTimeout); err != nil {
				result = errors.Join(result, err)
			}
		}
		if connection != nil {
			result = errors.Join(result, connection.Close())
		}
		close(session.errors)
	})
	return result
}

func bestEffortOffline(client Client, nodeID, registrationID string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := client.UpdateNodeStatus(ctx, &dtmv1.UpdateNodeStatusRequest{
		NodeId: nodeID, Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE, RegistrationId: registrationID,
	})
	switch status.Code(err) {
	case codes.OK, codes.NotFound, codes.FailedPrecondition:
		return nil
	default:
		return err
	}
}

func NewRegistrationID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate registration ID: %w", err)
	}
	return hex.EncodeToString(value), nil
}
