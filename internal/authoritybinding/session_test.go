package authoritybinding

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeRegistryClient struct {
	mu              sync.Mutex
	registers       []*dtmv1.RegisterNodeRequest
	heartbeats      int
	heartbeatErrors []error
	updates         []*dtmv1.UpdateNodeStatusRequest
	registerErr     error
}

func (client *fakeRegistryClient) RegisterNode(
	ctx context.Context,
	request *dtmv1.RegisterNodeRequest,
	_ ...grpc.CallOption,
) (*dtmv1.RegisterNodeResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.registers = append(client.registers, request)
	if client.registerErr != nil {
		return nil, client.registerErr
	}
	return &dtmv1.RegisterNodeResponse{Accepted: true}, nil
}

func (client *fakeRegistryClient) Heartbeat(
	ctx context.Context,
	request *dtmv1.HeartbeatRequest,
	_ ...grpc.CallOption,
) (*dtmv1.HeartbeatResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.heartbeats++
	if len(client.heartbeatErrors) > 0 {
		err := client.heartbeatErrors[0]
		client.heartbeatErrors = client.heartbeatErrors[1:]
		return nil, err
	}
	return &dtmv1.HeartbeatResponse{}, nil
}

func (client *fakeRegistryClient) UpdateNodeStatus(
	ctx context.Context,
	request *dtmv1.UpdateNodeStatusRequest,
	_ ...grpc.CallOption,
) (*dtmv1.UpdateNodeStatusResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	client.updates = append(client.updates, request)
	return &dtmv1.UpdateNodeStatusResponse{}, nil
}

type fakeConnection struct {
	mu     sync.Mutex
	closed bool
}

func (connection *fakeConnection) Close() error {
	connection.mu.Lock()
	connection.closed = true
	connection.mu.Unlock()
	return nil
}

func (client *fakeRegistryClient) counts() (registers, heartbeats, updates int) {
	client.mu.Lock()
	defer client.mu.Unlock()
	return len(client.registers), client.heartbeats, len(client.updates)
}

func newFakeSession(t *testing.T, client *fakeRegistryClient, connection *fakeConnection) *Session {
	t.Helper()
	session, err := New(Options{
		NodeID: "node-a", Capabilities: []string{"temperature_sensor"}, ExecutionAddress: "127.0.0.1:41001",
		HeartbeatInterval: 5 * time.Millisecond, CallTimeout: time.Second,
		Dial:              func(context.Context, string) (Client, io.Closer, error) { return client, connection, nil },
		NewRegistrationID: func() (string, error) { return "registration-1", nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func waitForSession(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timed out waiting for authority session state")
}

func TestSessionReregistersAfterNotFoundAndOfflinesOnClose(t *testing.T) {
	client := &fakeRegistryClient{heartbeatErrors: []error{status.Error(codes.NotFound, "registration expired")}}
	connection := &fakeConnection{}
	session := newFakeSession(t, client, connection)
	if err := session.Start(context.Background(), "127.0.0.1:42001"); err != nil {
		t.Fatal(err)
	}
	if !session.Ready() || session.RegistrationID() != "registration-1" || session.Target() != "127.0.0.1:42001" {
		t.Fatalf("session state after start: ready=%t registration=%q target=%q", session.Ready(), session.RegistrationID(), session.Target())
	}
	waitForSession(t, func() bool {
		registers, heartbeats, _ := client.counts()
		return registers >= 2 && heartbeats >= 1
	})
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
	registers, _, updates := client.counts()
	if registers < 2 || updates != 1 {
		t.Fatalf("registry lifecycle registers=%d updates=%d", registers, updates)
	}
	if !connection.closed {
		t.Fatal("authority connection was not closed")
	}
}

func TestSessionInitialRegistrationFailureAttemptsOfflineCompensation(t *testing.T) {
	client := &fakeRegistryClient{registerErr: status.Error(codes.Unavailable, "authority unavailable")}
	connection := &fakeConnection{}
	session := newFakeSession(t, client, connection)
	if err := session.Start(context.Background(), "127.0.0.1:42001"); err == nil {
		t.Fatal("Start() error = nil")
	}
	waitForSession(t, func() bool {
		_, _, updates := client.counts()
		return updates == 1
	})
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSessionFatalHeartbeatPublishesErrorAndBecomesNotReady(t *testing.T) {
	client := &fakeRegistryClient{heartbeatErrors: []error{status.Error(codes.PermissionDenied, "registration rejected")}}
	connection := &fakeConnection{}
	session := newFakeSession(t, client, connection)
	if err := session.Start(context.Background(), "127.0.0.1:42001"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-session.Errors():
		if err == nil {
			t.Fatal("received nil heartbeat error")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for heartbeat error")
	}
	waitForSession(t, func() bool { return !session.Ready() })
	if err := session.Close(); err != nil {
		t.Fatal(err)
	}
}
