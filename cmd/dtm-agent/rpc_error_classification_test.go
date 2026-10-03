package main

import (
	"context"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rpcMatrixClient struct {
	mu               sync.Mutex
	heartbeatErrors  []error
	heartbeatDefault error
	registerErrors   []error
	registerDefault  error
	heartbeatIDs     []string
	registerIDs      []string
	calls            chan string
}

func (client *rpcMatrixClient) Heartbeat(_ context.Context, request *dtmv1.HeartbeatRequest, _ ...grpc.CallOption) (*dtmv1.HeartbeatResponse, error) {
	client.mu.Lock()
	client.heartbeatIDs = append(client.heartbeatIDs, request.GetRegistrationId())
	err := client.heartbeatDefault
	if len(client.heartbeatErrors) > 0 {
		err = client.heartbeatErrors[0]
		client.heartbeatErrors = client.heartbeatErrors[1:]
	}
	client.mu.Unlock()
	client.calls <- "heartbeat"
	if err != nil {
		return nil, err
	}
	return &dtmv1.HeartbeatResponse{Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE}, nil
}

func (client *rpcMatrixClient) RegisterNode(_ context.Context, request *dtmv1.RegisterNodeRequest, _ ...grpc.CallOption) (*dtmv1.RegisterNodeResponse, error) {
	client.mu.Lock()
	client.registerIDs = append(client.registerIDs, request.GetRegistrationId())
	err := client.registerDefault
	if len(client.registerErrors) > 0 {
		err = client.registerErrors[0]
		client.registerErrors = client.registerErrors[1:]
	}
	client.mu.Unlock()
	client.calls <- "register"
	if err != nil {
		return nil, err
	}
	return &dtmv1.RegisterNodeResponse{Accepted: true}, nil
}

func (*rpcMatrixClient) UpdateNodeStatus(context.Context, *dtmv1.UpdateNodeStatusRequest, ...grpc.CallOption) (*dtmv1.UpdateNodeStatusResponse, error) {
	return &dtmv1.UpdateNodeStatusResponse{Accepted: true}, nil
}

func TestNODE017RegistrationErrorMatrix(t *testing.T) {
	tests := []struct {
		name      string
		code      codes.Code
		wantRetry bool
	}{
		{name: "unavailable", code: codes.Unavailable, wantRetry: true},
		{name: "deadline exceeded", code: codes.DeadlineExceeded, wantRetry: true},
		{name: "resource exhausted", code: codes.ResourceExhausted, wantRetry: true},
		{name: "aborted", code: codes.Aborted, wantRetry: true},
		{name: "failed precondition", code: codes.FailedPrecondition},
		{name: "invalid argument", code: codes.InvalidArgument},
		{name: "permission denied", code: codes.PermissionDenied},
		{name: "unauthenticated", code: codes.Unauthenticated},
		{name: "already exists", code: codes.AlreadyExists},
		{name: "not found again", code: codes.NotFound},
		{name: "internal", code: codes.Internal},
		{name: "unknown", code: codes.Unknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &rpcMatrixClient{
				heartbeatDefault: status.Error(codes.NotFound, "lease missing"),
				registerErrors:   []error{status.Error(test.code, "injected registration result")},
				calls:            make(chan string, 16),
			}
			done, cancel := runReregistrationLoopForTest(t, client, "registration-matrix")
			defer cancel()
			if test.wantRetry {
				waitForRPCCalls(t, client, 2, 2)
				assertStableRPCRegistrationID(t, client, "registration-matrix")
				cancel()
				if err := <-done; err != nil {
					t.Fatalf("runHeartbeatLoop() after retry cancellation=%v", err)
				}
				return
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("runHeartbeatLoop() terminal registration error=nil")
				}
			case <-time.After(time.Second):
				t.Fatal("runHeartbeatLoop() did not terminate after permanent registration error")
			}
			assertExactRPCCalls(t, client, 1, 1)
			assertStableRPCRegistrationID(t, client, "registration-matrix")
		})
	}
}

func TestNODE017HeartbeatErrorMatrix(t *testing.T) {
	tests := []struct {
		name           string
		code           codes.Code
		wantRetry      bool
		wantReregister bool
	}{
		{name: "unavailable", code: codes.Unavailable, wantRetry: true},
		{name: "deadline exceeded", code: codes.DeadlineExceeded, wantRetry: true},
		{name: "resource exhausted", code: codes.ResourceExhausted, wantRetry: true},
		{name: "aborted", code: codes.Aborted, wantRetry: true},
		{name: "not found", code: codes.NotFound, wantReregister: true},
		{name: "failed precondition", code: codes.FailedPrecondition},
		{name: "invalid argument", code: codes.InvalidArgument},
		{name: "permission denied", code: codes.PermissionDenied},
		{name: "unauthenticated", code: codes.Unauthenticated},
		{name: "already exists", code: codes.AlreadyExists},
		{name: "canceled by remote", code: codes.Canceled},
		{name: "internal", code: codes.Internal},
		{name: "unknown", code: codes.Unknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := &rpcMatrixClient{
				heartbeatErrors: []error{status.Error(test.code, "injected heartbeat result")},
				calls:           make(chan string, 16),
			}
			done, cancel := runReregistrationLoopForTest(t, client, "heartbeat-matrix")
			defer cancel()
			switch {
			case test.wantRetry:
				waitForRPCCalls(t, client, 2, 0)
			case test.wantReregister:
				waitForRPCCalls(t, client, 2, 1)
			default:
				select {
				case err := <-done:
					if err == nil {
						t.Fatal("runHeartbeatLoop() terminal heartbeat error=nil")
					}
				case <-time.After(time.Second):
					t.Fatal("runHeartbeatLoop() did not terminate after permanent heartbeat error")
				}
				assertExactRPCCalls(t, client, 1, 0)
				assertStableRPCRegistrationID(t, client, "heartbeat-matrix")
				return
			}
			assertStableRPCRegistrationID(t, client, "heartbeat-matrix")
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("runHeartbeatLoop() after continued heartbeat cancellation=%v", err)
			}
		})
	}
}

func TestNODE017HeartbeatContextCancellationIsNormalShutdown(t *testing.T) {
	client := &rpcMatrixClient{calls: make(chan string, 4)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() {
		done <- runHeartbeatLoop(ctx, client, "agent-fenced", "registration-cancel", time.Millisecond, registrationRequestForAgentTest("registration-cancel"))
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runHeartbeatLoop() active cancellation=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runHeartbeatLoop() did not stop after context cancellation")
	}
	assertExactRPCCalls(t, client, 0, 0)
}

func waitForRPCCalls(t *testing.T, client *rpcMatrixClient, wantHeartbeat, wantRegister int) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		heartbeats, registers := rpcCallCounts(client)
		if heartbeats >= wantHeartbeat && registers >= wantRegister {
			return
		}
		select {
		case <-client.calls:
		case <-timer.C:
			t.Fatalf("RPC counts before timeout: heartbeat=%d register=%d, want at least %d/%d", heartbeats, registers, wantHeartbeat, wantRegister)
		}
	}
}

func assertExactRPCCalls(t *testing.T, client *rpcMatrixClient, wantHeartbeat, wantRegister int) {
	t.Helper()
	heartbeats, registers := rpcCallCounts(client)
	if heartbeats != wantHeartbeat || registers != wantRegister {
		t.Fatalf("RPC counts: heartbeat=%d register=%d, want %d/%d", heartbeats, registers, wantHeartbeat, wantRegister)
	}
}

func rpcCallCounts(client *rpcMatrixClient) (int, int) {
	client.mu.Lock()
	defer client.mu.Unlock()
	return len(client.heartbeatIDs), len(client.registerIDs)
}

func assertStableRPCRegistrationID(t *testing.T, client *rpcMatrixClient, want string) {
	t.Helper()
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.heartbeatIDs) == 0 {
		t.Fatal("no Heartbeat calls recorded")
	}
	for _, registrationID := range append(append([]string(nil), client.heartbeatIDs...), client.registerIDs...) {
		if registrationID != want {
			t.Fatalf("registration IDs: heartbeat=%v register=%v, want %q", client.heartbeatIDs, client.registerIDs, want)
		}
	}
}

var _ dtmv1.NodeRegistryServiceClient = (*rpcMatrixClient)(nil)
