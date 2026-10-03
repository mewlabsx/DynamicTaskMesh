package grpcapi

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/capability"
	"dtm/internal/model"
	"dtm/internal/node"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRegisterNodeLogsIdentityCapabilityAndEndpoint(t *testing.T) {
	calls := &[]string{}
	var output bytes.Buffer
	server, err := NewRegistryServer(
		&recordingRegistry{calls: calls},
		&recordingEndpoints{calls: calls},
		WithRegistryLogger(log.New(&output, "", 0)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.RegisterNode(context.Background(), validRegisterRequest()); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"node registered",
		"node_id=node-1",
		"capabilities=temperature_sensor",
		"endpoint=127.0.0.1:50061",
	} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("registry log = %q, want %q", output.String(), want)
		}
	}
}

type recordingRegistry struct {
	calls        *[]string
	registered   []node.Node
	statusID     model.NodeID
	status       node.Status
	setStatusErr error
}

func (r *recordingRegistry) Register(n node.Node) {
	*r.calls = append(*r.calls, "registry.register")
	r.registered = append(r.registered, n)
}

func (r *recordingRegistry) SetStatus(id model.NodeID, nodeStatus node.Status) error {
	*r.calls = append(*r.calls, "registry.set_status")
	r.statusID = id
	r.status = nodeStatus
	return r.setStatusErr
}

type recordingEndpoints struct {
	calls      *[]string
	setID      model.NodeID
	setAddress string
	setErr     error
	deletedID  model.NodeID
	available  bool
}

func (e *recordingEndpoints) Prepare(id model.NodeID, address string) error {
	*e.calls = append(*e.calls, "endpoint.set")
	e.setID = id
	e.setAddress = address
	return e.setErr
}

func (e *recordingEndpoints) Activate(model.NodeID) { e.available = true }

func (e *recordingEndpoints) Available(model.NodeID) bool { return e.available }

func (e *recordingEndpoints) Delete(id model.NodeID) {
	*e.calls = append(*e.calls, "endpoint.delete")
	e.deletedID = id
	e.available = false
}

func TestNewRegistryServerRejectsNilAndTypedNilPorts(t *testing.T) {
	calls := []string{}
	validRegistry := &recordingRegistry{calls: &calls}
	validEndpoints := &recordingEndpoints{calls: &calls}
	var nilRegistry *recordingRegistry
	var nilEndpoints *recordingEndpoints

	tests := []struct {
		name      string
		registry  RegistryPort
		endpoints EndpointPort
		wantErr   error
	}{
		{name: "nil registry", endpoints: validEndpoints, wantErr: ErrInvalidRegistryPort},
		{name: "typed nil registry", registry: nilRegistry, endpoints: validEndpoints, wantErr: ErrInvalidRegistryPort},
		{name: "nil endpoints", registry: validRegistry, wantErr: ErrInvalidEndpointPort},
		{name: "typed nil endpoints", registry: validRegistry, endpoints: nilEndpoints, wantErr: ErrInvalidEndpointPort},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewRegistryServer(tt.registry, tt.endpoints); !errors.Is(err, tt.wantErr) {
				t.Fatalf("NewRegistryServer() error = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestRegisterNodeRejectsInvalidRequestsBeforePublishing(t *testing.T) {
	tests := []struct {
		name    string
		request *dtmv1.RegisterNodeRequest
	}{
		{name: "nil request"},
		{name: "nil node", request: &dtmv1.RegisterNodeRequest{}},
		{name: "blank registration ID", request: func() *dtmv1.RegisterNodeRequest {
			request := validRegisterRequest()
			request.RegistrationId = " "
			return request
		}()},
		{name: "blank ID", request: registerRequest(" ", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "127.0.0.1:50061")},
		{name: "blank execution address", request: registerRequest("node-1", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, " ")},
		{name: "no capabilities", request: registerRequest("node-1", nil, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "127.0.0.1:50061")},
		{name: "blank capability", request: registerRequest("node-1", []string{" "}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "127.0.0.1:50061")},
		{name: "duplicate capability", request: registerRequest("node-1", []string{"temperature_sensor", " temperature_sensor "}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "127.0.0.1:50061")},
		{name: "unspecified status", request: registerRequest("node-1", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_UNSPECIFIED, "127.0.0.1:50061")},
		{name: "offline status", request: registerRequest("node-1", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_OFFLINE, "127.0.0.1:50061")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, registry, endpoints, calls := newRecordingRegistryServer(t)

			response, err := server.RegisterNode(context.Background(), tt.request)

			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("RegisterNode() code = %v, want InvalidArgument (error %v)", status.Code(err), err)
			}
			if response != nil {
				t.Fatalf("RegisterNode() response = %#v, want nil", response)
			}
			if len(*calls) != 0 || len(registry.registered) != 0 || endpoints.setID != "" {
				t.Fatalf("invalid request published state: calls=%v registry=%v endpoint=%q", *calls, registry.registered, endpoints.setID)
			}
		})
	}
}

func TestRegisterNodePublishesEndpointBeforeRegistry(t *testing.T) {
	server, registry, endpoints, calls := newRecordingRegistryServer(t)

	response, err := server.RegisterNode(context.Background(), registerRequest(
		" node-1 ",
		[]string{" temperature_sensor ", "cooling_control"},
		dtmv1.NodeStatus_NODE_STATUS_ONLINE,
		" 127.0.0.1:50061 ",
	))

	if err != nil {
		t.Fatalf("RegisterNode() error = %v", err)
	}
	if response == nil || !response.GetAccepted() {
		t.Fatalf("RegisterNode() response = %#v, want accepted", response)
	}
	assertCalls(t, *calls, "endpoint.set", "registry.register", "registry.set_status")
	if endpoints.setID != model.NodeID("node-1") || endpoints.setAddress != "127.0.0.1:50061" {
		t.Fatalf("endpoint publication = (%q, %q)", endpoints.setID, endpoints.setAddress)
	}
	if len(registry.registered) != 1 {
		t.Fatalf("registered nodes = %d, want 1", len(registry.registered))
	}
	got := registry.registered[0]
	if got.ID() != model.NodeID("node-1") || got.Status() != node.StatusRegistered {
		t.Fatalf("registered node = (%q, %q)", got.ID(), got.Status())
	}
	if registry.statusID != model.NodeID("node-1") || registry.status != node.StatusActive {
		t.Fatalf("activated node = (%q, %q)", registry.statusID, registry.status)
	}
	capabilities := got.Capabilities()
	if len(capabilities) != 2 || capabilities[0] != model.Capability("temperature_sensor") || capabilities[1] != model.Capability("cooling_control") {
		t.Fatalf("capabilities = %v", capabilities)
	}
}

func TestRegisterNodeEndpointFailureDoesNotPublishRegistry(t *testing.T) {
	server, registry, endpoints, calls := newRecordingRegistryServer(t)
	endpoints.setErr = ErrInvalidEndpoint

	response, err := server.RegisterNode(context.Background(), validRegisterRequest())

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("RegisterNode() code = %v, want Unavailable (error %v)", status.Code(err), err)
	}
	if response != nil {
		t.Fatalf("RegisterNode() response = %#v, want nil", response)
	}
	assertCalls(t, *calls, "endpoint.set")
	if len(registry.registered) != 0 {
		t.Fatalf("registry received %d nodes, want 0", len(registry.registered))
	}
}

func TestRegisterNodeReplacesEndpointAndRegistryForSameID(t *testing.T) {
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	server, err := NewRegistryServer(registry, endpoints)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := server.RegisterNode(context.Background(), validRegisterRequest()); err != nil {
		t.Fatal(err)
	}
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-1", RegistrationId: "registration-1", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	second := registerRequest("node-1", []string{"cooling_control"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "127.0.0.1:50062")
	second.RegistrationId = "registration-2"
	if _, err := server.RegisterNode(context.Background(), second); err != nil {
		t.Fatal(err)
	}

	address, err := endpoints.Resolve(model.NodeID("node-1"))
	if err != nil {
		t.Fatal(err)
	}
	if address != "127.0.0.1:50062" {
		t.Fatalf("Resolve() = %q, want replacement address", address)
	}
	if got := registry.Discover(model.Capability("temperature_sensor")); len(got) != 0 {
		t.Fatalf("old capability still registered: %v", got)
	}
	if got := registry.Discover(model.Capability("cooling_control")); len(got) != 1 || got[0].ID() != model.NodeID("node-1") {
		t.Fatalf("replacement node = %v", got)
	}
}

func TestOldRegistrationCannotOfflineReplacementForSameNodeID(t *testing.T) {
	registry := capability.NewRegistry()
	endpoints := NewEndpointDirectory()
	server, err := NewRegistryServer(registry, endpoints)
	if err != nil {
		t.Fatal(err)
	}

	first := registerRequest("node-1", []string{"capability-a"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "address-a")
	first.RegistrationId = "registration-old"
	if _, err := server.RegisterNode(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	if _, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{NodeId: "node-1", RegistrationId: "registration-old", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE}); err != nil {
		t.Fatal(err)
	}
	second := registerRequest("node-1", []string{"capability-b"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "address-b")
	second.RegistrationId = "registration-new"
	if _, err := server.RegisterNode(context.Background(), second); err != nil {
		t.Fatal(err)
	}

	response, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{
		NodeId:         "node-1",
		Status:         dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		RegistrationId: "registration-old",
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("old UpdateNodeStatus() code = %v, want FailedPrecondition (error %v)", status.Code(err), err)
	}
	if response != nil {
		t.Fatalf("old UpdateNodeStatus() response = %#v, want nil", response)
	}
	if address, err := endpoints.Resolve(model.NodeID("node-1")); err != nil || address != "address-b" {
		t.Fatalf("replacement endpoint = %q, %v; want address-b", address, err)
	}
	if got := registry.Discover(model.Capability("capability-b")); len(got) != 1 || got[0].ID() != model.NodeID("node-1") {
		t.Fatalf("replacement online nodes = %v", got)
	}

	response, err = server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{
		NodeId:         "node-1",
		Status:         dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		RegistrationId: "registration-new",
	})
	if err != nil || response == nil || !response.GetAccepted() {
		t.Fatalf("new UpdateNodeStatus() response = %#v, error = %v", response, err)
	}
	if _, err := endpoints.Resolve(model.NodeID("node-1")); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("replacement endpoint after owner offline error = %v, want ErrEndpointNotFound", err)
	}
}

func TestConcurrentRegisterNodeKeepsEndpointAndRegistryPaired(t *testing.T) {
	registry := newConcurrentRegistry()
	endpoints := newBlockingEndpoints()
	server, err := NewRegistryServer(registry, endpoints)
	if err != nil {
		t.Fatal(err)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := server.RegisterNode(context.Background(), registerRequest(
			"node-1",
			[]string{"capability-a"},
			dtmv1.NodeStatus_NODE_STATUS_ONLINE,
			"address-a",
		))
		firstDone <- err
	}()
	<-endpoints.entered

	secondDone := make(chan error, 1)
	go func() {
		_, err := server.RegisterNode(context.Background(), registerRequest(
			"node-1",
			[]string{"capability-b"},
			dtmv1.NodeStatus_NODE_STATUS_ONLINE,
			"address-b",
		))
		secondDone <- err
	}()

	select {
	case <-registry.secondRegistration:
	case <-time.After(100 * time.Millisecond):
	}
	close(endpoints.release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}

	address, err := endpoints.Resolve(model.NodeID("node-1"))
	if err != nil {
		t.Fatal(err)
	}
	registered := registry.latestNode()
	if address != "address-b" || !registered.Has(model.Capability("capability-b")) {
		t.Fatalf("final endpoint/node mismatch: address=%q capabilities=%v", address, registered.Capabilities())
	}
}

func TestConcurrentRegisterAndOfflineCannotLeaveOnlineNodeWithoutEndpoint(t *testing.T) {
	registry := capability.NewRegistry()
	directory := NewEndpointDirectory()
	endpoints := newBlockingEndpointsWithDelegate(directory)
	server, err := NewRegistryServer(registry, endpoints)
	if err != nil {
		t.Fatal(err)
	}

	registerDone := make(chan error, 1)
	go func() {
		_, err := server.RegisterNode(context.Background(), registerRequest(
			"node-1",
			[]string{"capability-new"},
			dtmv1.NodeStatus_NODE_STATUS_ONLINE,
			"address-new",
		))
		registerDone <- err
	}()
	<-endpoints.entered

	offlineStarted := make(chan struct{})
	offlineDone := make(chan error, 1)
	go func() {
		close(offlineStarted)
		_, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{
			NodeId:         "node-1",
			Status:         dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
			RegistrationId: "registration-1",
		})
		offlineDone <- err
	}()
	<-offlineStarted

	var (
		offlineErr       error
		offlineCompleted bool
	)
	select {
	case offlineErr = <-offlineDone:
		offlineCompleted = true
	case <-time.After(100 * time.Millisecond):
	}
	close(endpoints.release)
	if err := <-registerDone; err != nil {
		t.Fatal(err)
	}
	if !offlineCompleted {
		offlineErr = <-offlineDone
	}
	if offlineErr != nil {
		t.Fatal(offlineErr)
	}

	if online := registry.Discover(model.Capability("capability-new")); len(online) != 0 {
		if _, err := directory.Resolve(model.NodeID("node-1")); errors.Is(err, ErrEndpointNotFound) {
			t.Fatalf("online node %q has no execution endpoint", online[0].ID())
		}
		t.Fatalf("node remained online after offline update: %v", online)
	}
	if _, err := directory.Resolve(model.NodeID("node-1")); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("Resolve() error = %v, want ErrEndpointNotFound", err)
	}
}

func TestUpdateNodeStatusOnlyAllowsOffline(t *testing.T) {
	tests := []struct {
		name    string
		request *dtmv1.UpdateNodeStatusRequest
	}{
		{name: "nil request"},
		{name: "blank node ID", request: &dtmv1.UpdateNodeStatusRequest{NodeId: " ", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE, RegistrationId: "registration-1"}},
		{name: "blank registration ID", request: &dtmv1.UpdateNodeStatusRequest{NodeId: "node-1", Status: dtmv1.NodeStatus_NODE_STATUS_OFFLINE, RegistrationId: " "}},
		{name: "unspecified", request: &dtmv1.UpdateNodeStatusRequest{NodeId: "node-1", Status: dtmv1.NodeStatus_NODE_STATUS_UNSPECIFIED, RegistrationId: "registration-1"}},
		{name: "online", request: &dtmv1.UpdateNodeStatusRequest{NodeId: "node-1", Status: dtmv1.NodeStatus_NODE_STATUS_ONLINE, RegistrationId: "registration-1"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, _, _, calls := newRecordingRegistryServer(t)

			response, err := server.UpdateNodeStatus(context.Background(), tt.request)

			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("UpdateNodeStatus() code = %v, want InvalidArgument (error %v)", status.Code(err), err)
			}
			if response != nil {
				t.Fatalf("UpdateNodeStatus() response = %#v, want nil", response)
			}
			if len(*calls) != 0 {
				t.Fatalf("invalid request calls = %v, want none", *calls)
			}
		})
	}
}

type concurrentRegistry struct {
	mu                 sync.Mutex
	latest             node.Node
	registrations      int
	secondRegistration chan struct{}
}

func newConcurrentRegistry() *concurrentRegistry {
	return &concurrentRegistry{secondRegistration: make(chan struct{})}
}

func (r *concurrentRegistry) Register(n node.Node) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.registrations++
	r.latest = n
	if r.registrations == 2 {
		close(r.secondRegistration)
	}
}

func (r *concurrentRegistry) SetStatus(model.NodeID, node.Status) error {
	return nil
}

func (r *concurrentRegistry) latestNode() node.Node {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.latest
}

type blockingEndpoints struct {
	delegate *EndpointDirectory
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func newBlockingEndpoints() *blockingEndpoints {
	return newBlockingEndpointsWithDelegate(NewEndpointDirectory())
}

func newBlockingEndpointsWithDelegate(delegate *EndpointDirectory) *blockingEndpoints {
	return &blockingEndpoints{
		delegate: delegate,
		entered:  make(chan struct{}),
		release:  make(chan struct{}),
	}
}

func (e *blockingEndpoints) Prepare(id model.NodeID, address string) error {
	if err := e.delegate.Prepare(id, address); err != nil {
		return err
	}
	blocked := false
	e.once.Do(func() {
		blocked = true
		close(e.entered)
	})
	if blocked {
		<-e.release
	}
	return nil
}

func (e *blockingEndpoints) Activate(id model.NodeID) { e.delegate.Activate(id) }

func (e *blockingEndpoints) Available(id model.NodeID) bool { return e.delegate.Available(id) }

func (e *blockingEndpoints) Resolve(id model.NodeID) (string, error) {
	return e.delegate.Resolve(id)
}

func (e *blockingEndpoints) Delete(id model.NodeID) {
	e.delegate.Delete(id)
}

func TestUpdateNodeStatusMarksOfflineBeforeDeletingEndpoint(t *testing.T) {
	server, registry, endpoints, calls := newRecordingRegistryServer(t)
	registerRecordingNode(t, server, calls)

	response, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{
		NodeId:         " node-1 ",
		Status:         dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		RegistrationId: " registration-1 ",
	})

	if err != nil {
		t.Fatalf("UpdateNodeStatus() error = %v", err)
	}
	if response == nil || !response.GetAccepted() {
		t.Fatalf("UpdateNodeStatus() response = %#v, want accepted", response)
	}
	assertCalls(t, *calls, "registry.set_status", "endpoint.delete")
	if registry.statusID != model.NodeID("node-1") || registry.status != node.StatusOffline {
		t.Fatalf("status update = (%q, %q)", registry.statusID, registry.status)
	}
	if endpoints.deletedID != model.NodeID("node-1") {
		t.Fatalf("deleted endpoint ID = %q", endpoints.deletedID)
	}
}

func TestUpdateNodeStatusUnknownNodeReturnsNotFoundWithoutDeletingEndpoint(t *testing.T) {
	server, registry, endpoints, calls := newRecordingRegistryServer(t)

	response, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{
		NodeId:         "node-1",
		Status:         dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		RegistrationId: "registration-1",
	})

	if status.Code(err) != codes.NotFound {
		t.Fatalf("UpdateNodeStatus() code = %v, want NotFound (error %v)", status.Code(err), err)
	}
	if response != nil {
		t.Fatalf("UpdateNodeStatus() response = %#v, want nil", response)
	}
	assertCalls(t, *calls)
	if registry.statusID != "" {
		t.Fatalf("SetStatus() called for %q", registry.statusID)
	}
	if endpoints.deletedID != "" {
		t.Fatalf("Delete() called for %q", endpoints.deletedID)
	}
}

func TestUpdateNodeStatusRegistryFailureDoesNotDeleteEndpoint(t *testing.T) {
	server, registry, endpoints, calls := newRecordingRegistryServer(t)
	registerRecordingNode(t, server, calls)
	registry.setStatusErr = errors.New("registry unavailable")

	_, err := server.UpdateNodeStatus(context.Background(), &dtmv1.UpdateNodeStatusRequest{
		NodeId:         "node-1",
		Status:         dtmv1.NodeStatus_NODE_STATUS_OFFLINE,
		RegistrationId: "registration-1",
	})

	if status.Code(err) != codes.Unavailable {
		t.Fatalf("UpdateNodeStatus() code = %v, want Unavailable (error %v)", status.Code(err), err)
	}
	assertCalls(t, *calls, "registry.set_status")
	if endpoints.deletedID != "" {
		t.Fatalf("Delete() called for %q", endpoints.deletedID)
	}
}

func newRecordingRegistryServer(t *testing.T) (*RegistryServer, *recordingRegistry, *recordingEndpoints, *[]string) {
	t.Helper()
	calls := &[]string{}
	registry := &recordingRegistry{calls: calls}
	endpoints := &recordingEndpoints{calls: calls}
	server, err := NewRegistryServer(registry, endpoints)
	if err != nil {
		t.Fatal(err)
	}
	return server, registry, endpoints, calls
}

func registerRecordingNode(t *testing.T, server *RegistryServer, calls *[]string) {
	t.Helper()
	if _, err := server.RegisterNode(context.Background(), validRegisterRequest()); err != nil {
		t.Fatal(err)
	}
	*calls = (*calls)[:0]
}

func validRegisterRequest() *dtmv1.RegisterNodeRequest {
	return registerRequest(
		"node-1",
		[]string{"temperature_sensor"},
		dtmv1.NodeStatus_NODE_STATUS_ONLINE,
		"127.0.0.1:50061",
	)
}

func registerRequest(id string, capabilities []string, nodeStatus dtmv1.NodeStatus, address string) *dtmv1.RegisterNodeRequest {
	return &dtmv1.RegisterNodeRequest{
		RegistrationId: "registration-1",
		Node: &dtmv1.Node{
			Id:               id,
			Capabilities:     capabilities,
			Status:           nodeStatus,
			ExecutionAddress: address,
		},
	}
}

func assertCalls(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("calls = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("calls = %v, want %v", got, want)
		}
	}
}

func TestRuntimeRegistrationDoesNotExposeNewEndpointBeforeActivation(t *testing.T) {
	registry := capability.NewRegistry()
	directory := NewEndpointDirectory()
	endpoints := newBlockingEndpointsWithDelegate(directory)
	server, err := NewRegistryServer(registry, endpoints)
	if err != nil {
		t.Fatal(err)
	}
	request := registerRequest("node-pending-endpoint", []string{"temperature_sensor"}, dtmv1.NodeStatus_NODE_STATUS_ONLINE, "endpoint-new")
	request.RegistrationId = "registration-new"
	done := make(chan error, 1)
	go func() { _, err := server.RegisterNode(context.Background(), request); done <- err }()
	<-endpoints.entered
	if _, err := directory.Resolve("node-pending-endpoint"); !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("pending endpoint Resolve() = %v", err)
	}
	if server.Eligible("node-pending-endpoint") || len(registry.Discover("temperature_sensor")) != 0 {
		t.Fatal("pending endpoint registration became schedulable")
	}
	close(endpoints.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if address, err := directory.Resolve("node-pending-endpoint"); err != nil || address != "endpoint-new" {
		t.Fatalf("activated endpoint = %q, %v", address, err)
	}
	if !server.Eligible("node-pending-endpoint") {
		t.Fatal("activated registration not eligible")
	}
}
