package ui

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/mesh/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultQueryTimeout     = 1500 * time.Millisecond
	defaultMaxTasks         = 100
	endpointFailureCooldown = 3 * time.Second
)

var greenhouseSequence atomic.Uint64

type ObserverOptions struct {
	BootstrapEndpoint  string
	BootstrapEndpoints []string
	Members            MemberSource
	QueryTimeout       time.Duration
	MaxTasks           int
	Now                func() time.Time
}

type Observer struct {
	bootstrapEndpoints      []string
	activeBootstrapEndpoint string
	bootstrapFailures       map[string]time.Time
	members                 MemberSource
	queryTimeout            time.Duration
	maxTasks                int
	now                     func() time.Time
	timeline                *Timeline

	refreshMu   sync.Mutex
	bootstrapMu sync.RWMutex
	stateMu     sync.RWMutex
	requester   protocol.Identity
	last        *Snapshot
	lastAt      time.Time
}

func NewObserver(options ObserverOptions) (*Observer, error) {
	bootstrapEndpoints := make([]string, 0, len(options.BootstrapEndpoints)+1)
	appendEndpoint := func(endpoint string) {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" {
			return
		}
		for _, existing := range bootstrapEndpoints {
			if existing == endpoint {
				return
			}
		}
		bootstrapEndpoints = append(bootstrapEndpoints, endpoint)
	}
	appendEndpoint(options.BootstrapEndpoint)
	for _, endpoint := range options.BootstrapEndpoints {
		appendEndpoint(endpoint)
	}
	if len(bootstrapEndpoints) == 0 {
		return nil, errors.New("runtime bootstrap endpoint is required")
	}
	if options.QueryTimeout <= 0 {
		options.QueryTimeout = defaultQueryTimeout
	}
	if options.MaxTasks <= 0 {
		options.MaxTasks = defaultMaxTasks
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	instanceID, err := protocol.NewRuntimeInstanceID()
	if err != nil {
		return nil, err
	}
	return &Observer{
		bootstrapEndpoints:      bootstrapEndpoints,
		activeBootstrapEndpoint: bootstrapEndpoints[0],
		bootstrapFailures:       make(map[string]time.Time),
		members:                 options.Members,
		queryTimeout:            options.QueryTimeout,
		maxTasks:                options.MaxTasks,
		now:                     options.Now,
		timeline:                NewTimeline(100),
		requester: protocol.Identity{
			MeshNamespace:   "dtm-ui",
			ProtocolMajor:   1,
			ProtocolMinor:   0,
			DTMVersion:      protocol.DTMVersion,
			NodeID:          "dtm-ui",
			RuntimeInstance: instanceID,
			ControlEndpoint: "127.0.0.1:1",
		},
	}, nil
}

// SubmitGreenhouseTask submits the bounded demo task through the observed
// dynamic Core ingress. It deliberately does not call planner, mapper, or
// executor internals directly.
func (observer *Observer) SubmitGreenhouseTask(ctx context.Context) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	snapshot := observer.Snapshot(ctx)
	if snapshot.Status == "UNAVAILABLE" {
		return "", fmt.Errorf("snapshot unavailable: %s", snapshot.Error)
	}
	endpoint := strings.TrimSpace(snapshot.Mesh.CoreEndpoint)
	if endpoint == "" {
		return "", errors.New("dynamic Core is not ready")
	}
	connection, err := observer.dial(ctx, endpoint)
	if err != nil {
		return "", fmt.Errorf("core submit: %w", err)
	}
	defer connection.Close()
	taskID := fmt.Sprintf("ui-greenhouse-%d-%d", observer.now().UTC().UnixNano(), greenhouseSequence.Add(1))
	callCtx, cancel := observer.callContext(ctx)
	response, err := dtmv1.NewCoreServiceClient(connection).SubmitTask(callCtx, &dtmv1.SubmitTaskRequest{
		Task: &dtmv1.Task{
			Id:          taskID,
			Intent:      "cool_environment",
			Constraints: map[string]string{"target_temperature": "26"},
		},
		Async:          true,
		IdempotencyKey: "ui-greenhouse-" + taskID,
	})
	cancel()
	if err != nil {
		return "", fmt.Errorf("submit greenhouse task: %w", err)
	}
	if response == nil {
		return "", errors.New("submit greenhouse task returned an empty response")
	}
	if message := strings.TrimSpace(response.GetError()); message != "" {
		return "", fmt.Errorf("submit greenhouse task: %s", message)
	}
	if response.GetStatus() == dtmv1.TaskStatus_TASK_STATUS_FAILED || response.GetStatus() == dtmv1.TaskStatus_TASK_STATUS_CANCELLED {
		return "", fmt.Errorf("submit greenhouse task returned status %s", response.GetStatus().String())
	}
	if returnedID := strings.TrimSpace(response.GetTaskId()); returnedID != "" {
		return returnedID, nil
	}
	return taskID, nil
}

// Snapshot polls the Runtime bootstrap, current Runtime members, Runtime
// advertisements, and the selected dynamic Core. Every network operation is
// bounded by ObserverOptions.QueryTimeout.
func (observer *Observer) Snapshot(ctx context.Context) Snapshot {
	if ctx == nil {
		ctx = context.Background()
	}
	observer.refreshMu.Lock()
	defer observer.refreshMu.Unlock()
	now := observer.now()

	bootstrap, bootstrapEndpoint, err := observer.bootstrapStatus(ctx)
	if err != nil {
		return observer.unavailableSnapshot(now, fmt.Errorf("bootstrap runtime: %w", err))
	}
	localIdentity := identityFromProto(bootstrap.GetRuntime())
	if localIdentity.MeshNamespace != "" {
		observer.stateMu.Lock()
		observer.requester.MeshNamespace = localIdentity.MeshNamespace
		observer.requester.ProtocolMajor = localIdentity.ProtocolMajor
		observer.requester.ProtocolMinor = localIdentity.ProtocolMinor
		observer.requester.DTMVersion = localIdentity.DTMVersion
		observer.stateMu.Unlock()
	}

	members := observer.memberObservations()
	observer.addConfiguredSeedObservations(ctx, members, bootstrapEndpoint)
	for key, member := range members {
		if member.Identity.MeshNamespace != "" && member.Identity.MeshNamespace != localIdentity.MeshNamespace {
			delete(members, key)
			continue
		}
		if member.Identity.ProtocolMajor != 0 && member.Identity.ProtocolMajor != localIdentity.ProtocolMajor {
			delete(members, key)
		}
	}
	addMemberObservation(members, MemberObservation{Identity: localIdentity, State: "ACTIVE"})
	if coordinator := coordinatorIdentity(localIdentity, bootstrap.GetCoordinator()); coordinator.NodeID != "" {
		addMemberObservation(members, MemberObservation{Identity: coordinator, State: "ACTIVE"})
	}
	records := observer.queryRuntimeRecords(ctx, members, bootstrap)
	mesh, coordinatorRecord := buildMeshOverview(records)
	resources := observer.queryResources(ctx, records)

	var tasks []TaskView
	var taskQueryError string
	if mesh.CoreEndpoint != "" {
		tasks, err = observer.queryTasks(ctx, mesh.CoreEndpoint)
		if err != nil {
			taskQueryError = err.Error()
		}
	}
	if mesh.CoreEndpoint == "" && coordinatorRecord != nil && coordinatorRecord.response != nil {
		taskQueryError = "dynamic Core is not ready"
	}

	snapshot := Snapshot{
		Timestamp:      now,
		Status:         mesh.Status,
		Observer:       observer.observerView(bootstrapEndpoint, localIdentity, "ACTIVE"),
		TaskQueryError: taskQueryError,
		Mesh:           mesh,
		Runtimes:       runtimeViews(records),
		Resources:      resources,
		Tasks:          tasks,
	}
	sortSnapshot(&snapshot)
	snapshot.Events = observer.timeline.Observe(snapshot, now)

	observer.stateMu.Lock()
	lastAt := now
	observer.lastAt = lastAt
	copy := cloneSnapshot(snapshot)
	observer.last = &copy
	observer.stateMu.Unlock()
	snapshot.LastSuccessfulSnapshot = &lastAt
	return snapshot
}

// addConfiguredSeedObservations makes configured multi-seed observation useful
// even when the UI host cannot receive the Runtime multicast announcements.
// Seeds remain read-only observation entry points; this does not change DTM
// discovery, membership, Coordinator selection, or Core authority.
func (observer *Observer) addConfiguredSeedObservations(ctx context.Context, members map[string]MemberObservation, bootstrapEndpoint string) {
	observer.bootstrapMu.RLock()
	endpoints := append([]string(nil), observer.bootstrapEndpoints...)
	observer.bootstrapMu.RUnlock()
	type result struct {
		identity protocol.Identity
		err      error
	}
	results := make(chan result, len(endpoints))
	var wait sync.WaitGroup
	for _, endpoint := range endpoints {
		if endpoint == bootstrapEndpoint || observer.bootstrapSuppressed(endpoint) {
			continue
		}
		wait.Add(1)
		go func(endpoint string) {
			defer wait.Done()
			response, err := observer.runtimeStatus(ctx, endpoint)
			if err != nil || response == nil {
				results <- result{err: err}
				return
			}
			results <- result{identity: identityFromProto(response.GetRuntime())}
		}(endpoint)
	}
	wait.Wait()
	close(results)
	for item := range results {
		if item.err != nil || item.identity.NodeID == "" || item.identity.ControlEndpoint == "" {
			continue
		}
		addMemberObservation(members, MemberObservation{Identity: item.identity, State: "ACTIVE"})
	}
}

func (observer *Observer) unavailableSnapshot(now time.Time, observationErr error) Snapshot {
	observer.stateMu.RLock()
	var snapshot Snapshot
	var lastAt *time.Time
	hadLast := observer.last != nil
	if observer.last != nil {
		snapshot = cloneSnapshot(*observer.last)
	}
	if !observer.lastAt.IsZero() {
		value := observer.lastAt
		lastAt = &value
	}
	observer.stateMu.RUnlock()
	snapshot.Timestamp = now
	if hadLast {
		snapshot.Status = "STALE"
	} else {
		snapshot.Status = "UNAVAILABLE"
	}
	snapshot.Error = observationErr.Error()
	snapshot.TaskQueryError = ""
	snapshot.Observer = observer.observerView(observer.currentBootstrapEndpoint(), protocol.Identity{NodeID: snapshot.Observer.NodeID}, snapshot.Status)
	snapshot.Mesh.Status = snapshot.Status
	snapshot.Mesh.CoordinatorState = "UNKNOWN"
	snapshot.Mesh.CoreState = "UNKNOWN"
	snapshot.Mesh.AuthorityState = "UNKNOWN"
	snapshot.Mesh.IngressState = "UNKNOWN"
	snapshot.Mesh.CoreEndpoint = ""
	snapshot.LastSuccessfulSnapshot = lastAt
	snapshot.Events = observer.timeline.Observe(snapshot, now)
	return snapshot
}

type bootstrapProbeResult struct {
	endpoint string
	response *dtmv1.GetRuntimeStatusResponse
	err      error
}

func (observer *Observer) bootstrapStatus(ctx context.Context) (*dtmv1.GetRuntimeStatusResponse, string, error) {
	candidates := observer.bootstrapCandidates()
	if len(candidates) == 0 {
		return nil, "", errors.New("no Runtime bootstrap candidates are available")
	}
	probeContext, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan bootstrapProbeResult, len(candidates))
	for _, endpoint := range candidates {
		go func(endpoint string) {
			response, err := observer.runtimeStatus(probeContext, endpoint)
			if err == nil && (response == nil || response.GetRuntime() == nil) {
				err = errors.New("runtime status response is empty")
			}
			if err != nil && probeContext.Err() == nil {
				observer.markBootstrapFailure(endpoint)
			}
			results <- bootstrapProbeResult{endpoint: endpoint, response: response, err: err}
		}(endpoint)
	}

	errorsSeen := make([]string, 0, len(candidates))
	for range candidates {
		select {
		case result := <-results:
			if result.err == nil {
				observer.markBootstrapSuccess(result.endpoint)
				cancel()
				return result.response, result.endpoint, nil
			}
			errorsSeen = append(errorsSeen, result.endpoint+": "+result.err.Error())
		case <-ctx.Done():
			return nil, "", ctx.Err()
		}
	}
	if len(errorsSeen) == 0 {
		return nil, "", errors.New("all Runtime bootstrap candidates failed")
	}
	return nil, "", fmt.Errorf("all Runtime bootstrap candidates failed: %s", strings.Join(errorsSeen, "; "))
}

func (observer *Observer) bootstrapCandidates() []string {
	observer.bootstrapMu.RLock()
	active := observer.activeBootstrapEndpoint
	configured := append([]string(nil), observer.bootstrapEndpoints...)
	observer.bootstrapMu.RUnlock()

	candidates := make([]string, 0, len(configured)+8)
	appendCandidate := func(endpoint string) {
		endpoint = strings.TrimSpace(endpoint)
		if endpoint == "" || observer.bootstrapSuppressed(endpoint) {
			return
		}
		for _, existing := range candidates {
			if existing == endpoint {
				return
			}
		}
		candidates = append(candidates, endpoint)
	}
	appendCandidate(active)
	for _, endpoint := range configured {
		appendCandidate(endpoint)
	}
	if observer.members != nil {
		for _, member := range observer.members() {
			if isExpiredMember(member) {
				continue
			}
			appendCandidate(member.Identity.ControlEndpoint)
		}
	}
	observer.stateMu.RLock()
	if observer.last != nil {
		for _, runtime := range observer.last.Runtimes {
			appendCandidate(runtime.ControlEndpoint)
		}
	}
	observer.stateMu.RUnlock()
	return candidates
}

func (observer *Observer) currentBootstrapEndpoint() string {
	observer.bootstrapMu.RLock()
	defer observer.bootstrapMu.RUnlock()
	return observer.activeBootstrapEndpoint
}

func (observer *Observer) markBootstrapSuccess(endpoint string) {
	observer.bootstrapMu.Lock()
	observer.activeBootstrapEndpoint = endpoint
	delete(observer.bootstrapFailures, endpoint)
	observer.bootstrapMu.Unlock()
}

func (observer *Observer) markBootstrapFailure(endpoint string) {
	observer.bootstrapMu.Lock()
	observer.bootstrapFailures[endpoint] = observer.now().Add(endpointFailureCooldown)
	observer.bootstrapMu.Unlock()
}

func (observer *Observer) bootstrapSuppressed(endpoint string) bool {
	observer.bootstrapMu.Lock()
	defer observer.bootstrapMu.Unlock()
	until, ok := observer.bootstrapFailures[endpoint]
	if !ok {
		return false
	}
	if !observer.now().Before(until) {
		delete(observer.bootstrapFailures, endpoint)
		return false
	}
	return true
}

func (observer *Observer) observerView(endpoint string, identity protocol.Identity, state string) ObserverView {
	observer.bootstrapMu.RLock()
	configured := append([]string(nil), observer.bootstrapEndpoints...)
	observer.bootstrapMu.RUnlock()
	source := "DISCOVERED_MEMBER"
	for index, candidate := range configured {
		if candidate != endpoint {
			continue
		}
		if index == 0 {
			source = "PRIMARY_SEED"
		} else {
			source = "SEED"
		}
		break
	}
	if state == "ACTIVE" && source != "PRIMARY_SEED" {
		state = "FALLBACK"
	}
	return ObserverView{
		Endpoint: endpoint, NodeID: identity.NodeID, State: state,
		Source: source, SeedCount: len(configured),
	}
}

func (observer *Observer) memberObservations() map[string]MemberObservation {
	result := make(map[string]MemberObservation)
	if observer.members == nil {
		return result
	}
	for _, member := range observer.members() {
		if member.Identity.NodeID == "" || member.Identity.ControlEndpoint == "" {
			continue
		}
		result[identityKey(member.Identity)] = member
	}
	return result
}

func addMemberObservation(members map[string]MemberObservation, member MemberObservation) {
	if member.Identity.NodeID == "" || member.Identity.ControlEndpoint == "" {
		return
	}
	key := identityKey(member.Identity)
	if existing, ok := members[key]; ok {
		if existing.LastSeen.After(member.LastSeen) {
			return
		}
		if member.State == "" {
			member.State = existing.State
		}
	}
	members[key] = member
}

type runtimeRecord struct {
	member   MemberObservation
	response *dtmv1.GetRuntimeStatusResponse
	err      error
}

func (observer *Observer) queryRuntimeRecords(ctx context.Context, members map[string]MemberObservation, bootstrap *dtmv1.GetRuntimeStatusResponse) map[string]runtimeRecord {
	result := make(map[string]runtimeRecord, len(members))
	bootstrapIdentity := identityFromProto(bootstrap.GetRuntime())
	bootstrapKey := identityKey(bootstrapIdentity)
	for key, member := range members {
		if key == bootstrapKey {
			result[key] = runtimeRecord{member: member, response: bootstrap}
			continue
		}
		result[key] = runtimeRecord{member: member}
	}
	type queryResult struct {
		key      string
		response *dtmv1.GetRuntimeStatusResponse
		err      error
	}
	queries := make(chan queryResult, len(result))
	var wait sync.WaitGroup
	for key, record := range result {
		if record.response != nil || isExpiredMember(record.member) {
			continue
		}
		if observer.bootstrapSuppressed(record.member.Identity.ControlEndpoint) {
			record.err = errors.New("runtime endpoint temporarily unavailable")
			result[key] = record
			continue
		}
		wait.Add(1)
		go func(key string, member MemberObservation) {
			defer wait.Done()
			response, err := observer.runtimeStatus(ctx, member.Identity.ControlEndpoint)
			queries <- queryResult{key: key, response: response, err: err}
		}(key, record.member)
	}
	wait.Wait()
	close(queries)
	for item := range queries {
		record := result[item.key]
		record.response = item.response
		record.err = item.err
		if item.response != nil {
			identity := identityFromProto(item.response.GetRuntime())
			if identity.NodeID != "" {
				if !runtimeIdentityMatches(record.member.Identity, identity) {
					record.response = nil
					record.err = fmt.Errorf("runtime identity changed for %s", record.member.Identity.NodeID)
				} else {
					record.member.Identity = mergeIdentity(record.member.Identity, identity)
				}
			}
		}
		result[item.key] = record
	}
	return result
}

func buildMeshOverview(records map[string]runtimeRecord) (MeshOverview, *runtimeRecord) {
	overview := MeshOverview{Status: "DEGRADED", Coordinator: "NONE", CoordinatorState: "NONE", CoreState: "UNKNOWN", AuthorityState: "UNKNOWN", IngressState: "UNKNOWN"}
	var coordinator *runtimeRecord
	for _, record := range records {
		if !isExpiredMember(record.member) {
			overview.RuntimeCount++
		}
		if record.response != nil && record.response.GetLocalIsCoordinator() {
			copy := record
			coordinator = &copy
			overview.CoordinatorCount++
		}
	}
	switch overview.CoordinatorCount {
	case 0:
		overview.CoordinatorState = "NONE"
		overview.Coordinator = "NONE"
	case 1:
		overview.CoordinatorState = "OK"
	default:
		overview.CoordinatorState = "CONFLICT"
		overview.Coordinator = "CONFLICT"
	}
	if overview.CoordinatorCount != 1 || coordinator == nil || coordinator.response == nil {
		return overview, coordinator
	}
	response := coordinator.response
	overview.Coordinator = response.GetRuntime().GetNodeId()
	if overview.Coordinator == "" {
		overview.Coordinator = "UNKNOWN"
	}
	if response.GetCoreReady() && response.GetCoreAddress() != "" {
		overview.CoreState = "ACTIVE"
		overview.CoreEndpoint = response.GetCoreAddress()
	} else {
		overview.CoreState = "INACTIVE"
	}
	if response.GetAuthorityReady() {
		overview.AuthorityState = "READY"
	} else {
		overview.AuthorityState = "NOT_READY"
	}
	if response.GetIngressReady() {
		overview.IngressState = "READY"
	} else {
		overview.IngressState = "NOT_READY"
	}
	if response.GetReadiness() == dtmv1.RuntimeReadiness_RUNTIME_READINESS_READY && response.GetCoreReady() && response.GetAuthorityReady() && response.GetIngressReady() {
		overview.Status = "READY"
	}
	return overview, coordinator
}

func runtimeViews(records map[string]runtimeRecord) []RuntimeView {
	result := make([]RuntimeView, 0, len(records))
	for _, record := range records {
		identity := record.member.Identity
		view := RuntimeView{
			NodeID: identity.NodeID, RuntimeInstanceID: identity.RuntimeInstance,
			ControlEndpoint: identity.ControlEndpoint, MembershipState: record.member.State,
		}
		if !record.member.LastSeen.IsZero() {
			lastSeen := record.member.LastSeen
			view.LastSeen = &lastSeen
		}
		if record.response == nil {
			view.Error = record.errString()
			result = append(result, view)
			continue
		}
		view.Coordinator = record.response.GetLocalIsCoordinator()
		view.CoreReady = record.response.GetCoreReady()
		view.CoreActive = view.CoreReady
		view.AuthorityReady = record.response.GetAuthorityReady()
		view.IngressReady = record.response.GetIngressReady()
		view.LocalAuthorityBinding = record.response.GetAuthorityStatus()
		view.CoreEndpoint = record.response.GetCoreAddress()
		result = append(result, view)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].NodeID != result[right].NodeID {
			return result[left].NodeID < result[right].NodeID
		}
		return result[left].RuntimeInstanceID < result[right].RuntimeInstanceID
	})
	return result
}

func (record runtimeRecord) errString() string {
	if record.err == nil {
		return ""
	}
	return record.err.Error()
}

func (observer *Observer) queryResources(ctx context.Context, records map[string]runtimeRecord) []ResourceView {
	type resourceResult struct {
		member MemberObservation
		items  []*dtmv1.MeshResourceDescriptor
		owner  *dtmv1.RuntimeIdentity
	}
	results := make(chan resourceResult, len(records))
	var wait sync.WaitGroup
	for _, record := range records {
		if record.member.Identity.ControlEndpoint == "" || isExpiredMember(record.member) || observer.bootstrapSuppressed(record.member.Identity.ControlEndpoint) {
			continue
		}
		wait.Add(1)
		go func(record runtimeRecord) {
			defer wait.Done()
			owner, items, err := observer.resourceAdvertisement(ctx, record.member.Identity.ControlEndpoint)
			if err != nil {
				return
			}
			results <- resourceResult{member: record.member, items: items, owner: owner}
		}(record)
	}
	wait.Wait()
	close(results)
	result := make([]ResourceView, 0)
	for item := range results {
		owner := item.member.Identity
		if item.owner != nil {
			owner = mergeIdentity(owner, identityFromProto(item.owner))
		}
		bound := false
		for _, record := range records {
			if identityKey(record.member.Identity) == identityKey(owner) && record.response != nil {
				bound = strings.EqualFold(record.response.GetAuthorityStatus(), "ready") || record.response.GetAuthorityReady()
				break
			}
		}
		for _, descriptor := range item.items {
			if descriptor == nil {
				continue
			}
			state := "DISCOVERED"
			if bound {
				state = "BOUND"
			}
			result = append(result, ResourceView{
				ResourceID: descriptor.GetResourceId(), Kind: descriptor.GetKind(), Capability: descriptor.GetType(),
				OwnerNodeID: owner.NodeID, OwnerRuntimeInstanceID: owner.RuntimeInstance,
				Endpoint: owner.ControlEndpoint, Discovered: true, AuthorityBound: bound,
				Published: "NOT_EXPOSED_BY_EXISTING_API", Eligible: "NOT_EXPOSED_BY_EXISTING_API", Status: state,
			})
		}
	}
	sort.Slice(result, func(left, right int) bool { return result[left].ResourceID < result[right].ResourceID })
	return result
}

func (observer *Observer) queryTasks(ctx context.Context, endpoint string) ([]TaskView, error) {
	connection, err := observer.dial(ctx, endpoint)
	if err != nil {
		return nil, fmt.Errorf("core query: %w", err)
	}
	defer connection.Close()
	client := dtmv1.NewCoreServiceClient(connection)
	var summaries []*dtmv1.TaskSummary
	pageToken := ""
	for page := 0; len(summaries) < observer.maxTasks && page < 16; page++ {
		callCtx, cancel := observer.callContext(ctx)
		response, callErr := client.ListTasks(callCtx, &dtmv1.ListTasksRequest{Limit: int32(observer.maxTasks), PageToken: pageToken})
		cancel()
		if callErr != nil {
			return nil, fmt.Errorf("list tasks: %w", callErr)
		}
		summaries = append(summaries, response.GetTasks()...)
		pageToken = response.GetNextPageToken()
		if pageToken == "" {
			break
		}
	}
	if len(summaries) > observer.maxTasks {
		summaries = summaries[:observer.maxTasks]
	}
	result := make([]TaskView, 0, len(summaries))
	var firstDetailErr error
	for _, summary := range summaries {
		if summary == nil {
			continue
		}
		view := taskViewFromSummary(summary)
		callCtx, cancel := observer.callContext(ctx)
		detail, detailErr := client.GetTask(callCtx, &dtmv1.GetTaskRequest{TaskId: summary.GetTaskId()})
		cancel()
		if detailErr != nil || detail == nil || detail.GetTask() == nil {
			if firstDetailErr == nil {
				firstDetailErr = detailErr
				if firstDetailErr == nil {
					firstDetailErr = errors.New("task detail response is empty")
				}
			}
			result = append(result, view)
			continue
		}
		view = taskViewFromDetails(detail.GetTask())
		callCtx, cancel = observer.callContext(ctx)
		executions, executionErr := client.GetTaskExecutions(callCtx, &dtmv1.GetTaskExecutionsRequest{TaskId: summary.GetTaskId()})
		cancel()
		if executionErr != nil {
			if firstDetailErr == nil {
				firstDetailErr = executionErr
			}
		} else {
			applyExecutions(&view, executions.GetExecutions())
		}
		result = append(result, view)
	}
	sort.Slice(result, func(left, right int) bool { return result[left].TaskID < result[right].TaskID })
	if firstDetailErr != nil {
		return result, fmt.Errorf("task detail query: %w", firstDetailErr)
	}
	return result, nil
}

func (observer *Observer) resourceAdvertisement(ctx context.Context, endpoint string) (*dtmv1.RuntimeIdentity, []*dtmv1.MeshResourceDescriptor, error) {
	connection, err := observer.dial(ctx, endpoint)
	if err != nil {
		return nil, nil, err
	}
	defer connection.Close()
	callCtx, cancel := observer.callContext(ctx)
	response, err := dtmv1.NewRuntimeControlServiceClient(connection).GetResourceAdvertisement(callCtx, &dtmv1.GetResourceAdvertisementRequest{Requester: observer.requesterProto()})
	cancel()
	if err != nil {
		return nil, nil, err
	}
	return response.GetOwner(), response.GetResources(), nil
}

func (observer *Observer) runtimeStatus(ctx context.Context, endpoint string) (*dtmv1.GetRuntimeStatusResponse, error) {
	connection, err := observer.dial(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	callCtx, cancel := observer.callContext(ctx)
	response, err := dtmv1.NewRuntimeControlServiceClient(connection).GetRuntimeStatus(callCtx, &dtmv1.GetRuntimeStatusRequest{Requester: observer.requesterProto()})
	cancel()
	return response, err
}

func (observer *Observer) dial(ctx context.Context, endpoint string) (*grpc.ClientConn, error) {
	callCtx, cancel := observer.callContext(ctx)
	defer cancel()
	return grpc.DialContext(callCtx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}

func (observer *Observer) callContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, observer.queryTimeout)
}

func (observer *Observer) requesterProto() *dtmv1.RuntimeIdentity {
	observer.stateMu.RLock()
	defer observer.stateMu.RUnlock()
	return &dtmv1.RuntimeIdentity{
		MeshNamespace: observer.requester.MeshNamespace, ProtocolMajor: observer.requester.ProtocolMajor,
		ProtocolMinor: observer.requester.ProtocolMinor, DtmVersion: observer.requester.DTMVersion,
		NodeId: observer.requester.NodeID, RuntimeInstanceId: observer.requester.RuntimeInstance,
		AdvertisedControlEndpoint: observer.requester.ControlEndpoint,
	}
}

func taskViewFromSummary(summary *dtmv1.TaskSummary) TaskView {
	return TaskView{
		TaskID: summary.GetTaskId(), TaskType: summary.GetIntent(), Status: taskStatusName(summary.GetStatus()),
		CreatedAt: optionalTimestamp(summary.GetCreatedAt()), UpdatedAt: optionalTimestamp(summary.GetUpdatedAt()),
		FailureCode: summary.GetFailureCode(), Failure: summary.GetFailureMessage(), Steps: []StepView{},
	}
}

func taskViewFromDetails(task *dtmv1.TaskDetails) TaskView {
	view := TaskView{
		TaskID: task.GetTaskId(), TaskType: task.GetIntent(), Status: taskStatusName(task.GetStatus()),
		CreatedAt: optionalTimestamp(task.GetCreatedAt()), UpdatedAt: optionalTimestamp(task.GetUpdatedAt()),
		FailureCode: task.GetFailureCode(), Failure: task.GetFailureMessage(), Steps: make([]StepView, 0, len(task.GetSteps())),
	}
	for _, step := range task.GetSteps() {
		if step == nil {
			continue
		}
		mappingSource := "recorded_task_step_node"
		if step.GetAssignedNodeId() == "" {
			mappingSource = "recorded_task_step_without_node"
		}
		view.Steps = append(view.Steps, StepView{
			StepID: step.GetStepId(), Sequence: step.GetSequence(), Capability: step.GetCapability(),
			MappedNode: step.GetAssignedNodeId(), Status: stepStatusName(step.GetStatus()),
			MappingSource: mappingSource, ResourceRefHistory: "PARTIAL",
			Failure: step.GetFailureMessage(),
		})
	}
	return view
}

func applyExecutions(task *TaskView, executions []*dtmv1.TaskExecution) {
	latest := make(map[string]*dtmv1.TaskExecution)
	for _, execution := range executions {
		if execution == nil {
			continue
		}
		current := latest[execution.GetStepId()]
		currentUpdated := optionalTimestamp(current.GetUpdatedAt())
		executionUpdated := optionalTimestamp(execution.GetUpdatedAt())
		newer := current == nil || execution.GetAttemptNumber() > current.GetAttemptNumber()
		if !newer && currentUpdated != nil && executionUpdated != nil {
			newer = executionUpdated.After(*currentUpdated)
		}
		if newer {
			latest[execution.GetStepId()] = execution
		}
	}
	for index := range task.Steps {
		step := &task.Steps[index]
		execution := latest[step.StepID]
		if execution == nil {
			continue
		}
		step.ExecutionStatus = executionStatusName(execution.GetStatus())
		if step.MappedNode == "" {
			step.MappedNode = execution.GetNodeId()
		}
		if execution.GetResponse() != nil {
			step.Result = execution.GetResponse().AsMap()
		}
		if execution.GetFailureMessage() != "" {
			step.Failure = execution.GetFailureMessage()
		}
	}
}

func identityFromProto(identity *dtmv1.RuntimeIdentity) protocol.Identity {
	if identity == nil {
		return protocol.Identity{}
	}
	return protocol.Identity{
		MeshNamespace: identity.GetMeshNamespace(), ProtocolMajor: identity.GetProtocolMajor(), ProtocolMinor: identity.GetProtocolMinor(),
		DTMVersion: identity.GetDtmVersion(), NodeID: identity.GetNodeId(), RuntimeInstance: identity.GetRuntimeInstanceId(),
		ControlEndpoint: identity.GetAdvertisedControlEndpoint(),
	}
}

func coordinatorIdentity(local protocol.Identity, coordinator *dtmv1.RuntimeIdentity) protocol.Identity {
	if coordinator == nil {
		return protocol.Identity{}
	}
	return mergeIdentity(local, identityFromProto(coordinator))
}

func mergeIdentity(base, partial protocol.Identity) protocol.Identity {
	result := base
	if partial.MeshNamespace != "" {
		result.MeshNamespace = partial.MeshNamespace
	}
	if partial.ProtocolMajor != 0 {
		result.ProtocolMajor = partial.ProtocolMajor
	}
	if partial.ProtocolMinor != 0 {
		result.ProtocolMinor = partial.ProtocolMinor
	}
	if partial.DTMVersion != "" {
		result.DTMVersion = partial.DTMVersion
	}
	if partial.NodeID != "" {
		result.NodeID = partial.NodeID
	}
	if partial.RuntimeInstance != "" {
		result.RuntimeInstance = partial.RuntimeInstance
	}
	if partial.ControlEndpoint != "" {
		result.ControlEndpoint = partial.ControlEndpoint
	}
	return result
}

func identityKey(identity protocol.Identity) string {
	if identity.RuntimeInstance != "" {
		return identity.NodeID + "\x00" + identity.RuntimeInstance
	}
	return identity.NodeID + "\x00" + identity.ControlEndpoint
}

func isExpiredMember(member MemberObservation) bool {
	return strings.EqualFold(strings.TrimSpace(member.State), "EXPIRED")
}

func runtimeIdentityMatches(observed, actual protocol.Identity) bool {
	if observed.NodeID != "" && actual.NodeID != "" && observed.NodeID != actual.NodeID {
		return false
	}
	if observed.RuntimeInstance != "" && actual.RuntimeInstance != "" && observed.RuntimeInstance != actual.RuntimeInstance {
		return false
	}
	return true
}
