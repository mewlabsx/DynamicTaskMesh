package runtimehost

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/mesh/coordinator"
	"dtm/internal/mesh/protocol"
	"dtm/internal/mesh/resourceview"
	"dtm/internal/resourcedirectory"
)

const (
	AuthorityStatusNotConfigured    = "not_configured"
	AuthorityStatusNotRequired      = "not_required"
	AuthorityStatusNotReady         = "not_ready"
	AuthorityStatusBinding          = "binding"
	AuthorityStatusReady            = "ready"
	AuthorityStatusIdentityConflict = "identity_conflict"
	AuthorityStatusError            = "error"
)

var (
	ErrNoCoordinator     = errors.New("no coordinator is currently known")
	ErrNotCoordinator    = errors.New("runtime is not the selected coordinator")
	ErrCoreNotReady      = errors.New("coordinator Core is not ready")
	ErrAuthorityNotReady = errors.New("coordinator authority is not ready")
	ErrIdentityConflict  = errors.New("stable NodeID has an active identity conflict")
)

// AuthoritySession is the shared v0.4 Register/Heartbeat/offline lifecycle
// boundary used by static Agent and Runtime. RuntimeHost owns only target
// selection and readiness projection; it does not reproduce Node/Lease logic.
type AuthoritySession interface {
	Start(context.Context, string) error
	Ready() bool
	Errors() <-chan error
	Close() error
}

type AuthorityFactory func() (AuthoritySession, error)

// RuntimeStatus is a local diagnostic projection. AuthorityStatus describes
// this Runtime's local authority binding session; AuthorityReady describes
// Coordinator aggregate readiness. NodeGeneration,
// registration_id, Lease, Endpoint, and ResourceGeneration remain owned by
// the v0.4 authority and are intentionally absent here.
type RuntimeStatus struct {
	Role            coordinator.RoleSnapshot
	CoreAddress     string
	CoreReady       bool
	AuthorityReady  bool
	IngressReady    bool
	AuthorityStatus string
	AuthorityError  string
}

func (host *RuntimeHost) StatusSnapshot() RuntimeStatus {
	if host == nil {
		return RuntimeStatus{}
	}
	role := host.CoordinatorRoleSnapshot()
	coreReady := host.coreReadyForRole(role)
	authorityReady := host.authorityReadyForRole(role, coreReady)
	host.mu.RLock()
	coreAddress := host.coreEndpoint
	authorityStatus := host.authorityStatus
	authorityError := host.authorityError
	host.mu.RUnlock()
	// An empty Resource Runtime does not bind a local authority session. Keep
	// that diagnostic deterministic even if the role controller has not yet
	// completed the asynchronous reconciliation pass.
	if host.membership != nil && role.HasCoordinator && !role.LocalIsCoordinator && !host.ownsResources &&
		authorityStatus != AuthorityStatusError && authorityStatus != AuthorityStatusIdentityConflict {
		authorityStatus = AuthorityStatusNotRequired
	}
	if authorityStatus == "" {
		if host.membership == nil {
			authorityStatus = AuthorityStatusReady
		} else if !role.HasCoordinator {
			authorityStatus = AuthorityStatusNotReady
		} else if host.identityConflict() {
			authorityStatus = AuthorityStatusIdentityConflict
		} else if authorityReady {
			authorityStatus = AuthorityStatusReady
		} else {
			authorityStatus = AuthorityStatusNotReady
		}
	}
	return RuntimeStatus{
		Role: role, CoreAddress: coreAddress, CoreReady: coreReady,
		AuthorityReady: authorityReady, IngressReady: role.LocalIsCoordinator && coreReady && authorityReady,
		AuthorityStatus: authorityStatus, AuthorityError: authorityError,
	}
}

func (host *RuntimeHost) AuthorityReady() bool { return host.StatusSnapshot().AuthorityReady }
func (host *RuntimeHost) IngressReady() bool   { return host.StatusSnapshot().IngressReady }
func (host *RuntimeHost) CoreAddress() string {
	if host == nil {
		return ""
	}
	host.mu.RLock()
	defer host.mu.RUnlock()
	return host.coreEndpoint
}

func (host *RuntimeHost) coreReadyForRole(role coordinator.RoleSnapshot) bool {
	host.mu.RLock()
	core := host.core
	closed := host.closed
	host.mu.RUnlock()
	if closed || core == nil || !core.Ready() {
		return false
	}
	if host.membership == nil {
		return true
	}
	return role.LocalIsCoordinator
}

func (host *RuntimeHost) authorityReadyForRole(role coordinator.RoleSnapshot, coreReady bool) bool {
	if !coreReady {
		return false
	}
	if host.membership == nil {
		return true
	}
	if !role.LocalIsCoordinator || host.identityConflict() || host.resourceView == nil {
		return false
	}
	host.mu.RLock()
	authority := host.authority
	core := host.core
	host.mu.RUnlock()
	if core == nil || core.Resources == nil {
		return false
	}
	if host.ownsResources && (authority == nil || !authority.Ready()) {
		return false
	}
	snapshot := host.MembershipSnapshot()
	for _, member := range snapshot.ActiveMembers {
		if !host.resourceView.AdvertisementReady(member.Identity) {
			return false
		}
	}
	return authoritativeResourcesReady(core.Resources, host.resourceView)
}

func authoritativeResourcesReady(directory *resourcedirectory.Directory, view *resourceview.View) bool {
	if directory == nil || view == nil {
		return false
	}
	allRecords := directory.ListAll()
	byID := make(map[string]resourcedirectory.ResourceRecordView, len(allRecords))
	for _, record := range allRecords {
		byID[record.Descriptor.ID.String()] = record
	}
	for _, entry := range view.Snapshot().ActiveResources {
		record, exists := byID[entry.Descriptor.ID.String()]
		if !exists || record.PublicationState != resourcedirectory.PublicationStatePublished || !record.Eligible ||
			string(record.Descriptor.OwnerNodeID) != entry.Owner.NodeID ||
			record.Descriptor.Kind.String() != entry.Descriptor.Kind.String() ||
			record.Descriptor.Type.String() != entry.Descriptor.Type.String() ||
			record.NodeGeneration < 1 || record.Descriptor.Generation < 1 || strings.TrimSpace(record.RegistrationID) == "" {
			return false
		}
	}
	return true
}

func (host *RuntimeHost) identityConflict() bool {
	if host.membership == nil {
		return false
	}
	for _, nodeID := range host.MembershipSnapshot().IdentityConflicts {
		if nodeID == host.nodeID {
			return true
		}
	}
	return false
}

func (host *RuntimeHost) runtimeStatus(ctx context.Context, request *dtmv1.GetRuntimeStatusRequest) (*dtmv1.GetRuntimeStatusResponse, error) {
	if request == nil {
		request = &dtmv1.GetRuntimeStatusRequest{}
	}
	snapshot := host.StatusSnapshot()
	response := &dtmv1.GetRuntimeStatusResponse{
		Runtime:            runtimeIdentityProto(host.meshIdentity),
		Readiness:          runtimeReadiness(snapshot),
		CoreAddress:        snapshot.CoreAddress,
		LocalIsCoordinator: snapshot.Role.LocalIsCoordinator,
		CoreReady:          snapshot.CoreReady,
		AuthorityReady:     snapshot.AuthorityReady,
		IngressReady:       snapshot.IngressReady,
		AuthorityStatus:    snapshot.AuthorityStatus,
		Error:              snapshot.AuthorityError,
	}
	if snapshot.Role.HasCoordinator {
		response.Coordinator = &dtmv1.RuntimeIdentity{
			NodeId:                    snapshot.Role.Selection.NodeID,
			RuntimeInstanceId:         snapshot.Role.Selection.RuntimeInstanceID,
			AdvertisedControlEndpoint: snapshot.Role.Selection.ControlEndpoint,
		}
	}
	return response, nil
}

func runtimeReadiness(snapshot RuntimeStatus) dtmv1.RuntimeReadiness {
	switch {
	case !snapshot.Role.HasCoordinator:
		return dtmv1.RuntimeReadiness_RUNTIME_READINESS_NO_COORDINATOR
	case !snapshot.Role.LocalIsCoordinator:
		return dtmv1.RuntimeReadiness_RUNTIME_READINESS_NOT_COORDINATOR
	case !snapshot.CoreReady:
		return dtmv1.RuntimeReadiness_RUNTIME_READINESS_CORE_NOT_READY
	case !snapshot.AuthorityReady:
		return dtmv1.RuntimeReadiness_RUNTIME_READINESS_AUTHORITY_NOT_READY
	default:
		return dtmv1.RuntimeReadiness_RUNTIME_READINESS_READY
	}
}

func runtimeIdentityProto(identity protocol.Identity) *dtmv1.RuntimeIdentity {
	if identity == (protocol.Identity{}) {
		return nil
	}
	return &dtmv1.RuntimeIdentity{
		MeshNamespace: identity.MeshNamespace, ProtocolMajor: identity.ProtocolMajor,
		ProtocolMinor: identity.ProtocolMinor, DtmVersion: identity.DTMVersion,
		NodeId: identity.NodeID, RuntimeInstanceId: identity.RuntimeInstance,
		AdvertisedControlEndpoint: identity.ControlEndpoint,
	}
}

func (host *RuntimeHost) startRoleController(parent context.Context) {
	if host.membership == nil || host.coreFactory == nil {
		return
	}
	host.mu.Lock()
	if host.roleControlDone != nil {
		host.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(parent)
	host.roleControlCancel = cancel
	host.roleControlDone = make(chan struct{})
	done := host.roleControlDone
	host.mu.Unlock()
	go host.runRoleController(ctx, done)
}

func (host *RuntimeHost) runRoleController(ctx context.Context, done chan struct{}) {
	defer close(done)
	interval := host.membershipInterval
	if interval <= 0 || interval > 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	host.reconcileRole(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-host.roleWake:
			host.reconcileRole(ctx)
		case <-ticker.C:
			host.reconcileRole(ctx)
		}
	}
}

func (host *RuntimeHost) notifyRoleController() {
	if host.roleWake == nil {
		return
	}
	select {
	case host.roleWake <- struct{}{}:
	default:
	}
}

func (host *RuntimeHost) reconcileRole(ctx context.Context) {
	role := host.CoordinatorRoleSnapshot()
	if host.identityConflict() {
		host.stopAuthority()
		host.setAuthorityState(AuthorityStatusIdentityConflict, "")
		if !role.LocalIsCoordinator {
			host.deactivateCore()
		}
		return
	}
	if role.LocalIsCoordinator {
		if err := host.ensureCore(ctx); err != nil {
			host.setAuthorityState(AuthorityStatusError, err.Error())
			return
		}
	} else {
		host.deactivateCore()
	}
	if !role.HasCoordinator {
		host.setAuthorityState(AuthorityStatusNotReady, "")
		host.stopAuthority()
		return
	}
	target, err := host.resolveAuthorityTarget(ctx, role)
	if err != nil {
		if errors.Is(err, ErrCoreNotReady) {
			host.setAuthorityState(AuthorityStatusNotReady, "")
		} else {
			host.setAuthorityState(AuthorityStatusError, err.Error())
		}
		host.stopAuthority()
		return
	}
	if err := host.ensureAuthority(ctx, target); err != nil {
		host.setAuthorityState(AuthorityStatusError, err.Error())
		return
	}
	if host.membership != nil && !host.ownsResources {
		host.setAuthorityState(AuthorityStatusNotRequired, "")
		return
	}
	host.mu.RLock()
	currentAuthorityStatus := host.authorityStatus
	host.mu.RUnlock()
	if currentAuthorityStatus == AuthorityStatusNotConfigured || currentAuthorityStatus == AuthorityStatusError {
		return
	}
	if host.localAuthorityReady() {
		host.setAuthorityState(AuthorityStatusReady, "")
	} else {
		host.setAuthorityState(AuthorityStatusBinding, "")
	}
}

func (host *RuntimeHost) localAuthorityReady() bool {
	if host.membership == nil {
		return true
	}
	host.mu.RLock()
	authority := host.authority
	host.mu.RUnlock()
	return authority != nil && authority.Ready()
}

func (host *RuntimeHost) ensureCore(ctx context.Context) error {
	if host.coreReadyForRole(host.CoordinatorRoleSnapshot()) {
		return nil
	}
	host.mu.RLock()
	core := host.core
	host.mu.RUnlock()
	if core != nil {
		host.deactivateCore()
	}
	return host.ActivateCore(ctx)
}

func (host *RuntimeHost) resolveAuthorityTarget(ctx context.Context, role coordinator.RoleSnapshot) (string, error) {
	if role.LocalIsCoordinator {
		if !host.coreReadyForRole(role) {
			return "", ErrCoreNotReady
		}
		address := host.CoreAddress()
		if strings.TrimSpace(address) == "" {
			return "", ErrCoreNotReady
		}
		return address, nil
	}
	if host.runtimeClient == nil {
		return "", ErrCoreNotReady
	}
	response, err := host.runtimeClient.GetRuntimeStatus(ctx, role.Selection.ControlEndpoint)
	if err != nil {
		return "", err
	}
	if !response.GetCoreReady() || strings.TrimSpace(response.GetCoreAddress()) == "" {
		return "", ErrCoreNotReady
	}
	return response.GetCoreAddress(), nil
}

func (host *RuntimeHost) ensureAuthority(ctx context.Context, target string) error {
	if host.membership != nil && !host.ownsResources {
		host.stopAuthority()
		host.setAuthorityState(AuthorityStatusNotRequired, "")
		return nil
	}
	if host.authorityFactory == nil {
		host.setAuthorityState(AuthorityStatusNotConfigured, "")
		return nil
	}
	host.mu.RLock()
	current := host.authority
	currentTarget := host.authorityTarget
	host.mu.RUnlock()
	if current != nil && currentTarget == target && current.Ready() {
		return nil
	}
	if current != nil {
		host.stopAuthority()
	}
	session, err := host.authorityFactory()
	if err != nil {
		return err
	}
	if session == nil {
		return errors.New("authority factory returned nil session")
	}
	host.setAuthorityState(AuthorityStatusBinding, "")
	if err := session.Start(ctx, target); err != nil {
		_ = session.Close()
		return err
	}
	host.mu.Lock()
	if host.closed {
		host.mu.Unlock()
		_ = session.Close()
		return ErrCapabilityClosed
	}
	host.authority = session
	host.authorityTarget = target
	host.mu.Unlock()
	return nil
}

func (host *RuntimeHost) setAuthorityState(state, message string) {
	host.mu.Lock()
	host.authorityStatus = state
	host.authorityError = message
	host.mu.Unlock()
}

func (host *RuntimeHost) stopAuthority() {
	host.mu.Lock()
	session := host.authority
	host.authority = nil
	host.authorityTarget = ""
	host.authorityStatus = AuthorityStatusNotReady
	host.authorityError = ""
	host.mu.Unlock()
	if session != nil {
		_ = session.Close()
	}
}

func (host *RuntimeHost) deactivateCore() {
	host.mu.Lock()
	core := host.core
	if core == nil && !host.activatingCore {
		host.coreEndpoint = ""
		host.mu.Unlock()
		return
	}
	if host.activatingCore && host.coreActivationCancel != nil {
		host.coreActivationCancel()
	}
	done := host.activationDone
	host.core = nil
	host.coreEndpoint = ""
	host.mu.Unlock()
	if done != nil {
		<-done
	}
	if core != nil {
		_ = core.Close()
	}
}

func coreEndpointForListener(listener net.Listener, requested string) string {
	if listener == nil {
		return ""
	}
	address := listener.Addr().String()
	requestedHost, _, requestedErr := net.SplitHostPort(requested)
	actualHost, actualPort, actualErr := net.SplitHostPort(address)
	if requestedErr == nil && actualErr == nil && (actualHost == "0.0.0.0" || actualHost == "::" || actualHost == "[::]") {
		return net.JoinHostPort(requestedHost, actualPort)
	}
	return address
}
