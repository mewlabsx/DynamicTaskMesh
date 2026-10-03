package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"dtm/internal/lifecycle"
	"dtm/internal/model"
	"dtm/internal/node"
	storageport "dtm/internal/storage"
)

func (repository *Repository) UpsertNode(ctx context.Context, record storageport.NodeRecord) (returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if err := validateNodeRecord(record); err != nil {
		return err
	}
	capabilitiesJSON, err := encodeNodeCapabilities(record.Capabilities)
	if err != nil {
		return err
	}
	metadataJSON, err := encodeJSON("node metadata", record.Metadata)
	if err != nil {
		return err
	}
	createdAt, _ := encodeTime("node created_at", record.CreatedAt)
	updatedAt, _ := encodeTime("node updated_at", record.UpdatedAt)
	registeredAt, _ := encodeTime("node registered_at", record.RegisteredAt)
	lastHeartbeatAt, _ := encodeTime("node last_heartbeat_at", record.LastHeartbeatAt)
	leaseExpiresAt, _ := encodeTime("node lease_expires_at", record.LeaseExpiresAt)
	result, err := repository.db.ExecContext(ctx, `
INSERT INTO nodes (
	node_id, endpoint, capabilities_json, status, generation, registration_id,
	metadata_json, registered_at, last_heartbeat_at, lease_expires_at,
	created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(node_id) DO UPDATE SET
	endpoint = excluded.endpoint,
	capabilities_json = excluded.capabilities_json,
	status = excluded.status,
	generation = excluded.generation,
	registration_id = excluded.registration_id,
	metadata_json = excluded.metadata_json,
	registered_at = excluded.registered_at,
	last_heartbeat_at = excluded.last_heartbeat_at,
	lease_expires_at = excluded.lease_expires_at,
	updated_at = excluded.updated_at
WHERE excluded.generation >= nodes.generation
  AND excluded.updated_at >= nodes.updated_at`,
		record.ID, record.Endpoint, capabilitiesJSON, record.Status, record.Generation,
		record.RegistrationID, metadataJSON, registeredAt, lastHeartbeatAt,
		leaseExpiresAt, createdAt, updatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert node %q: %w", record.ID, err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect node %q upsert: %w", record.ID, err)
	}
	if affected > 0 {
		return nil
	}
	current, err := repository.GetNode(ctx, record.ID)
	if err != nil {
		return err
	}
	switch {
	case record.Generation < current.Generation:
		return fmt.Errorf("%w: node %q generation %d is older than %d", storageport.ErrNodeGenerationConflict, record.ID, record.Generation, current.Generation)
	case record.UpdatedAt.Before(current.UpdatedAt):
		return nodeTimeRegression(record.ID, "upsert updated_at", record.UpdatedAt, current.UpdatedAt)
	default:
		return fmt.Errorf("%w: node %q upsert lost optimistic race", storageport.ErrNodeConcurrentMutation, record.ID)
	}
}

func (repository *Repository) RegisterNode(
	ctx context.Context,
	request storageport.RegisterNodeRequest,
) (record storageport.NodeRecord, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	prepared, err := prepareNodeRegistration(request)
	if err != nil {
		return storageport.NodeRecord{}, err
	}
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return storageport.NodeRecord{}, fmt.Errorf("begin register node %q: %w", prepared.request.ID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("register node %q", prepared.request.ID))
	record, err = registerNodeInTransaction(ctx, transaction, prepared, false)
	if err != nil {
		return storageport.NodeRecord{}, err
	}
	if err := transaction.Commit(); err != nil {
		return storageport.NodeRecord{}, fmt.Errorf("commit node %q registration: %w", prepared.request.ID, err)
	}
	return record, nil
}

type preparedNodeRegistration struct {
	request          storageport.RegisterNodeRequest
	capabilitiesJSON string
	metadataJSON     string
	registeredAt     int64
	leaseExpiresAt   int64
}

func prepareNodeRegistration(request storageport.RegisterNodeRequest) (preparedNodeRegistration, error) {
	request.ID = model.NodeID(strings.TrimSpace(string(request.ID)))
	request.Endpoint = strings.TrimSpace(request.Endpoint)
	request.RegistrationID = strings.TrimSpace(request.RegistrationID)
	request.RegisteredAt = request.RegisteredAt.UTC()
	request.LeaseExpiresAt = request.LeaseExpiresAt.UTC()
	if request.ID == "" || request.Endpoint == "" || request.RegistrationID == "" ||
		request.Metadata == nil || request.RegisteredAt.IsZero() ||
		!request.LeaseExpiresAt.After(request.RegisteredAt) {
		return preparedNodeRegistration{}, invalidData("node %q registration fields", request.ID)
	}
	validated, err := node.New(request.ID, request.Capabilities, node.StatusActive)
	if err != nil || len(validated.Capabilities()) != len(request.Capabilities) {
		return preparedNodeRegistration{}, invalidData("node %q registration capabilities", request.ID)
	}
	capabilitiesJSON, err := encodeNodeCapabilities(request.Capabilities)
	if err != nil {
		return preparedNodeRegistration{}, err
	}
	metadataJSON, err := encodeJSON("node metadata", request.Metadata)
	if err != nil {
		return preparedNodeRegistration{}, err
	}
	registeredAt, _ := encodeTime("node registered_at", request.RegisteredAt)
	leaseExpiresAt, _ := encodeTime("node lease_expires_at", request.LeaseExpiresAt)
	return preparedNodeRegistration{
		request: request, capabilitiesJSON: capabilitiesJSON, metadataJSON: metadataJSON,
		registeredAt: registeredAt, leaseExpiresAt: leaseExpiresAt,
	}, nil
}

func registerNodeInTransaction(ctx context.Context, transaction *sql.Tx, prepared preparedNodeRegistration, forceGenerationAdvance bool) (storageport.NodeRecord, error) {
	request := prepared.request
	var currentRegistration, currentStatus string
	var currentGeneration int64
	var currentRegisteredAtRaw, currentHeartbeatRaw, currentUpdatedAtRaw any
	err := transaction.QueryRowContext(ctx, `
SELECT registration_id, generation, status, registered_at, last_heartbeat_at, updated_at
FROM nodes WHERE node_id = ?`, request.ID).Scan(
		&currentRegistration, &currentGeneration, &currentStatus,
		&currentRegisteredAtRaw, &currentHeartbeatRaw, &currentUpdatedAtRaw,
	)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		_, err = transaction.ExecContext(ctx, `
INSERT INTO nodes (
	node_id, endpoint, capabilities_json, status, generation, registration_id,
	metadata_json, registered_at, last_heartbeat_at, lease_expires_at,
	created_at, updated_at
) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?)`,
			request.ID, request.Endpoint, prepared.capabilitiesJSON, node.StatusActive,
			request.RegistrationID, prepared.metadataJSON, prepared.registeredAt, prepared.registeredAt,
			prepared.leaseExpiresAt, prepared.registeredAt, prepared.registeredAt,
		)
		if err != nil {
			return storageport.NodeRecord{}, classifyCreateError("node", string(request.ID), err)
		}
	case err != nil:
		return storageport.NodeRecord{}, fmt.Errorf("inspect node %q registration: %w", request.ID, err)
	default:
		currentRegisteredAt, err := decodeTime("node registered_at", currentRegisteredAtRaw)
		if err != nil {
			return storageport.NodeRecord{}, fmt.Errorf("inspect node %q registration: %w", request.ID, err)
		}
		currentHeartbeat, err := decodeTime("node last_heartbeat_at", currentHeartbeatRaw)
		if err != nil {
			return storageport.NodeRecord{}, fmt.Errorf("inspect node %q registration: %w", request.ID, err)
		}
		currentUpdatedAt, err := decodeTime("node updated_at", currentUpdatedAtRaw)
		if err != nil {
			return storageport.NodeRecord{}, fmt.Errorf("inspect node %q registration: %w", request.ID, err)
		}
		minimum := latestTime(currentRegisteredAt, currentHeartbeat, currentUpdatedAt)
		if request.RegisteredAt.Before(minimum) {
			return storageport.NodeRecord{}, nodeTimeRegression(request.ID, "registration replay", request.RegisteredAt, minimum)
		}
		newGeneration := currentGeneration
		if forceGenerationAdvance || currentRegistration != request.RegistrationID ||
			node.Status(currentStatus) == node.StatusStale || node.Status(currentStatus) == node.StatusOffline {
			newGeneration++
		}
		result, err := transaction.ExecContext(ctx, `
UPDATE nodes
SET endpoint = ?, capabilities_json = ?, status = ?, generation = ?,
    registration_id = ?, metadata_json = ?, registered_at = ?,
    last_heartbeat_at = ?, lease_expires_at = ?, updated_at = ?
WHERE node_id = ? AND registration_id = ? AND generation = ?
  AND status = ? AND updated_at = ?`,
			request.Endpoint, prepared.capabilitiesJSON, node.StatusActive, newGeneration,
			request.RegistrationID, prepared.metadataJSON, prepared.registeredAt, prepared.registeredAt,
			prepared.leaseExpiresAt, prepared.registeredAt, request.ID, currentRegistration, currentGeneration,
			currentStatus, currentUpdatedAtRaw,
		)
		if err != nil {
			return storageport.NodeRecord{}, fmt.Errorf("update node %q registration: %w", request.ID, err)
		}
		if err := classifyNodeMutation(ctx, transaction, result, request.ID, currentRegistration, currentGeneration, node.Status(currentStatus), request.RegisteredAt, "registration"); err != nil {
			return storageport.NodeRecord{}, err
		}
	}
	record, err := getNodeWithQueryer(ctx, transaction, request.ID)
	if err != nil {
		return storageport.NodeRecord{}, err
	}
	return record, nil
}

func (repository *Repository) RenewNodeLease(ctx context.Context, request storageport.RenewNodeLeaseRequest) (returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	request.ID = model.NodeID(strings.TrimSpace(string(request.ID)))
	request.RegistrationID = strings.TrimSpace(request.RegistrationID)
	request.LastHeartbeatAt = request.LastHeartbeatAt.UTC()
	request.LeaseExpiresAt = request.LeaseExpiresAt.UTC()
	if request.ID == "" || request.RegistrationID == "" || request.LastHeartbeatAt.IsZero() ||
		!request.LeaseExpiresAt.After(request.LastHeartbeatAt) {
		return invalidData("node %q heartbeat fields", request.ID)
	}
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin renew node %q lease: %w", request.ID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("renew node %q lease", request.ID))
	current, err := getNodeWithQueryer(ctx, transaction, request.ID)
	if err != nil {
		return err
	}
	if current.RegistrationID != request.RegistrationID {
		return fmt.Errorf("%w: node %q heartbeat registration %q is not current", storageport.ErrStaleRegistration, request.ID, request.RegistrationID)
	}
	if current.Status == node.StatusStale || current.Status == node.StatusOffline {
		return fmt.Errorf("%w: node %q registration %q must be re-established from %q", storageport.ErrNodeReregistrationRequired, request.ID, request.RegistrationID, current.Status)
	}
	if current.Status != node.StatusRegistered && current.Status != node.StatusActive && current.Status != node.StatusSuspect {
		return fmt.Errorf("%w: node %q cannot heartbeat from %q", storageport.ErrNodeStateConflict, request.ID, current.Status)
	}
	minimum := latestTime(current.RegisteredAt, current.LastHeartbeatAt, current.UpdatedAt)
	if request.LastHeartbeatAt.Before(minimum) {
		return nodeTimeRegression(request.ID, "heartbeat", request.LastHeartbeatAt, minimum)
	}
	heartbeatAt, _ := encodeTime("node last_heartbeat_at", request.LastHeartbeatAt)
	expiresAt, _ := encodeTime("node lease_expires_at", request.LeaseExpiresAt)
	currentUpdatedAt, _ := encodeTime("node current updated_at", current.UpdatedAt)
	result, err := transaction.ExecContext(ctx, `
UPDATE nodes
SET status = ?, last_heartbeat_at = ?, lease_expires_at = ?, updated_at = ?
WHERE node_id = ? AND registration_id = ? AND generation = ?
  AND status = ? AND last_heartbeat_at = ? AND updated_at = ?`,
		node.StatusActive, heartbeatAt, expiresAt, heartbeatAt,
		request.ID, request.RegistrationID, current.Generation, current.Status,
		mustEncodeTime(current.LastHeartbeatAt), currentUpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("renew node %q lease: %w", request.ID, err)
	}
	if err := classifyNodeMutation(ctx, transaction, result, request.ID, request.RegistrationID, current.Generation, current.Status, request.LastHeartbeatAt, "heartbeat"); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit node %q heartbeat: %w", request.ID, err)
	}
	return nil
}

func (repository *Repository) SetNodeOffline(ctx context.Context, request storageport.SetNodeOfflineRequest) (returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	request.ID = model.NodeID(strings.TrimSpace(string(request.ID)))
	request.RegistrationID = strings.TrimSpace(request.RegistrationID)
	request.UpdatedAt = request.UpdatedAt.UTC()
	if request.ID == "" || request.RegistrationID == "" || request.UpdatedAt.IsZero() {
		return invalidData("node %q offline fields", request.ID)
	}
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin set node %q offline: %w", request.ID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("set node %q offline", request.ID))
	current, err := getNodeWithQueryer(ctx, transaction, request.ID)
	if err != nil {
		return err
	}
	if current.RegistrationID != request.RegistrationID {
		return fmt.Errorf("%w: node %q offline registration %q is not current", storageport.ErrStaleRegistration, request.ID, request.RegistrationID)
	}
	if current.Status == node.StatusStale {
		return fmt.Errorf("%w: node %q cannot set offline from stale", storageport.ErrNodeStateConflict, request.ID)
	}
	minimum := latestTime(current.RegisteredAt, current.LastHeartbeatAt, current.UpdatedAt)
	if request.UpdatedAt.Before(minimum) {
		return nodeTimeRegression(request.ID, "offline updated_at", request.UpdatedAt, minimum)
	}
	updatedAt, _ := encodeTime("node offline updated_at", request.UpdatedAt)
	result, err := transaction.ExecContext(ctx, `
UPDATE nodes SET status = ?, updated_at = ?
WHERE node_id = ? AND registration_id = ? AND generation = ?
  AND status = ? AND updated_at = ?`,
		node.StatusOffline, updatedAt, request.ID, request.RegistrationID,
		current.Generation, current.Status, mustEncodeTime(current.UpdatedAt),
	)
	if err != nil {
		return fmt.Errorf("set node %q offline: %w", request.ID, err)
	}
	if err := classifyNodeMutation(ctx, transaction, result, request.ID, request.RegistrationID, current.Generation, current.Status, request.UpdatedAt, "offline"); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit node %q offline: %w", request.ID, err)
	}
	return nil
}

func (repository *Repository) FailClosedNode(ctx context.Context, nodeID model.NodeID, at time.Time) (returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	nodeID = model.NodeID(strings.TrimSpace(string(nodeID)))
	at = at.UTC()
	if nodeID == "" || at.IsZero() {
		return invalidData("node %q fail-closed fields", nodeID)
	}
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin fail-close node %q: %w", nodeID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("fail-close node %q", nodeID))
	current, err := getNodeWithQueryer(ctx, transaction, nodeID)
	if errors.Is(err, storageport.ErrNodeNotFound) {
		if err := transaction.Commit(); err != nil {
			return fmt.Errorf("commit absent node %q fail-close: %w", nodeID, err)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if current.Status == node.StatusOffline || current.Status == node.StatusStale {
		if err := transaction.Commit(); err != nil {
			return fmt.Errorf("commit node %q fail-close no-op: %w", nodeID, err)
		}
		return nil
	}
	minimum := latestTime(current.RegisteredAt, current.LastHeartbeatAt, current.UpdatedAt)
	if at.Before(minimum) {
		return nodeTimeRegression(nodeID, "fail-closed updated_at", at, minimum)
	}
	result, err := transaction.ExecContext(ctx, `
UPDATE nodes SET status = ?, updated_at = ?
WHERE node_id = ? AND generation = ? AND status = ? AND updated_at = ?`,
		node.StatusOffline, mustEncodeTime(at), nodeID, current.Generation,
		current.Status, mustEncodeTime(current.UpdatedAt),
	)
	if err != nil {
		return fmt.Errorf("fail-close node %q: %w", nodeID, err)
	}
	if err := classifyNodeMutation(ctx, transaction, result, nodeID, current.RegistrationID, current.Generation, current.Status, at, "fail-closed"); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("commit fail-close node %q: %w", nodeID, err)
	}
	return nil
}

func (repository *Repository) ExpireNodeLeases(ctx context.Context, now time.Time) (count int64, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if now.IsZero() {
		return 0, invalidData("node lease expiry time")
	}
	now = now.UTC()
	nowValue := mustEncodeTime(now)
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin expire node leases: %w", err)
	}
	defer rollbackOnError(transaction, &returnErr, "expire node leases")
	var regressionCount int
	if err := transaction.QueryRowContext(ctx, `
SELECT COUNT(*) FROM nodes
WHERE status IN (?, ?, ?, ?) AND lease_expires_at <= ? AND updated_at > ?`,
		node.StatusRegistered, node.StatusActive, node.StatusSuspect, node.StatusRecovering,
		nowValue, nowValue,
	).Scan(&regressionCount); err != nil {
		return 0, fmt.Errorf("inspect node lease expiry times: %w", err)
	}
	if regressionCount > 0 {
		return 0, fmt.Errorf("%w: expire node leases at %s would regress %d record(s)", storageport.ErrNodeTimeRegression, now.Format(time.RFC3339Nano), regressionCount)
	}
	result, err := transaction.ExecContext(ctx, `
UPDATE nodes SET status = ?, updated_at = ?
WHERE status IN (?, ?, ?, ?) AND lease_expires_at <= ?`,
		node.StatusOffline, nowValue, node.StatusRegistered, node.StatusActive,
		node.StatusSuspect, node.StatusRecovering, nowValue,
	)
	if err != nil {
		return 0, fmt.Errorf("expire node leases: %w", err)
	}
	count, err = result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect expired node leases: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return 0, fmt.Errorf("commit expired node leases: %w", err)
	}
	return count, nil
}

func (repository *Repository) MarkNodesStale(ctx context.Context, now time.Time) (count int64, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if now.IsZero() {
		return 0, invalidData("node stale time")
	}
	now = now.UTC()
	nowValue := mustEncodeTime(now)
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin mark historical nodes stale: %w", err)
	}
	defer rollbackOnError(transaction, &returnErr, "mark historical nodes stale")
	var regressionCount int
	if err := transaction.QueryRowContext(ctx, `
SELECT COUNT(*) FROM nodes
WHERE status IN (?, ?, ?, ?) AND updated_at > ?`,
		node.StatusRegistered, node.StatusActive, node.StatusSuspect, node.StatusRecovering, nowValue,
	).Scan(&regressionCount); err != nil {
		return 0, fmt.Errorf("inspect historical node times: %w", err)
	}
	if regressionCount > 0 {
		return 0, fmt.Errorf("%w: stale time %s would regress %d record(s)", storageport.ErrNodeTimeRegression, now.Format(time.RFC3339Nano), regressionCount)
	}
	result, err := transaction.ExecContext(ctx, `
UPDATE nodes SET status = ?, updated_at = ?
WHERE status IN (?, ?, ?, ?)`,
		node.StatusStale, nowValue, node.StatusRegistered, node.StatusActive,
		node.StatusSuspect, node.StatusRecovering,
	)
	if err != nil {
		return 0, fmt.Errorf("mark historical nodes stale: %w", err)
	}
	count, err = result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("inspect stale node count: %w", err)
	}
	if err := transaction.Commit(); err != nil {
		return 0, fmt.Errorf("commit historical node stale update: %w", err)
	}
	return count, nil
}

func (repository *Repository) GetNode(ctx context.Context, nodeID model.NodeID) (record storageport.NodeRecord, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if strings.TrimSpace(string(nodeID)) == "" {
		return storageport.NodeRecord{}, invalidData("node ID is required")
	}
	return getNodeWithQueryer(ctx, repository.db, nodeID)
}

type nodeQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getNodeWithQueryer(ctx context.Context, queryer nodeQueryer, nodeID model.NodeID) (storageport.NodeRecord, error) {
	var record storageport.NodeRecord
	var capabilitiesJSON, status, metadataJSON string
	var createdAt, updatedAt, registeredAt, lastHeartbeatAt, leaseExpiresAt any
	err := queryer.QueryRowContext(ctx, `
SELECT node_id, endpoint, capabilities_json, status, generation,
       registration_id, metadata_json, registered_at, last_heartbeat_at,
       lease_expires_at, created_at, updated_at
FROM nodes WHERE node_id = ?`, nodeID).Scan(
		&record.ID, &record.Endpoint, &capabilitiesJSON, &status, &record.Generation,
		&record.RegistrationID, &metadataJSON, &registeredAt, &lastHeartbeatAt,
		&leaseExpiresAt, &createdAt, &updatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return storageport.NodeRecord{}, fmt.Errorf("%w: node %q", storageport.ErrNodeNotFound, nodeID)
	}
	if err != nil {
		return storageport.NodeRecord{}, fmt.Errorf("get node %q: %w", nodeID, err)
	}
	record.Status = node.Status(status)
	if err := decodeJSON("node capabilities", capabilitiesJSON, &record.Capabilities); err != nil {
		return storageport.NodeRecord{}, fmt.Errorf("get node %q: %w", nodeID, err)
	}
	if err := decodeJSON("node metadata", metadataJSON, &record.Metadata); err != nil {
		return storageport.NodeRecord{}, fmt.Errorf("get node %q: %w", nodeID, err)
	}
	for label, item := range map[string]struct {
		raw    any
		target *time.Time
	}{
		"created_at":        {createdAt, &record.CreatedAt},
		"updated_at":        {updatedAt, &record.UpdatedAt},
		"registered_at":     {registeredAt, &record.RegisteredAt},
		"last_heartbeat_at": {lastHeartbeatAt, &record.LastHeartbeatAt},
		"lease_expires_at":  {leaseExpiresAt, &record.LeaseExpiresAt},
	} {
		value, err := decodeTime("node "+label, item.raw)
		if err != nil {
			return storageport.NodeRecord{}, fmt.Errorf("get node %q: %w", nodeID, err)
		}
		*item.target = value
	}
	if err := validateNodeRecord(record); err != nil {
		return storageport.NodeRecord{}, fmt.Errorf("get node %q: %w", nodeID, err)
	}
	return record, nil
}

func encodeNodeCapabilities(capabilities []model.Capability) (string, error) {
	copy := append([]model.Capability(nil), capabilities...)
	sort.Slice(copy, func(i, j int) bool { return copy[i] < copy[j] })
	return encodeJSON("node capabilities", copy)
}

func classifyNodeMutation(
	ctx context.Context,
	queryer nodeQueryer,
	result sql.Result,
	nodeID model.NodeID,
	expectedRegistration string,
	expectedGeneration int64,
	expectedStatus node.Status,
	attemptedAt time.Time,
	operation string,
) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect node %q %s: %w", nodeID, operation, err)
	}
	if affected > 0 {
		return nil
	}
	current, err := getNodeWithQueryer(ctx, queryer, nodeID)
	if err != nil {
		return err
	}
	switch {
	case expectedRegistration != "" && current.RegistrationID != expectedRegistration:
		return fmt.Errorf("%w: node %q %s registration %q is no longer current", storageport.ErrStaleRegistration, nodeID, operation, expectedRegistration)
	case current.Generation != expectedGeneration:
		return fmt.Errorf("%w: node %q %s generation changed from %d to %d", storageport.ErrNodeGenerationConflict, nodeID, operation, expectedGeneration, current.Generation)
	case current.Status != expectedStatus:
		return fmt.Errorf("%w: node %q %s state changed from %q to %q", storageport.ErrNodeStateConflict, nodeID, operation, expectedStatus, current.Status)
	case attemptedAt.Before(latestTime(current.RegisteredAt, current.LastHeartbeatAt, current.UpdatedAt)):
		return nodeTimeRegression(nodeID, operation, attemptedAt, latestTime(current.RegisteredAt, current.LastHeartbeatAt, current.UpdatedAt))
	default:
		return fmt.Errorf("%w: node %q %s lost optimistic race", storageport.ErrNodeConcurrentMutation, nodeID, operation)
	}
}
func nodeTimeRegression(nodeID model.NodeID, operation string, received, minimum time.Time) error {
	return fmt.Errorf("%w: node %q %s time %s precedes %s", storageport.ErrNodeTimeRegression, nodeID, operation, received.Format(time.RFC3339Nano), minimum.Format(time.RFC3339Nano))
}

func latestTime(values ...time.Time) time.Time {
	latest := values[0]
	for _, value := range values[1:] {
		if value.After(latest) {
			latest = value
		}
	}
	return latest
}

func mustEncodeTime(value time.Time) int64 {
	encoded, _ := encodeTime("node time", value)
	return encoded
}

func (repository *Repository) AppendTaskEvent(ctx context.Context, event storageport.TaskEvent) (returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if err := validateEventForAppend(event); err != nil {
		return err
	}
	return insertTaskEvent(ctx, repository.db, event)
}

func (repository *Repository) ListTaskEvents(ctx context.Context, taskID model.TaskID) (events []storageport.TaskEvent, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if strings.TrimSpace(string(taskID)) == "" {
		return nil, invalidData("task ID is required")
	}
	rows, err := repository.db.QueryContext(ctx, `
SELECT event_id, task_id, step_id, event_type, from_state, to_state,
       detail_json, created_at
FROM task_events
WHERE task_id = ?
ORDER BY created_at, event_id`, taskID)
	if err != nil {
		return nil, fmt.Errorf("list task %q events: %w", taskID, err)
	}
	defer rows.Close()
	events = make([]storageport.TaskEvent, 0)
	for rows.Next() {
		var event storageport.TaskEvent
		var stepID, fromState, toState, detailJSON sql.NullString
		var createdAt any
		if err := rows.Scan(
			&event.ID, &event.TaskID, &stepID, &event.Type,
			&fromState, &toState, &detailJSON, &createdAt,
		); err != nil {
			return nil, fmt.Errorf("scan task %q event: %w", taskID, err)
		}
		if stepID.Valid {
			value := model.StepID(stepID.String)
			event.StepID = &value
		}
		event.FromState = fromState.String
		event.ToState = toState.String
		if detailJSON.Valid {
			if err := decodeJSON("task event detail", detailJSON.String, &event.Detail); err != nil {
				return nil, fmt.Errorf("list task %q event %d: %w", taskID, event.ID, err)
			}
		}
		event.CreatedAt, err = decodeTime("task event created_at", createdAt)
		if err != nil {
			return nil, fmt.Errorf("list task %q event %d: %w", taskID, event.ID, err)
		}
		if err := validateEvent(event); err != nil {
			return nil, fmt.Errorf("list task %q event %d: %w", taskID, event.ID, err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read task %q events: %w", taskID, err)
	}
	return events, nil
}

func validateTaskStateEvent(
	taskID model.TaskID,
	expected lifecycle.State,
	next lifecycle.State,
	event storageport.TaskEvent,
) error {
	if err := validateEventForAppend(event); err != nil {
		return err
	}
	if event.TaskID != taskID || event.StepID != nil ||
		event.FromState != string(expected) || event.ToState != string(next) {
		return invalidData("task %q state event does not match transition", taskID)
	}
	return nil
}

func validateStepStateEvent(
	taskID model.TaskID,
	stepID model.StepID,
	expected lifecycle.StepState,
	next lifecycle.StepState,
	event storageport.TaskEvent,
) error {
	if err := validateEventForAppend(event); err != nil {
		return err
	}
	if event.TaskID != taskID || event.StepID == nil || *event.StepID != stepID ||
		event.FromState != string(expected) || event.ToState != string(next) {
		return invalidData("task %q step %q event does not match transition", taskID, stepID)
	}
	return nil
}

func validateEventForAppend(event storageport.TaskEvent) error {
	if event.ID != 0 {
		return invalidData("new task event ID must be zero")
	}
	return validateEvent(event)
}

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertTaskEvent(ctx context.Context, executor sqlExecutor, event storageport.TaskEvent) error {
	var detailJSON any
	if event.Detail != nil {
		encoded, err := encodeJSON("task event detail", event.Detail)
		if err != nil {
			return err
		}
		detailJSON = encoded
	}
	createdAt, err := encodeTime("task event created_at", event.CreatedAt)
	if err != nil {
		return err
	}
	var stepID any
	if event.StepID != nil {
		stepID = *event.StepID
	}
	_, err = executor.ExecContext(ctx, `
INSERT INTO task_events (
	task_id, step_id, event_type, from_state, to_state, detail_json, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		event.TaskID, stepID, event.Type,
		nullableString(event.FromState), nullableString(event.ToState), detailJSON, createdAt,
	)
	if err != nil {
		if isSQLiteForeignKeyConstraint(err) {
			return fmt.Errorf("%w: task %q or referenced step for event %q: %w",
				storageport.ErrNotFound, event.TaskID, event.Type, err)
		}
		return fmt.Errorf("append task %q event %q: %w", event.TaskID, event.Type, err)
	}
	return nil
}

func classifyConditionalUpdate(
	ctx context.Context,
	transaction *sql.Tx,
	result sql.Result,
	objectType string,
	objectID string,
	existsQuery string,
	existsArguments ...any,
) error {
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("inspect %s %q state update: %w", objectType, objectID, err)
	}
	if affected > 0 {
		return nil
	}
	var exists int
	err = transaction.QueryRowContext(ctx, existsQuery, existsArguments...).Scan(&exists)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%w: %s %q", storageport.ErrNotFound, objectType, objectID)
	}
	if err != nil {
		return fmt.Errorf("inspect %s %q existence: %w", objectType, objectID, err)
	}
	return fmt.Errorf("%w: %s %q expected state mismatch", storageport.ErrConflict, objectType, objectID)
}

func classifyCreateError(objectType, objectID string, err error) error {
	if isSQLiteUniqueConstraint(err) {
		return fmt.Errorf("%w: %s %q: %w", storageport.ErrAlreadyExists, objectType, objectID, err)
	}
	if isSQLiteConstraint(err) {
		return fmt.Errorf("%w: create %s %q: %w", storageport.ErrInvalidData, objectType, objectID, err)
	}
	return fmt.Errorf("create %s %q: %w", objectType, objectID, err)
}

func isSQLiteUniqueConstraint(err error) bool {
	code, ok := sqliteErrorCode(err)
	return ok && (code == 1555 || code == 2067)
}

func isSQLiteConstraint(err error) bool {
	code, ok := sqliteErrorCode(err)
	return ok && code&0xff == 19
}

func isSQLiteForeignKeyConstraint(err error) bool {
	code, ok := sqliteErrorCode(err)
	return ok && code == 787
}

func sqliteErrorCode(err error) (int, bool) {
	var sqliteError interface{ Code() int }
	if !errors.As(err, &sqliteError) {
		return 0, false
	}
	return sqliteError.Code(), true
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
