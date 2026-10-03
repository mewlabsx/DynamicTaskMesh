package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
	storageport "dtm/internal/storage"
)

type resourceRegistrationFaultPoint string

const (
	resourceFaultBeforeNodeWrite    resourceRegistrationFaultPoint = "before_node_write"
	resourceFaultAfterNodeWrite     resourceRegistrationFaultPoint = "after_node_write_before_resource"
	resourceFaultAfterFirstResource resourceRegistrationFaultPoint = "after_first_resource_write"
	resourceFaultAfterTombstone     resourceRegistrationFaultPoint = "after_tombstone_write"
	resourceFaultBeforeCommit       resourceRegistrationFaultPoint = "before_commit"
)

type resourceRegistrationFaultInjector func(context.Context, resourceRegistrationFaultPoint) error

type resourceQueryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func (repository *Repository) LoadAllResourceRecords(ctx context.Context) (records []resourcedirectory.ResourceRecordView, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	return loadResourceRecords(ctx, repository.db, "", nil)
}

func (repository *Repository) LoadResourceRecordsByNode(ctx context.Context, nodeID model.NodeID) (records []resourcedirectory.ResourceRecordView, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if _, err := model.NewResourceRef("resource-query", 1, nodeID, 1, "resource-query"); err != nil {
		return nil, invalidArgument("resource owner node %q", nodeID)
	}
	return loadResourceRecords(ctx, repository.db, "WHERE r.owner_node_id = ?", []any{nodeID})
}

func (repository *Repository) GetResourceRecord(ctx context.Context, resourceID model.ResourceID) (record resourcedirectory.ResourceRecordView, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if err := resourceID.Validate(); err != nil {
		return resourcedirectory.ResourceRecordView{}, invalidArgument("resource ID: %v", err)
	}
	records, err := loadResourceRecords(ctx, repository.db, "WHERE r.resource_id = ?", []any{resourceID})
	if err != nil {
		return resourcedirectory.ResourceRecordView{}, err
	}
	if len(records) == 0 {
		return resourcedirectory.ResourceRecordView{}, fmt.Errorf("%w: resource %q", storageport.ErrNotFound, resourceID)
	}
	return records[0], nil
}

func (repository *Repository) ApplyNodeResourceSnapshot(
	ctx context.Context,
	snapshot resourcedirectory.NodeResourceSnapshot,
	at time.Time,
) (plan resourcedirectory.TransitionPlan, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	if at.IsZero() {
		return resourcedirectory.TransitionPlan{}, invalidArgument("resource snapshot timestamp")
	}
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return resourcedirectory.TransitionPlan{}, fmt.Errorf("begin resource snapshot for node %q: %w", snapshot.NodeID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("resource snapshot for node %q", snapshot.NodeID))
	if err := validateResourceOwnerFence(ctx, transaction, snapshot); err != nil {
		return resourcedirectory.TransitionPlan{}, err
	}
	current, err := loadResourceRecords(ctx, transaction, "", nil)
	if err != nil {
		return resourcedirectory.TransitionPlan{}, err
	}
	plan, err = resourcedirectory.PlanNodeResourceSnapshot(current, snapshot)
	if err != nil {
		return resourcedirectory.TransitionPlan{}, err
	}
	if err := persistResourceTransition(ctx, transaction, current, plan, at.UTC(), nil); err != nil {
		return resourcedirectory.TransitionPlan{}, err
	}
	if err := repository.commitResourceTransaction(transaction); err != nil {
		return resourcedirectory.TransitionPlan{}, fmt.Errorf("commit resource snapshot for node %q: %w", snapshot.NodeID, err)
	}
	return plan.Clone(), nil
}

func (repository *Repository) RegisterNodeWithResources(
	ctx context.Context,
	request storageport.RegisterNodeRequest,
	requested []model.ResourceDescriptor,
	forceNodeGenerationAdvance bool,
) (node storageport.NodeRecord, plan resourcedirectory.TransitionPlan, returnErr error) {
	defer classifyRepositoryReturn(ctx, &returnErr)
	prepared, err := prepareNodeRegistration(request)
	if err != nil {
		return storageport.NodeRecord{}, resourcedirectory.TransitionPlan{}, err
	}
	transaction, err := repository.db.BeginTx(ctx, nil)
	if err != nil {
		return storageport.NodeRecord{}, resourcedirectory.TransitionPlan{}, fmt.Errorf("begin node and resource registration %q: %w", prepared.request.ID, err)
	}
	defer rollbackOnError(transaction, &returnErr, fmt.Sprintf("node and resource registration %q", prepared.request.ID))
	if err := repository.injectResourceFault(ctx, resourceFaultBeforeNodeWrite); err != nil {
		return storageport.NodeRecord{}, resourcedirectory.TransitionPlan{}, err
	}
	nodeRecord, err := registerNodeInTransaction(ctx, transaction, prepared, forceNodeGenerationAdvance)
	if err != nil {
		return storageport.NodeRecord{}, resourcedirectory.TransitionPlan{}, err
	}
	if err := repository.injectResourceFault(ctx, resourceFaultAfterNodeWrite); err != nil {
		return storageport.NodeRecord{}, resourcedirectory.TransitionPlan{}, err
	}
	current, err := loadResourceRecords(ctx, transaction, "", nil)
	if err != nil {
		return storageport.NodeRecord{}, resourcedirectory.TransitionPlan{}, err
	}
	requested = descriptorsWithExpectedGenerations(requested, current)
	snapshot, err := resourcedirectory.NewNodeResourceSnapshot(
		nodeRecord.ID, nodeRecord.Generation, nodeRecord.RegistrationID, requested,
	)
	if err != nil {
		return storageport.NodeRecord{}, resourcedirectory.TransitionPlan{}, err
	}
	plan, err = resourcedirectory.PlanNodeResourceSnapshot(current, snapshot)
	if err != nil {
		return storageport.NodeRecord{}, resourcedirectory.TransitionPlan{}, err
	}
	if err := persistResourceTransition(ctx, transaction, current, plan, prepared.request.RegisteredAt, repository.injectResourceFault); err != nil {
		return storageport.NodeRecord{}, resourcedirectory.TransitionPlan{}, err
	}
	if err := repository.injectResourceFault(ctx, resourceFaultBeforeCommit); err != nil {
		return storageport.NodeRecord{}, resourcedirectory.TransitionPlan{}, err
	}
	if err := repository.commitResourceTransaction(transaction); err != nil {
		return storageport.NodeRecord{}, resourcedirectory.TransitionPlan{}, fmt.Errorf("commit node and resource registration %q: %w", prepared.request.ID, err)
	}
	return nodeRecord, plan.Clone(), nil
}

func (repository *Repository) commitResourceTransaction(transaction *sql.Tx) error {
	if repository.resourceCommit != nil {
		return repository.resourceCommit(transaction)
	}
	return transaction.Commit()
}

func validateResourceOwnerFence(ctx context.Context, transaction *sql.Tx, snapshot resourcedirectory.NodeResourceSnapshot) error {
	if err := snapshot.Validate(); err != nil {
		return err
	}
	var generation int64
	var registrationID string
	err := transaction.QueryRowContext(ctx, `SELECT generation, registration_id FROM nodes WHERE node_id = ?`, snapshot.NodeID).Scan(&generation, &registrationID)
	if err == sql.ErrNoRows {
		return fmt.Errorf("%w: owner node %q", storageport.ErrNodeNotFound, snapshot.NodeID)
	}
	if err != nil {
		return fmt.Errorf("load owner node fence %q: %w", snapshot.NodeID, err)
	}
	if generation != snapshot.NodeGeneration {
		return fmt.Errorf("%w: owner node %q generation is %d, snapshot has %d", storageport.ErrNodeGenerationConflict, snapshot.NodeID, generation, snapshot.NodeGeneration)
	}
	if registrationID != snapshot.RegistrationID {
		return fmt.Errorf("%w: owner node %q registration is not current", storageport.ErrStaleRegistration, snapshot.NodeID)
	}
	return nil
}

func descriptorsWithExpectedGenerations(requested []model.ResourceDescriptor, current []resourcedirectory.ResourceRecordView) []model.ResourceDescriptor {
	byID := make(map[model.ResourceID]model.ResourceGeneration, len(current))
	for _, record := range current {
		byID[record.Descriptor.ID] = record.Descriptor.Generation
	}
	result := make([]model.ResourceDescriptor, len(requested))
	for index := range requested {
		result[index] = requested[index].Clone()
		if generation, exists := byID[result[index].ID]; exists {
			result[index].Generation = generation
		} else {
			result[index].Generation = 1
		}
	}
	return result
}

func (repository *Repository) injectResourceFault(ctx context.Context, point resourceRegistrationFaultPoint) error {
	if repository.resourceRegistrationFault == nil {
		return nil
	}
	if err := repository.resourceRegistrationFault(ctx, point); err != nil {
		return fmt.Errorf("resource registration fault at %s: %w", point, err)
	}
	return nil
}

func loadResourceRecords(ctx context.Context, queryer resourceQueryer, where string, arguments []any) ([]resourcedirectory.ResourceRecordView, error) {
	query := `
SELECT r.resource_id, r.owner_node_id, r.resource_kind, r.resource_type,
       r.resource_generation, r.owner_node_generation, r.registration_id,
       r.publication_state, r.descriptor_encoding_version,
       r.operations_json, r.attributes_json, r.created_at, r.updated_at,
       EXISTS(SELECT 1 FROM nodes n WHERE n.node_id = r.owner_node_id)
FROM resources r ` + where + ` ORDER BY r.resource_id`
	rows, err := queryer.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, fmt.Errorf("load resource records: %w", err)
	}
	defer rows.Close()
	result := []resourcedirectory.ResourceRecordView{}
	for rows.Next() {
		var resourceID, ownerNodeID, kind, resourceType, generation, registrationID, publicationState string
		var ownerGeneration, encodingVersion int64
		var operationsJSON, attributesJSON string
		var createdRaw, updatedRaw any
		var ownerExists int
		if err := rows.Scan(
			&resourceID, &ownerNodeID, &kind, &resourceType, &generation,
			&ownerGeneration, &registrationID, &publicationState, &encodingVersion,
			&operationsJSON, &attributesJSON, &createdRaw, &updatedRaw, &ownerExists,
		); err != nil {
			return nil, fmt.Errorf("scan resource record: %w", err)
		}
		if ownerExists != 1 {
			return nil, invalidData("resource %q owner node %q is missing", resourceID, ownerNodeID)
		}
		if err := validateResourceEncodingVersion(encodingVersion); err != nil {
			return nil, resourceCodecError(model.ResourceID(resourceID), err)
		}
		resourceGeneration, err := decodeResourceGeneration(generation)
		if err != nil {
			return nil, resourceCodecError(model.ResourceID(resourceID), err)
		}
		operations, err := decodeResourceOperations(operationsJSON)
		if err != nil {
			return nil, resourceCodecError(model.ResourceID(resourceID), err)
		}
		attributes, err := decodeResourceAttributes(attributesJSON)
		if err != nil {
			return nil, resourceCodecError(model.ResourceID(resourceID), err)
		}
		descriptor, err := model.NewResourceDescriptor(
			model.ResourceID(resourceID), model.ResourceKind(kind), model.ResourceType(resourceType),
			model.NodeID(ownerNodeID), resourceGeneration, operations, attributes,
		)
		if err != nil {
			return nil, resourceCodecError(model.ResourceID(resourceID), invalidData("descriptor: %v", err))
		}
		if _, err := model.NewResourceRef(descriptor.ID, descriptor.Generation, descriptor.OwnerNodeID, ownerGeneration, registrationID); err != nil {
			return nil, resourceCodecError(descriptor.ID, invalidData("fence: %v", err))
		}
		state := resourcedirectory.PublicationState(publicationState)
		if state != resourcedirectory.PublicationStatePublished && state != resourcedirectory.PublicationStateWithdrawn {
			return nil, resourceCodecError(descriptor.ID, invalidData("publication state %q", state))
		}
		createdAt, err := decodeTime("resource created_at", createdRaw)
		if err != nil {
			return nil, resourceCodecError(descriptor.ID, err)
		}
		updatedAt, err := decodeTime("resource updated_at", updatedRaw)
		if err != nil || updatedAt.Before(createdAt) {
			return nil, resourceCodecError(descriptor.ID, invalidData("timestamps"))
		}
		result = append(result, resourcedirectory.ResourceRecordView{
			Descriptor: descriptor, NodeGeneration: ownerGeneration,
			RegistrationID: registrationID, PublicationState: state,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read resource records: %w", err)
	}
	return result, nil
}

func persistResourceTransition(
	ctx context.Context,
	transaction *sql.Tx,
	current []resourcedirectory.ResourceRecordView,
	plan resourcedirectory.TransitionPlan,
	at time.Time,
	inject func(context.Context, resourceRegistrationFaultPoint) error,
) error {
	currentByID := make(map[model.ResourceID]resourcedirectory.ResourceRecordView, len(current))
	for _, record := range current {
		currentByID[record.Descriptor.ID] = record
	}
	writes := 0
	for _, record := range plan.Records {
		previous, exists := currentByID[record.Descriptor.ID]
		if exists && persistedResourceViewsEqual(previous, record) {
			continue
		}
		generation, err := encodeResourceGeneration(record.Descriptor.Generation)
		if err != nil {
			return resourceCodecError(record.Descriptor.ID, err)
		}
		operations, err := encodeResourceOperations(record.Descriptor.Operations)
		if err != nil {
			return resourceCodecError(record.Descriptor.ID, err)
		}
		attributes, err := encodeResourceAttributes(record.Descriptor.Attributes)
		if err != nil {
			return resourceCodecError(record.Descriptor.ID, err)
		}
		timestamp, err := encodeTime("resource updated_at", at.UTC())
		if err != nil {
			return err
		}
		if !exists {
			_, err = transaction.ExecContext(ctx, `
INSERT INTO resources (
 resource_id, owner_node_id, resource_kind, resource_type, resource_generation,
 owner_node_generation, registration_id, publication_state, descriptor_encoding_version,
 operations_json, attributes_json, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				record.Descriptor.ID, record.Descriptor.OwnerNodeID, record.Descriptor.Kind,
				record.Descriptor.Type, generation, record.NodeGeneration, record.RegistrationID,
				record.PublicationState, resourceDescriptorEncodingVersion, operations, attributes,
				timestamp, timestamp,
			)
			if err != nil {
				return classifyCreateError("resource", string(record.Descriptor.ID), err)
			}
		} else {
			oldGeneration, _ := encodeResourceGeneration(previous.Descriptor.Generation)
			result, err := transaction.ExecContext(ctx, `
UPDATE resources
SET resource_type = ?, resource_generation = ?, owner_node_generation = ?,
    registration_id = ?, publication_state = ?, descriptor_encoding_version = ?,
    operations_json = ?, attributes_json = ?, updated_at = ?
WHERE resource_id = ? AND owner_node_id = ? AND resource_kind = ?
  AND resource_generation = ? AND owner_node_generation = ?
  AND registration_id = ? AND publication_state = ?`,
				record.Descriptor.Type, generation, record.NodeGeneration, record.RegistrationID,
				record.PublicationState, resourceDescriptorEncodingVersion, operations, attributes, timestamp,
				record.Descriptor.ID, previous.Descriptor.OwnerNodeID, previous.Descriptor.Kind,
				oldGeneration, previous.NodeGeneration, previous.RegistrationID, previous.PublicationState,
			)
			if err != nil {
				return fmt.Errorf("update resource %q: %w", record.Descriptor.ID, err)
			}
			affected, err := result.RowsAffected()
			if err != nil {
				return fmt.Errorf("inspect resource %q update: %w", record.Descriptor.ID, err)
			}
			if affected != 1 {
				return fmt.Errorf("%w: resource %q update affected %d rows", storageport.ErrConflict, record.Descriptor.ID, affected)
			}
		}
		writes++
		if inject != nil && writes == 1 {
			if err := inject(ctx, resourceFaultAfterFirstResource); err != nil {
				return err
			}
		}
		if inject != nil && record.PublicationState == resourcedirectory.PublicationStateWithdrawn {
			if err := inject(ctx, resourceFaultAfterTombstone); err != nil {
				return err
			}
		}
	}
	return nil
}

func persistedResourceViewsEqual(left, right resourcedirectory.ResourceRecordView) bool {
	if left.Descriptor.ID != right.Descriptor.ID || left.Descriptor.OwnerNodeID != right.Descriptor.OwnerNodeID ||
		left.Descriptor.Kind != right.Descriptor.Kind || left.Descriptor.Type != right.Descriptor.Type ||
		left.Descriptor.Generation != right.Descriptor.Generation || left.NodeGeneration != right.NodeGeneration ||
		left.RegistrationID != right.RegistrationID || left.PublicationState != right.PublicationState {
		return false
	}
	leftOperations, err := encodeResourceOperations(left.Descriptor.Operations)
	if err != nil {
		return false
	}
	rightOperations, err := encodeResourceOperations(right.Descriptor.Operations)
	if err != nil || leftOperations != rightOperations {
		return false
	}
	leftAttributes, err := encodeResourceAttributes(left.Descriptor.Attributes)
	if err != nil {
		return false
	}
	rightAttributes, err := encodeResourceAttributes(right.Descriptor.Attributes)
	return err == nil && leftAttributes == rightAttributes
}
