package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
	storageport "dtm/internal/storage"
)

func TestMigration006ResourceSchemaAndIndexes(t *testing.T) {
	repository := newRepository(t)
	var migrationName string
	if err := repository.db.QueryRow(`SELECT name FROM schema_migrations WHERE version = 6`).Scan(&migrationName); err != nil {
		t.Fatal(err)
	}
	if migrationName != "v04_resource_foundation" {
		t.Fatalf("migration 6 name = %q", migrationName)
	}
	for _, index := range []string{"idx_resources_owner_state_id", "idx_resources_owner_fence"} {
		var count int
		if err := repository.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?`, index).Scan(&count); err != nil || count != 1 {
			t.Fatalf("index %s count=%d error=%v", index, count, err)
		}
	}
}

func TestMigration006UpgradesV5WithoutHistoricalBackfill(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v5.db")
	createM7R2DatabaseAtVersion(t, path, "core", 5)
	repository, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer repository.Close()
	var version, resources int
	if err := repository.db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := repository.db.QueryRow(`SELECT COUNT(*) FROM resources`).Scan(&resources); err != nil {
		t.Fatal(err)
	}
	if version != 6 || resources != 0 {
		t.Fatalf("v5 upgrade version=%d resources=%d", version, resources)
	}
}

func TestResourceCodecCanonicalGenerationAndJSON(t *testing.T) {
	for _, generation := range []model.ResourceGeneration{1, model.ResourceGeneration(math.MaxUint64)} {
		encoded, err := encodeResourceGeneration(generation)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeResourceGeneration(encoded)
		if err != nil || decoded != generation {
			t.Fatalf("generation round trip %d => %q => %d, %v", generation, encoded, decoded, err)
		}
	}
	for _, invalid := range []string{"", "0", "01", "+1", "-1", " 1", "18446744073709551616"} {
		if _, err := decodeResourceGeneration(invalid); !errors.Is(err, storageport.ErrInvalidData) {
			t.Fatalf("decode generation %q error=%v", invalid, err)
		}
	}
	descriptor := mustResourceDescriptor(t, "node-codec", "temperature_sensor", 1)
	operations, err := encodeResourceOperations(descriptor.Operations)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeResourceOperations(operations); err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []string{"null", "[] ", strings.Replace(operations, `"operation_id"`, `"unknown"`, 1), operations + `{}`} {
		if _, err := decodeResourceOperations(invalid); !errors.Is(err, storageport.ErrInvalidData) {
			t.Fatalf("decode operations %q error=%v", invalid, err)
		}
	}
	attributes, err := encodeResourceAttributes(map[string]string{"empty": "", "zone": "north"})
	if err != nil {
		t.Fatal(err)
	}
	decodedAttributes, err := decodeResourceAttributes(attributes)
	if err != nil || decodedAttributes["empty"] != "" || decodedAttributes["zone"] != "north" {
		t.Fatalf("attribute round trip=%#v err=%v", decodedAttributes, err)
	}
}

func TestResourceRepositoryPersistsMultipleInstancesOfOneType(t *testing.T) {
	repository := newRepository(t)
	base := mustResourceDescriptor(t, "node-multi-resource", "temperature_sensor", 1)
	first := base.Clone()
	first.ID = "sensor-instance-a"
	first.Attributes = map[string]string{"slot": "a"}
	second := base.Clone()
	second.ID = "sensor-instance-b"
	second.Attributes = map[string]string{"slot": "b"}
	_, plan, err := repository.RegisterNodeWithResources(context.Background(), nodeRegistration("node-multi-resource", "registration", "endpoint", []model.Capability{"temperature_sensor"}, testTime(0)), []model.ResourceDescriptor{first, second}, false)
	if err != nil || len(plan.Records) != 2 {
		t.Fatalf("multiple resource instances plan=%#v err=%v", plan, err)
	}
	if plan.Records[0].Descriptor.Type != plan.Records[1].Descriptor.Type || plan.Records[0].Descriptor.ID == plan.Records[1].Descriptor.ID {
		t.Fatalf("multiple resource instances=%#v", plan.Records)
	}
}

func TestRegisterNodeWithResourcesIsAtomicAtEveryFaultBoundary(t *testing.T) {
	points := []resourceRegistrationFaultPoint{
		resourceFaultBeforeNodeWrite, resourceFaultAfterNodeWrite, resourceFaultAfterFirstResource,
		resourceFaultBeforeCommit,
	}
	for _, point := range points {
		t.Run(string(point), func(t *testing.T) {
			repository := newRepository(t)
			repository.resourceRegistrationFault = func(_ context.Context, got resourceRegistrationFaultPoint) error {
				if got == point {
					return errors.New("injected")
				}
				return nil
			}
			descriptor := mustResourceDescriptor(t, "node-atomic", "temperature_sensor", 1)
			_, _, err := repository.RegisterNodeWithResources(context.Background(), nodeRegistration("node-atomic", "registration", "endpoint", []model.Capability{"temperature_sensor"}, testTime(0)), []model.ResourceDescriptor{descriptor}, false)
			if err == nil {
				t.Fatal("RegisterNodeWithResources() error = nil")
			}
			assertResourceTableCount(t, repository, "nodes", 0)
			assertResourceTableCount(t, repository, "resources", 0)
		})
	}
}

func TestRegisterNodeWithResourcesRollsBackTombstoneFault(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	now := testTime(0)
	temperature := mustResourceDescriptor(t, "node-tombstone-fault", "temperature_sensor", 1)
	cooling := mustResourceDescriptor(t, "node-tombstone-fault", "cooling_control", 1)
	if _, _, err := repository.RegisterNodeWithResources(ctx, nodeRegistration("node-tombstone-fault", "registration-1", "endpoint", []model.Capability{"temperature_sensor", "cooling_control"}, now), []model.ResourceDescriptor{temperature, cooling}, false); err != nil {
		t.Fatal(err)
	}
	repository.resourceRegistrationFault = func(_ context.Context, point resourceRegistrationFaultPoint) error {
		if point == resourceFaultAfterTombstone {
			return errors.New("injected tombstone failure")
		}
		return nil
	}
	if _, _, err := repository.RegisterNodeWithResources(ctx, nodeRegistration("node-tombstone-fault", "registration-2", "endpoint-2", []model.Capability{"temperature_sensor"}, now.Add(time.Second)), []model.ResourceDescriptor{temperature}, false); err == nil {
		t.Fatal("RegisterNodeWithResources() error = nil")
	}
	nodeRecord, err := repository.GetNode(ctx, "node-tombstone-fault")
	if err != nil || nodeRecord.Generation != 1 || nodeRecord.RegistrationID != "registration-1" {
		t.Fatalf("node after rollback=%#v err=%v", nodeRecord, err)
	}
	records, err := repository.LoadResourceRecordsByNode(ctx, "node-tombstone-fault")
	if err != nil || len(records) != 2 {
		t.Fatalf("resources after rollback=%#v err=%v", records, err)
	}
	for _, record := range records {
		if record.PublicationState != resourcedirectory.PublicationStatePublished || record.Descriptor.Generation != 1 {
			t.Fatalf("resource changed after rollback: %#v", record)
		}
	}
}

func TestResourceRepositoryRoundTripTombstoneRepublishAndRestart(t *testing.T) {
	repository := newRepository(t)
	ctx := context.Background()
	now := testTime(0)
	temperature := mustResourceDescriptor(t, "node-roundtrip-resource", "temperature_sensor", 1)
	cooling := mustResourceDescriptor(t, "node-roundtrip-resource", "cooling_control", 1)
	nodeRecord, first, err := repository.RegisterNodeWithResources(ctx, nodeRegistration("node-roundtrip-resource", "registration-1", "endpoint", []model.Capability{"temperature_sensor", "cooling_control"}, now), []model.ResourceDescriptor{temperature, cooling}, false)
	if err != nil || nodeRecord.Generation != 1 || len(first.Records) != 2 {
		t.Fatalf("first registration node=%#v plan=%#v err=%v", nodeRecord, first, err)
	}
	nodeRecord, second, err := repository.RegisterNodeWithResources(ctx, nodeRegistration("node-roundtrip-resource", "registration-2", "endpoint-2", []model.Capability{"temperature_sensor"}, now.Add(time.Second)), []model.ResourceDescriptor{temperature}, false)
	if err != nil || nodeRecord.Generation != 2 {
		t.Fatalf("second registration node=%#v err=%v", nodeRecord, err)
	}
	assertResourceState(t, second.Records, temperature.ID, resourcedirectory.PublicationStatePublished, 2)
	assertResourceState(t, second.Records, cooling.ID, resourcedirectory.PublicationStateWithdrawn, 1)
	nodeRecord, third, err := repository.RegisterNodeWithResources(ctx, nodeRegistration("node-roundtrip-resource", "registration-3", "endpoint-3", []model.Capability{"temperature_sensor", "cooling_control"}, now.Add(2*time.Second)), []model.ResourceDescriptor{temperature, cooling}, false)
	if err != nil || nodeRecord.Generation != 3 {
		t.Fatalf("third registration node=%#v err=%v", nodeRecord, err)
	}
	assertResourceState(t, third.Records, temperature.ID, resourcedirectory.PublicationStatePublished, 3)
	assertResourceState(t, third.Records, cooling.ID, resourcedirectory.PublicationStatePublished, 2)
	restored, err := repository.LoadAllResourceRecords(ctx)
	if err != nil || len(restored) != 2 {
		t.Fatalf("LoadAllResourceRecords() len=%d err=%v", len(restored), err)
	}
	directory := resourcedirectory.New()
	if err := directory.RestoreRecords(restored); err != nil {
		t.Fatal(err)
	}
	if len(directory.ListEligible()) != 0 || len(directory.ListIneligible()) != 2 {
		t.Fatalf("restored eligibility eligible=%d ineligible=%d", len(directory.ListEligible()), len(directory.ListIneligible()))
	}
}

func TestResourceRepositoryStrictReadRejectsNonCanonicalPersistedData(t *testing.T) {
	repository := newRepository(t)
	descriptor := mustResourceDescriptor(t, "node-corrupt-resource", "temperature_sensor", 1)
	if _, _, err := repository.RegisterNodeWithResources(context.Background(), nodeRegistration("node-corrupt-resource", "registration", "endpoint", []model.Capability{"temperature_sensor"}, testTime(0)), []model.ResourceDescriptor{descriptor}, false); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.db.Exec(`PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.db.Exec(`UPDATE resources SET resource_generation = '01' WHERE resource_id = ?`, descriptor.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := repository.LoadAllResourceRecords(context.Background()); !errors.Is(err, storageport.ErrInvalidData) {
		t.Fatalf("LoadAllResourceRecords() error=%v", err)
	}
}

func mustResourceDescriptor(t *testing.T, nodeID model.NodeID, capability model.Capability, generation model.ResourceGeneration) model.ResourceDescriptor {
	t.Helper()
	descriptor, err := model.AdaptLegacyCapability(nodeID, capability, generation)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func assertResourceTableCount(t *testing.T, repository *Repository, table string, want int) {
	t.Helper()
	var got int
	if err := repository.db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&got); err != nil || got != want {
		t.Fatalf("%s count=%d want=%d error=%v", table, got, want, err)
	}
}

func TestApplyNodeResourceSnapshotEnforcesOwnerFence(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		name           string
		createNode     bool
		generation     int64
		registrationID string
		want           error
	}{
		{name: "missing owner", generation: 1, registrationID: "registration-a", want: storageport.ErrNodeNotFound},
		{name: "wrong generation", createNode: true, generation: 999, registrationID: "registration-a", want: storageport.ErrNodeGenerationConflict},
		{name: "wrong registration", createNode: true, generation: 1, registrationID: "registration-x", want: storageport.ErrStaleRegistration},
	} {
		t.Run(test.name, func(t *testing.T) {
			repository := newRepository(t)
			if test.createNode {
				if _, err := repository.RegisterNode(ctx, nodeRegistration("node-fence", "registration-a", "endpoint", []model.Capability{"temperature_sensor"}, testTime(0))); err != nil {
					t.Fatal(err)
				}
			}
			descriptor := mustResourceDescriptor(t, "node-fence", "temperature_sensor", 1)
			snapshot, err := resourcedirectory.NewNodeResourceSnapshot("node-fence", test.generation, test.registrationID, []model.ResourceDescriptor{descriptor})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = repository.ApplyNodeResourceSnapshot(ctx, snapshot, testTime(1)); !errors.Is(err, test.want) {
				t.Fatalf("ApplyNodeResourceSnapshot() error = %v, want %v", err, test.want)
			}
			assertResourceTableCount(t, repository, "resources", 0)
		})
	}
}

func TestRegisterNodeWithResourcesCommitOutcomeMayBeAmbiguous(t *testing.T) {
	repository := newRepository(t)
	commitReturned := errors.New("commit result unavailable")
	repository.resourceCommit = func(transaction *sql.Tx) error {
		if err := transaction.Commit(); err != nil {
			return err
		}
		return commitReturned
	}
	descriptor := mustResourceDescriptor(t, "node-ambiguous", "temperature_sensor", 1)
	_, _, err := repository.RegisterNodeWithResources(context.Background(), nodeRegistration("node-ambiguous", "registration-a", "endpoint", []model.Capability{"temperature_sensor"}, testTime(0)), []model.ResourceDescriptor{descriptor}, false)
	if !errors.Is(err, commitReturned) {
		t.Fatalf("RegisterNodeWithResources() error = %v", err)
	}
	// The caller observed an error while SQLite committed. This proves why the
	// transport must not publish memory state and must converge by reload/retry.
	assertResourceTableCount(t, repository, "nodes", 1)
	assertResourceTableCount(t, repository, "resources", 1)
}

func assertResourceState(t *testing.T, records []resourcedirectory.ResourceRecordView, id model.ResourceID, state resourcedirectory.PublicationState, generation model.ResourceGeneration) {
	t.Helper()
	for _, record := range records {
		if record.Descriptor.ID == id {
			if record.PublicationState != state || record.Descriptor.Generation != generation {
				t.Fatalf("resource %s state=%s generation=%d", id, record.PublicationState, record.Descriptor.Generation)
			}
			return
		}
	}
	t.Fatalf("resource %s missing", id)
}
