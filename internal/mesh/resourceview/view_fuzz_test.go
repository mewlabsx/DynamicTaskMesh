package resourceview

import (
	"testing"

	dtmv1 "dtm/api/proto/dtm/v1"
	"dtm/internal/mesh/protocol"
	"dtm/internal/model"
	"google.golang.org/protobuf/proto"
)

const maxResourceAdvertisementFuzzInput = 1 << 20

func FuzzResourceAdvertisement(f *testing.F) {
	seed := &dtmv1.MeshResourceDescriptor{
		ResourceId:   "resource-1",
		Kind:         string(model.ResourceKindCapability),
		Type:         "temperature_sensor",
		OperationIds: []string{"read_temperature"},
		Attributes:   map[string]string{"unit": "celsius"},
	}
	encoded, err := proto.Marshal(seed)
	if err != nil {
		f.Fatalf("proto.Marshal(seed) = %v", err)
	}
	f.Add(encoded)
	f.Add([]byte{})
	f.Add([]byte{0x0a, 0x01, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxResourceAdvertisementFuzzInput {
			return
		}
		wire := new(dtmv1.MeshResourceDescriptor)
		if err := proto.Unmarshal(data, wire); err != nil {
			return
		}
		descriptor := fuzzDescriptorFromProto(wire)
		local := fuzzResourceIdentity()
		members := func() MembershipSnapshot {
			return MembershipSnapshot{Members: []protocol.Identity{local}, ActiveMembers: []protocol.Identity{local}}
		}
		view, err := New(local, members, nil)
		if err != nil {
			t.Fatalf("New(local) = %v", err)
		}
		observeErr := view.Observe(Advertisement{Owner: local, Resources: []Descriptor{descriptor}})
		snapshot := view.Snapshot()
		if observeErr != nil {
			if len(snapshot.Entries) != 0 || len(snapshot.ActiveResources) != 0 {
				t.Fatalf("invalid advertisement changed View: entries=%d active=%d", len(snapshot.Entries), len(snapshot.ActiveResources))
			}
			return
		}
		if len(snapshot.Entries) != 1 || len(snapshot.ActiveResources) != 1 {
			t.Fatalf("valid advertisement snapshot = entries:%d active:%d", len(snapshot.Entries), len(snapshot.ActiveResources))
		}
		if err := snapshot.Entries[0].Descriptor.Validate(); err != nil {
			t.Fatalf("stored descriptor is invalid: %v", err)
		}
	})
}

func fuzzDescriptorFromProto(wire *dtmv1.MeshResourceDescriptor) Descriptor {
	operations := make([]model.OperationID, len(wire.GetOperationIds()))
	for i, operation := range wire.GetOperationIds() {
		operations[i] = model.OperationID(operation)
	}
	attributes := make(map[string]string, len(wire.GetAttributes()))
	for key, value := range wire.GetAttributes() {
		attributes[key] = value
	}
	return Descriptor{
		ID:         model.ResourceID(wire.GetResourceId()),
		Kind:       model.ResourceKind(wire.GetKind()),
		Type:       model.ResourceType(wire.GetType()),
		Operations: operations,
		Attributes: attributes,
	}
}

func fuzzResourceIdentity() protocol.Identity {
	return protocol.Identity{
		MeshNamespace:   "mesh-a",
		ProtocolMajor:   1,
		ProtocolMinor:   0,
		DTMVersion:      protocol.DTMVersion,
		NodeID:          "node-a",
		RuntimeInstance: "session-a",
		ControlEndpoint: "127.0.0.1:45893",
	}
}
