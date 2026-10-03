package protocol

import "testing"

const maxPresenceFuzzInput = 1 << 20

func FuzzDecodePresence(f *testing.F) {
	valid := Identity{
		MeshNamespace:   "mesh-a",
		ProtocolMajor:   1,
		ProtocolMinor:   0,
		DTMVersion:      DTMVersion,
		NodeID:          "node-a",
		RuntimeInstance: "session-a",
		ControlEndpoint: "127.0.0.1:45893",
	}
	encoded, err := Encode(valid)
	if err != nil {
		f.Fatalf("Encode(valid) = %v", err)
	}
	f.Add(encoded)
	f.Add([]byte{})
	f.Add([]byte("{"))
	f.Add([]byte(`{"mesh_namespace":"mesh-a"}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > maxPresenceFuzzInput {
			return
		}
		got, err := Decode(data)
		if err != nil {
			return
		}
		if err := got.Validate(); err != nil {
			t.Fatalf("Decode returned an invalid identity: %v", err)
		}

		reencoded, err := Encode(got)
		if err != nil {
			t.Fatalf("Encode(decoded identity) = %v", err)
		}
		roundTrip, err := Decode(reencoded)
		if err != nil {
			t.Fatalf("Decode(round-trip) = %v", err)
		}
		if roundTrip != got {
			t.Fatalf("round-trip identity = %+v, want %+v", roundTrip, got)
		}
	})
}
