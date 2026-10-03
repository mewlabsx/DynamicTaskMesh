package protocol

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestPresenceEncodeDecodeRoundTrip(t *testing.T) {
	want := Identity{MeshNamespace: "mesh-a", ProtocolMajor: 1, ProtocolMinor: 7, DTMVersion: DTMVersion, NodeID: "node-a", RuntimeInstance: "session-a", ControlEndpoint: "127.0.0.1:45893"}
	encoded, err := Encode(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("Decode() = %+v, want %+v", got, want)
	}
}

func TestPresenceRejectsMalformedValues(t *testing.T) {
	valid := Identity{MeshNamespace: "mesh-a", ProtocolMajor: 1, DTMVersion: DTMVersion, NodeID: "node-a", RuntimeInstance: "session-a", ControlEndpoint: "127.0.0.1:45893"}
	missingVersion := valid
	missingVersion.DTMVersion = " "
	missingEncoded, _ := json.Marshal(missingVersion)
	for _, value := range [][]byte{[]byte("{"), []byte(`{"mesh_namespace":"mesh-a"}`), missingEncoded} {
		if _, err := Decode(value); !errors.Is(err, ErrInvalidPresence) {
			t.Fatalf("Decode(%q) = %v", value, err)
		}
	}
}

func TestRuntimeInstanceIDsAreFresh(t *testing.T) {
	first, err := NewRuntimeInstanceID()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewRuntimeInstanceID()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 32 || first == second {
		t.Fatalf("IDs = %q %q", first, second)
	}
}
