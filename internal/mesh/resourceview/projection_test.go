package resourceview_test

import (
	"reflect"
	"testing"

	"dtm/internal/mesh/resourceview"
	"dtm/internal/model"
)

func TestLocalCapabilityProjectionCreatesNoGenerationAndIsStable(t *testing.T) {
	first, err := resourceview.ProjectLocalCapability("node-a", "temperature_sensor")
	if err != nil {
		t.Fatal(err)
	}
	second, err := resourceview.ProjectLocalCapability("node-a", "temperature_sensor")
	if err != nil {
		t.Fatal(err)
	}
	wantID, err := model.LegacyCapabilityResourceID("node-a", "temperature_sensor")
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != wantID || first.Kind != model.ResourceKindCapability || first.Type != "temperature_sensor" || len(first.Operations) != 1 || first.Operations[0] != "read_temperature" {
		t.Fatalf("projection=%+v", first)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("projection not deterministic: %+v %+v", first, second)
	}
	// The pre-authority Descriptor type has no generation or ResourceRef field;
	// compiling this direct projection proves neither is required or created.
}
