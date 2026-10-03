package capability

import "testing"

func TestCatalogContainsStableCapabilities(t *testing.T) {
	definitions := All()
	if len(definitions) != 2 {
		t.Fatalf("All() length = %d, want 2", len(definitions))
	}
	if definitions[0].Name != CoolingControl || definitions[1].Name != TemperatureSensor {
		t.Fatalf("All() = %#v, want stable sorted catalog", definitions)
	}
	for _, definition := range definitions {
		if definition.Description == "" {
			t.Fatalf("capability %q has no description", definition.Name)
		}
		if got, exists := Lookup(definition.Name); !exists || got != definition {
			t.Fatalf("Lookup(%q) = %#v/%v", definition.Name, got, exists)
		}
	}
	if _, exists := Lookup("cool_environment"); exists {
		t.Fatal("legacy task intent must not be registered as a capability")
	}
}
