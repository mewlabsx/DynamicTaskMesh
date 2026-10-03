package resource

import (
	"errors"
	"testing"

	"dtm/internal/kernel/identity"
)

func TestInvariant_StoredResourceIdentityMustMatchLookupKey(t *testing.T) {
	manager := NewManager()
	resourceObject, err := manager.CreateResource("scope-a")
	if err != nil {
		t.Fatalf("CreateResource() error = %v", err)
	}

	manager.mu.Lock()
	stored := manager.resources[resourceObject.ID].Clone()
	stored.ID = ResourceID("resource-other")
	manager.resources[resourceObject.ID] = stored
	manager.mu.Unlock()

	if _, err := manager.GetResource(resourceObject.ID); !errors.Is(err, ErrInvalidResource) {
		t.Fatalf("GetResource() error = %v, want ErrInvalidResource", err)
	}
}

func TestInvariant_StoredResourceIdentityGuardsProtectMutations(t *testing.T) {
	declarationID, err := identity.NewCapabilityDeclarationID()
	if err != nil {
		t.Fatalf("NewCapabilityDeclarationID() error = %v", err)
	}

	for _, test := range []struct {
		name string
		call func(*Manager, ResourceID, CapabilityDeclarationID) error
	}{
		{name: "state", call: func(manager *Manager, id ResourceID, _ CapabilityDeclarationID) error {
			return manager.UpdateState(id, StateActive)
		}},
		{name: "availability", call: func(manager *Manager, id ResourceID, _ CapabilityDeclarationID) error {
			return manager.UpdateAvailability(id, AvailabilityUnavailable)
		}},
		{name: "attach declaration", call: func(manager *Manager, id ResourceID, declarationID CapabilityDeclarationID) error {
			return manager.AttachCapabilityDeclaration(id, declarationID)
		}},
		{name: "detach declaration", call: func(manager *Manager, id ResourceID, declarationID CapabilityDeclarationID) error {
			return manager.DetachCapabilityDeclaration(id, declarationID)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			manager := NewManager()
			resourceObject, err := manager.CreateResource("scope-a")
			if err != nil {
				t.Fatalf("CreateResource() error = %v", err)
			}

			manager.mu.Lock()
			stored := manager.resources[resourceObject.ID].Clone()
			stored.ID = ResourceID("resource-other")
			manager.resources[resourceObject.ID] = stored
			manager.mu.Unlock()

			if err := test.call(manager, resourceObject.ID, declarationID); !errors.Is(err, ErrInvalidResource) {
				t.Fatalf("%s mutation error = %v, want ErrInvalidResource", test.name, err)
			}
		})
	}
}
