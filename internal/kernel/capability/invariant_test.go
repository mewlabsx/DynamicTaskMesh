package capability

import (
	"errors"
	"testing"

	"dtm/internal/kernel/resource"
)

func TestInvariant_HandleIsAuthorityReference(t *testing.T) {
	fixture := newCapabilityFixture(t)
	validated, err := fixture.manager.ValidateHandleRequest(validationRequestFor(fixture))
	if err != nil {
		t.Fatalf("ValidateHandleRequest() error = %v", err)
	}
	if validated.CapabilityHandle.SubjectExecutionContextID != validated.ExecutionContext.ID {
		t.Fatalf("handle subject context = %q, want %q", validated.CapabilityHandle.SubjectExecutionContextID, validated.ExecutionContext.ID)
	}
	if validated.CapabilityHandle.TargetCapabilityInstanceID != validated.CapabilityInstance.ID {
		t.Fatalf("handle target instance = %q, want %q", validated.CapabilityHandle.TargetCapabilityInstanceID, validated.CapabilityInstance.ID)
	}

	// A CapabilityInstance ID is not accepted as a CapabilityHandle authority
	// reference merely because both are string-backed identity values.
	request := validationRequestFor(fixture)
	request.CapabilityHandleID = CapabilityHandleID(fixture.instance.ID)
	if _, err := fixture.manager.ValidateHandleRequest(request); !errors.Is(err, ErrHandleNotFound) {
		t.Fatalf("instance ID used as handle ID error = %v, want ErrHandleNotFound", err)
	}
}

func TestInvariant_StoredHandleIdentityMustMatchLookupKey(t *testing.T) {
	fixture := newCapabilityFixture(t)
	fixture.manager.mu.Lock()
	stored := fixture.manager.handles[fixture.handle.ID].Clone()
	stored.ID = CapabilityHandleID("handle-other")
	fixture.manager.handles[fixture.handle.ID] = stored
	fixture.manager.mu.Unlock()

	if _, err := fixture.manager.GetHandle(fixture.handle.ID); !errors.Is(err, ErrInvalidHandle) {
		t.Fatalf("GetHandle() error = %v, want ErrInvalidHandle", err)
	}
	provider := &recordingCapabilityProvider{}
	err := fixture.manager.InvokeWithProvider(
		validationRequestFor(fixture),
		InvocationRequest{Operation: "read"},
		provider,
	)
	if !errors.Is(err, ErrRelationshipMismatch) {
		t.Fatalf("InvokeWithProvider() error = %v, want ErrRelationshipMismatch", err)
	}
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.calls)
	}
}

func TestInvariant_StoredDeclarationIdentityMustMatchLookupKey(t *testing.T) {
	fixture := newCapabilityFixture(t)
	fixture.manager.mu.Lock()
	stored := fixture.manager.declarations[fixture.declaration.ID].Clone()
	stored.ID = CapabilityDeclarationID("declaration-other")
	fixture.manager.declarations[fixture.declaration.ID] = stored
	fixture.manager.mu.Unlock()

	if _, err := fixture.manager.GetDeclaration(fixture.declaration.ID); !errors.Is(err, ErrInvalidDeclaration) {
		t.Fatalf("GetDeclaration() error = %v, want ErrInvalidDeclaration", err)
	}
}

func TestInvariant_StoredInstanceIdentityMustMatchLookupKey(t *testing.T) {
	fixture := newCapabilityFixture(t)
	fixture.manager.mu.Lock()
	stored := fixture.manager.instances[fixture.instance.ID].Clone()
	stored.ID = CapabilityInstanceID("instance-other")
	fixture.manager.instances[fixture.instance.ID] = stored
	fixture.manager.mu.Unlock()

	if _, err := fixture.manager.GetInstance(fixture.instance.ID); !errors.Is(err, ErrInvalidInstance) {
		t.Fatalf("GetInstance() error = %v, want ErrInvalidInstance", err)
	}
}

func TestInvariant_StoredIdentityGuardsProtectCapabilityMutations(t *testing.T) {
	t.Run("instance state mutation", func(t *testing.T) {
		fixture := newCapabilityFixture(t)
		fixture.manager.mu.Lock()
		stored := fixture.manager.instances[fixture.instance.ID].Clone()
		stored.ID = CapabilityInstanceID("instance-other")
		fixture.manager.instances[fixture.instance.ID] = stored
		fixture.manager.mu.Unlock()

		if err := fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateActive); !errors.Is(err, ErrInvalidInstance) {
			t.Fatalf("UpdateInstanceState() error = %v, want ErrInvalidInstance", err)
		}
	})

	for _, test := range []struct {
		name string
		call func(*capabilityFixture) error
	}{
		{name: "activate handle", call: func(fixture *capabilityFixture) error {
			return fixture.manager.ActivateHandle(fixture.handle.ID)
		}},
		{name: "revoke handle", call: func(fixture *capabilityFixture) error {
			return fixture.manager.RevokeHandle(fixture.handle.ID)
		}},
		{name: "release handle", call: func(fixture *capabilityFixture) error {
			return fixture.manager.ReleaseHandle(fixture.handle.ID)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCapabilityFixture(t)
			fixture.manager.mu.Lock()
			stored := fixture.manager.handles[fixture.handle.ID].Clone()
			stored.ID = CapabilityHandleID("handle-other")
			fixture.manager.handles[fixture.handle.ID] = stored
			fixture.manager.mu.Unlock()

			if err := test.call(&fixture); !errors.Is(err, ErrInvalidHandle) {
				t.Fatalf("%s error = %v, want ErrInvalidHandle", test.name, err)
			}
		})
	}
}

func TestInvariant_NoImplicitDelegationAcrossSameScope(t *testing.T) {
	fixture := newCapabilityFixture(t)
	other, err := fixture.executions.CreateContext("runtime-b", fixture.context.Scope)
	if err != nil {
		t.Fatalf("CreateContext() error = %v", err)
	}
	request := HandleValidationRequest{
		ExecutionContextID: other.ID,
		CapabilityHandleID: fixture.handle.ID,
		CallerIdentity:     other.Subject,
		Scope:              other.Scope,
	}
	provider := &recordingCapabilityProvider{}
	err = fixture.manager.InvokeWithProvider(request, InvocationRequest{Operation: "read"}, provider)
	if !errors.Is(err, ErrContextMismatch) {
		t.Fatalf("InvokeWithProvider() error = %v, want ErrContextMismatch", err)
	}
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.calls)
	}
}

func TestInvariant_RelationshipIdentityIntegrity(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(capabilityFixture)
		want   error
	}{
		{
			name: "instance record identity differs from handle target",
			mutate: func(fixture capabilityFixture) {
				fixture.manager.mu.Lock()
				instance := fixture.manager.instances[fixture.instance.ID].Clone()
				instance.ID = CapabilityInstanceID("instance-other")
				fixture.manager.instances[fixture.instance.ID] = instance
				fixture.manager.mu.Unlock()
			},
			want: ErrRelationshipMismatch,
		},
		{
			name: "structurally identical declaration has wrong identity",
			mutate: func(fixture capabilityFixture) {
				fixture.manager.mu.Lock()
				declaration := fixture.manager.declarations[fixture.declaration.ID].Clone()
				declaration.ID = CapabilityDeclarationID("declaration-other")
				fixture.manager.declarations[fixture.declaration.ID] = declaration
				fixture.manager.mu.Unlock()
			},
			want: ErrRelationshipMismatch,
		},
		{
			name: "declaration redirects to another resource",
			mutate: func(fixture capabilityFixture) {
				fixture.manager.mu.Lock()
				declaration := fixture.manager.declarations[fixture.declaration.ID].Clone()
				declaration.ResourceID = resource.ResourceID("resource-other")
				fixture.manager.declarations[fixture.declaration.ID] = declaration
				fixture.manager.mu.Unlock()
			},
			want: ErrRelationshipMismatch,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCapabilityFixture(t)
			test.mutate(fixture)
			provider := &recordingCapabilityProvider{}
			err := fixture.manager.InvokeWithProvider(
				validationRequestFor(fixture),
				InvocationRequest{Operation: "read"},
				provider,
			)
			if !errors.Is(err, test.want) {
				t.Fatalf("InvokeWithProvider() error = %v, want %v", err, test.want)
			}
			if provider.calls != 0 {
				t.Fatalf("provider calls = %d, want 0", provider.calls)
			}
		})
	}
}

func TestInvariant_LifecycleIndependentFromAvailability(t *testing.T) {
	tests := []struct {
		name         string
		state        InstanceState
		availability AvailabilityState
		want         error
		calls        int
	}{
		{
			name:         "active and available",
			state:        InstanceStateActive,
			availability: AvailabilityAvailable,
			calls:        1,
		},
		{
			name:         "active and unavailable",
			state:        InstanceStateActive,
			availability: AvailabilityUnavailable,
			want:         ErrInstanceUnavailable,
		},
		{
			name:         "revoked and available",
			state:        InstanceStateRevoked,
			availability: AvailabilityAvailable,
			want:         ErrInstanceRevoked,
		},
		{
			name:         "destroyed and available",
			state:        InstanceStateDestroyed,
			availability: AvailabilityAvailable,
			want:         ErrInstanceUnavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCapabilityFixture(t)
			fixture.manager.mu.Lock()
			instance := fixture.manager.instances[fixture.instance.ID].Clone()
			instance.State = test.state
			instance.Availability = test.availability
			fixture.manager.instances[fixture.instance.ID] = instance
			fixture.manager.mu.Unlock()

			provider := &recordingCapabilityProvider{}
			err := fixture.manager.InvokeWithProvider(
				validationRequestFor(fixture),
				InvocationRequest{Operation: "read"},
				provider,
			)
			if test.want == nil {
				if err != nil {
					t.Fatalf("InvokeWithProvider() error = %v, want success", err)
				}
			} else if !errors.Is(err, test.want) {
				t.Fatalf("InvokeWithProvider() error = %v, want %v", err, test.want)
			}
			if provider.calls != test.calls {
				t.Fatalf("provider calls = %d, want %d", provider.calls, test.calls)
			}
			stored, getErr := fixture.manager.GetInstance(fixture.instance.ID)
			if getErr != nil {
				t.Fatalf("GetInstance() error = %v", getErr)
			}
			if stored.State != test.state || stored.Availability != test.availability {
				t.Fatalf("stored instance state/availability = %q/%q, want %q/%q", stored.State, stored.Availability, test.state, test.availability)
			}
		})
	}
}

func TestInvariant_ValidationPrecedesProviderSideEffect(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*capabilityFixture, *HandleValidationRequest, *InvocationRequest)
		want   error
	}{
		{
			name: "invalid authority precedes invalid operation",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest, invocation *InvocationRequest) {
				request.CapabilityHandleID = CapabilityHandleID("missing-handle")
				invocation.Operation = "\x00"
			},
			want: ErrHandleNotFound,
		},
		{
			name: "scope mismatch",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest, invocation *InvocationRequest) {
				request.Scope = "scope-other"
			},
			want: ErrScopeMismatch,
		},
		{
			name: "invalid operation after valid authority",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest, invocation *InvocationRequest) {
				invocation.Operation = "\x00"
			},
			want: ErrInvalidInvocation,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCapabilityFixture(t)
			request := validationRequestFor(fixture)
			invocation := InvocationRequest{Operation: "read"}
			test.mutate(&fixture, &request, &invocation)
			provider := &recordingCapabilityProvider{}
			err := fixture.manager.InvokeWithProvider(request, invocation, provider)
			if !errors.Is(err, test.want) {
				t.Fatalf("InvokeWithProvider() error = %v, want %v", err, test.want)
			}
			if provider.calls != 0 {
				t.Fatalf("provider calls = %d, want 0", provider.calls)
			}
		})
	}
}
