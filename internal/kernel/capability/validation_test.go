package capability

import (
	"errors"
	"testing"

	"dtm/internal/kernel/execution"
	"dtm/internal/kernel/identity"
	"dtm/internal/kernel/resource"
)

type recordingCapabilityProvider struct {
	calls     int
	operation ProviderOperation
	err       error
}

func (provider *recordingCapabilityProvider) Invoke(operation ProviderOperation) error {
	provider.calls++
	provider.operation = operation.Clone()
	return provider.err
}

func validationRequestFor(fixture capabilityFixture) HandleValidationRequest {
	return HandleValidationRequest{
		ExecutionContextID: fixture.context.ID,
		CapabilityHandleID: fixture.handle.ID,
		CallerIdentity:     fixture.context.Subject,
		Scope:              fixture.context.Scope,
	}
}

func TestK2CHandleValidatorAcceptsValidAuthoritySnapshot(t *testing.T) {
	fixture := newCapabilityFixture(t)
	validator := NewHandleValidator(fixture.manager)

	validated, err := validator.Validate(validationRequestFor(fixture))
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if validated.ExecutionContext.ID != fixture.context.ID ||
		validated.CapabilityHandle.ID != fixture.handle.ID ||
		validated.CapabilityInstance.ID != fixture.instance.ID ||
		validated.CapabilityDeclaration.ID != fixture.declaration.ID ||
		validated.Resource.ID != fixture.resource.ID {
		t.Fatalf("validated chain = %#v", validated)
	}

	provider := &recordingCapabilityProvider{}
	request := InvocationRequest{Operation: "read", Payload: []byte("payload")}
	if err := fixture.manager.InvokeWithProvider(validationRequestFor(fixture), request, provider); err != nil {
		t.Fatalf("InvokeWithProvider() error = %v", err)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
	if provider.operation.Operation != request.Operation ||
		string(provider.operation.Payload) != string(request.Payload) ||
		provider.operation.CapabilityDeclarationID != fixture.declaration.ID ||
		provider.operation.CapabilityInstanceID != fixture.instance.ID ||
		provider.operation.ResourceID != fixture.resource.ID {
		t.Fatalf("provider operation = %#v", provider.operation)
	}
}

func TestK2CProviderIsNotCalledOnValidationFailure(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*capabilityFixture, *HandleValidationRequest) error
		want   error
	}{
		{
			name: "missing handle",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				request.CapabilityHandleID = CapabilityHandleID("missing-handle")
				return nil
			},
			want: ErrHandleNotFound,
		},
		{
			name: "destroyed handle",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				return fixture.manager.ReleaseHandle(fixture.handle.ID)
			},
			want: ErrHandleDestroyed,
		},
		{
			name: "revoked handle",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				return fixture.manager.RevokeHandle(fixture.handle.ID)
			},
			want: ErrHandleRevoked,
		},
		{
			name: "expired handle",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				fixture.manager.mu.Lock()
				handle := fixture.manager.handles[fixture.handle.ID].Clone()
				handle.State = HandleStateExpired
				handle.Availability = AvailabilityUnavailable
				fixture.manager.handles[fixture.handle.ID] = handle
				fixture.manager.mu.Unlock()
				return nil
			},
			want: ErrHandleExpired,
		},
		{
			name: "suspended handle",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				fixture.manager.mu.Lock()
				handle := fixture.manager.handles[fixture.handle.ID].Clone()
				handle.State = HandleStateSuspended
				handle.Availability = AvailabilityUnavailable
				fixture.manager.handles[fixture.handle.ID] = handle
				fixture.manager.mu.Unlock()
				return nil
			},
			want: ErrInvalidHandle,
		},
		{
			name: "context mismatch same scope",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				other, err := fixture.executions.CreateContext("runtime-b", fixture.context.Scope)
				if err != nil {
					return err
				}
				request.ExecutionContextID = other.ID
				request.CallerIdentity = other.Subject
				return nil
			},
			want: ErrContextMismatch,
		},
		{
			name: "scope mismatch",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				request.Scope = "scope-b"
				return nil
			},
			want: ErrScopeMismatch,
		},
		{
			name: "missing context",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				request.ExecutionContextID = ExecutionContextID("missing-context")
				request.CallerIdentity = "runtime-a"
				return nil
			},
			want: execution.ErrContextNotFound,
		},
		{
			name: "caller identity mismatch",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				request.CallerIdentity = "runtime-b"
				return nil
			},
			want: ErrCallerIdentityMismatch,
		},
		{
			name: "unavailable handle",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				fixture.manager.mu.Lock()
				handle := fixture.manager.handles[fixture.handle.ID].Clone()
				handle.Availability = AvailabilityUnavailable
				fixture.manager.handles[fixture.handle.ID] = handle
				fixture.manager.mu.Unlock()
				return nil
			},
			want: ErrInvalidHandle,
		},
		{
			name: "missing instance",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				fixture.manager.mu.Lock()
				delete(fixture.manager.instances, fixture.instance.ID)
				fixture.manager.mu.Unlock()
				return nil
			},
			want: ErrInstanceNotFound,
		},
		{
			name: "invalid instance lifecycle",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				return fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateSuspended)
			},
			want: ErrInstanceUnavailable,
		},
		{
			name: "unavailable provider resource",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				return fixture.resources.UpdateAvailability(fixture.resource.ID, resource.AvailabilityUnavailable)
			},
			want: ErrInstanceUnavailable,
		},
		{
			name: "removed provider resource",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				return fixture.resources.RemoveResource(fixture.resource.ID)
			},
			want: ErrInstanceUnavailable,
		},
		{
			name: "stored handle scope mismatch",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				fixture.manager.mu.Lock()
				handle := fixture.manager.handles[fixture.handle.ID].Clone()
				handle.Scope = "scope-b"
				fixture.manager.handles[fixture.handle.ID] = handle
				fixture.manager.mu.Unlock()
				return nil
			},
			want: ErrScopeMismatch,
		},
		{
			name: "missing declaration",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				fixture.manager.mu.Lock()
				delete(fixture.manager.declarations, fixture.declaration.ID)
				fixture.manager.mu.Unlock()
				return nil
			},
			want: ErrDeclarationNotFound,
		},
		{
			name: "relationship mismatch",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				fixture.manager.mu.Lock()
				declaration := fixture.manager.declarations[fixture.declaration.ID].Clone()
				declaration.ResourceID = resource.ResourceID("resource-other")
				fixture.manager.declarations[fixture.declaration.ID] = declaration
				fixture.manager.mu.Unlock()
				return nil
			},
			want: ErrRelationshipMismatch,
		},
		{
			name: "swapped instance record",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				fixture.manager.mu.Lock()
				instance := fixture.manager.instances[fixture.instance.ID].Clone()
				instance.ID = CapabilityInstanceID("instance-other")
				fixture.manager.instances[fixture.instance.ID] = instance
				fixture.manager.mu.Unlock()
				return nil
			},
			want: ErrRelationshipMismatch,
		},
		{
			name: "missing provider resource",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				fixture.manager.mu.Lock()
				instance := fixture.manager.instances[fixture.instance.ID].Clone()
				instance.ProviderResourceID = resource.ResourceID("resource-missing")
				declaration := fixture.manager.declarations[fixture.declaration.ID].Clone()
				declaration.ResourceID = instance.ProviderResourceID
				fixture.manager.instances[fixture.instance.ID] = instance
				fixture.manager.declarations[fixture.declaration.ID] = declaration
				fixture.manager.mu.Unlock()
				return nil
			},
			want: ErrInstanceUnavailable,
		},
		{
			name: "swapped declaration",
			mutate: func(fixture *capabilityFixture, request *HandleValidationRequest) error {
				fixture.manager.mu.Lock()
				instance := fixture.manager.instances[fixture.instance.ID].Clone()
				declaration := fixture.manager.declarations[fixture.declaration.ID].Clone()
				instance.DeclarationID = CapabilityDeclarationID("declaration-other")
				declaration.ID = instance.DeclarationID
				fixture.manager.instances[fixture.instance.ID] = instance
				fixture.manager.declarations[declaration.ID] = declaration
				fixture.manager.mu.Unlock()
				return nil
			},
			want: ErrRelationshipMismatch,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCapabilityFixture(t)
			request := validationRequestFor(fixture)
			if test.mutate != nil {
				if err := test.mutate(&fixture, &request); err != nil {
					t.Fatalf("mutate() error = %v", err)
				}
			}
			provider := &recordingCapabilityProvider{}
			err := fixture.manager.InvokeWithProvider(request, InvocationRequest{Operation: "read"}, provider)
			if !errors.Is(err, test.want) {
				t.Fatalf("InvokeWithProvider() error = %v, want %v", err, test.want)
			}
			if provider.calls != 0 {
				t.Fatalf("provider calls = %d, want 0", provider.calls)
			}
		})
	}
}

// TestK2EContextPreflightErrorPrecedenceCharacterization freezes the current
// first-failure behavior before the Phase 1E context-preflight extraction.
// These cases intentionally make a downstream Handle fact invalid at the same
// time so a reordered or prefetched capability chain would change the result.
func TestK2EContextPreflightErrorPrecedenceCharacterization(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*capabilityFixture, *HandleValidationRequest) error
		want   error
	}{
		{
			name: "missing context wins over missing handle",
			mutate: func(_ *capabilityFixture, request *HandleValidationRequest) error {
				request.ExecutionContextID = execution.ExecutionContextID("context-missing")
				request.CapabilityHandleID = CapabilityHandleID("handle-missing")
				return nil
			},
			want: execution.ErrContextNotFound,
		},
		{
			name: "terminated context wins over revoked handle",
			mutate: func(fixture *capabilityFixture, _ *HandleValidationRequest) error {
				if err := fixture.manager.RevokeHandle(fixture.handle.ID); err != nil {
					return err
				}
				return fixture.executions.TerminateContext(fixture.context.ID)
			},
			want: execution.ErrContextTerminated,
		},
		{
			name: "invalid context identity wins over invalid handle identity",
			mutate: func(_ *capabilityFixture, request *HandleValidationRequest) error {
				request.ExecutionContextID = execution.ExecutionContextID("context\x00invalid")
				request.CapabilityHandleID = CapabilityHandleID("handle\x00invalid")
				return nil
			},
			want: execution.ErrInvalidContext,
		},
		{
			name: "caller mismatch wins over missing handle",
			mutate: func(_ *capabilityFixture, request *HandleValidationRequest) error {
				request.CallerIdentity = "runtime-other"
				request.CapabilityHandleID = CapabilityHandleID("handle-missing")
				return nil
			},
			want: ErrCallerIdentityMismatch,
		},
		{
			name: "scope mismatch wins over missing handle",
			mutate: func(_ *capabilityFixture, request *HandleValidationRequest) error {
				request.Scope = "scope-other"
				request.CapabilityHandleID = CapabilityHandleID("handle-missing")
				return nil
			},
			want: ErrScopeMismatch,
		},
		{
			name: "valid context still reaches revoked handle",
			mutate: func(fixture *capabilityFixture, _ *HandleValidationRequest) error {
				return fixture.manager.RevokeHandle(fixture.handle.ID)
			},
			want: ErrHandleRevoked,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCapabilityFixture(t)
			request := validationRequestFor(fixture)
			if err := test.mutate(&fixture, &request); err != nil {
				t.Fatalf("mutate() error = %v", err)
			}
			_, firstErr := fixture.manager.ValidateHandleRequest(request)
			if !errors.Is(firstErr, test.want) {
				t.Fatalf("ValidateHandleRequest() error = %v, want %v", firstErr, test.want)
			}
			if firstErr == nil {
				t.Fatal("ValidateHandleRequest() error = nil, want characterized failure")
			}
			firstMessage := firstErr.Error()
			for attempt := 0; attempt < 7; attempt++ {
				_, repeatedErr := fixture.manager.ValidateHandleRequest(request)
				if !errors.Is(repeatedErr, test.want) || repeatedErr.Error() != firstMessage {
					t.Fatalf("repeated validation error = %v, want stable %v", repeatedErr, firstMessage)
				}
			}
		})
	}
}

func TestK2CInvalidHandleAndInvalidOperationReturnsAuthorityErrorFirst(t *testing.T) {
	fixture := newCapabilityFixture(t)
	validation := validationRequestFor(fixture)
	validation.CapabilityHandleID = CapabilityHandleID("missing-handle")
	request := InvocationRequest{Operation: "\x00"}

	var previous string
	for attempt := 0; attempt < 32; attempt++ {
		provider := &recordingCapabilityProvider{}
		err := fixture.manager.InvokeWithProvider(validation, request, provider)
		if !errors.Is(err, ErrHandleNotFound) {
			t.Fatalf("InvokeWithProvider() error = %v, want ErrHandleNotFound before ErrInvalidInvocation", err)
		}
		if attempt > 0 && err.Error() != previous {
			t.Fatalf("precedence error changed: previous=%q current=%q", previous, err.Error())
		}
		if provider.calls != 0 {
			t.Fatalf("provider calls = %d, want 0", provider.calls)
		}
		previous = err.Error()
	}
}

func TestK2CInstanceLifecycleAndAvailabilityRejectBeforeProvider(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(capabilityFixture) error
		want    error
		check   func(*testing.T, capabilityFixture)
	}{
		{
			name: "revoked instance",
			prepare: func(fixture capabilityFixture) error {
				return fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateRevoked)
			},
			want: ErrInstanceRevoked,
		},
		{
			name: "destroyed instance",
			prepare: func(fixture capabilityFixture) error {
				return fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateDestroyed)
			},
			want: ErrInstanceUnavailable,
		},
		{
			name: "active instance with unavailable provider",
			prepare: func(fixture capabilityFixture) error {
				if err := fixture.manager.UpdateInstanceState(fixture.instance.ID, InstanceStateActive); err != nil {
					return err
				}
				return fixture.resources.UpdateAvailability(fixture.resource.ID, resource.AvailabilityUnavailable)
			},
			want: ErrInstanceUnavailable,
			check: func(t *testing.T, fixture capabilityFixture) {
				instance, err := fixture.manager.GetInstance(fixture.instance.ID)
				if err != nil {
					t.Fatalf("GetInstance() error = %v", err)
				}
				if instance.State != InstanceStateActive || instance.Availability != AvailabilityAvailable {
					t.Fatalf("instance state/availability = %q/%q, want ACTIVE/AVAILABLE", instance.State, instance.Availability)
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newCapabilityFixture(t)
			if err := test.prepare(fixture); err != nil {
				t.Fatalf("prepare() error = %v", err)
			}
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
			if test.check != nil {
				test.check(t, fixture)
			}
		})
	}
}

func TestK2CActiveUnavailableInstanceRejectsBeforeProvider(t *testing.T) {
	fixture := newCapabilityFixture(t)
	fixture.manager.mu.Lock()
	instance := fixture.manager.instances[fixture.instance.ID].Clone()
	instance.State = InstanceStateActive
	instance.Availability = AvailabilityUnavailable
	fixture.manager.instances[fixture.instance.ID] = instance
	fixture.manager.mu.Unlock()

	provider := &recordingCapabilityProvider{}
	err := fixture.manager.InvokeWithProvider(
		validationRequestFor(fixture),
		InvocationRequest{Operation: "read"},
		provider,
	)
	if !errors.Is(err, ErrInstanceUnavailable) {
		t.Fatalf("InvokeWithProvider() error = %v, want ErrInstanceUnavailable", err)
	}
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.calls)
	}
}

func TestK2CValidationIsDeterministicAndSideEffectFree(t *testing.T) {
	fixture := newCapabilityFixture(t)
	request := validationRequestFor(fixture)
	request.Scope = "scope-b"

	first, firstErr := fixture.manager.ValidateHandleRequest(request)
	second, secondErr := fixture.manager.ValidateHandleRequest(request)
	if firstErr == nil || secondErr == nil || firstErr.Error() != secondErr.Error() {
		t.Fatalf("non-deterministic validation errors: first=%v second=%v", firstErr, secondErr)
	}
	if first.CapabilityHandle.ID != "" || second.CapabilityHandle.ID != "" || first.Resource.ID != "" || second.Resource.ID != "" {
		t.Fatalf("failed validation returned a non-empty snapshot: first=%#v second=%#v", first, second)
	}

	valid, err := fixture.manager.ValidateHandleRequest(validationRequestFor(fixture))
	if err != nil {
		t.Fatalf("valid ValidateHandleRequest() error = %v", err)
	}
	valid.CapabilityHandle.Permissions[0] = "mutated"
	valid.CapabilityDeclaration.InputMetadata["schema"] = "mutated"
	valid.Resource.CapabilityDeclarationIDs[0] = "mutated-declaration"
	gotHandle, err := fixture.manager.GetHandle(fixture.handle.ID)
	if err != nil {
		t.Fatalf("GetHandle() error = %v", err)
	}
	gotDeclaration, err := fixture.manager.GetDeclaration(fixture.declaration.ID)
	if err != nil {
		t.Fatalf("GetDeclaration() error = %v", err)
	}
	gotResource, err := fixture.resources.GetResource(fixture.resource.ID)
	if err != nil {
		t.Fatalf("GetResource() error = %v", err)
	}
	if gotHandle.Permissions[0] == "mutated" || gotDeclaration.InputMetadata["schema"] == "mutated" || gotResource.CapabilityDeclarationIDs[0] == "mutated-declaration" {
		t.Fatal("validation result mutation changed authoritative manager state")
	}
}

func TestK2CMalformedDeclarationValidationIsDeterministic(t *testing.T) {
	fixture := newCapabilityFixture(t)
	fixture.manager.mu.Lock()
	declaration := fixture.manager.declarations[fixture.declaration.ID].Clone()
	declaration.InputMetadata = map[string]string{
		"\x00-z": "invalid",
		"\x00-a": "invalid",
	}
	fixture.manager.declarations[fixture.declaration.ID] = declaration
	fixture.manager.mu.Unlock()

	request := validationRequestFor(fixture)
	var previous string
	for attempt := 0; attempt < 32; attempt++ {
		_, err := fixture.manager.ValidateHandleRequest(request)
		if !errors.Is(err, ErrInvalidDeclaration) {
			t.Fatalf("ValidateHandleRequest() error = %v, want ErrInvalidDeclaration", err)
		}
		if attempt > 0 && err.Error() != previous {
			t.Fatalf("malformed declaration error changed: previous=%q current=%q", previous, err.Error())
		}
		previous = err.Error()
	}
}

func TestK2CProviderErrorIsReturnedWithoutRetry(t *testing.T) {
	fixture := newCapabilityFixture(t)
	providerError := errors.New("provider failed")
	provider := &recordingCapabilityProvider{err: providerError}

	err := fixture.manager.InvokeWithProvider(
		validationRequestFor(fixture),
		InvocationRequest{Operation: "read"},
		provider,
	)
	if !errors.Is(err, providerError) {
		t.Fatalf("InvokeWithProvider() error = %v, want provider error", err)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
}

func TestK2CInvalidInvocationDoesNotCallProvider(t *testing.T) {
	fixture := newCapabilityFixture(t)
	provider := &recordingCapabilityProvider{}

	err := fixture.manager.InvokeWithProvider(
		validationRequestFor(fixture),
		InvocationRequest{Operation: "\x00"},
		provider,
	)
	if !errors.Is(err, ErrInvalidInvocation) {
		t.Fatalf("InvokeWithProvider() error = %v, want ErrInvalidInvocation", err)
	}
	if provider.calls != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.calls)
	}
}

func TestK2CPreservesObjectKindIntentRejection(t *testing.T) {
	reference := identity.ObjectReference{Kind: identity.ObjectKindIntent, ID: "intent-1"}
	if err := reference.Validate(); !errors.Is(err, identity.ErrInvalidObjectReference) {
		t.Fatalf("ObjectKindIntent validation error = %v, want ErrInvalidObjectReference", err)
	}
}

func TestK2CNoImplicitDelegationAcrossSameScope(t *testing.T) {
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
	if _, err := fixture.manager.ValidateHandleRequest(request); !errors.Is(err, ErrContextMismatch) {
		t.Fatalf("ValidateHandleRequest() error = %v, want ErrContextMismatch", err)
	}
}

func TestK2CLegacyValidateHandlePreservesHandleStatePrecedence(t *testing.T) {
	fixture := newCapabilityFixture(t)
	if err := fixture.manager.RevokeHandle(fixture.handle.ID); err != nil {
		t.Fatalf("RevokeHandle() error = %v", err)
	}
	if err := fixture.executions.TerminateContext(fixture.context.ID); err != nil {
		t.Fatalf("TerminateContext() error = %v", err)
	}
	if err := fixture.manager.ValidateHandle(fixture.handle.ID); !errors.Is(err, ErrHandleRevoked) {
		t.Fatalf("legacy ValidateHandle() error = %v, want ErrHandleRevoked", err)
	}
	if _, err := fixture.manager.ValidateHandleRequest(validationRequestFor(fixture)); !errors.Is(err, execution.ErrContextTerminated) {
		t.Fatalf("explicit ValidateHandleRequest() error = %v, want ErrContextTerminated", err)
	}
}
