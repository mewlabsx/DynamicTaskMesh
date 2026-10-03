package invocation

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"dtm/internal/model"
	"dtm/internal/resourcedirectory"
)

var (
	ErrInvalidTargetResolver = errors.New("invalid invocation target resolver")
	ErrInvalidResourceLookup = errors.New("invalid invocation resource lookup")
	ErrInvalidEndpointLookup = errors.New("invalid invocation endpoint lookup")
)

type ResourceLookup interface {
	GetByID(model.ResourceID) (resourcedirectory.ResourceRecordView, error)
}

type EndpointLookup interface {
	Resolve(model.NodeID) (string, error)
}

// AuthoritativeTargetResolver is the M1 compatibility adapter between the
// exact Resource Directory fence and the node-level legacy endpoint directory.
// The two directories do not expose one shared lock, so resolution uses a
// bounded double validation and fails closed if the exact snapshot changes.
type AuthoritativeTargetResolver struct {
	resources ResourceLookup
	endpoints EndpointLookup
}

func NewAuthoritativeTargetResolver(resources ResourceLookup, endpoints EndpointLookup) (*AuthoritativeTargetResolver, error) {
	if isNilDependency(resources) {
		return nil, ErrInvalidResourceLookup
	}
	if isNilDependency(endpoints) {
		return nil, ErrInvalidEndpointLookup
	}
	return &AuthoritativeTargetResolver{resources: resources, endpoints: endpoints}, nil
}

// NewTargetResolver is a concise constructor alias used by composition code.
func NewTargetResolver(resources ResourceLookup, endpoints EndpointLookup) (*AuthoritativeTargetResolver, error) {
	return NewAuthoritativeTargetResolver(resources, endpoints)
}

func (resolver *AuthoritativeTargetResolver) Resolve(
	ctx context.Context,
	ref model.ResourceRef,
	operation model.OperationID,
) ([]ResolvedInvocationTarget, error) {
	if resolver == nil || isNilDependency(resolver.resources) || isNilDependency(resolver.endpoints) {
		return nil, &InvocationError{Code: ErrorCodeInvalidRequest, Message: ErrInvalidTargetResolver.Error(), Source: "resolver", Classification: "invalid_dependency"}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	if err := ref.Validate(); err != nil {
		return nil, &InvocationError{Code: ErrorCodeInvalidRequest, Message: "invalid resource ref", Cause: err, Source: "resolver", Classification: "request_validation"}
	}
	if err := operation.Validate(); err != nil {
		return nil, &InvocationError{Code: ErrorCodeInvalidRequest, Message: "invalid operation", Cause: err, Source: "resolver", Classification: "request_validation"}
	}

	first, err := resolver.resources.GetByID(ref.ResourceID)
	if err != nil {
		return nil, resolver.lookupError(err, "initial resource lookup")
	}
	if err := contextErr(ctx); err != nil {
		return nil, err
	}
	if err := validateResourceSnapshot(ref, first, operation); err != nil {
		return nil, err
	}

	address, err := resolver.endpoints.Resolve(ref.OwnerNodeID)
	if err != nil {
		return nil, &InvocationError{
			Code: ErrorCodeResourceNotFound, Message: fmt.Sprintf("resolve endpoint for node %q: %v", ref.OwnerNodeID, err),
			Cause: err, Retryable: true, Source: "endpoint-directory", Classification: "compatibility_endpoint_missing",
		}
	}
	if strings.TrimSpace(address) == "" {
		return nil, &InvocationError{Code: ErrorCodeResourceNotFound, Message: "resolved endpoint is empty", Retryable: true, Source: "endpoint-directory", Classification: "compatibility_endpoint_missing"}
	}
	if err := contextErr(ctx); err != nil {
		return nil, err
	}

	// EndpointDirectory and Resource Directory currently have independent
	// locks. Re-read the exact ResourceRef immediately before constructing the
	// dispatch wrapper; any fence/publication change is rejected.
	second, err := resolver.resources.GetByID(ref.ResourceID)
	if err != nil {
		return nil, resolver.lookupError(err, "final resource lookup")
	}
	if err := validateResourceSnapshot(ref, second, operation); err != nil {
		return nil, err
	}

	target := ResolvedInvocationTarget{
		ResourceRef: ref,
		Operation:   operation,
		Endpoint: ResourceEndpoint{
			TransportID: TransportGRPC,
			Address:     address,
		},
	}
	if err := target.Validate(); err != nil {
		return nil, &InvocationError{Code: ErrorCodeProtocolError, Message: "construct resolved invocation target", Cause: err, Source: "resolver", Classification: "invariant"}
	}
	return []ResolvedInvocationTarget{target}, nil
}

// ResolveTarget is an explicit alias for callers that prefer the domain name.
func (resolver *AuthoritativeTargetResolver) ResolveTarget(ctx context.Context, ref model.ResourceRef, operation model.OperationID) ([]ResolvedInvocationTarget, error) {
	return resolver.Resolve(ctx, ref, operation)
}

func (resolver *AuthoritativeTargetResolver) lookupError(err error, stage string) error {
	code := ErrorCodeResourceNotFound
	classification := "resource_lookup"
	if errors.Is(err, resourcedirectory.ErrStaleResourceGeneration) || errors.Is(err, resourcedirectory.ErrStaleNodeGeneration) || errors.Is(err, resourcedirectory.ErrStaleRegistration) {
		code = ErrorCodeResourceStale
		classification = "resource_lookup_stale"
	}
	return &InvocationError{
		Code: code, Message: fmt.Sprintf("%s: %v", stage, err), Cause: err,
		Source: "resource-directory", Classification: classification,
	}
}

func validateResourceSnapshot(ref model.ResourceRef, view resourcedirectory.ResourceRecordView, operation model.OperationID) error {
	if !resourceRefMatchesView(ref, view) {
		return &InvocationError{
			Code:    ErrorCodeResourceStale,
			Message: fmt.Sprintf("resource ref %q is not the current exact directory fence", ref.ResourceID),
			Source:  "resource-directory", Classification: "exact_ref_mismatch",
		}
	}
	if !view.Eligible {
		return &InvocationError{
			Code:    ErrorCodeFenceRejected,
			Message: fmt.Sprintf("resource ref %q is not eligible: %s", ref.ResourceID, view.IneligibleReason),
			Source:  "resource-directory", Classification: "lifecycle_or_publication_fence",
		}
	}
	found := false
	for _, descriptor := range view.Descriptor.Operations {
		if descriptor.ID == operation {
			found = true
			break
		}
	}
	if !found {
		return &InvocationError{
			Code:    ErrorCodeUnsupportedOperation,
			Message: fmt.Sprintf("resource %q does not support operation %q", ref.ResourceID, operation),
			Source:  "resource-directory", Classification: "operation_not_published",
		}
	}
	return nil
}

func resourceRefMatchesView(ref model.ResourceRef, view resourcedirectory.ResourceRecordView) bool {
	return view.Descriptor.ID == ref.ResourceID &&
		view.Descriptor.Generation == ref.ResourceGeneration &&
		view.Descriptor.OwnerNodeID == ref.OwnerNodeID &&
		view.NodeGeneration == ref.OwnerNodeGeneration &&
		view.RegistrationID == ref.RegistrationID
}

func contextErr(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return &InvocationError{Code: ErrorCodeTimeout, Message: "invocation resolution deadline exceeded", Cause: err, Source: "resolver", Classification: "context"}
		}
		return &InvocationError{Code: ErrorCodeCanceled, Message: "invocation resolution canceled", Cause: err, Source: "resolver", Classification: "context"}
	}
	return nil
}

func isNilDependency(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	switch reflected.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return reflected.IsNil()
	default:
		return false
	}
}

var _ ResourceLookup = (*resourcedirectory.Directory)(nil)
