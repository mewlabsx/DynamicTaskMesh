# Resource identity and authority

[简体中文](resource-authority.zh-CN.md)

## Reference profile

A Resource is an autonomous/provider ownership boundary, not a Capability, device or network address. A Resource explicitly declares CapabilityDeclaration; a declaration can produce CapabilityInstance, and CapabilityHandle references an instance within ExecutionContext. Identity is distinct from locator, endpoint and topology. Device/bus mapping belongs behind Owner/Gateway adapters. Discovery advertises candidate facts; it is not authorization.

The existing directory/mapping profile requires Owner Node, live Lease, Endpoint, registration_id and generation. Registration replacement and stale generations must not silently preserve execution eligibility. These checks are the reference profile's fencing rules. They have not been mapped into the new Kernel OwnershipFence or made one shared persistent authority model.

## Security boundary

CapabilityHandle is the only capability authority reference. A CapabilityInstance ID, ResourceID, ProviderID, permission string, copied Handle ID, trusted-process ingress or User Space assertion is insufficient. Required bindings include caller identity to ExecutionContext, authoritative Handle lookup, subject and strict ExecutionAuthorityScope equality, lifecycle, target instance and Resource relation, declared operation/parameters, current availability and execution-side revalidation before side effects. Missing or invalid required evidence fails closed.

PermissionSet currently carries structurally checked metadata, not a complete authorization decision. The security model specifies future authentication/authorization evidence; it does not implement credential issuance, cryptographic trust, RBAC/ACL/Policy Engine, complete parameter policy, Scope inheritance or Delegation. Release, expiration and revocation have different meanings; unavailability is not revocation. Security correctness does not replace device functional safety or physical interlocks.

LifecycleState and AvailabilityState are distinct. UNAVAILABLE belongs to availability and must not be silently used as a lifecycle state. EventRecord is an immutable audit fact, not mutable current state.

## Execution capacity

Authority is validated before allocation. Resource owns execution capacity; its live exact allocation-claim set is the sole authoritative source of capacity consumption and availability. There is no parallel mutable capacity counter. ExecutionAllocation.CurrentOccupancy is the separate authoritative conclusion for one allocation, not capacity ownership. These distinct facts are linked by invariants; claim presence alone cannot distinguish NOT_ESTABLISHED from UNKNOWN.

Allocation fixes an immutable ExecutionDescriptor/ExecutionAllocation binding: Context, Handle, declaration, instance, Resource, operation and dedicated Resource-issued OwnershipFence. Provider identity is resolved at Dispatch time after last-mile validation, not frozen as an unverified allocation-time route. A single controlled dispatch claim prevents a second Provider crossing. Resource.TryAcquireExecution/ReleaseExecution are internal coordination primitives; normal execution uses the allocation/release facade and never allows direct User Space claim manipulation.

## Resolution authority

Execution Authority, Provider Authority and Occupancy Resolution Authority are separate. ResolutionEvidence is transition input, not a Kernel Object or proof by itself. An independently admitted trusted Resource-side evidence source must supply or vouch for resolution authority. A User Space request cannot create, infer or elevate it; the facade must not obtain a Resource-issued authority solely because a caller asked to resolve an allocation.

Authority and evidence must bind the exact allocation, Resource and same OwnershipFence. A newer fence grants no retrospective authority over an old UNKNOWN allocation. Fence advancement alone is not termination evidence. Ordinary observation, cancellation, timeout and Resource unavailability do not authorize claim release. See [execution contract](execution-contract.md) and [recovery/idempotency](recovery-idempotency.md).
