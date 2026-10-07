# Execution contract

[简体中文](execution-contract.zh-CN.md)

## Logical facade

The accepted Logical Execution Facade provides RequestExecution, ObserveInvocation and SubmitOccupancyResolution within one local Kernel lifetime. It exposes immutable value snapshots through a narrow port; callers need no internal Manager pointers, mutable objects or locks. It is an in-process semantic contract, not a physical syscall, RPC, wire/authentication protocol or persistence ABI. Physical ABI is DEFERRED.

RequestExecution requires a caller-owned stable InvocationID, ExecutionContext, CapabilityHandle, Operation and opaque Payload. RootTaskRef/ChildTaskRef are optional external lineage; RequestID is optional message correlation. Resource/Provider/instance IDs and a caller-selected fence cannot replace Handle authority. Missing/malformed required input returns INVALID_REQUEST before Provider crossing; invalid/stale/foreign/terminated/scope-mismatched authority fails closed.

## Identity binding

The first accepted invocation fixes immutable InvocationBinding: Context, Handle, Operation, exact Kernel-visible opaque Payload, RootTaskRef and ChildTaskRef. RequestID is never part of InvocationID identity. Caller/context authority facts are validated; Resource, instance, declaration and OwnershipFence are Kernel-derived allocation facts.

The same InvocationID and exactly equal binding returns/retrieves the existing invocation without a second allocation or Provider call. Any different bound field returns INVOCATION_CONFLICT without mutating the original. Payload equality is exact opaque value equality; the local []byte representation uses exact byte sequence equality. Equal bytes establish Kernel-visible identity, not business equivalence; different bytes do not prove different business meaning. False conflict is acceptable. Kernel performs no schema-aware canonicalization; required business/encoding normalization occurs before this boundary.

One accepted InvocationID has at most one exact ExecutionAllocationID. Allocation IDs are read-only correlation, not authority or release tokens. ObserveInvocation is read-only, checks optional exact allocation correlation, and never creates authority, releases claims or rewrites identity.

## Results and crossing

Results carry InvocationID, optional ExecutionAllocationID, RequestStatus, ProviderCrossingAvailable/ProviderCrossing, immutable ExecutionObservation, CurrentOccupancy, ReconciliationRequired, RetrySafetyFact, structured ErrorInfo and optional immutable EventReference. ErrorInfo distinguishes Category, Phase, crossing validity, correlation and reconciliation facts; a diagnostic string alone cannot determine safety.

RequestStatus includes REJECTED, ACCEPTED, DISPATCHED, RUNNING, COMPLETED, FAILED and UNKNOWN under the semantic model. Observation distinguishes NOT_KNOWN, ACCEPTED_NOT_ALLOCATED, ALLOCATED_NOT_CROSSED, DEFINITIVE_OBSERVATION, UNRESOLVED_UNKNOWN, ENDED_RELEASED and INVARIANT_UNKNOWN. Absence of an in-process record is not proof of non-execution.

Current local crossing facts are NOT_CROSSED_DEFINITE and CROSSED. ProviderCrossingAvailable=false means no interpretable crossing fact; NOT_KNOWN does not become safe to replay. CROSSING_UNCERTAIN/PROVIDER_CROSSING_UNCERTAIN is future/reserved, not an implemented local crossing state. After CROSSED, a Provider error without definitive occupancy evidence leaves outcome/occupancy UNKNOWN, with the exact claim held. Kernel cannot invent a definite pre-provider failure.

RetrySafetyFact is NOT_DECLARED, EXPLICITLY_SAFE or UNSAFE_OR_UNKNOWN, not SHOULD_RETRY. Error phases distinguish REQUEST_VALIDATION, AUTHORITY_VALIDATION, ALLOCATION, PRE_PROVIDER, PROVIDER_CROSSING, POST_PROVIDER, OBSERVATION, RESOLUTION and RELEASE. Categories distinguish INVALID_REQUEST, INVALID_OR_STALE_AUTHORITY, CAPACITY_UNAVAILABLE, INVOCATION_CONFLICT, PROVIDER_NOT_AVAILABLE, EXECUTION_UNRESOLVED, RESOLUTION_NOT_ALLOWED, ALREADY_RELEASED, ALREADY_RESOLVED, INVARIANT_VIOLATION and INTERNAL_FAILURE. AUTHORIZATION_DENIED expresses a separately available explicit denial; it does not imply a complete authorization evaluator. Missing required evidence never implies approval.

## Ownership and same-fence resolution

Authority precedes exact allocation. Last-mile validation and dispatch-time Provider resolution precede the single controlled Provider crossing. ExecutionObservation is immutable history; later resolution cannot rewrite it. CurrentOccupancy is current per-allocation state: NOT_ESTABLISHED, UNKNOWN or ENDED. Resource exact claims independently govern capacity accounting.

UNKNOWN retains the exact claim. Cancellation request != execution ended. Same-fence authoritative ENDED resolution atomically removes the exact claim and establishes CurrentOccupancy=ENDED/Lifecycle=RELEASED with exactly one accepted immutable resolution EventRecord. ResolutionEvidence is not a Kernel Object. ResolutionAuthorityFence must equal ExecutionAllocation.OwnershipFence; an E18 authority cannot resolve an E17 allocation. Cross-fence resolution, fence-transition runtime and retrospective authority remain future/reserved.

SubmitOccupancyResolution requires an independently admitted trusted Resource-side authority source and exact binding; it must not derive authority from the ordinary request. RESOLVED establishes exact release; UNRESOLVED preserves uncertainty. ALREADY_RESOLVED requires the original immutable UNKNOWN observation and equivalent resolution binding, exact terminal ENDED/RELEASED state and absence of the exact claim. Terminal state alone is not resolution history. Failed validation or event commit must not partially change the claim/state/event boundary.

## Bounded consistency

Structural validation may observe a point-in-time snapshot. Last-mile revalidation occurs before crossing. Exact claim presence/absence, CurrentOccupancy and allocation lifecycle form one bounded local ownership boundary; neither capacity claims nor occupancy state substitutes for the other.

Mandatory EventRecord atomic coupling is limited to the accepted delayed Phase 2 same-fence resolution. Immediate definitive ENDED in DispatchExecution, allocation, ordinary dispatch/observation/release do not claim identical event coupling. There is no generic all-transitions event atomicity, general multi-manager transaction, global linearizability or global snapshot isolation. External Phase 2 resolution must remain visible to the facade; a facade-local marker is not occupancy authority, nor can allocation existence imply crossing.

The pre-freeze logical facade implementation is accepted/verified within its bounded scope; its recorded milestone_closed remains NO. Gate B Phase 1 and Phase 2 are bounded CLOSED, Full Gate B is RESERVED, KernelIntent runtime is NOT_IMPLEMENTED, and Final Architecture Freeze is NOT_CLAIMED. These distinct statuses do not change through this documentation consolidation. See [status](status.md), [authority](resource-authority.md) and [recovery](recovery-idempotency.md).
