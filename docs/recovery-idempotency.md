# Recovery and idempotency

## Persisted reference profile

The Core/Agent reference profile persists Task/Step/Execution state, submission keys and Resource records in SQLite. Migrations are append-only. Registration identity/generation, Owner Lease and Endpoint validation remain required. Atomic Resource persistence precedes publication into the directory; database state, not an optimistic memory view, is authoritative for that profile.

Submission deduplication protects task admission. Execution/attempt replay guards protect another boundary. Neither establishes exactly-once Provider side effects. Recovery is conservative: do not replay an uncertain effect merely because an Agent vanished, a Lease expired, a response was lost or a record is incomplete. Lease expiry changes ownership eligibility, not proof of non-execution. Details of the supported reference behavior remain in the runtime and SQLite regression tests.

## In-process Kernel lifetime

The new Kernel allocation, occupancy, invocation binding and trusted resolution state are bounded local memory mechanisms. They do not inherit the old SQLite persistence, replay or crash-recovery guarantees. A facade InvocationID is stable correlation within the admitted in-process scope, not a restart-global deduplication store. ObserveInvocation NOT_KNOWN has no authoritative crossing fact and does not prove safe replay.

InvocationID+exact InvocationBinding prevents a second local allocation/crossing on equivalent submission. Changed input conflicts. Query/transport correlation RequestID does not create a new invocation. Outcome UNKNOWN and occupancy UNKNOWN must remain explicit. Physical non-idempotent operations are treated conservatively unless a Provider/Capability contract explicitly proves repetition safe; a timeout or cancellation is not such a proof.

## Resolution and restart limits

Only trusted Resource-authoritative evidence bound to the exact allocation and same OwnershipFence may resolve post-Provider UNKNOWN occupancy. Original observation remains immutable; ENDED/RELEASED plus exact claim absence is not sufficient for ALREADY_RESOLVED unless equivalent prior resolution is proven. Automatic exact release belongs only to the accepted authoritative transition, not an ordinary retry or cleanup.

Cross-fence resolution, ownership authority migration, cancellation/termination, durable Kernel recovery and remote reconciliation remain outside implemented scope. A newer fence cannot terminate an older UNKNOWN execution by implication. Integrating the profiles requires an explicit decision about persistence authority, effect identity, ownership/lease/fence mapping and crash boundaries, followed by fault/restart evidence; linking packages or retaining both logs is insufficient.

See [execution](execution-contract.md), [authority](resource-authority.md), [integration](integration.md) and [validation](validation.md). Historical PASS describes its original baseline; it is not current crash-recovery evidence for the new Kernel.


[简体中文](recovery-idempotency.zh-CN.md)
