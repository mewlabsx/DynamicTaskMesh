# Task model

## Two execution profiles

The existing Core/Agent profile models Task, Step and Execution: a Task contains work, a Step is its schedulable unit, and an Execution records an attempt on a selected target. Planner, Mapper and Runtime cooperate with the gRPC Agent. SQLite persists this profile's records. This path has not been replaced by, or fully integrated with, the bounded Kernel/User Space components.

The newer components model explicit User Space work and Root/Child Tasks. These are local in-process policy records, not Kernel Objects or an automatic planner. The software version and Kernel specification version are separate axes; naming a release v0.7 does not establish Kernel v0.7.

## Root and Child ownership

Root Runtime owns caller-supplied Root identity, an opaque GoalDescriptor, finite Child membership and goal-level lifecycle. Membership is explicitly OPEN/CLOSED. Root states are OPEN, ACTIVE, SUCCEEDED, FAILED, UNKNOWN and CANCELLED. A deterministic caller-supplied closure predicate returns SUCCEEDED/FAILED/UNKNOWN using authoritative Child snapshots. Child success alone does not prove Root goal satisfaction. The runtime does not interpret a goal as a prompt or generate a plan.

Child Runtime owns the immutable selected-work binding and local lifecycle. The orchestrator submits explicit work through a narrow Kernel port, then projects returned execution facts; it does not manufacture Kernel authority or occupancy evidence. RootTaskRef and ChildTaskRef are opaque lineage, not authority. One selected Child work item permits at most one Capability invocation in this bounded model; broader workflows require explicit decomposition, not hidden extra dispatch.

## Creation and cancellation

US-3 CreateRoot delegates only Root creation. CreateChild reserves an authoritative pristine CREATED Child with exact Root lineage. RegisterChild rereads the authoritative Child snapshot and registers its identity; a caller-authored snapshot is insufficient. Creation, membership registration, execution and closure evaluation are distinct operations. Failure does not silently trigger compensation, retry or additional work.

A cancellation request at User Space is not proof of Provider termination, ENDED occupancy or a releasable Resource claim. Unresolved execution remains UNKNOWN and must be reconciled through the existing authority boundary. There is no automatic retry, remapping, replanning, crash recovery or persistent identity guarantee for these local task components. The separately reviewed Goal Loop/Workbench experiment is omitted from this release.

## Code and related contracts

See [User Space](../internal/userspace/), [task creation](../internal/userspace/taskcreation/), [Root runtime](../internal/userspace/roottask/) and [execution contract](execution-contract.md). [Integration](integration.md) describes the conditions for replacing parallel profiles with one supported path.


[简体中文](task-model.zh-CN.md)
