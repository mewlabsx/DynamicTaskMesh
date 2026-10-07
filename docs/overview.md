# Architecture overview

[简体中文](overview.zh-CN.md)

DTM keeps task policy, resource identity and execution boundaries explicit. The repository contains several evolutionary layers; their capabilities must not be combined into an unverified single production system.

## Existing execution reference

```text
dtm-submit -> Core Task services -> Planner -> Mapper -> Runtime
                                                   -> gRPC Agent -> simulated Capability
dtm-query  -> Core query services -> SQLite Task/Step/Execution state
```

The reference profile uses capability/resource discovery and node ownership, leases and fencing to choose valid execution targets. SQLite stores the reference profile's state, with append-only migrations and conservative recovery. Submission deduplication and execution replay contracts guard different boundaries; neither alone implies Exactly-once side effects.

The Level 1 mesh adds discovery, handshake, membership, resource views and deterministic coordinator selection through `internal/mesh` and `internal/runtimehost`. The v0.6 invocation foundation supplies transport-independent invocation semantics with a native gRPC reference. These remain compatibility profiles, not a claim of multi-Core HA or an implemented Binary transport.

## Bounded Kernel and User Space

```text
Explicit User Space policy / selected work
  -> taskcreation: CreateRoot / CreateChild / RegisterChild
  -> Child Task Runtime -> User Space Execution Orchestrator
  -> narrow Kernel port -> Logical Execution Facade -> Provider seam

Root Task Runtime <- authoritative Child snapshots
                  -> explicit closure predicate
```

The Kernel owns Resource, Capability, Execution and immutable EventRecord mechanisms. A CapabilityHandle is the capability authority reference; strict execution-authority scope equality does not imply delegation or scope hierarchy. Resource and Capability are distinct concepts.

The facade is an in-process composition boundary, not a physical syscall or RPC ABI. Gate B Phase 1 supplies bounded Resource-owned execution capacity and controlled dispatch. Phase 2 resolves exact post-Provider occupancy only under the same ownership fence. Provider observations, authoritative state transitions and audit events are separate.

User Space owns business interpretation and policy. US-3 materializes already-decided tasks; it does not plan, retry, derive work or evaluate a Root automatically. Child execution success does not establish Root goal success. Cancellation does not establish that a Provider ended, and UNKNOWN must remain explicit rather than becoming an implicit retry or release.

## Authority and future scope

[Architecture State](../architecture/README.md), the effective semantic governance record and their referenced corrections define current facts. [Capability status](status.md) summarizes boundaries. A historical roadmap or completed design cannot promote a future mechanism to an implemented capability.

UserIntent belongs to User Space. KernelIntentSpec and KernelIntent describe a prospective opaque representation boundary; current Intent validation remains disabled. Full autonomous Gate B, production admission, new-Kernel persistence, remote ABI and final Architecture Freeze remain outside current claims.

## Reference state and task boundaries

The existing SQLite profile owns its Task/Step/Execution records, submission keys and conservative restart behavior. Submission deduplication and execution attempt replay protect different boundaries; neither establishes exactly-once side effects or persistent Kernel authority. Resource Invocation remains the bounded native gRPC reference; no Binary transport or broader frozen implementation is implied.

User Space owns local work and task projections, explicit Child selection, Root closure predicates and already-decided task creation. Kernel owns execution allocation, authority and occupancy facts. Child snapshots inform Root evaluation; successful Child execution alone is not Root goal success. These new task mechanisms are in-process and do not inherit the reference profile's SQLite recovery automatically. Removing historical implementation reports from this release does not change these code boundaries or acceptance states.
