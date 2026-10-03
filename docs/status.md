# Capability status

Software line: v0.7. Kernel specification: v0.1. This describes current source and existing evidence; current candidate validation is recorded separately in [validation](validation.md).

| Area | Source / status | Boundary |
|---|---|---|
| Core/Agent task execution | Existing reference implementation and tests | Simulated demo; legacy execution profile |
| SQLite state, queries and recovery | Existing reference profile | Does not imply new Kernel object persistence |
| Submission deduplication and execution protection | Layer-specific implementations | No blanket Exactly-once guarantee |
| Resource identity, directory and mapping | v0.4 reference foundation | Owner, lease, registration and generation remain required |
| Discovery, membership and coordinator | Level 1 mesh reference | No multi-Core HA or cross-domain autonomy claim |
| Resource Invocation | Native gRPC foundation, frozen at M2-R1 | Binary transport excluded |
| Kernel objects and invariants | Bounded implementation | Internal manager API is not a User Space ABI |
| Execution ownership | Gate B Phase 1 bounded closure | Resource capacity, immutable binding and controlled dispatch |
| Occupancy resolution | Gate B Phase 2 bounded closure | Same-fence only; cross-fence remains reserved/deferred |
| Logical Execution Facade | Accepted bounded in-process implementation | No persistence, remote invocation or physical ABI |
| User Space orchestrator and Child/Root runtime | Bounded implementations | Explicit work and closure; cancellation is not ended |
| Task creation | US-3 bounded implementation | Creation/registration only, not automatic planning or execution |
| Goal Loop and Workbench | Separate local experimental review bundle | Final acceptance pending; omitted from this candidate |

The current semantic freeze does not establish final Architecture Freeze. Full autonomous Gate B, KernelIntent runtime, production Resource Admission, broader authorization, new-Kernel persistence and remote protocols remain outside implemented claims. Future scope remains future scope, not a permanent architectural prohibition.

Existing historical PASS records describe their own source versions, environments and bounded tests. Raw evidence archives referenced in them are not shipped here and have not been reverified for this candidate. The absence of those archives must not be presented as fresh verification.


[简体中文](status.zh-CN.md)



## Recorded status

These are carried-forward engineering acceptance/governance facts from the selected development baseline, not new review or release-test results. Detailed phase reports and raw evidence are retained locally. Earlier DESIGN_ONLY, NOT_IMPLEMENTED and REVIEW_PENDING stage headers remain historical and do not override the current records.

| State boundary | Recorded value | Limit |
|---|---|---|
| K2-C / K2-D / K2-E | BOUNDED_IMPLEMENTED / CLOSED / CLOSED | Local mechanisms and bounded convergence only |
| Gate B Phase 1 | CLOSED, BOUNDED_IMPLEMENTED | Resource execution ownership only |
| Gate B Phase 2 | CLOSED, design ACCEPTED, review PASS, implementation BOUNDED_IMPLEMENTED, implementation review PASS | Same-fence local occupancy resolution; Full Gate B not closed |
| Pre-freeze Logical Facade | Design ACCEPTED/PASS; implementation BOUNDED_IMPLEMENTED; acceptance ACCEPTED and verification PASS | runtime_changes BOUNDED_LOGICAL_FACADE_ONLY; internal_manager_dependency NO; milestone_closed NO |
| Semantic governance | FROZEN | Reviewed semantic envelope only, not new runtime behavior |
| Intent boundary | FROZEN_SEMANTICS_ONLY / NOT_IMPLEMENTED | Persistent opaque KernelIntent is prospective |
| R0 checkpoint | FREEZE_CANDIDATE | Carried-forward checkpoint, not final freeze |
| Full Gate B | RESERVED | No autonomous-loop completion claim |
| Physical ABI / Scope governance | DEFERRED | Not a transport or delegation implementation |
| Complete authorization / global cross-manager consistency / production admission | OPEN | Existing local mechanisms do not close these boundaries |
| Final Architecture Freeze | NOT_CLAIMED | Separate review/sign-off and reproducible baseline required |

Read [execution](execution-contract.md), [boundary](kernel-userspace-boundary.md) and [authority](resource-authority.md) for exact current scope. Machine-record fields such as closure_record or implementation_document now point to this consolidated status summary, not a publicly available original report. Original hashes and mapping are retained in the local publication audit.
