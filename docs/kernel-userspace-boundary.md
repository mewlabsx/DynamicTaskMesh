# Kernel and User Space boundary

[简体中文](kernel-userspace-boundary.zh-CN.md)

## Ownership

Kernel owns structural Resource/Capability/Context/Handle validation, exact execution allocation, controlled Provider dispatch, bounded occupancy resolution and immutable EventRecord facts. Manager Go APIs are internal implementation APIs, not a User Space syscall or security ABI. User Space owns business interpretation, planning, scheduling policy, Task generation, World Model, satisfaction evaluation, reconciliation and retry policy. Task is external policy-managed data, not a Kernel Object. An Agent or LLM is optional policy outside deterministic Kernel mechanisms.

## Intent boundary

The machine-record interpretation anchors point here for the current rule. Their retained section labels `2.3 Intent Boundary` and `5 Frozen Decisions / item 9` identify original K1 historical statements, not headings in this consolidated document. Scoped supersession and retained aspects remain unchanged.

UserIntent is User Space-owned application/domain/human/agent intention. Intent Formulation means interpretation, normalization and translation into KernelIntentSpec; it is distinct from Satisfaction Planning. KernelIntentSpec is normalized prospective admission data with opaque business semantics, not a plan, Task, Handle or authority reference.

KernelIntent is the future admitted Kernel-managed persistent opaque representation: Kernel would own identity, persistence/lifecycle, revision, ownership/scope and structural admission/authority semantics. Business-semantic evaluation, planning, reconciliation and Task generation remain outside it. KernelIntent grants no Capability authority. This is FROZEN_SEMANTICS_ONLY architecture; its runtime is NOT_IMPLEMENTED and ObjectKindIntent remains rejected by current ObjectReference validation. NOT_IMPLEMENTED is not ARCHITECTURALLY_FORBIDDEN.

EvaluationState remains User Space data, not a required KernelIntent lifecycle dimension. SATISFIED/UNSATISFIED needs validated observations or a deterministic mechanism; UNKNOWN is valid. EvaluationState UNKNOWN is not invocation-outcome UNKNOWN: satisfaction uncertainty must not overwrite execution uncertainty. Contradictory observations and their provenance must remain distinguishable rather than being silently collapsed. No implicit probabilistic or LLM evaluation is admitted to Kernel.

## Scope and admission

ExecutionAuthorityScope requires strict Context/Handle equality. It does not grant hierarchical authority or Delegation. AutonomyScope, IntentScope and RoleScope remain RESERVED; MeshOrRegionScope is DEFERRED. Region/discovery grouping is not execution authority.

The local K2-E bridge consumes an already-authoritative ResourceRecordView. Projection and discovery do not admit production authority or mint Handles. Broader Resource Admission, complete authorization and K1 global cross-manager consistency remain OPEN. Security and authority checks are described in [resource authority](resource-authority.md).

## Effective state and governance

The five current [Architecture State YAML files](../architecture/README.md) record effective facts and scoped supersession. The retained [semantic governance sign-off](../architecture/reviews/kernel-semantic-freeze-governance-signoff-2026-09-06.md) freezes the reviewed semantic envelope. A later ordinary document, test, refactor or newer timestamp does not supersede it. Changing a frozen boundary requires an explicit scoped decision, Boundary Delta, review and governance approval.

R0 remains a carried-forward checkpoint/FREEZE_CANDIDATE. DEFERRED, RESERVED, OPEN and NEEDS_REVIEW are not FORBIDDEN or REMOVED. Current semantic governance FROZEN does not establish Final Architecture Freeze, which remains NOT_CLAIMED. Full Gate B remains RESERVED and future mechanisms need separate design and authorization. See [status](status.md); historical acceptance states are not fresh release validation.
