# DTM Kernel Concept Map

[简体中文](concept-map.zh-CN.md)

> **This document is a reading map for [`architecture/concepts.yaml`](../architecture/concepts.yaml).**
> `concepts.yaml` is the **authoritative definition**; this document does not repeat its `definition` fields. It supplies the three things that file does not:
> **① the relations between concepts; ② a suggested reading order; ③ a terminology trap list.**
>
> ⚠️ If this document conflicts with `concepts.yaml`, **`concepts.yaml` prevails**.

---

## 1. Three intuitions to establish first

Before reading any concept, hold these three — they explain why DTM's object model looks the way it does:

| # | Intuition | Immediate consequence |
|---|---|---|
| 1 | **Identity is not authority** | `Resource` and `CapabilityInstance` are **not** authority; a `CapabilityHandle` is required |
| 2 | **Observation is not state** | What a Provider reports back is **evidence**, not authoritative state; admission stands in between |
| 3 | **Task lives outside the Kernel** | `Task` is not a Kernel Object; the Kernel keeps only opaque lineage references |

---

## 2. Kernel object relation diagram

(`D-KERNEL-OBJECT-FOUNDATION`, status `FROZEN`)

```text
┌─────────────────────────────────────────────────────────────────────┐
│  Resource                                        【FROZEN】          │
│  An autonomy and Provider boundary. Not a Capability, not a generic  │
│  permission. Owns execution capacity (fixed unweighted slots,       │
│  effective default 1).                                              │
│                                                                     │
│      │ declares capabilities through CapabilityDeclaration           │
│      ▼                                                              │
│  CapabilityDeclaration                           【FROZEN】          │
│  A Resource-attached capability contract that can produce a          │
│  CapabilityInstance.                                                │
│                                                                     │
│      │ can produce                                                  │
│      ▼                                                              │
│  CapabilityInstance                              【FROZEN】          │
│  Provider-backed Kernel state. ★ Not authority by itself.            │
│                                                                     │
│      │ referenced by                                                │
│      ▼                                                              │
│  CapabilityHandle                                【FROZEN】          │
│  ★ The only authority reference. Context-bound + ExecutionAuthority- │
│    Scope-bound.                                                     │
│                                                                     │
│      ├── bound to ──► ExecutionContext            【FROZEN】          │
│      │                The caller execution boundary: subject /        │
│      │                ExecutionAuthorityScope / lifecycle / derived   │
│      │                availability and binding capability authority  │
│      │                                                              │
│      └── Scope must be 【strictly equal】 to ExecutionContext.Scope   │
│          A mismatch fails closed and does NOT imply equality in      │
│          other Scope domains                                        │
└─────────────────────────────────────────────────────────────────────┘

  EventRecord                                      【FROZEN】
  An immutable audit fact. ★ Not current state; User Space may not
  publish or rewrite it.
```

### Execution-side object chain

(Introduced by Gate B; `ExecutionAllocation` addresses exactly one thing: "this execution occupies one unit of Resource capacity")

```text
ExecutionRequest                                  【BOUNDED_IMPLEMENTED】
  └─ RootTaskRef / ChildTaskRef are 【opaque】 lineage values
     The Kernel does not own task lifecycle, priority, ordering, scheduling,
     planning or satisfaction evaluation

ExecutionDescriptor                               【BOUNDED_IMPLEMENTED】
  └─ The canonical execution binding frozen when Resource capacity is granted
     Embedded in ExecutionAllocation; ★ not a separately addressable object

ExecutionAllocation                               【BOUNDED_IMPLEMENTED】
  └─ A Kernel-managed runtime object proving that one concrete execution has
     been granted Resource capacity
     Embeds an immutable ExecutionDescriptor
     Lifecycle has only RUNNING / RELEASED
     ★ Holds CurrentOccupancy (the authoritative current occupancy conclusion)

ExecutionObservation                              【BOUNDED_IMPLEMENTED】
  └─ An 【immutable historical observation】 captured at the Provider boundary
     Carries the orthogonal dimensions ExecutionOutcome and OccupancyConclusion
     ★ Not the Kernel's current authoritative occupancy conclusion; later
       resolution 【cannot rewrite it】

CurrentOccupancy                                  【ACCEPTED / BOUNDED_IMPLEMENTED】
  └─ The current authoritative occupancy conclusion on an ExecutionAllocation
     ★ kernel_object: false (a dimension of the allocation, not a standalone object)

ResolutionEvidence                                【ACCEPTED / BOUNDED_IMPLEMENTED】
  └─ Validated 【state-transition input】, admissible when an authorized source
     proves occupancy has ENDED
     ★ Not a Kernel Object, lifecycle, EventRecord, manager or store

OccupancyResolutionAuthority                      【ACCEPTED / BOUNDED_IMPLEMENTED】
OwnershipFence                                    【ACCEPTED / BOUNDED_IMPLEMENTED】
```

> **`kernel_object: false` is an important marker**: `CurrentOccupancy`, `ResolutionEvidence`, `OccupancyResolutionAuthority` and `OwnershipFence` are **not independently addressable Kernel objects** — they are state dimensions or transition inputs of an allocation. **Do not build managers or stores for them.**

---

## 3. State vocabularies (three of them — do not mix)

`INV-LIFECYCLE-AVAILABILITY-SEPARATION` (`CRITICAL`): **lifecycle and availability are separate vocabularies.**

| Vocabulary | Belongs to | Values | Note |
|---|---|---|---|
| **LifecycleState** | object lifecycle | see below | `UNAVAILABLE` is **not** a lifecycle state |
| **AvailabilityState** | availability | includes `UNAVAILABLE` | not lifecycle |
| **EvaluationState** | User Space / a future evaluation boundary | `SATISFIED` / `UNSATISFIED` / `UNKNOWN` | ★ **not** a required KernelIntent lifecycle dimension |

### Resource lifecycle is a strict subset

`INV-RESOURCE-LIFECYCLE-SUBSET` (`HIGH`):

```text
Resource accepts: CREATED · ACTIVE · SUSPENDED · REMOVED
Resource does not accept: TERMINATED (★ not a Resource lifecycle state)
```

### The three execution occupancy conclusions

| `CurrentOccupancy` | Meaning | Exact claim |
|---|---|---|
| `NOT_ESTABLISHED` | The Provider boundary has **not** been crossed | HELD |
| `UNKNOWN` | Boundary crossed, **cannot prove occupancy ended** | **still HELD** |
| `ENDED` | Authoritative evidence that it **can no longer occupy** the Resource | NONE (must already be released) |

> **`UNKNOWN` is neither** "not started", **nor** failure, **nor** timeout (`INV-UNKNOWN-OCCUPANCY-RETAINS-CLAIM`).
> And **execution outcome certainty** and **occupancy certainty** are **orthogonal** dimensions (`INV-OBSERVATION-OUTCOME-OCCUPANCY-SEPARATION`).

### ExecutionAllocation lifecycle

```text
RUNNING ──► RELEASED        (only two values)
```

Supporting invariants:

- `INV-RELEASED-IMPLIES-ENDED`: `Lifecycle == RELEASED` ⟹ `CurrentOccupancy == ENDED` and the exact claim is NONE
- **A RELEASED allocation cannot retain `UNKNOWN` occupancy**

---

## 4. Scope domains: five names, only one usable as authority

`INV-SCOPE-DOMAIN-SEPARATION` (`CRITICAL`) — **they imply no containment, inheritance, equality or delegation between one another.**

| # | Scope | Status | Meaning | Usable as authority? |
|---|---|---|---|---|
| 1 | **`ExecutionAuthorityScope`** | `FROZEN` | the current K1 Handle/Context authority domain | ✅ **the only one**. `CapabilityHandle.Scope` must equal `ExecutionContext.Scope` |
| 2 | `AutonomyScope` | `RESERVED` | a future autonomy/participation boundary | ❌ **not mapped** to current Handle/Context fields |
| 3 | `IntentScope` | `RESERVED` | the intent boundary | ❌ **distinct** from `ExecutionAuthorityScope`; grants no execution authority |
| 4 | `RoleScope` | `RESERVED` | a future role eligibility/governance boundary | ❌ role selection does **not** grant or widen Handle authority |
| 5 | `MeshOrRegionScope` | `DEFERRED` | Mesh/Region search, trust and fault boundaries | ❌ region/discovery grouping is **not** execution authority |

> ⚠️ **`Resource.Scope` is not automatically an `ExecutionAuthorityScope`.**

---

## 5. Five boundary concepts that must be kept apart

These five are most easily mistaken for "one thing", yet **all of them are separate**:

| Concept | Owner | In one sentence | Key invariant |
|---|---|---|---|
| **`Observation`** | Provider / external evidence source; Kernel admission owner **`OPEN`** | a **report** about the physical/external world | `INV-OBSERVATION-EVIDENCE-SEPARATION`: **untrusted or bounded evidence** until admitted |
| **`KernelStateTransition`** | Kernel object manager / admission boundary | an authoritative state **mutation** | an observation or provider result does **not** become a transition by itself |
| **`EventRecord`** | Kernel / EventManager | an immutable audit **fact** | `INV-EVENT-IMMUTABLE-SEMANTIC`: User Space **cannot** publish or rewrite |
| **`ProviderDeclaration`** | Provider/Adapter → Resource admission | a provider **claim** | `DEFERRED`: does **not** establish Resource or Capability authority by itself |
| **`RegistrationRequest`** | Provider/Node/Runtime → Resource admission | carries an admission **claim** | `DEFERRED`: **not** a Resource identity and **not** proof the legacy fence is satisfied |

**The only legitimate path to an authoritative state transition** (`D-R0-OBSERVATION-EVENT-SEPARATION`):

```text
Physical / external world
  → ProviderEvidence / Observation        ← untrusted evidence
    → Validation / Admission               ← ★ the only gate
      → Authoritative Kernel state transition
        → Immutable Kernel EventRecord
```

---

## 6. The three Intent layers

(`D-R0.1-INTENT-DUAL-LAYER` + `D-R0.1-INTENT-PERSISTENT-REPRESENTATION`, both `FROZEN_SEMANTICS_ONLY`)

| Layer | Owner | What it is | Status |
|---|---|---|---|
| **`UserIntent`** | User Space | application/domain/human/agent intention, **may be ambiguous, may carry policy** | `FROZEN_SEMANTICS_ONLY` |
| **`KernelIntentSpec`** | User Space → Kernel admission boundary | **normalized desired-state boundary data**; business semantics **opaque** to the Kernel | `FROZEN_SEMANTICS_ONLY` |
| **`KernelIntent`** | Kernel / future admission boundary | a **future** admitted Kernel-managed persistent opaque representation | `FROZEN_SEMANTICS_ONLY`, runtime **`NOT_IMPLEMENTED`** |

```text
UserIntent
   │ Intent Formulation —— a User Space responsibility: interpret, normalize, translate
   ▼
KernelIntentSpec        ← Boundary data. Not a Kernel Object / authority reference /
                           Task / plan / Handle
   │ future admission boundary
   ▼
KernelIntent            ← Kernel owns: identity, persistence/lifecycle, revision,
                           ownership/scope, structural admission and authority semantics
                          Kernel does not own: business-semantic interpretation,
                           satisfaction evaluation, World Model, reconciliation,
                           Satisfaction Planning, Task generation, capability authority
```

**Three hard constraints**:

| Invariant | Rule |
|---|---|
| `INV-R0.1-INTENT-LAYER-SEPARATION` | the two layers **cannot** silently substitute for each other |
| `INV-R0.1-KERNELINTENT-NOT-EXECUTION-AUTHORITY` | KernelIntent is **not** a Handle, Instance, Resource, Context, authorization proof, Task owner, Planner, Scheduler, evaluator, World Model or implicit execution request |
| `INV-R0.1-EVALUATION-REQUIRES-OBSERVATION` | `SATISFIED`/`UNSATISFIED` **requires** validated observation or another deterministic mechanism; `UNKNOWN` **remains valid** until evidence exists |

> **`Task` relation rule**: one `KernelIntent` may lead to **0 / 1 / many** Tasks; one `UserIntent` may lead to **0 / 1 / many** `KernelIntent`s. Mapping and generation strategy belong to User Space.
>
> **`ObjectKindIntent` is currently rejected by Kernel ObjectReference validation** (`D-R0-INTENT-REFERENCE-BOUNDARY`).
> `NOT_IMPLEMENTED` is **not** `ARCHITECTURALLY_FORBIDDEN`.

---

## 7. Task and Capability lineage cardinality

`INV-TASK-EXTERNAL-POLICY-MANAGED` (`CRITICAL`, `FROZEN_SEMANTICS_ONLY`):

> **Task is owned by User Space / Policy Layer.** It is not a Kernel Object, not a Kernel-managed lifecycle, not a Kernel scheduler entity.
> The Kernel does **not** create, decompose, schedule, retry, aggregate, complete or evaluate Tasks.
> `RootTaskRef` and `ChildTaskRef` are **opaque lineage references** — the Kernel uses them only to state where an execution originated.

**Lineage cardinality rule** (an external Task/Policy contract, not a Kernel Object rule):

```text
One Child Task
  ├─ at most ── 1 concrete Capability execution request
  ├─ at most ── 1 ExecutionRequest
  └─ at most ── 1 Capability invocation
```

> Broader workflows **require explicit decomposition** and **cannot** rely on hidden extra dispatch.

---

## 8. The four layers of authorization and allocation

These four happen **in sequence and none replaces another**:

```text
① PermissionSet              (metadata carrier)
   D-PERMISSIONSET-METADATA: structural validation only. Not RBAC, not an ACL,
   not a policy decision, not complete authorization proof.

② Authority validation       (capability authority)
   INV-CAPABILITY-AUTHORITY: requires a valid CapabilityHandle.
   INV-HANDLE-CONTEXT-SCOPE: Handle.Scope must == ExecutionContext.Scope.

③ Capacity allocation        (allocation)
   INV-ALLOCATION-IS-NOT-AUTHORITY: ★ a valid Handle + authority validation
   does 【NOT create】 an ExecutionAllocation. Capacity is consumed only by
   TryAllocateExecution.

④ Provider invocation        (dispatch)
   INV-ALLOCATION-DISPATCH-BARRIER: ★ no valid RUNNING ExecutionAllocation
   means 【zero】 Provider calls. One allocation claims the normal dispatch
   boundary at most once.
```

**Three authority roles are mutually independent** (`INV-RESOLUTION-AUTHORITY-SEPARATION`):

| Role | Held by | Does not imply |
|---|---|---|
| **Execution Authority** | `CapabilityHandle` | Occupancy Resolution Authority |
| **Provider Authority** | the Provider's own contract | resolution authority, unless the contract explicitly says so |
| **Occupancy Resolution Authority** | the Resource execution-ownership authority | may resolve only an allocation whose fence **exactly matches** its own |

---

## 9. Terminology trap list

### `NEEDS_REVIEW` overloaded terms

These names **carry several meanings or have no frozen owner** in this repository. Before changing code, **you must** check `owner` and `definition` in `concepts.yaml`.

| Term | Problem | Status |
|---|---|---|
| **`Capability`** | historical material uses it as a matching/value concept; the current Kernel freezes `CapabilityDeclaration` / `CapabilityInstance` / `CapabilityHandle` as **three distinct concepts** and has **no** standalone K0.5 Kernel object named `Capability` | `NEEDS_REVIEW` |
| **`State`** | current Kernel material separates `LifecycleState` and `AvailabilityState`; `EvaluationState` is User Space/future vocabulary | `NEEDS_REVIEW` |
| **`Context`** | `ExecutionContext` is a frozen Kernel concept, but generic `Context` also names legacy Runtime cancellation/deadline propagation | `NEEDS_REVIEW` |
| **`Runtime`** | `risk: concept_collision`. Legacy Runtime executes mapped plans; K1 material uses "Kernel Runtime" for a **semantic boundary** whose runtime is **not implemented**. **Different contexts do not necessarily denote one concept** | `NEEDS_REVIEW` |
| **`Memory`** | **no** K0.5/K1 definition or Kernel object was found in the reviewed material | `NEEDS_REVIEW` |
| **`Membership`** | autonomy material discusses Runtime participation and membership, but the K0.5 object model defines **no** Membership object or frozen owner | `NEEDS_REVIEW` |

### Partially superseded historical terms

| Term | Status | Note |
|---|---|---|
| **`Intent`** (unqualified) | `FROZEN` (`superseded_umbrella_concept`) | retained only as a historical and compatibility umbrella. The current architecture distinguishes `UserIntent` / `KernelIntentSpec` / `KernelIntent` |

---

## 10. Concept status summary

(Grouped by `concepts.yaml` `owner` / `layer`; 35 concepts in total)

### Kernel objects (across the whole object chain)

| Status | Concepts |
|---|---|
| `FROZEN` | `Resource`, `CapabilityDeclaration`, `CapabilityInstance`, `CapabilityHandle`, `ExecutionContext`, `EventRecord`, `KernelStateTransition`, `ExecutionAuthorityScope` |
| `BOUNDED_IMPLEMENTED` | `ExecutionRequest`, `ExecutionDescriptor`, `ExecutionAllocation`, `ExecutionObservation` |
| `ACCEPTED` (`BOUNDED_IMPLEMENTED`) | `CurrentOccupancy`, `ResolutionEvidence`, `OccupancyResolutionAuthority`, `OwnershipFence` |

### External / boundary data / future mechanisms

| Status | Concepts |
|---|---|
| `FROZEN_SEMANTICS_ONLY` | `Task`, `UserIntent`, `KernelIntentSpec`, `KernelIntent` |
| `RESERVED` | `AutonomyScope`, `IntentScope`, `RoleScope`, `AutonomousLoop` |
| `DEFERRED` | `ProviderDeclaration`, `RegistrationRequest` |
| `NEEDS_REVIEW` | `Capability`, `Observation`, `State`, `Memory`, `Context`, `Membership`, `Runtime` |

> **Complete `definition` / `owner` / `sources` fields are authoritative in [`architecture/concepts.yaml`](../architecture/concepts.yaml).**

---

## 11. Suggested reading order

For someone meeting the Kernel object model for the first time:

| Order | Read | Purpose |
|---|---|---|
| 1 | **Sections 1–3 of this document** | establish "identity ≠ authority, observation ≠ state, Task outside the Kernel" and the state vocabularies |
| 2 | [`docs/kernel-userspace-boundary.md`](../docs/kernel-userspace-boundary.md) | the Kernel / User Space division of responsibility |
| 3 | [`architecture/boundaries.yaml`](../architecture/boundaries.yaml) | the 10 layers' current responsibilities and what they explicitly do **not** own |
| 4 | **Sections 2 and 4 of this document, revisited** | object chain + Scope domains |
| 5 | [`architecture/invariants.yaml`](../architecture/invariants.yaml) | 48 invariants; filter `CRITICAL` by `severity` |
| 6 | [`docs/execution-contract.md`](../docs/execution-contract.md) | the full contract for execution ownership and same-fence resolution |
| 7 | [`docs/task-model.md`](../docs/task-model.md) | Root/Child Task ownership and lineage |

> Readers who only want the system's positioning can stop at the repository root `README` and `overview` in the same directory; they **need not** enter this document.

---

<sub>This document is a concept-map draft, intended for use alongside [`architecture/concepts.yaml`](../architecture/concepts.yaml). Where it conflicts with the authoritative YAML records, the YAML prevails. The object chain and state vocabularies are taken from `concepts.yaml`, `invariants.yaml`, `boundaries.yaml` and the four contract documents in `docs/`; the `kernel_object: false` markers are taken from the corresponding `concepts.yaml` entries.</sub>
