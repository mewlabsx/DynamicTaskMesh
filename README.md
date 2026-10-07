# Dynamic Task Mesh (DTM)

> **An experimental, task-oriented Resource orchestration project.**
> It does not try to be a "task runtime" — quite the opposite: it separates **mechanism from business policy** completely. The Kernel only governs execution correctness and authority boundaries; business semantics, planning and scheduling policy all stay in User Space.
>
> | Axis | Version |
> |---|---|
> | **Software line** | `v0.7` (Autonomous Kernel) |
> | **Kernel specification** | `v0.1` |
>
> **The two axes evolve independently** (`D-KERNEL-VERSION-AXES`) — a v0.7 software line does not mean Kernel v0.7.

> ### ⚠️ One rule to know before reading this repository
>
> **Authority ≠ evidence, and a newer timestamp does not override a frozen decision.**
>
> ```text
> Authority (the only control surface)
>   architecture/{baseline,boundaries,concepts,decisions,invariants}.yaml
>   + architecture/reviews/kernel-semantic-freeze-governance-signoff-2026-09-06.md
>         │
>         │  A later decision may override within its scope ONLY if it is
>         │  [explicitly referenced by Architecture State, declares its scope,
>         │  carries an explicit supersedes/freeze-break relation, and has a
>         │  completed Boundary Delta + architecture review + governance approval]
>         ▼
> Evidence (cannot override authority)
>   Historical design documents · milestone drafts · audit findings · untracked implementations
> ```
>
> **Three rules for reading**:
> 1. **A newer timestamp does not override a frozen decision** — an ordinary document, a passing test or a newer date **cannot** replace frozen semantics (`D-R0-SOURCE-AUTHORITY`)
> 2. **A historical PASS is not current validation** — existing PASS records describe their own source version, environment and bounded tests
> 3. **Do not widen implementation claims from historical design or passing tests**

---

## Quick navigation

| I want to… | Go to |
|---|---|
| See it run | [Quickstart](#quickstart-run-the-core--agent-reference-path) |
| Understand the Kernel / User Space split | [Kernel and User Space](#kernel-and-user-space) |
| **Understand how the two lines will be merged (a primary next step)** | [Cross-profile constraints](#cross-profile-constraints-why-this-can-be-written-now) |
| Know what is really implemented and what is only design | [capability status](#implementation-boundaries) |
| Look up authoritative architecture definitions | [`architecture/`](architecture/README.md) — the five YAML files are the **only control surface** |

---

## What problem this project solves

DTM is concerned with: **how one execution is safely authorized, allocated, observed and released.**

In an environment where devices and resources are dispersed and capabilities come and go, the genuinely hard part is never "how to call a function". It is:

| Problem | How DTM handles it |
|---|---|
| How does a caller prove it is **entitled** to cause this side effect? | `CapabilityHandle` is the only authority reference; `CapabilityInstance` and Resource IDs are **not authority in themselves** |
| Who ensures an action that may already have produced a physical side effect is not blindly retried? | `UNKNOWN` must stay explicit — **never converted to FAILED, never an implicit retry signal** |
| A side effect may have happened but the outcome is unknown; is the resource still occupied? | `CurrentOccupancy` has three values (`NOT_ESTABLISHED` / `UNKNOWN` / `ENDED`); under `UNKNOWN` the **exact claim is retained and it fails closed** |
| Authorization passed — who accounts for Resource capacity? | The Resource owns execution capacity; the **live exact claim set is the sole accounting source** |
| Does an observed phenomenon count as "authoritative state"? | Observation ≠ state ≠ audit fact. They are separate, and only an admitted transition may mutate state |
| Where do business semantics (intent, planning, retry policy) belong? | **All in User Space.** The Kernel does not touch them |

> In one sentence: DTM's complexity guards three real boundaries — **authority correctness, execution occupancy consistency, and audit immutability** — not "because distributed systems usually work this way".

---

## Three layers (do not conflate them)

The repository contains **evolutionary layers**, and their capabilities **must not be combined into an unverified single production system** (defined explicitly in `docs/overview.md`). Ordered by "can this be used in production":

```text
┌─────────────────────────────────────────────────────────────────────┐
│ ① Existing execution reference (Core / Agent)     ← real processes, │
│    dtm-submit → Core → Planner → Mapper → Runtime → gRPC Agent      │
│    dtm-query  → Core query services → SQLite Task/Step/Execution     │
│    Capabilities: capability/resource discovery, node ownership,      │
│                  lease and fencing, submission dedup, conservative   │
│                  recovery, append-only migration                     │
│    ⚠️ This path does NOT yet run through Kernel / User Space         │
├─────────────────────────────────────────────────────────────────────┤
│ ② Bounded Kernel / User Space              ← the direction, partial  │
│    in-process logical facade + bounded User Space orchestration      │
│    Boundary: see Section 3                                           │
├─────────────────────────────────────────────────────────────────────┤
│ ③ Compatibility references (no production claim)                     │
│    Level 1 mesh (discovery / handshake / membership / coordinator)   │
│    Resource Invocation (transport-independent; native gRPC frozen    │
│      at M2-R1; Binary transport explicitly excluded)                 │
└─────────────────────────────────────────────────────────────────────┘
```

**Why the layers must be stated separately**: describing ①②③ as one combined capability set is equivalent to claiming a system that has never been verified. This is the first discipline of this project's documentation.

**And actually merging ①② into a single execution path is one of the primary next steps** — see [Cross-profile constraints](#cross-profile-constraints-why-this-can-be-written-now).

> ⚠️ The Core/Agent CLI and the Kernel/User Space implementation are **currently separate**; this preview does **not** claim an integrated autonomous runtime.

---

## Kernel and User Space

This is the core of DTM's current architecture. Authoritative definitions live in [`docs/kernel-userspace-boundary.md`](docs/kernel-userspace-boundary.md) and [`architecture/boundaries.yaml`](architecture/boundaries.yaml).

### Division of responsibility

**The Kernel owns mechanism, authority, lifecycle correctness and execution correctness** (`boundaries.yaml` → `layer: Kernel`, status `FROZEN`):

```text
The Kernel owns:
  · Resource / CapabilityDeclaration / CapabilityInstance / CapabilityHandle semantics
  · ExecutionContext and EventRecord semantics
  · Authority, lifecycle, availability, relationship, Handle, Scope and
    fail-closed structural boundaries
  · Execution capacity ownership: ExecutionRequest → Resource-owned allocation
    → frozen binding → a single dispatch claim → orthogonal observation → release
  · Same-fence occupancy resolution: CurrentOccupancy / Resource-issued resolution
    authority / immutable OwnershipFence snapshot / exact claim release /
    resolution EventRecord
  · Immutable audit facts (EventManager)

The Kernel explicitly does not own:
  · A User Space ABI, transport, authentication service, a complete authorization
    evaluator, persistence, or remote trust
  · Task scheduling, Planner/workflow interpretation, business policy, or
    device/business logic
  · Task creation, decomposition, lifecycle, retry policy, result aggregation,
    completion evaluation, root-task satisfaction evaluation, child-task planning
  · KernelIntent admission runtime, evaluation engine, reconciliation
  · Cross-fence or retrospective execution resolution, fence migration,
    crash reconstruction
```

**User Space owns business interpretation and policy** (`layer: Application/UserSpace`, status `FROZEN`):

```text
User Space owns:
  · UserIntent, Task creation, Task decomposition, Task lifecycle
  · Requirements, application state, workflow interpretation, business policy,
    task-level behavior
  · Intent Formulation: interpretation, normalization, translation into KernelIntentSpec
  · Satisfaction Planning: selecting Tasks, capabilities, sequencing, scheduling,
    reconciliation
  · The external Task/Policy and execution-lineage contract
  · Read-only observation of EventRecord; Kernel capability authority only through
    the approved boundary

User Space does not own:
  · KernelIntent object authority
  · Publication or rewriting of EventRecord
  · Implicit Handle creation from UserIntent / KernelIntent
```

### The most counter-intuitive rule: **Task is not a Kernel Object**

This is where DTM differs most from a "general task runtime" (`INV-TASK-EXTERNAL-POLICY-MANAGED`, `CRITICAL`):

> **Task is owned by User Space / Policy Layer. It is not a Kernel Object, not a Kernel-managed lifecycle, not a Kernel scheduler entity. The Kernel does not create, decompose, schedule, retry, aggregate, complete or evaluate Tasks.**
> `RootTaskRef` and `ChildTaskRef` are **opaque lineage references only**.

The accompanying lineage cardinality rule (an external Task/Policy contract, not a Kernel Object rule):

> **One Child Task maps to at most one concrete Capability execution request, at most one ExecutionRequest, and at most one Capability invocation.**

So the accurate description is: **DTM is a Resource/execution orchestration Kernel, not a task engine.** Task semantics live outside it.

### Intent also has two layers

(`D-R0.1-INTENT-DUAL-LAYER` + `D-R0.1-INTENT-PERSISTENT-REPRESENTATION`)

```text
UserIntent           ← User Space-owned: application/domain/human/agent intention,
                        may be ambiguous, may carry policy
      │ Intent Formulation (a User Space responsibility: interpret, normalize, translate)
      ▼
KernelIntentSpec     ← Boundary data: a normalized desired state submitted to a
                        future Kernel admission boundary
                        Business semantics are OPAQUE to the Kernel
                        Not a Kernel Object, authority reference, Task or plan
      ▼
KernelIntent         ← A future admitted persistent Kernel-managed representation
                          Kernel owns: stable identity, persistence/lifecycle,
                                       revision, ownership/scope, structural admission
                                       and authority semantics
                          Kernel does not own: business-semantic interpretation,
                                       satisfaction evaluation, World Model,
                                       reconciliation, Satisfaction Planning,
                                       Task generation, capability execution authority
```

**Current status**: `FROZEN_SEMANTICS_ONLY` / `NOT_IMPLEMENTED`. `ObjectKindIntent` remains a reserved vocabulary marker and is **currently rejected by Kernel ObjectReference validation**.
> `NOT_IMPLEMENTED` ≠ `ARCHITECTURALLY_FORBIDDEN` (`INV-DEFERRED-NOT-NEGATED`).

### How the boundary is enforced

Key invariants (all `CRITICAL`):

| ID | Rule |
|---|---|
| `INV-KERNEL-USERSPACE-BOUNDARY` | The Kernel/User Space boundary is **semantic**; **internal Kernel APIs are not automatically a User Space ABI**; the frozen K1 semantic contract is distinct from any wire/transport/language ABI |
| `INV-CAPABILITY-AUTHORITY` | CapabilityInstance and Resource IDs are **not authority**; capability use requires a valid `CapabilityHandle` |
| `INV-HANDLE-CONTEXT-SCOPE` | A Handle is bound to one ExecutionContext; `Scope` must be **strictly equal** or it fails closed; it does **not** imply equality in other Scope domains |
| `INV-EVENT-IMMUTABLE-SEMANTIC` | EventRecord is an immutable Kernel fact, not current state; User Space or a provider observation **cannot publish or rewrite** it |
| `INV-OBSERVATION-EVIDENCE-SEPARATION` | A physical/external observation is **untrusted or bounded evidence** until admitted; it is not an authoritative state transition or an EventRecord by itself |
| `INV-FAIL-CLOSED-AUTHORITY` | Invalid/revoked/expired/destroyed Handles **fail closed**; side-effecting execution requires **execution-side revalidation** |
| `INV-UNKNOWN-NO-BLIND-RETRY` | An execution that may already have produced a physical side effect must **not be blindly retried** when its result is unknown |
| `INV-SCOPE-DOMAIN-SEPARATION` | `ExecutionAuthorityScope` is the only current Handle/Context Scope domain; `AutonomyScope` / `IntentScope` / `RoleScope` / Mesh-Region meanings stay separate and **do not imply** containment, inheritance, equality or delegation |

**How the boundary maps onto code**:

```text
internal/kernel/           ← Kernel: identity / resource / capability / execution / event
                              + kernel.go (public facade)
                              + execution_ownership.go (Gate B Phase 1)
                              + logical_execution_facade.go (bounded logical facade)
                              + local_runtime.go (bounded World A→B admission bridge)

internal/userspace/        ← User Space: a narrow value-oriented port
                              + orchestrator.go (bounded synchronous execution owner)
                              + roottask/     (Root identity, finite Child membership,
                                               deterministic closure predicate)
                              + taskruntime/  (Child lifecycle, one-shot submission boundary)
                              + taskcreation/ (narrow materialization boundary for
                                               already-decided requests)
```

**There is exactly one crossing point** (`internal/userspace/port.go`):

```go
// ExecutionKernelPort is the complete Kernel dependency of the bounded
// execution orchestrator. Both operations return value snapshots. The port
// deliberately does not expose allocation, Resource, Provider, or occupancy
// mutation primitives.
type ExecutionKernelPort interface {
    RequestExecution(kernel.LogicalExecutionRequest) (kernel.LogicalExecutionResult, error)
    ObserveInvocation(kernel.InvocationID) (kernel.LogicalInvocationObservation, error)
}
```

> Note: the User Space package **does import `dtm/internal/kernel`**, but depends only on this two-method interface and receives value snapshots. It is an **intentional in-process composition boundary, not a physical ABI**.

---

## Cross-profile constraints (why this can be written now)

> **One of the primary next steps is to consolidate and merge the two development lines into a single primary execution path.**
> Authoritative statements: [`docs/integration.md`](docs/integration.md) and [`docs/recovery-idempotency.md`](docs/recovery-idempotency.md).

### How the two lines currently relate

```text
① Core / Agent reference profile (persisted)
   Task / Step / Execution records · submission keys · Resource records → SQLite
   DB state (not an optimistic memory view) is authoritative for that profile
   Registration identity/generation · Owner Lease · Endpoint validation remain required
   Recovery is conservative: never replay an uncertain effect merely because an Agent
   vanished, a Lease expired, a response was lost or a record is incomplete
                    │
                    │  ⚠️ The two lines are currently NOT integrated
                    │     Linking packages or retaining both logs does not count
                    ▼
② Bounded Kernel / User Space (in-process)
   allocation · occupancy · invocation binding · trusted resolution → bounded local memory
   ★ Does NOT inherit ①'s SQLite persistence, replay or crash-recovery guarantees
   A facade InvocationID is stable correlation WITHIN the admitted in-process scope,
   not a restart-global deduplication store
```

**This asymmetry is the core difficulty of the merge**: ① recovers reference state from SQLite, while ②'s Kernel authority and occupancy state **currently live in memory**.

### The target shape after merging

> **One primary execution path**:

```text
explicit task input
  → User Space task/execution orchestration
    → Kernel logical facade
      → a Provider adapter
        → existing Agent execution
```

**Principle**: existing CLI, transport and query functionality **should be reused wherever their contracts remain valid**, rather than rewritten.

### Six things that must be defined before implementation

(`docs/integration.md`)

| # | Must be defined |
|---|---|
| 1 | Stable **task / work / invocation / allocation / attempt bindings** |
| 2 | **registry-to-Kernel authority mappings** |
| 3 | Provider **outcome** and **occupancy** evidence |
| 4 | A **single execution owner** |
| 5 | **Restart behavior** (must be addressed explicitly, not avoided) |
| 6 | Explicit decisions on **persistence authority, effect identity, ownership/lease/fence mapping and crash boundaries** |

**The two easiest traps**:

> ⚠️ **A discovery record is not execution authority.**
> ⚠️ **An old registration generation does not automatically become a Kernel ownership fence.**

**On the single execution owner**:

> Existing retries, remapping and recovery **must not also control** work owned by the new path.
> Otherwise two schedulers exist side by side — which is exactly the "parallel feature-development track" this is meant to remove.

### The boundary of the first bounded integrated profile

What is permitted:

> The first bounded profile **may use durable isolation plus fail-closed recovery** instead of complete Kernel persistence.

**But it must never**:

| Forbidden | Reason |
|---|---|
| Silently redispatch **unresolved** work | Unknown outcome ≠ safe to retry |
| Reconstruct an **empty Resource as free** after restart | Empty ≠ free |
| Let a new fence implicitly terminate an older Unknown execution | `INV-OWNERSHIP-FENCE-NOT-TERMINATION-PROOF` |
| Treat `ObserveInvocation NOT_KNOWN` as safe replay evidence | It has **no authoritative crossing fact** |

### Acceptance criteria (real execution, not unit tests)

The first accepted integrated profile **must demonstrate all of**:

| # | Must demonstrate |
|---|---|
| 1 | **Real** command → Agent execution |
| 2 | Authorization rejection **without side effects** |
| 3 | Duplicate-request protection |
| 4 | `UNKNOWN` **visible**, with **no** implicit retry or release |
| 5 | Safe behavior at crash/restart boundaries |
| 6 | **Existing reference scenarios continue to pass** |

> **Successful normal execution alone is insufficient.**

### When the default path can actually switch

Conditions for switching to the new default path (all of them):

- Public primary scenarios, query/idempotency behavior and the stated recovery guarantees are **all covered**
- **No old scheduler bypass remains**
- Independent review has **no blocking findings**

Only then can the old execution-ownership logic retire; reusable compatibility adapters may remain.

### Development discipline from now on

| Item | Rule |
|---|---|
| **New execution features** | Should target the **Kernel / User Space boundary** |
| **The old profile** | During migration, limited to **necessary compatibility and correctness work** |
| **Purpose** | Avoid two parallel feature-development tracks |
| Not prerequisites | Full autonomous operation, Intent runtime and broader admission are **not** prerequisites for the first bounded profile |

> `docs/integration.md` **proposes direction only**: it does **not authorize implementation** and does **not change frozen milestone statuses**.

---

## Three-profile layering quick reference

Three evolutionary layers coexist in the same codebase. **Their profile names, command entry points, persistence and authority sources all differ** — mixing them up is the easiest mistake to make.

| Profile | Command entry | Persistence | Authority source | Status |
|---|---|---|---|---|
| **① Reference profile** | `dtm-core` + `dtm-agent` + `dtm-submit` + `dtm-query` | **SQLite** (append-only migration) | **DB state**, not an optimistic memory view | existing implementation and tests |
| **② Bounded Kernel / User Space** | in-process composition (`internal/kernel` + `internal/userspace`) | **bounded local memory** | Kernel objects and invariants | bounded implementation |
| **③ Compatibility references** | `dtm-runtime` (mesh), `dtm-ui` (read-only observation) | — | Level 1 mesh / v0.6 invocation foundation | compatibility reference, no production claim |

**Three boundaries that cannot be crossed**:

| # | Rule |
|---|---|
| 1 | ① and ② **are not** the same execution path; ② does **not inherit** ①'s persistence, replay or crash-recovery guarantees |
| 2 | ②'s facade InvocationID is stable only **within the admitted in-process scope**; it is **not** a restart-global deduplication store |
| 3 | ③'s discovery records and Mesh Resource View are **not** execution authority (`INV-RESOURCE-ADMISSION-BEFORE-AUTHORITY`) |

> **Describing the three lines as one combined capability set is equivalent to claiming a system that has never been verified** — this is the concrete unfolding of the discipline in Section 2.
> The merge plan for ① and ② is in [Cross-profile constraints](#cross-profile-constraints-why-this-can-be-written-now).

**② has only three occupancy conclusions, and `UNKNOWN` is the conservative side**:

| Conclusion | allocation | exact claim | Meaning |
|---|---|---|---|
| `NOT_ESTABLISHED` | RUNNING | HELD | The Provider boundary has **not** been crossed |
| `UNKNOWN` | RUNNING | **still HELD** | Boundary crossed, **cannot exclude continued occupancy** |
| `ENDED` | RELEASED | NONE | Stable terminal state |

> `UNKNOWN` is **neither** "not started" **nor** failure or timeout — it retains occupancy pending explicitly authorized resolution. Mechanism details: [`docs/execution-contract.md`](docs/execution-contract.md).

---

## Quickstart: run the Core / Agent reference path

> This path does **not** invoke the new Kernel Goal Loop and does not involve real equipment (`docs/quickstart.md`).
> It uses the **reference profile** and does **not yet** run through Kernel / User Space — the merge plan is in [Cross-profile constraints](#cross-profile-constraints-why-this-can-be-written-now).

### Build

Go 1.24 or a compatible toolchain is required. Protobuf generated files are checked in, so **an ordinary build does not need protoc**.

```powershell
# Windows PowerShell
New-Item -ItemType Directory -Force bin | Out-Null
go build -o ./bin/ ./cmd/...
```

```sh
# POSIX shell
mkdir -p bin
go build -o ./bin/ ./cmd/...
```

### Start three services (three terminals)

```powershell
# Terminal 1: Core
./bin/dtm-core.exe -config ./configs/demo/core.yaml

# Terminal 2: temperature Agent
./bin/dtm-agent.exe -config ./configs/demo/sensor-agent.yaml

# Terminal 3: cooling Agent
./bin/dtm-agent.exe -config ./configs/demo/cooling-agent-001.yaml
```

Wait for `dtm-core listening` and each Agent's `register success`.

| Service | Address |
|---|---|
| Core | `127.0.0.1:50051` |
| temperature Agent | `:50061` |
| cooling Agent | `:50062` (`cooling-agent-002.yaml` uses `:50063`) |

Database paths resolve relative to the configuration file directory, placing demo state under `data/` at the candidate root.

### Submit and query (a fourth terminal)

```powershell
./bin/dtm-submit.exe -core 127.0.0.1:50051 -target-temperature 26

./bin/dtm-query.exe get        --task-id <returned-task-id> --core-address 127.0.0.1:50051
./bin/dtm-query.exe executions --task-id <returned-task-id> --core-address 127.0.0.1:50051
./bin/dtm-query.exe list --limit 10 --core-address 127.0.0.1:50051
```

Expected: a `succeeded` task containing `sensor-node-001` and `cooling-node-001` results; the simulated sensor reports temperature 30 and the cooling result includes `cooling_started`.

> ⚠️ This is **execution success**, not measured physical cooling, and it does not prove the new Root Task goal predicate.

### Verification

```powershell
go test -count=1 -timeout=15m ./...
go build ./cmd/...
go vet ./...
```

The `Makefile` additionally provides `make test` / `make build` / `make verify`, plus six fuzz targets (`make fuzz`, `FUZZTIME` defaults to 5s): `FuzzDecodePresence`, `FuzzHandshakeRequest`, `FuzzObserveAndTick`, `FuzzResourceAdvertisement`, `FuzzNewTask`, `FuzzNativeInvocationRequest`.

### Stop and start over

Ctrl+C in each terminal. For a fresh demonstration: **stop all processes first**, then move the `data/` directory to a backup location.

> **Do not remove a live database, and do not assume a restart will automatically retry uncertain side effects.**

---

## Implementation boundaries

(`docs/status.md` + `architecture/baseline.yaml`)

### Implemented (bounded)

| Area | Status | Boundary |
|---|---|---|
| Core/Agent task execution | existing reference implementation and tests | simulated demo; **legacy execution profile** |
| SQLite state, queries and recovery | existing reference profile | **does not imply new Kernel object persistence** |
| Submission deduplication and execution protection | layer-specific implementations | **no blanket Exactly-once guarantee** |
| Resource identity, directory and mapping | v0.4 reference foundation | Owner / Lease / registration / generation remain required |
| Discovery, membership and coordinator | Level 1 mesh reference | no multi-Core HA, no cross-domain autonomy claim |
| Resource Invocation | native gRPC foundation, frozen at M2-R1 | **Binary transport excluded** |
| Kernel objects and invariants | bounded implementation | **the internal manager API is not a User Space ABI** |
| Execution ownership | Gate B Phase 1 bounded closure | Resource capacity, immutable binding, controlled dispatch |
| Occupancy resolution | Gate B Phase 2 bounded closure | **same-fence only**; cross-fence reserved/deferred |
| Logical execution facade | accepted bounded in-process implementation | **no persistence, no remote invocation, no physical ABI** |
| User Space orchestrator + Child/Root runtime | bounded implementations | explicit work and closure; **cancellation is not ended** |
| Task creation (US-3) | bounded implementation | **creation/registration only**, not automatic planning or execution |
| Goal Loop and local Workbench | separate local experimental review bundle | final acceptance pending; **omitted from this candidate** |

### Governance status (`baseline.yaml`)

| Boundary | Recorded value | Limit |
|---|---|---|
| K2-C / K2-D / K2-E | `BOUNDED_IMPLEMENTED` / `CLOSED` / `CLOSED` | local mechanisms and bounded convergence only |
| Gate B Phase 1 | `CLOSED`, `BOUNDED_IMPLEMENTED` | Resource execution ownership only |
| Gate B Phase 2 | `CLOSED`, design `ACCEPTED`, review `PASS`, implementation `BOUNDED_IMPLEMENTED` | **same-fence local occupancy resolution only; Full Gate B not closed** |
| Pre-freeze logical facade | design `ACCEPTED`/`PASS`; implementation `BOUNDED_IMPLEMENTED`; acceptance `ACCEPTED`; verification `PASS` | `internal_manager_dependency: NO`; **`milestone_closed: NO`** |
| Semantic governance | `FROZEN` | the reviewed semantic envelope only, not new runtime behavior |
| Intent boundary | `FROZEN_SEMANTICS_ONLY` / `NOT_IMPLEMENTED` | persistent opaque KernelIntent is prospective |
| R0 checkpoint | `FREEZE_CANDIDATE` | a carried-forward stage checkpoint, **not a final freeze** |
| **Full Gate B** | **`RESERVED`** | no autonomous-loop completion claim |
| Physical ABI / Scope governance | `DEFERRED` | not a transport or delegation implementation |
| Complete authorization / global cross-manager consistency / production admission | `OPEN` | existing local mechanisms do not close these boundaries |
| **Final Architecture Freeze** | **`NOT_CLAIMED`** | separate review/sign-off and a reproducible baseline required |

### Explicit non-claims

DTM currently does **not** claim: production readiness, final Architecture Freeze, KernelIntent runtime, production Resource Admission, new-Kernel persistence, remote ABI, multi-Core HA, Binary transport, unrestricted derivation, or functional safety.

> **An unknown side-effect outcome must not trigger an implicit retry.**

### Reserved future mechanisms (not forbidden)

| Mechanism | Status | Layer |
|---|---|---|
| CrossFenceResolution | `RESERVED` | future ownership / recovery / reconciliation |
| CrossManagerConsistency | `OPEN` | future Kernel operation boundary |
| MultiCore | `RESERVED` | future Kernel/Mesh topology |
| RemoteTrust | `DEFERRED` | future distributed security |
| Membership / RoleSelection / RoleMigration / Gateway | `RESERVED` | Mesh / governance / deployment |
| Discovery / AutonomyScopeHierarchy / IntentReconciliation | `DEFERRED` | Mesh / autonomy / User Space |

(`INV-DEFERRED-NOT-NEGATED`: `DEFERRED`/`RESERVED`/`OPEN`/`NEEDS_REVIEW` are **not** `FORBIDDEN` or `REMOVED`.)

---

## Architecture principles (`AP-01` … `AP-11`)

`architecture/baseline.yaml`'s `active_architecture_principles` records 11 effective principles (sourced from [`AGENTS.md`](AGENTS.md)). **Four of them determine this project's shape**:

| ID | Principle | Why it explains how DTM looks |
|---|---|---|
| **AP-01** | **Complexity must guard a real boundary** — a new mechanism must state which safety, correctness or consistency boundary it guards, what goes wrong without it, and whether a simpler deterministic alternative exists | Explains why DTM introduces no consensus, PKI or global directory |
| **AP-05** | **Prefer deterministic convergence over default consensus** | When identical input yields a unique result through a pure deterministic function, do not introduce consensus or timing dependence |
| **AP-08** | **Authorization Invariant** — every side effect must satisfy explicit authorization conditions that the executing side can verify; any failure fails closed, and the security burden cannot rest on the caller alone | Explains why `CapabilityHandle` is the only authority reference |
| **AP-09** | **An unknown execution result must not be blindly retried** — physical side effects default to `NON_IDEMPOTENT` | Explains why `UNKNOWN` does not release occupancy |

The remaining principles (AP-02 task-oriented, not device-oriented · AP-03 Resource is an autonomy boundary · AP-04 recursive autonomy · AP-06 identity separate from locator · AP-07 prefer local namespaces · AP-10 incremental compatibility · AP-11 explicit system boundaries) are in [`AGENTS.md`](AGENTS.md).

---

## Going deeper

Top-level depth is deliberately kept out of this document to avoid duplicating authoritative records. When needed:

| I want to look up… | Go to |
|---|---|
| **How the code is organized** (6 processes / 41 packages / contracts / migrations / config / common change sites) | [`docs/code-map.md`](docs/code-map.md) |
| **How concepts relate** (object chain / state vocabularies / Scope domains / terminology traps / reading order) | [`docs/concept-map.md`](docs/concept-map.md) |
| **Authoritative definitions of 35 concepts** | [`architecture/concepts.yaml`](architecture/concepts.yaml) |
| **48 invariants (with `severity`)** | [`architecture/invariants.yaml`](architecture/invariants.yaml) |
| **The 10 layers' responsibilities and explicit non-ownership** | [`architecture/boundaries.yaml`](architecture/boundaries.yaml) |
| **39 decisions** | [`architecture/decisions.yaml`](architecture/decisions.yaml) |
| **The full execution contract (including same-fence resolution)** | [`docs/execution-contract.md`](docs/execution-contract.md) |
| **Resource identity and authority** | [`docs/resource-authority.md`](docs/resource-authority.md) |
| **Item-by-item capability status** | [`docs/status.md`](docs/status.md) |

> `BUILD-INFO.json` records the **assembly-time** state of the 2026-10-03 candidate, so its
> `public_repository` / `license` / `public_release` fields still read `PENDING`. Root
> [`README.md`](README.md) and [`LICENSE`](LICENSE) state the current values.

---

## Documentation index

**Entry points** (bilingual, `docs/`):
[Quickstart](docs/quickstart.md) ·
[Architecture overview](docs/overview.md) ·
[Capability status](docs/status.md) ·
[Examples](docs/examples.md) ·
[Contracts](docs/contracts.md) ·
[Release scope](docs/release-readiness.md)

**Primary next step — merging the two lines**:
[Integration direction](docs/integration.md) ·
[Recovery and idempotency](docs/recovery-idempotency.md) ·
[Resource authority](docs/resource-authority.md) ·
[Execution contract](docs/execution-contract.md)

**Boundaries and contracts**:
[Kernel and User Space boundary](docs/kernel-userspace-boundary.md) ·
[Execution contract](docs/execution-contract.md) ·
[Resource authority](docs/resource-authority.md) ·
[Task model](docs/task-model.md) ·
[Recovery and idempotency](docs/recovery-idempotency.md)

**Evidence and governance**:
[Architecture State](architecture/README.md) ·
[Semantic freeze sign-off](architecture/reviews/kernel-semantic-freeze-governance-signoff-2026-09-06.md) ·
[Selected development history](docs/history.md) ·
[Candidate validation](docs/validation.md) ·
[Protocol generation](docs/protocol-generation.md)

---

## License and maintenance

Copyright (c) 2026 Zhao Tao (赵涛).

Unless otherwise identified, DTM's own code and documentation are licensed under the **GNU Affero General Public License, version 3 only (`AGPL-3.0-only`)**; see [`LICENSE`](LICENSE). Third-party components retain their respective licenses; see [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).

Commercial use and modification are permitted under the license. Distribution requires compliance with its corresponding-source obligations. **If you modify the program and users interact with your version remotely over a computer network, you must prominently offer those users an opportunity to obtain the corresponding source free of charge** (section 13). This does not require submitting changes to this repository or automatically publishing every private modification. The license text governs; the software is provided without warranty to the extent permitted by law.

This is a personally maintained project focused on sharing code and technical documentation. **There is no commitment to response times, long-term support or acceptance of pull requests.** Before distributing a modified network-enabled version, provide an appropriate source access mechanism.

---

<sub>This document is a README draft aimed at readers encountering the project for the first time, supplementing the scope statement in the root `README.md` with a conceptual and structural view. All factual statements are taken from the five YAML files in `architecture/`, the existing documents in `docs/`, and the local code structure; no capability that is not declared is described as implemented. Code-size and build/test results are locally reproduced records.</sub>


[简体中文](README.zh-CN.md)
