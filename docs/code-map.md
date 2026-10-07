# DTM Code Map

> **An index that starts from "I want to change something".**
> This document answers three questions: **which binary does what**, **which package is responsible for what**, and **where the contracts and migrations live**.
>
> All paths are relative to the repository root. Code-size figures are locally reproduced (see Section 7).
>
> ⚠️ **This document does not describe capability status.** A package existing **does not mean** the corresponding capability is implemented — check [`docs/status.md`](../docs/status.md) and [`architecture/baseline.yaml`](../architecture/baseline.yaml).

---

## 1. Six executable processes (`cmd/`)

| Process | Mode | Purpose | Key entry files |
|---|---|---|---|
| **`dtm-core`** | Core service | load core YAML → assemble SQLite (prints `storage_ready`) → listen on TCP and register Core/NodeRegistry gRPC services → lease expiry sweep → graceful shutdown | `main.go`, `composition.go`, `resource_candidate_readiness.go` |
| **`dtm-agent`** | `mode=static` | requires `core.address`. Registers/heartbeats with the Core and exposes the Agent execution gRPC service; includes RPC error classification (continue/retry/reregister/terminate/shutdown) and re-registration fencing | `main.go`, `composition.go` |
| **`dtm-submit`** | CLI | submit a task or query status. Includes Runtime bootstrap and protojson output | `main.go` |
| **`dtm-query`** | CLI | subcommands `get` / `list` / `executions` | `main.go`, `command.go`, `output.go` |
| **`dtm-runtime`** | `mode=runtime` | assembles ResourceExecutionCapability + UDP multicast discovery + membership/protocol/handshake, running the Level 1 mesh | `main.go` |
| **`dtm-ui`** | read-only Web UI | `-http` defaults to `127.0.0.1:46080`, includes Lab mode | `main.go` |

### Command-line flags at a glance

**`dtm-submit`**

```text
-core                <addr>      Core address
-runtime             <addr>      Runtime address (optional)
-target-temperature  <int>       target temperature, default 26
-task-id             <string>    explicit task ID
-async                           asynchronous submission
-idempotency-key     <string>    submission idempotency key
-status              <task-id>   query status
```

**`dtm-query`**

```text
-config         <path>          config file
-core-address   <addr>          Core address
-timeout        <duration>      timeout
-output         text|json       output format
-task-id        <id>            for get / executions
-status         <state>         list filter
-created-after / -created-before <RFC3339>
-limit          <int>           keyset pagination
-page-token     <token>
```

**`dtm-query` exit-code contract**

| Exit code | Meaning |
| ---: | --- |
| 0 | success |
| 1 | internal or unclassified error |
| 2 | argument, configuration, or `InvalidArgument` |
| 3 | Task not found |
| 4 | Core unavailable |
| 5 | RPC timeout |
| 6 | request cancelled |

**`dtm-ui`**

```text
-runtime <addr> (repeatable)
-runtime-seed <list>            comma-separated
-http                           defaults to 127.0.0.1:46080
-discovery-group / -discovery-port / -discovery-interface
-query-timeout
-enable-test-controls
-lab-manifest / -lab-timeout
```

---

## 2. `internal/` package responsibilities (41 packages with non-test code)

### 2.1 Kernel side

| Package | Responsibility |
|---|---|
| `internal/kernel` | public facade: type aliases aggregating identity/resource/capability/execution/event; `kernel.go` composition root; `execution_ownership.go` (Gate B Phase 1); `logical_execution_facade.go` (bounded logical facade); `local_runtime.go` (bounded World A→B admission bridge) |
| `internal/kernel/identity` | identity semantics: all ID types, `ObjectKind`/`ObjectReference`, `Lifecycle`/`Availability`/`Evaluation` state validity (`ObjectKindIntent` reserved but rejected by `ObjectReference` validation) |
| `internal/kernel/resource` | the `Resource` object and Manager: lifecycle/availability, execution capacity claims (`executionClaims`), `OwnershipFence`, resolution authority tokens |
| `internal/kernel/capability` | `CapabilityDeclaration` / `CapabilityInstance` / `CapabilityHandle` models and validation; Handle validation, Scope equality, fail-closed |
| `internal/kernel/execution` | `ExecutionContext` model and Manager; `ownership.go` defines `ExecutionRequest`/`Descriptor`/`Allocation`/`ExecutionObservation`/`CurrentOccupancy`/`ResolutionEvidence` |
| `internal/kernel/event` | immutable `EventRecord` model and Manager (append-only audit facts such as `Resource.Created`) |

### 2.2 User Space side

| Package | Responsibility |
|---|---|
| `internal/userspace` | the bounded User Space execution boundary: a narrow value-oriented Kernel port (`port.go`) + `Orchestrator` (USEO, the bounded synchronous execution owner) + WorkState |
| `internal/userspace/roottask` | the bounded Root Task aggregate: Root identity, finite Child membership, Root lifecycle, deterministic closure predicate |
| `internal/userspace/taskruntime` | bounded Child Task lifecycle and one-shot submission boundary (**no** scheduling/retry/persistence) |
| `internal/userspace/taskcreation` | a narrow materialization boundary above Root/Child: accepts already-decided requests and coordinates existing lifecycle owners; does **not** derive work, execute, or evaluate closure |

### 2.3 Reference profile (Core / Agent path)

| Package | Responsibility |
|---|---|
| `internal/transport/grpcapi` | gRPC transport adaptation: core_server, agent_server, registry_server, task_query_server, resource_invocation_server, remote_executor, endpoints, step/submission idempotency |
| `internal/platform/sqlite` | SQLite implementation: Database, embedded migrations, schema manifest/checksums, `persistence_*` repositories, resource/execution repository |
| `internal/storage` | storage ports and types: Database/Query/Persistence interfaces, `TaskFilter`/`TaskPage`/`RecoveryCursor`, error classification, diagnostics and startup-failure output |
| `internal/application` | application service layer: `TaskService`, `AsyncTaskService`, `TaskQueryService`, `RecoveryController`, `TaskRepository`, resource evidence |
| `internal/resourcedirectory` | Resource directory: `ResourceRecordView`, Node/registration/generation fence, publication/quiesce state, deterministic `TransitionPlan` |
| `internal/mapper` | Planner plan → Resource mapping: `ResourceCandidateQuery`/Source, Resource Scheduling Map, mapping validation, recovery re-resolution, legacy capability mapping |
| `internal/planner` | the reference profile's intent → step Planner (**single intent**: `cool_environment`) |
| `internal/runtime` | legacy DTM Runtime: executes mapped plans, retry/remap policy, cancellation and deadline propagation |
| `internal/execution` | legacy execution results and `StepResult`, `ExecutionFenceMode` evidence enum |
| `internal/lifecycle` | Task and Step lifecycle state machines and legal transitions |
| `internal/task` | Task value object (ID/Intent/Requirements/Constraints) and validation |
| `internal/node` | Node value object and state transition validation (registered/active/suspect/offline/recovering/stale) |
| `internal/lease` | Node Lease manager (TTL, expiry sweep, stale registration detection) |
| `internal/nodelifecycle` | node lifecycle controller: coordinates registry/endpoints/lease, rejects stale registration |
| `internal/capability` | the reference profile's capability catalog (`temperature_sensor`, `cooling_control`) and Node Registry |
| `internal/agent` | Agent-side Capability Handler routing (Router, duplicate capability detection) |
| `internal/agentexecution` | Agent execution record repository and request fingerprint (idempotency-key reuse conflict detection) |
| `internal/authoritybinding` | NodeRegistry registration + heartbeat session adaptation shared by Runtime and static Agent (registrationID generation, CallTimeout) |
| `internal/model` | domain base types: `TaskID`/`NodeID`/`StepID`/`ExecutionID`/`Capability`, the Resource ID family, `IdempotencyMode`, legacy capability-resource mapping |

### 2.4 Mesh and Resource Invocation

| Package | Responsibility |
|---|---|
| `internal/mesh/discovery` | multicast/UDP peer discovery (windows/unix platform-specific sockets), handshake confirmation and identity mismatch detection |
| `internal/mesh/handshake` | gRPC handshake between Runtimes, including rejection reasons, timeout and fuzz tests |
| `internal/mesh/membership` | Mesh member active/suspect/expired state machine and snapshots (**not** a Kernel Object) |
| `internal/mesh/protocol` | Mesh wire protocol values: Identity (MeshNamespace/ProtocolMajor/RuntimeInstance/ControlEndpoint), `DTMVersion`, presence encoding |
| `internal/mesh/coordinator` | deterministically selects a single coordinator session from a membership snapshot (NodeID ordering; conflicts fail closed) |
| `internal/mesh/resourceview` | Mesh Resource View projection: authority-free Descriptor, bound constraints, owner/namespace validation |
| `internal/mesh/resourcesync` | synchronizer pulling Resource Advertisements from owners (bounded concurrency/queue) |
| `internal/invocation` | Resource Invocation service: target resolution (double validation, fail-closed), native gRPC and legacy/compatibility transport, executor, domain model, fence evidence |
| `internal/runtimehost` | process assembly layer: `CoreCapability`, `ResourceExecutionCapability`, `MeshOptions`, ResourceEvidence writer |

### 2.5 Other

| Package | Responsibility |
|---|---|
| `internal/ui` | read-only observation UI: Observer snapshots, HTTP handler + embedded web assets, Timeline, LabMode/LabHarness |
| `internal/config` | YAML configuration loading and defaults (Core/Agent/Runtime, lease/heartbeat/retry/storage/mesh/multicast) |
| `internal/demo` | simulated capability handlers: temperature sensing and cooling control |

### 2.6 Packages that are easy to look for in the wrong place

| You might expect | Actual location |
|---|---|
| `internal/resource` | ❌ does not exist → `internal/kernel/resource` |
| `internal/event` | ❌ does not exist → `internal/kernel/event` |
| `internal/execution` | ⚠️ **there are two**: `internal/execution` (legacy results/fence) and `internal/kernel/execution` (Kernel object chain) |
| `internal/capability` | ⚠️ **there are two**: `internal/capability` (reference profile catalog/registry) and `internal/kernel/capability` (Kernel object chain) |
| `internal/mesh` (as a package itself) | ❌ does not exist; only its 7 subpackages |
| `internal/facade` | ❌ does not exist → `internal/kernel/logical_execution_facade.go` (`package kernel`) |
| `internal/child` | ❌ does not exist → `internal/userspace/taskruntime` |
| `internal/root` | ❌ does not exist → `internal/userspace/roottask` |
| `internal/scheduler`, `internal/intent`, `internal/eventstore`, `internal/observability` | ❌ none of these exist |

---

## 3. Contracts and protocols

### 3.1 `api/proto/dtm/v1/`

| File | Contents |
|---|---|
| `dtm.proto` | **4 services**: `CoreService` (SubmitTask / GetTaskStatus / GetTask / ListTasks / GetTaskExecutions), `NodeRegistryService` (RegisterNode / Heartbeat / UpdateNodeStatus), `AgentExecutionService` (ExecuteStep), `RuntimeControlService` (Handshake / GetResourceAdvertisement / GetRuntimeStatus). About 32 messages |
| `invocation.proto` | `ResourceInvocationService.InvokeResource` + `ResourceRef`/`InvocationMetadata`/`Request`/`Result`/`Error`/`Response` |
| `*.pb.go`, `*_grpc.pb.go` | **generated code is checked in**; an ordinary build **does not need protoc** |
| `*_proto_test.go` | proto contract tests |

**Regeneration**: see [`docs/protocol-generation.md`](../docs/protocol-generation.md) and `scripts/generate-proto.ps1` (toolchain versions protoc 30.2 / protoc-gen-go v1.36.6 / protoc-gen-go-grpc 1.5.1).

### 3.2 Migrations (append-only)

```text
internal/platform/sqlite/migrations/core/
  001_v026_core_schema
  002_m2_persistence_access
  003_m3_node_lifecycle
  004_m4c_step_recovery_qualification
  005_m5_task_submission_idempotency
  006_v04_resource_foundation

internal/platform/sqlite/migrations/agent/
  001_v025_agent_execution_schema
```

> ⚠️ **Migrations are append-only.** Schema drift and a migration version/name/checksum mismatch are rejected **before any write** with `SchemaMismatch` (exit code 12).
> **Do not edit migration history to bypass an error.**

---

## 4. Configuration

### 4.1 File layout

| Location | Count | Purpose |
|---|---|---|
| `configs/core.yaml`, `configs/agent.yaml` | 2 | root examples (single-machine Core + sensor node) |
| `configs/demo/` | 5 | `core.yaml`, `sensor-agent.yaml`, `cooling-agent.yaml`, `cooling-agent-001.yaml`, `cooling-agent-002.yaml` |
| `configs/lab/` | 3 | used by `dtm-ui` Lab Mode |

### 4.2 Key defaults

**`configs/core.yaml`**

```text
server.address            127.0.0.1:50051
lease.ttl                 10s
lease.sweep_interval      1s
storage.sqlite.path       ../data/dtm.db     (resolved relative to the config file directory)
storage.sqlite.journal    WAL
storage.sqlite.synchronous NORMAL
storage.sqlite.busy_timeout 5s
storage.sqlite.auto_migrate true
retry.max_attempts        3
retry.backoff             100ms
```

**`configs/agent.yaml`**

```text
node.id                   sensor-node-001
node capabilities         temperature_sensor
server.listen_address     127.0.0.1:50061
node.advertise_address    127.0.0.1:50061
core.address              127.0.0.1:50051
heartbeat.interval        2s
storage.sqlite.path       ../data/sensor-node-001.db
```

### 4.3 Demo port assignment

| Service | Address |
|---|---|
| Core | `127.0.0.1:50051` |
| temperature Agent | `127.0.0.1:50061` |
| cooling Agent 001 | `127.0.0.1:50062` |
| cooling Agent 002 | `127.0.0.1:50063` |

> Since V0.2.6 the standard fields are `server.listen_address` and `node.advertise_address`; the older fields remain readable for compatibility.

---

## 5. Build, verification and tooling

### 5.1 `Makefile`

| target | command |
|---|---|
| `make test` | `go test ./...` |
| `make build` | `go build ./cmd/...` |
| `make verify` | `go test ./...` + `go build ./cmd/...` + `go vet ./...` + `$(MAKE) fuzz` |
| `make fuzz` | six fuzz targets, each `-fuzztime=$(FUZZTIME)` (default `5s`) |

### 5.2 Fuzz targets

| Target | Location |
|---|---|
| `FuzzDecodePresence` | `internal/mesh/protocol` |
| `FuzzHandshakeRequest` | `internal/mesh/handshake` |
| `FuzzObserveAndTick` | `internal/mesh/membership` |
| `FuzzResourceAdvertisement` | `internal/mesh/resourceview` |
| `FuzzNewTask` | `internal/task` |
| `FuzzNativeInvocationRequest` | `internal/transport/grpcapi` |

### 5.3 Recommended verification commands

```powershell
go build ./...
go vet ./...
go test -count=1 -timeout=15m ./...
```

> **Go build cache**: if the default cache directory is not writable, point it inside the workspace:
> ```powershell
> $env:GOCACHE = "<repo>\..\.gocache"
> $env:GOMODCACHE = "<repo>\..\.gomodcache"
> ```

### 5.4 Where the test assets live

```text
tests/integration/                end-to-end integration tests (cool_environment_test.go, grpc_demo_test.go)
tests/DTM_v0.2.5_完整可靠动态网络测试方案.md
internal/**/invariant_test.go     invariant regression (present in every Kernel-side package)
internal/kernel/freeze_invariants_test.go
internal/kernel/public_contracts_test.go
internal/kernel/architecture_invariant_test.go
api/proto/dtm/v1/*_proto_test.go  proto contract tests
```

---

## 6. UI assets

`internal/ui/web/` is embedded via `go:embed web/index.html web/app.js web/style.css`:

| File | Size |
|---|---|
| `index.html` | 5,314 B |
| `app.js` | 17,077 B |
| `style.css` | 7,132 B |

> Changing these three files **requires a rebuild** (embed takes effect at compile time).

---

## 7. Code size (locally reproduced)

| Metric | Value |
|---|---|
| Go files | **307** (of which **161** are test files) |
| Total Go lines | **77,886** |
| ├─ test lines | 41,198 |
| └─ non-test lines | about 36,688 |
| `.proto` | 2 |
| SQL migrations | 7 |
| `docs/` `.md` files | 36 (18 topics × en/zh) |
| `architecture/` YAML | 5 (1,776 scalar values) |
| `configs/` YAML | 10 |
| `internal/` package dirs with non-test code | 41 |
| `cmd/` executable dirs | 6 |

### Largest packages (total lines include tests)

| Package | Files | Tests | Total lines | Non-test |
|---|---|---|---|---|
| `internal/platform/sqlite` | 35 | 20 | 10,923 | 5,856 |
| `internal/transport/grpcapi` | 29 | 19 | 8,156 | 2,544 |
| `internal/kernel` | 19 | 11 | 6,725 | 2,771 |
| `api/proto/dtm/v1` | 6 | 2 | 4,340 | 4,135 |
| `internal/application` | 12 | 6 | 4,255 | 1,988 |
| `internal/runtimehost` | 11 | 6 | 3,619 | 1,772 |
| `cmd/dtm-core` | 12 | 7 | 2,997 | 242 |
| `internal/ui` | 13 | 6 | 2,822 | 2,088 |
| `internal/kernel/capability` | 6 | 3 | 2,720 | 1,299 |
| `internal/invocation` | 10 | 3 | 2,594 | 1,614 |
| `internal/resourcedirectory` | 7 | 4 | 2,288 | 848 |
| `internal/mapper` | 14 | 7 | 2,176 | 755 |
| `internal/userspace/taskruntime` | 4 | 2 | 2,090 | 887 |
| `internal/userspace/roottask` | 5 | 2 | 1,530 | 824 |
| `internal/userspace` | 4 | 1 | 1,347 | 584 |

> Every remaining package is under 1,900 lines. **The test-to-production ratio is close to 1.1 : 1.**

---

## 8. Common change sites

| I want to… | Change here |
|---|---|
| add a new capability | reference profile: `internal/capability` + `internal/agent` + `configs/`; **new features should target the `internal/userspace` boundary** (see `docs/integration.md`) |
| change task submission parameters | `cmd/dtm-submit/main.go` + `api/proto/dtm/v1/dtm.proto` (changing the proto requires regeneration) |
| change query filtering/pagination | `cmd/dtm-query/` + `internal/application` + `internal/storage` |
| change SQLite table structure | **create** `internal/platform/sqlite/migrations/core/00N_*.sql` (append-only; do not edit existing migrations) |
| change Kernel object semantics | `internal/kernel/*/model.go` + `architecture/concepts.yaml` + the matching `invariant_test.go`. **Note**: changing frozen semantics requires an explicit supersedes/freeze-break decision |
| add an invariant test | `invariant_test.go` under the relevant package (present in every Kernel-side package) |
| change mesh discovery/handshake | `internal/mesh/{discovery,handshake,protocol}` |
| change the read-only UI | `internal/ui/web/*` (requires a rebuild; embed takes effect at compile time) + `internal/ui/` |
| change the exit-code contract | `cmd/dtm-query/`, and update `docs/` plus Section 1 of this map |

---

<sub>This document is a code-map draft. Package responsibilities are taken from each package's source comments and README; code sizes, ports, defaults and command flags are locally reproduced statistics against the public source (Go 1.24.4 / Windows amd64). <strong>This document is not a capability claim</strong> — a package existing does not mean the corresponding capability is implemented; capability status is governed by <code>docs/status.md</code> and <code>architecture/baseline.yaml</code>.</sub>


[简体中文](code-map.zh-CN.md)
