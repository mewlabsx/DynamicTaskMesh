# DTM 代码地图

> **从"我想改某样东西"出发的索引。**
> 本文档回答三个问题：**哪个二进制做什么**、**哪个包负责什么**、**契约与迁移在哪里**。
>
> 所有路径均相对于仓库根。代码规模统计为本机复现（见第 7 节）。
>
> ⚠️ **本文档不描述能力状态**。某个包存在**不代表**对应能力已实现——请查 [`docs/status.md`](status.md) 与 [`architecture/baseline.yaml`](../architecture/baseline.yaml)。

---

## 1. 六个可执行进程（`cmd/`）

| 进程 | 模式 | 用途 | 关键入口文件 |
|---|---|---|---|
| **`dtm-core`** | Core 服务 | 载入 core YAML → 装配 SQLite（打印 `storage_ready`）→ 监听 TCP 并注册 Core/NodeRegistry gRPC 服务 → lease 过期扫描 → 优雅停止 | `main.go`、`composition.go`、`resource_candidate_readiness.go` |
| **`dtm-agent`** | `mode=static` | 要求 `core.address`。向 Core 注册/心跳并暴露 Agent 执行 gRPC 服务；含 RPC 错误分类（continue/retry/reregister/terminate/shutdown）与重新注册 fencing | `main.go`、`composition.go` |
| **`dtm-submit`** | CLI | 提交任务或查询状态。含 Runtime bootstrap 与 protojson 输出 | `main.go` |
| **`dtm-query`** | CLI | 子命令 `get` / `list` / `executions` | `main.go`、`command.go`、`output.go` |
| **`dtm-runtime`** | `mode=runtime` | 装配 ResourceExecutionCapability + UDP 组播 discovery + membership/protocol/handshake，运行 Level 1 mesh | `main.go` |
| **`dtm-ui`** | 只读 Web UI | `-http` 默认 `127.0.0.1:46080`，含 Lab 模式 | `main.go` |

### 命令行参数速查

**`dtm-submit`**

```text
-core                <addr>      Core 地址
-runtime             <addr>      Runtime 地址（可选）
-target-temperature  <int>       目标温度，默认 26
-task-id             <string>    指定任务 ID
-async                           异步提交
-idempotency-key     <string>    提交幂等键
-status              <task-id>   查询状态
```

**`dtm-query`**

```text
-config         <path>          配置文件
-core-address   <addr>          Core 地址
-timeout        <duration>      超时
-output         text|json       输出格式
-task-id        <id>            get / executions 用
-status         <state>         list 过滤
-created-after / -created-before <RFC3339>
-limit          <int>           keyset 分页
-page-token     <token>
```

**`dtm-query` 退出码契约**

| 退出码 | 含义 |
| ---: | --- |
| 0 | 成功 |
| 1 | 内部或未分类错误 |
| 2 | 参数、配置或 `InvalidArgument` |
| 3 | Task 不存在 |
| 4 | Core 不可用 |
| 5 | RPC 超时 |
| 6 | 请求取消 |

**`dtm-ui`**

```text
-runtime <addr>（可重复）
-runtime-seed <list>            逗号分隔
-http                           默认 127.0.0.1:46080
-discovery-group / -discovery-port / -discovery-interface
-query-timeout
-enable-test-controls
-lab-manifest / -lab-timeout
```

---

## 2. `internal/` 包职责（41 个含非测试代码的包）

### 2.1 Kernel 侧

| 包 | 职责 |
|---|---|
| `internal/kernel` | 公共门面：类型别名聚合 identity/resource/capability/execution/event；`kernel.go` 组合根；`execution_ownership.go`（Gate B Phase 1）；`logical_execution_facade.go`（有界逻辑 facade）；`local_runtime.go`（World A→B 有界准入桥） |
| `internal/kernel/identity` | 标识语义：各类 ID、`ObjectKind`/`ObjectReference`、`Lifecycle`/`Availability`/`Evaluation` 状态合法性（`ObjectKindIntent` 保留但被 `ObjectReference` 校验拒绝） |
| `internal/kernel/resource` | `Resource` 对象与 Manager：生命周期/可用性、执行容量占用（`executionClaims`）、`OwnershipFence`、解析授权令牌 |
| `internal/kernel/capability` | `CapabilityDeclaration` / `CapabilityInstance` / `CapabilityHandle` 模型与校验；Handle 校验、Scope 相等、fail-closed |
| `internal/kernel/execution` | `ExecutionContext` 模型与 Manager；`ownership.go` 定义 `ExecutionRequest`/`Descriptor`/`Allocation`/`ExecutionObservation`/`CurrentOccupancy`/`ResolutionEvidence` |
| `internal/kernel/event` | 不可变 `EventRecord` 模型与 Manager（只追加审计事实，如 `Resource.Created`） |

### 2.2 User Space 侧

| 包 | 职责 |
|---|---|
| `internal/userspace` | 有界 User Space 执行边界：窄 value-oriented Kernel port（`port.go`）+ `Orchestrator`（USEO，有界同步执行所有者）+ WorkState |
| `internal/userspace/roottask` | 有界 Root Task 聚合：Root 身份、有限 Child 成员关系、Root 生命周期、确定性闭包判定 |
| `internal/userspace/taskruntime` | 有界 Child Task 生命周期与一次性提交边界（**无**调度/重试/持久化） |
| `internal/userspace/taskcreation` | Root/Child 之上的窄物化边界：接受已决策请求并协调既有生命周期所有者；**不**派生工作、**不**执行、**不**评估闭包 |

### 2.3 参考 profile（Core / Agent 路径）

| 包 | 职责 |
|---|---|
| `internal/transport/grpcapi` | gRPC 传输适配：core_server、agent_server、registry_server、task_query_server、resource_invocation_server、remote_executor、endpoints、step/submission 幂等 |
| `internal/platform/sqlite` | SQLite 实现：Database、embed 迁移、schema manifest/校验和、`persistence_*` 仓储、resource/execution repository |
| `internal/storage` | 存储端口与类型：Database/Query/Persistence 接口、`TaskFilter`/`TaskPage`/`RecoveryCursor`、错误分类、诊断与启动失败输出 |
| `internal/application` | 应用服务层：`TaskService`、`AsyncTaskService`、`TaskQueryService`、`RecoveryController`、`TaskRepository`、资源证据 |
| `internal/resourcedirectory` | Resource 目录：`ResourceRecordView`、Node/registration/generation fence、publication/quiesce 状态、确定性 `TransitionPlan` |
| `internal/mapper` | Planner 计划 → Resource 映射：`ResourceCandidateQuery`/Source、Resource Scheduling Map、映射校验、恢复重解析、legacy capability 映射 |
| `internal/planner` | 参考 profile 的意图 → 步骤 Planner（**唯一 intent**：`cool_environment`） |
| `internal/runtime` | 遗留 DTM Runtime：执行 mapped plan、重试/重映射策略、取消与 deadline 传播 |
| `internal/execution` | 遗留执行结果与 `StepResult`、`ExecutionFenceMode` 证据枚举 |
| `internal/lifecycle` | Task 与 Step 生命周期状态机与合法转换 |
| `internal/task` | Task 值对象（ID/Intent/Requirements/Constraints）与校验 |
| `internal/node` | Node 值对象与状态转换校验（registered/active/suspect/offline/recovering/stale） |
| `internal/lease` | Node Lease 管理器（TTL、过期扫描、stale registration 检测） |
| `internal/nodelifecycle` | 节点生命周期控制器：协调 registry/endpoints/lease，拒绝陈旧注册 |
| `internal/capability` | 参考 profile 的能力目录（`temperature_sensor`、`cooling_control`）与 Node Registry |
| `internal/agent` | Agent 侧 Capability Handler 路由（Router，重复能力检测） |
| `internal/agentexecution` | Agent 执行记录仓储与请求 Fingerprint（幂等键复用冲突检测） |
| `internal/authoritybinding` | Runtime / 静态 Agent 共享的 NodeRegistry 注册+心跳会话适配（registrationID 生成、CallTimeout） |
| `internal/model` | 领域基础类型：`TaskID`/`NodeID`/`StepID`/`ExecutionID`/`Capability`、Resource 系列 ID、`IdempotencyMode`、legacy capability-resource 映射 |

### 2.4 Mesh 与 Resource Invocation

| 包 | 职责 |
|---|---|
| `internal/mesh/discovery` | 组播/UDP Peer Discovery（windows/unix 分平台 socket）、握手确认与 identity mismatch 检测 |
| `internal/mesh/handshake` | Runtime 间 gRPC 握手，含拒绝原因、timeout 与 fuzz 测试 |
| `internal/mesh/membership` | Mesh 成员 active/suspect/expired 状态机与快照（**非** Kernel Object） |
| `internal/mesh/protocol` | Mesh 线上协议值：Identity（MeshNamespace/ProtocolMajor/RuntimeInstance/ControlEndpoint）、`DTMVersion`、presence 编解码 |
| `internal/mesh/coordinator` | 从 membership 快照确定性选出单一 coordinator 会话（NodeID 排序，冲突则 fail-closed） |
| `internal/mesh/resourceview` | Mesh Resource View 投影：无 authority 的 Descriptor、上限约束、owner/namespace 校验 |
| `internal/mesh/resourcesync` | 从各 owner 拉取 Resource Advertisement 的同步器（有界并发/队列） |
| `internal/invocation` | Resource Invocation 服务：目标解析（双重校验 fail-closed）、native gRPC 与 legacy/compatibility 传输、执行器、领域模型、fence 证据 |
| `internal/runtimehost` | 进程装配层：`CoreCapability`、`ResourceExecutionCapability`、`MeshOptions`、ResourceEvidence writer |

### 2.5 其他

| 包 | 职责 |
|---|---|
| `internal/ui` | 只读观测 UI：Observer 快照、HTTP handler + embed web 资源、Timeline、LabMode/LabHarness |
| `internal/config` | YAML 配置加载与默认值（Core/Agent/Runtime、lease/heartbeat/retry/storage/mesh/multicast） |
| `internal/demo` | 模拟能力处理器：温度传感与冷却控制 |

### 2.6 容易找错的包

| 你可能以为存在 | 实际位置 |
|---|---|
| `internal/resource` | ❌ 不存在 → `internal/kernel/resource` |
| `internal/event` | ❌ 不存在 → `internal/kernel/event` |
| `internal/execution` | ⚠️ **有两个**：`internal/execution`（遗留结果/fence）与 `internal/kernel/execution`（Kernel 对象链） |
| `internal/capability` | ⚠️ **有两个**：`internal/capability`（参考 profile 目录/注册表）与 `internal/kernel/capability`（Kernel 对象链） |
| `internal/mesh`（作为包本身） | ❌ 不存在，只有 7 个子包 |
| `internal/facade` | ❌ 不存在 → `internal/kernel/logical_execution_facade.go`（`package kernel`） |
| `internal/child` | ❌ 不存在 → `internal/userspace/taskruntime` |
| `internal/root` | ❌ 不存在 → `internal/userspace/roottask` |
| `internal/scheduler`、`internal/intent`、`internal/eventstore`、`internal/observability` | ❌ 均不存在 |

---

## 3. 契约与协议

### 3.1 `api/proto/dtm/v1/`

| 文件 | 内容 |
|---|---|
| `dtm.proto` | **4 个 service**：`CoreService`（SubmitTask / GetTaskStatus / GetTask / ListTasks / GetTaskExecutions）、`NodeRegistryService`（RegisterNode / Heartbeat / UpdateNodeStatus）、`AgentExecutionService`（ExecuteStep）、`RuntimeControlService`（Handshake / GetResourceAdvertisement / GetRuntimeStatus）。约 32 个 message |
| `invocation.proto` | `ResourceInvocationService.InvokeResource` + `ResourceRef`/`InvocationMetadata`/`Request`/`Result`/`Error`/`Response` |
| `*.pb.go`、`*_grpc.pb.go` | **已检入生成代码**，普通构建**不需要 protoc** |
| `*_proto_test.go` | proto 契约测试 |

**重生成**：见 [`docs/protocol-generation.md`](protocol-generation.md) 与 `scripts/generate-proto.ps1`（工具链版本 protoc 30.2 / protoc-gen-go v1.36.6 / protoc-gen-go-grpc 1.5.1）。

### 3.2 迁移（append-only）

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

> ⚠️ **迁移是 append-only 的**。Schema 漂移、migration 版本/名称/checksum 不匹配都会在**写入前**以 `SchemaMismatch` 拒绝启动（退出码 12）。
> **不要编辑迁移历史来绕过错误。**

---

## 4. 配置

### 4.1 配置文件布局

| 位置 | 数量 | 用途 |
|---|---|---|
| `configs/core.yaml`、`configs/agent.yaml` | 2 | 根示例（Core 单机 + sensor 节点） |
| `configs/demo/` | 5 | `core.yaml`、`sensor-agent.yaml`、`cooling-agent.yaml`、`cooling-agent-001.yaml`、`cooling-agent-002.yaml` |
| `configs/lab/` | 3 | 供 `dtm-ui` 的 Lab Mode 使用 |

### 4.2 关键默认值

**`configs/core.yaml`**

```text
server.address            127.0.0.1:50051
lease.ttl                 10s
lease.sweep_interval      1s
storage.sqlite.path       ../data/dtm.db     （相对配置文件目录解析）
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

### 4.3 Demo 端口分配

| 服务 | 地址 |
|---|---|
| Core | `127.0.0.1:50051` |
| 温度 Agent | `127.0.0.1:50061` |
| 冷却 Agent 001 | `127.0.0.1:50062` |
| 冷却 Agent 002 | `127.0.0.1:50063` |

> V0.2.6 起统一使用 `server.listen_address` 与 `node.advertise_address`；旧字段仍可兼容读取。

---

## 5. 构建、验证与工具

### 5.1 `Makefile`

| target | 命令 |
|---|---|
| `make test` | `go test ./...` |
| `make build` | `go build ./cmd/...` |
| `make verify` | `go test ./...` + `go build ./cmd/...` + `go vet ./...` + `$(MAKE) fuzz` |
| `make fuzz` | 6 个 fuzz target，各 `-fuzztime=$(FUZZTIME)`（默认 `5s`） |

### 5.2 Fuzz target

| Target | 位置 |
|---|---|
| `FuzzDecodePresence` | `internal/mesh/protocol` |
| `FuzzHandshakeRequest` | `internal/mesh/handshake` |
| `FuzzObserveAndTick` | `internal/mesh/membership` |
| `FuzzResourceAdvertisement` | `internal/mesh/resourceview` |
| `FuzzNewTask` | `internal/task` |
| `FuzzNativeInvocationRequest` | `internal/transport/grpcapi` |

### 5.3 建议的验证命令

```powershell
go build ./...
go vet ./...
go test -count=1 -timeout=15m ./...
```

> **Go 构建缓存**：若默认缓存目录不可写，可指到工作区内：
> ```powershell
> $env:GOCACHE = "<repo>\..\.gocache"
> $env:GOMODCACHE = "<repo>\..\.gomodcache"
> ```

### 5.4 测试资产位置

```text
tests/integration/                端到端集成测试（cool_environment_test.go、grpc_demo_test.go）
tests/DTM_v0.2.5_完整可靠动态网络测试方案.md
internal/**/invariant_test.go     不变量回归（Kernel 侧每个包都有）
internal/kernel/freeze_invariants_test.go
internal/kernel/public_contracts_test.go
internal/kernel/architecture_invariant_test.go
api/proto/dtm/v1/*_proto_test.go  proto 契约测试
```

---

## 6. UI 资源

`internal/ui/web/` 通过 `go:embed web/index.html web/app.js web/style.css` 嵌入：

| 文件 | 大小 |
|---|---|
| `index.html` | 5,314 B |
| `app.js` | 17,077 B |
| `style.css` | 7,132 B |

> 改这三个文件**需要重新编译**（embed 在编译期生效）。

---

## 7. 代码规模（本机复现）

| 指标 | 数值 |
|---|---|
| Go 文件 | **307**（其中测试文件 **161**） |
| Go 总行数 | **77,886** |
| ├─ 测试行数 | 41,198 |
| └─ 非测试行数 | 约 36,688 |
| `.proto` | 2 |
| SQL 迁移 | 7 |
| `docs/` 文档 `.md` | 36（18 主题 × 中英） |
| `architecture/` YAML | 5（1,776 个标量值） |
| `configs/` YAML | 10 |
| `internal/` 含非测试代码的包目录 | 41 |
| `cmd/` 可执行目录 | 6 |

### 主要包规模（总行数含测试）

| 包 | 文件 | 测试 | 总行数 | 非测试 |
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

> 其余包规模均小于 1,900 行。**测试/生产比接近 1.1 : 1。**

---

## 8. 常见改动落点

| 我想…… | 改哪里 |
|---|---|
| 加一个新能力（Capability） | 参考 profile：`internal/capability` + `internal/agent` + `configs/`；**新功能应针对 `internal/userspace` 边界**（见 `docs/integration.md`） |
| 改任务提交的参数 | `cmd/dtm-submit/main.go` + `api/proto/dtm/v1/dtm.proto`（改 proto 需重生成） |
| 改查询过滤/分页 | `cmd/dtm-query/` + `internal/application` + `internal/storage` |
| 改 SQLite 表结构 | **新建** `internal/platform/sqlite/migrations/core/00N_*.sql`（append-only，不要改已有迁移） |
| 改 Kernel 对象语义 | `internal/kernel/*/model.go` + `architecture/concepts.yaml` + 对应 `invariant_test.go`。**注意**：冻结语义的变更需要显式的 supersedes/freeze-break 决策 |
| 加不变量测试 | 对应包下的 `invariant_test.go`（Kernel 侧每个包都有） |
| 改 Mesh 发现/握手 | `internal/mesh/{discovery,handshake,protocol}` |
| 改只读 UI | `internal/ui/web/*`（需重新编译，embed 在编译期生效）+ `internal/ui/` |
| 改退出码契约 | `cmd/dtm-query/`，并同步 `docs/` 与本地图第 1 节 |

---

<sub>本文档是代码地图草稿。包职责取自各包源码注释与 README；代码规模、端口、默认值与命令参数为本机对公开版源码的复现统计（Go 1.24.4 / Windows amd64）。<strong>本文档不构成能力声明</strong>——某包存在不代表对应能力已实现，能力状态以 <code>docs/status.md</code> 与 <code>architecture/baseline.yaml</code> 为准。</sub>


[English](code-map.md)
