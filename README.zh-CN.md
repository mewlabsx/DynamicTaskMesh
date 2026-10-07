# Dynamic Task Mesh (DTM)

> **一个实验性的、以任务为中心的 Resource 编排项目。**
> 它不试图成为"任务运行时"——恰恰相反，它把**机制与业务策略彻底分开**：内核只管执行正确性与授权边界，业务语义、规划、调度策略全部留在 User Space。
>
> | 轴 | 版本 |
> |---|---|
> | **软件线** | `v0.7`（Autonomous Kernel） |
> | **Kernel 规范** | `v0.1` |
>
> **两条轴独立演进**（`D-KERNEL-VERSION-AXES`）——软件线叫 v0.7 不代表 Kernel 也是 v0.7。

> ### ⚠️ 读这份仓库前必须知道的一条规则
>
> **权威 ≠ 证据，时间戳新不覆盖冻结决策。**
>
> ```text
> 权威（唯一控制面）
>   architecture/{baseline,boundaries,concepts,decisions,invariants}.yaml
>   + architecture/reviews/kernel-semantic-freeze-governance-signoff-2026-09-06.md
>         │
>         │  只有在出现【被 Architecture State 显式引用、声明了 scope、
>         │  带显式 supersedes/freeze-break 关系、且完成 Boundary Delta +
>         │  架构评审 + 治理批准】的后续决策时，才可在该 scope 内覆盖
>         ▼
> 证据（不能覆盖权威）
>   历史设计文档 · 里程碑草稿 · 审计发现 · 未跟踪的实现
> ```
>
> **三条读法**：
> 1. **时间戳更新不覆盖冻结决策**——普通文档、测试通过或更新的日期**都不能**取代已冻结语义（`D-R0-SOURCE-AUTHORITY`）
> 2. **历史 PASS 不是本次验证**——已有 PASS 记录描述的是它们自己的源码版本、环境与有界测试
> 3. **不要从历史设计或测试通过扩大实现声明**

---

## 快速导航

| 我想…… | 去这里 |
|---|---|
| 跑起来看效果 | [快速开始](#快速开始跑通-core--agent-参考路径) |
| 理解 Kernel 与 User Space 的分工 | [Kernel 与 User Space](#kernel-与-user-space) |
| **了解两条线如何合并（下一步主要工作）** | [跨 profile 约束](#跨-profile-约束为什么现在就能写) |
| 知道哪些是真的实现了、哪些只是设计 | [能力状态](#implementation-boundaries) |
| 查权威架构定义 | [`architecture/`](architecture/README.md) —— 5 份 YAML 是**唯一控制面** |

---

## 这个项目解决什么问题

DTM 关注的是：**一次执行如何被安全地授权、占用、观测和释放。**

在设备与资源分散、能力动态上下的环境里，真正难的从来不是"怎么调用一个函数"，而是：

| 问题 | DTM 的处理方式 |
|---|---|
| 调用方怎么证明它**有权**执行这个副作用？ | `CapabilityHandle` 是唯一的授权引用；`CapabilityInstance` 和 Resource ID **本身不是授权** |
| 谁来保证"已经产生了物理副作用"的动作不被盲目重试？ | `UNKNOWN` 必须保持显式，**不转成 FAILED、不触发隐式重试** |
| 发生了副作用但结果未知，这个资源占用算不算还在？ | `CurrentOccupancy` 三态（`NOT_ESTABLISHED` / `UNKNOWN` / `ENDED`），`UNKNOWN` 时**保留精确占用，fail-closed** |
| 授权通过了，资源容量谁来记账？ | Resource 拥有执行容量；**活跃精确 claim 集合是容量记账的唯一来源** |
| 观测到的现象算不算"权威状态"？ | 观测 ≠ 状态 ≠ 审计事实。三者分离，只有经过准入的转移才能改状态 |
| 业务语义（意图、规划、重试策略）该放哪？ | **全部在 User Space**，内核不碰 |

> 一句话：DTM 的复杂度守卫的是**授权正确性、执行占用一致性、审计不可变性**这三个真实边界，而不是"分布式系统通常这么做"。

---

## 三层结构（不要混为一谈）

仓库里有**演化层**，它们的能力**不能拼成一个未经验证的生产系统**（`docs/overview.md` 明确定义）。按"能不能用于生产"排序：

```text
┌─────────────────────────────────────────────────────────────────────┐
│ ① 现有执行参考路径（Core / Agent）              ← 有真实进程、SQLite  │
│    dtm-submit → Core → Planner → Mapper → Runtime → gRPC Agent      │
│    dtm-query  → Core 查询服务 → SQLite Task/Step/Execution           │
│    能力：能力/资源发现、节点所有权、Lease 与 fencing、提交去重、       │
│          保守恢复、append-only migration                            │
│    ⚠️ 这条路径【尚未】经过 Kernel / User Space 运行                   │
├─────────────────────────────────────────────────────────────────────┤
│ ② 有界 Kernel / User Space                      ← 架构方向，部分落地 │
│    in-process 逻辑 facade + 有界 User Space 编排                     │
│    边界：见第 3 节                                                   │
├─────────────────────────────────────────────────────────────────────┤
│ ③ 兼容性参考（不作为生产声明）                                        │
│    Level 1 mesh（discovery / handshake / membership / coordinator）  │
│    Resource Invocation（transport-independent，native gRPC 冻结于    │
│      M2-R1；Binary transport 明确排除）                              │
└─────────────────────────────────────────────────────────────────────┘
```

**为什么必须分层说明**：把 ①②③ 的能力合并描述，就等于声明一个从未验证过的系统。这是本项目文档的第一条纪律。

**而把 ①② 真正合成一条执行路径，正是下一步的主要工作之一**——见[跨 profile 约束](#跨-profile-约束为什么现在就能写)。

> ⚠️ Core/Agent 的 CLI 与 Kernel/User Space 的实现**目前是分离的**；本预览**不声明**已集成一个自治运行时。

---

## Kernel 与 User Space

这是 DTM 当前架构的核心。权威定义在 [`docs/kernel-userspace-boundary.md`](docs/kernel-userspace-boundary.md) 与 [`architecture/boundaries.yaml`](architecture/boundaries.yaml)。

### 职责划分

**Kernel 拥有机制、授权、生命周期正确性、执行正确性**（`boundaries.yaml` → `layer: Kernel`，状态 `FROZEN`）：

```text
Kernel 拥有：
  · Resource / CapabilityDeclaration / CapabilityInstance / CapabilityHandle 语义
  · ExecutionContext 与 EventRecord 语义
  · 授权、生命周期、可用性、关系、Handle、Scope、fail-closed 结构边界
  · 执行容量归属：ExecutionRequest → Resource 拥有的分配 → 冻结绑定
    → 单次 dispatch claim → 正交观测 → 释放
  · same-fence 占用解析：CurrentOccupancy / Resource 签发的解析授权 /
    不可变 OwnershipFence 快照 / 精确 claim 释放 / 解析 EventRecord
  · 不可变审计事实（EventManager）

Kernel 明确不拥有：
  · User Space ABI、传输、认证服务、完整授权求值器、持久化、远程信任
  · Task 调度、Planner / 工作流解释、业务策略、设备或业务逻辑
  · Task 的创建、分解、生命周期、重试策略、结果聚合、完成判定、
    root-task 满意度评估、child-task 规划
  · KernelIntent 准入运行时、评估引擎、reconciliation
  · 跨 fence 或回溯性执行解析、fence 迁移、崩溃重建
```

**User Space 拥有业务解释与策略**（`layer: Application/UserSpace`，状态 `FROZEN`）：

```text
User Space 拥有：
  · UserIntent、Task 创建、Task 分解、Task 生命周期
  · 需求、应用状态、工作流解释、业务策略、task 级行为
  · Intent Formulation：解释、规范化、翻译成 KernelIntentSpec
  · Satisfaction Planning：选择 Task、能力、时序、调度、reconciliation
  · 外部 Task/Policy 与执行血缘契约
  · 只读观测 EventRecord；只通过已批准边界使用 Kernel 能力授权

User Space 不拥有：
  · KernelIntent 对象授权
  · EventRecord 的发布或改写
  · 从 UserIntent / KernelIntent 隐式铸造 Handle
```

### 最反直觉的一条：**Task 不是 Kernel Object**

这是 DTM 与"通用任务运行时"最大的区别（`INV-TASK-EXTERNAL-POLICY-MANAGED`，`CRITICAL`）：

> **Task 由 User Space / Policy Layer 拥有，不是 Kernel Object、不是 Kernel 管理的生命周期、不是 Kernel 调度实体。Kernel 不创建、不分解、不调度、不重试、不聚合、不完成、不评估 Task。**
> `RootTaskRef` 和 `ChildTaskRef` **只是不透明的血缘引用**。

配套的血缘基数规则（外部 Task/Policy 契约，非 Kernel Object 规则）：

> **一个 Child Task 至多映射到一次具体的 Capability 执行请求、至多一个 ExecutionRequest、至多一次 Capability 调用。**

所以准确的说法是：**DTM 是 Resource/执行编排内核，不是任务引擎。** 任务语义在它外面。

### Intent 也分两层

（`D-R0.1-INTENT-DUAL-LAYER` + `D-R0.1-INTENT-PERSISTENT-REPRESENTATION`）

```text
UserIntent           ← User Space 自有：应用/领域/人/agent 的意图，可有歧义、可含策略
      │ Intent Formulation（User Space 职责：解释、规范化、翻译）
      ▼
KernelIntentSpec     ← 边界数据：提交给未来 Kernel 准入的规范化期望态
      │                  业务语义对 Kernel **不透明**
      │                  不是 Kernel Object、不是授权引用、不是 Task、不是计划
      ▼
KernelIntent         ← 未来被准入的 Kernel 管理持久表示
                          Kernel 拥有：稳定身份、持久化/生命周期、revision、
                                      所有权/scope、结构准入与授权语义
                          Kernel 不拥有：业务语义解释、满意度评估、World Model、
                                        reconciliation、Satisfaction Planning、
                                        Task 生成、能力执行授权
```

**当前状态**：`FROZEN_SEMANTICS_ONLY` / `NOT_IMPLEMENTED`。`ObjectKindIntent` 仍是保留词汇标记，**当前被 Kernel ObjectReference 校验拒绝**。
> `NOT_IMPLEMENTED` ≠ `ARCHITECTURALLY_FORBIDDEN`（`INV-DEFERRED-NOT-NEGATED`）。

### 边界怎么被强制

关键不变量（全部 `CRITICAL`）：

| ID | 规则 |
|---|---|
| `INV-KERNEL-USERSPACE-BOUNDARY` | Kernel/User Space 边界是**语义**边界；**内部 Kernel API 不自动构成 User Space ABI**；冻结的 K1 语义契约与任何 wire/transport/语言 ABI 是两件事 |
| `INV-CAPABILITY-AUTHORITY` | CapabilityInstance 和 Resource ID **不是授权**；能力使用必须持有有效 `CapabilityHandle` |
| `INV-HANDLE-CONTEXT-SCOPE` | Handle 绑定到一个 ExecutionContext，`Scope` 必须**严格相等**，不匹配 fail-closed；**不蕴含**其他 Scope 域的相等 |
| `INV-EVENT-IMMUTABLE-SEMANTIC` | EventRecord 是不可变 Kernel 事实，不是当前状态；User Space 或 provider 观测**不能发布或改写** |
| `INV-OBSERVATION-EVIDENCE-SEPARATION` | 物理/外部观测在经准入前是**不可信或有界证据**，本身不是权威状态转移、也不是 EventRecord |
| `INV-FAIL-CLOSED-AUTHORITY` | 无效/已撤销/已过期/已销毁的 Handle **fail-closed**；有副作用的执行需要**执行侧重新校验** |
| `INV-UNKNOWN-NO-BLIND-RETRY` | 可能已产生物理副作用而结果未知的执行，**不得盲目重试** |
| `INV-SCOPE-DOMAIN-SEPARATION` | `ExecutionAuthorityScope` 是当前唯一的 Handle/Context Scope 域；`AutonomyScope` / `IntentScope` / `RoleScope` / Mesh/Region 语义保持分离，**不蕴含**包含、继承、相等或委派 |

**代码层面的边界映射**：

```text
internal/kernel/           ← Kernel：identity / resource / capability / execution / event
                              + kernel.go（公共门面）
                              + execution_ownership.go（Gate B Phase 1）
                              + logical_execution_facade.go（有界逻辑 facade）
                              + local_runtime.go（World A→B 有界准入桥）

internal/userspace/        ← User Space：窄 value-oriented port
                              + orchestrator.go（有界同步执行所有者）
                              + roottask/     （Root 身份、有限 Child 成员、确定性闭包判定）
                              + taskruntime/  （Child 生命周期、一次性提交边界）
                              + taskcreation/ （已决策请求的窄物化边界）
```

**跨界点只有一个**（`internal/userspace/port.go`）：

```go
// ExecutionKernelPort 是有界执行编排器对 Kernel 的完整依赖。
// 两个操作都返回 value snapshot；该 port 刻意不暴露
// allocation / Resource / Provider / occupancy 变更原语。
type ExecutionKernelPort interface {
    RequestExecution(kernel.LogicalExecutionRequest) (kernel.LogicalExecutionResult, error)
    ObserveInvocation(kernel.InvocationID) (kernel.LogicalInvocationObservation, error)
}
```

> 注意：User Space 包**确实 import 了 `dtm/internal/kernel`**，但只依赖这个两层接口、只拿值快照。这是**有意的进程内组合边界，不是物理 ABI**。

---

## 跨 profile 约束（为什么现在就能写）

> **下一步的主要工作之一，是把两条开发线整理并合并成一条主执行路径。**
> 权威表述见 [`docs/integration.md`](docs/integration.md) 与 [`docs/recovery-idempotency.md`](docs/recovery-idempotency.md)。

### 两条线现在是什么关系

```text
① Core / Agent 参考 profile（持久化）
   Task / Step / Execution 记录 · 提交键 · Resource 记录  → SQLite
   DB 状态（而非乐观内存视图）是该 profile 的权威
   Registration identity/generation · Owner Lease · Endpoint 校验仍然必需
   Recovery 是保守的：不因 Agent 消失、Lease 过期、响应丢失或记录不完整而重放不确定的副作用
                    │
                    │  ⚠️ 两条线目前【未集成】
                    │     包链接或保留两份日志，都不算集成
                    ▼
② 有界 Kernel / User Space（进程内）
   allocation · occupancy · invocation binding · trusted resolution  → 有界本地内存
   ★ 不继承 ① 的 SQLite 持久化、重放或崩溃恢复保证
   facade 的 InvocationID 是【已准入的进程内 scope 内】的稳定关联，
   不是重启全局的去重存储
```

**这个不对称是合并的核心难点**：① 从 SQLite 恢复参考状态，而 ② 的 Kernel 授权与占用状态**当前在内存里**。

### 合并后的目标形态

> **一条主执行路径**：

```text
显式任务输入
  → User Space 任务/执行编排
    → Kernel 逻辑 facade
      → Provider adapter
        → 现有 Agent 执行
```

**原则**：现有的 CLI、传输与查询功能，**在契约仍然有效的地方应予复用**，而不是重写。

### 实现前必须先定下来的六件事

（`docs/integration.md`）

| # | 必须定义 |
|---|---|
| 1 | 稳定的 **task / work / invocation / allocation / attempt 绑定** |
| 2 | **registry → Kernel 的授权映射** |
| 3 | Provider **结果**与**占用**证据 |
| 4 | **单一执行所有者** |
| 5 | **重启行为**（必须显式处理，不能回避） |
| 6 | **持久化权威、effect identity、ownership/lease/fence 映射、崩溃边界**的显式决策 |

**两条最容易踩的陷阱**：

> ⚠️ **discovery 记录不是执行授权。**
> ⚠️ **旧的 registration generation 不会自动变成 Kernel ownership fence。**

**关于单一执行所有者**：

> 现有的重试、重映射与恢复**不得同时控制**新路径所拥有的工作。
> 否则就会同时存在两个调度器——这正是要消除的"并行功能开发轨道"。

### 第一条有界集成 profile 的边界

允许的做法：

> 第一条有界 profile **可以用持久隔离 + fail-closed 恢复**来替代完整的 Kernel 持久化。

**但它绝不能**：

| 禁止 | 原因 |
|---|---|
| 静默重新派发**未解决**的工作 | 结果未知 ≠ 可以重试 |
| 重启后把**空 Resource 重建为可用** | 空 ≠ 空闲 |
| 让新 fence 隐含终止旧 Unknown 执行 | `INV-OWNERSHIP-FENCE-NOT-TERMINATION-PROOF` |
| 把 `ObserveInvocation NOT_KNOWN` 当作安全的可重放证据 | 它**没有权威的跨越事实** |

### 验收标准（真实执行，不是单元测试）

第一条被接受的集成 profile **必须同时证明**：

| # | 必须证明 |
|---|---|
| 1 | **真实的** command → Agent 执行 |
| 2 | 授权被拒绝时**没有副作用** |
| 3 | 重复请求保护 |
| 4 | `UNKNOWN` **可见**，且**没有**隐式重试或释放 |
| 5 | 崩溃/重启边界上的安全行为 |
| 6 | **现有参考场景继续通过** |

> **仅仅"正常执行成功"是不够的。**

### 什么时候可以真正切默认路径

切换到新默认路径的条件（全部满足）：

- 公开主场景、查询/幂等行为与已声明的恢复保证**都被覆盖**
- **不残留旧的调度器旁路**
- 独立评审**没有阻塞性发现**

之后旧的执行所有权逻辑才可以退役；可复用的兼容适配器可以保留。

### 从现在起的开发纪律

| 事项 | 规则 |
|---|---|
| **新执行功能** | 应针对 **Kernel / User Space 边界**开发 |
| **旧 profile** | 在迁移期间**只限于必要的兼容性与正确性工作** |
| **目的** | 避免两条并行的功能开发轨道 |
| 不是前置条件 | 完全自治运行、Intent runtime、更广的准入**都不是**第一条有界 profile 的前置条件 |

> `docs/integration.md` **只提出方向**：它**不授权实现**，也**不改变已冻结的里程碑状态**。

---

## 三个 profile 的分层速查

同一份代码里并存三层演化产物。**它们的 profile 名、命令入口、持久化方式与权威来源都不同**——混用是最容易出错的地方。

| Profile | 命令入口 | 持久化 | 权威来源 | 状态 |
|---|---|---|---|---|
| **① 参考 profile**（reference） | `dtm-core` + `dtm-agent` + `dtm-submit` + `dtm-query` | **SQLite**（append-only migration） | **DB 状态**，而非乐观内存视图 | 现有实现与测试 |
| **② 有界 Kernel / User Space** | 进程内组合（`internal/kernel` + `internal/userspace`） | **有界本地内存** | Kernel 对象与不变量 | 有界实现 |
| **③ 兼容性参考** | `dtm-runtime`（mesh）、`dtm-ui`（只读观测） | — | Level 1 mesh / v0.6 调用基础 | 兼容参照，非生产声明 |

**三条不可跨越的边界**：

| # | 规则 |
|---|---|
| 1 | ① 与 ② **不是**同一条执行路径；② **不继承** ① 的持久化、重放与崩溃恢复保证 |
| 2 | ② 的 facade InvocationID 只在**已准入的进程内 scope** 内稳定，**不是**重启全局去重存储 |
| 3 | ③ 的 discovery 记录与 Mesh Resource View **不是**执行授权（`INV-RESOURCE-ADMISSION-BEFORE-AUTHORITY`） |

> **把三条线的能力合并描述，就等于声明一个从未验证过的系统**——这正是第 2 节那条纪律的具体展开。
> ① 与 ② 的合并计划见[跨 profile 约束](#跨-profile-约束为什么现在就能写)。

**② 的占用判定只有三种结论，且 `UNKNOWN` 是保守的一侧**：

| 结论 | allocation | 精确 claim | 含义 |
|---|---|---|---|
| `NOT_ESTABLISHED` | RUNNING | HELD | Provider 边界**尚未**跨越 |
| `UNKNOWN` | RUNNING | **仍 HELD** | 边界已跨越，**无法排除仍在占用** |
| `ENDED` | RELEASED | NONE | 稳定终态 |

> `UNKNOWN` **既不是**"未开始"、**也不是**失败或超时——它保持占用，等待被明确授权的解析。具体机制见 [`docs/execution-contract.md`](docs/execution-contract.md)。

---

## 快速开始：跑通 Core / Agent 参考路径

> 这条路径**不调用**新的 Kernel Goal Loop，也不涉及真实设备（`docs/quickstart.md`）。
> 它走的是**参考 profile**，**尚未**经由 Kernel / User Space 运行——两条线的合并计划见[跨 profile 约束](#跨-profile-约束为什么现在就能写)。

### 构建

需要 Go 1.24 或兼容工具链。Protobuf 生成文件已检入，**普通构建不需要 protoc**。

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

### 启动三个服务（三个终端）

```powershell
# 终端 1：Core
./bin/dtm-core.exe -config ./configs/demo/core.yaml

# 终端 2：温度 Agent
./bin/dtm-agent.exe -config ./configs/demo/sensor-agent.yaml

# 终端 3：冷却 Agent
./bin/dtm-agent.exe -config ./configs/demo/cooling-agent-001.yaml
```

等 `dtm-core listening` 和每个 Agent 的 `register success`。

| 服务 | 地址 |
|---|---|
| Core | `127.0.0.1:50051` |
| 温度 Agent | `:50061` |
| 冷却 Agent | `:50062`（另有 `cooling-agent-002.yaml` 用 `:50063`） |

数据库路径相对配置文件目录解析，demo 状态落在根目录 `data/`。

### 提交并查询（第四个终端）

```powershell
./bin/dtm-submit.exe -core 127.0.0.1:50051 -target-temperature 26

./bin/dtm-query.exe get        --task-id <返回的-task-id> --core-address 127.0.0.1:50051
./bin/dtm-query.exe executions --task-id <返回的-task-id> --core-address 127.0.0.1:50051
./bin/dtm-query.exe list --limit 10 --core-address 127.0.0.1:50051
```

预期：任务 `succeeded`，包含 `sensor-node-001` 与 `cooling-node-001` 结果；模拟传感器报温度 30，冷却结果含 `cooling_started`。

> ⚠️ 这是**执行成功**，不是测得的物理降温，也不证明新的 Root Task 目标谓词。

### 验证

```powershell
go test -count=1 -timeout=15m ./...
go build ./cmd/...
go vet ./...
```

`Makefile` 另提供 `make test` / `make build` / `make verify`，以及 6 个 fuzz target（`make fuzz`，`FUZZTIME` 默认 5s）：`FuzzDecodePresence`、`FuzzHandshakeRequest`、`FuzzObserveAndTick`、`FuzzResourceAdvertisement`、`FuzzNewTask`、`FuzzNativeInvocationRequest`。

### 停止与重来

各终端 Ctrl+C。要全新演示：**先停掉所有进程**，再把 `data/` 目录移到备份位置。

> **不要删除活跃数据库，也不要假设重启会自动重试不确定的副作用。**

---

## Implementation boundaries

（`docs/status.md` + `architecture/baseline.yaml`）

### 已实现（有界）

| 领域 | 状态 | 边界 |
|---|---|---|
| Core/Agent 任务执行 | 现有参考实现与测试 | 模拟 demo；**遗留执行 profile** |
| SQLite 状态、查询与恢复 | 现有参考 profile | **不代表新的 Kernel 对象持久化** |
| 提交去重与执行保护 | 分层实现 | **无 blanket Exactly-once 保证** |
| Resource 身份、目录与映射 | v0.4 参考基础 | Owner / Lease / registration / generation 仍是必需 |
| Discovery、Membership、Coordinator | Level 1 mesh 参考 | 无 multi-Core HA、无跨域自治声明 |
| Resource Invocation | native gRPC 基础，冻结于 M2-R1 | **Binary transport 排除** |
| Kernel 对象与不变量 | 有界实现 | **内部 manager API 不是 User Space ABI** |
| 执行所有权 | Gate B Phase 1 有界收口 | Resource 容量、不可变绑定、受控 dispatch |
| 占用解析 | Gate B Phase 2 有界收口 | **仅 same-fence**；跨 fence 保留/延后 |
| 逻辑执行 facade | 已接受的有界进程内实现 | **无持久化、无远程调用、无物理 ABI** |
| User Space 编排器 + Child/Root 运行时 | 有界实现 | 显式工作与闭包；**取消不等于结束** |
| Task 创建（US-3） | 有界实现 | **仅创建/注册**，不自动规划或执行 |
| Goal Loop 与本地 Workbench | 独立本地实验评审包 | 最终验收待定；**不包含在本候选中** |

### 治理状态（`baseline.yaml`）

| 边界 | 记录值 | 限制 |
|---|---|---|
| K2-C / K2-D / K2-E | `BOUNDED_IMPLEMENTED` / `CLOSED` / `CLOSED` | 仅本地机制与有界收敛 |
| Gate B Phase 1 | `CLOSED`, `BOUNDED_IMPLEMENTED` | 仅 Resource 执行所有权 |
| Gate B Phase 2 | `CLOSED`，设计 `ACCEPTED`，评审 `PASS`，实现 `BOUNDED_IMPLEMENTED` | **仅 same-fence 本地占用解析；Full Gate B 未关闭** |
| Pre-freeze 逻辑 facade | 设计 `ACCEPTED`/`PASS`；实现 `BOUNDED_IMPLEMENTED`；验收 `ACCEPTED`；验证 `PASS` | `internal_manager_dependency: NO`；**`milestone_closed: NO`** |
| 语义治理 | `FROZEN` | 仅已评审的语义包络，不是新运行时行为 |
| Intent 边界 | `FROZEN_SEMANTICS_ONLY` / `NOT_IMPLEMENTED` | 持久不透明 KernelIntent 是前瞻性的 |
| R0 检查点 | `FREEZE_CANDIDATE` | 结转的阶段性检查点，**不是最终冻结** |
| **Full Gate B** | **`RESERVED`** | 无自治闭环完成声明 |
| 物理 ABI / Scope 治理 | `DEFERRED` | 不是传输或委派实现 |
| 完整授权 / 全局跨 manager 一致性 / 生产准入 | `OPEN` | 现有本地机制不关闭这些边界 |
| **最终 Architecture Freeze** | **`NOT_CLAIMED`** | 需要单独评审/签署与可复现基线 |

### 明确不声明

DTM 当前**不声明**：生产就绪、最终 Architecture Freeze、KernelIntent 运行时、生产 Resource Admission、新 Kernel 持久化、远程 ABI、multi-Core HA、Binary transport、无限制派生、functional safety。

> **未知副作用结果不得触发隐式重试。**

### 保留的未来机制（不是禁止）

| 机制 | 状态 | 层 |
|---|---|---|
| CrossFenceResolution | `RESERVED` | 未来所有权/恢复/reconciliation |
| CrossManagerConsistency | `OPEN` | 未来 Kernel 操作边界 |
| MultiCore | `RESERVED` | 未来 Kernel/Mesh 拓扑 |
| RemoteTrust | `DEFERRED` | 未来分布式安全 |
| Membership / RoleSelection / RoleMigration / Gateway | `RESERVED` | Mesh / 治理 / 部署 |
| Discovery / AutonomyScopeHierarchy / IntentReconciliation | `DEFERRED` | Mesh / 自治 / User Space |

（`INV-DEFERRED-NOT-NEGATED`：`DEFERRED`/`RESERVED`/`OPEN`/`NEEDS_REVIEW` **不等于** `FORBIDDEN` 或 `REMOVED`。）

---

## 架构原则（`AP-01` … `AP-11`）

`architecture/baseline.yaml` 的 `active_architecture_principles` 记录了 11 条生效原则（来源 [`AGENTS.md`](AGENTS.md)）。其中**决定本项目形态的四条**：

| ID | 原则 | 为什么它能解释 DTM 的样子 |
|---|---|---|
| **AP-01** | **复杂度必须守卫真实边界**——新机制必须说明它守卫什么安全/正确性/一致性边界、不引入会出什么错、是否有更简单的确定性替代 | 解释了为什么 DTM 不引入共识、PKI、全局目录 |
| **AP-05** | **优先确定性收敛，而非默认共识** | 能由相同输入纯确定性得到唯一结果时，不用共识或时序依赖 |
| **AP-08** | **Authorization Invariant**——任何有副作用的执行必须有明确授权条件，执行侧能验证；任一失败 fail-closed，安全责任不能只落在调用方 | 解释 `CapabilityHandle` 为什么是唯一授权引用 |
| **AP-09** | **未知执行结果不得盲目重试**——物理副作用默认按 `NON_IDEMPOTENT` 处理 | 解释 `UNKNOWN` 为什么不释放占用 |

其余原则（AP-02 Task-oriented 而非 Device-oriented · AP-03 Resource 是自治边界 · AP-04 递归自治 · AP-06 Identity 与 Locator 分离 · AP-07 优先局部命名空间 · AP-10 渐进兼容 · AP-11 明确系统边界）见 [`AGENTS.md`](AGENTS.md)。

---

## 深入材料

顶层的深度细节不放在这份文档里，避免重复权威记录。需要时去：

| 我想查…… | 去这里 |
|---|---|
| **代码怎么组织**（6 个进程 / 41 个包 / 契约 / 迁移 / 配置 / 常见改动落点） | [`docs/code-map.md`](docs/code-map.zh-CN.md) |
| **概念之间什么关系**（对象链 / 状态词汇 / Scope 域 / 术语避坑 / 阅读顺序） | [`docs/concept-map.md`](docs/concept-map.zh-CN.md) |
| **35 个概念的权威定义** | [`architecture/concepts.yaml`](architecture/concepts.yaml) |
| **48 条不变量（含 `severity`）** | [`architecture/invariants.yaml`](architecture/invariants.yaml) |
| **10 个 layer 的责任与"明确不拥有"** | [`architecture/boundaries.yaml`](architecture/boundaries.yaml) |
| **39 条决策** | [`architecture/decisions.yaml`](architecture/decisions.yaml) |
| **完整执行契约（含 same-fence 解析）** | [`docs/execution-contract.md`](docs/execution-contract.md) |
| **Resource 身份与授权** | [`docs/resource-authority.md`](docs/resource-authority.md) |
| **能力状态逐项** | [`docs/status.md`](docs/status.md) |

> `BUILD-INFO.json` 记录的是 2026-10-03 候选的**组装时**状态，因此其
> `public_repository` / `license` / `public_release` 三个字段仍为 `PENDING`。当前取值以
> 根 [`README.md`](README.md) 与 [`LICENSE`](LICENSE) 为准。

---

## 文档索引

**入口**（中英双语，`docs/`）：
[Quickstart](docs/quickstart.md) ·
[Architecture overview](docs/overview.md) ·
[Capability status](docs/status.md) ·
[Examples](docs/examples.md) ·
[Contracts](docs/contracts.md) ·
[Release scope](docs/release-readiness.md)

**下一步主要工作——两条线合并**：
[Integration direction](docs/integration.md) ·
[Recovery and idempotency](docs/recovery-idempotency.md) ·
[Resource authority](docs/resource-authority.md) ·
[Execution contract](docs/execution-contract.md)

**边界与契约**：
[Kernel and User Space boundary](docs/kernel-userspace-boundary.md) ·
[Execution contract](docs/execution-contract.md) ·
[Resource authority](docs/resource-authority.md) ·
[Task model](docs/task-model.md) ·
[Recovery and idempotency](docs/recovery-idempotency.md)

**证据与治理**：
[Architecture State](architecture/README.md) ·
[Semantic freeze sign-off](architecture/reviews/kernel-semantic-freeze-governance-signoff-2026-09-06.md) ·
[Selected development history](docs/history.md) ·
[Candidate validation](docs/validation.md) ·
[Protocol generation](docs/protocol-generation.md)

---

## 许可与维护

Copyright (c) 2026 Zhao Tao（赵涛）。

除另有标识外，本项目自身代码与文档按 **GNU Affero General Public License, version 3 only (`AGPL-3.0-only`)** 许可，见 [`LICENSE`](LICENSE)。第三方组件保留各自许可，见 [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md)。

许可允许商业使用与修改；分发需遵守对应源码义务。**若修改本程序并通过网络让用户与之交互，必须显著地向这些用户提供获取对应源码的机会**（AGPL 第 13 条）。这不要求把改动提交回本仓库，也不要求自动公开每一处私有修改。以许可文本为准；软件在适用法律允许范围内不提供担保。

本项目为个人维护，聚焦分享代码与技术文档，**不对响应时间、长期支持或接受 PR 作出承诺**。分发修改后的网络版本前，请自行提供适当的源码获取机制。

---

<sub>本文档为 README 草稿，面向"第一次接触本项目"的读者，补充 `README.md`（范围声明）之外的概念与结构视角。所有事实性表述均取自 `architecture/` 五份 YAML、`docs/` 现有文档与本地代码结构；未声明的能力一律不作为已实现能力描述。代码规模与构建/测试结果为本机复现记录。</sub>


[English](README.md)
