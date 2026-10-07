# DTM Kernel 概念地图

[English](concept-map.md) | 简体中文

> **这份文档是 [`architecture/concepts.yaml`](../architecture/concepts.yaml) 的阅读地图。**
> `concepts.yaml` 是**权威定义**；本文档不重复它的 `definition` 字段，而是补上它没有的三样东西：
> **① 概念之间的对象关系；② 建议的阅读顺序；③ 容易混淆的术语避坑清单。**
>
> ⚠️ 如果本文档与 `concepts.yaml` 冲突，**以 `concepts.yaml` 为准**。

---

## 1. 先建立三条直觉

在读任何概念之前，先记住这三条——它们解释了为什么 DTM 的对象模型长成这样：

| # | 直觉 | 直接后果 |
|---|---|---|
| 1 | **身份不等于授权** | `Resource` 和 `CapabilityInstance` 都**不是**授权；必须持有一个 `CapabilityHandle` |
| 2 | **观测不等于状态** | Provider 报回来的东西是**证据**，不是权威状态；中间必须经过准入 |
| 3 | **Task 在核外** | `Task` 不是 Kernel Object；Kernel 只保留不透明的血缘引用 |

---

## 2. Kernel 对象关系图

（`D-KERNEL-OBJECT-FOUNDATION`，状态 `FROZEN`）

```text
┌─────────────────────────────────────────────────────────────────────┐
│  Resource                                        【FROZEN】          │
│  自治边界与 Provider 边界。不是 Capability，不是通用权限。            │
│  拥有执行容量（固定无权重槽位，有效默认 1）                            │
│                                                                     │
│      │ 通过 CapabilityDeclaration 声明能力                            │
│      ▼                                                              │
│  CapabilityDeclaration                           【FROZEN】          │
│  Resource 附带的能力契约，可产出 CapabilityInstance                   │
│                                                                     │
│      │ 可产生                                                        │
│      ▼                                                              │
│  CapabilityInstance                              【FROZEN】          │
│  Provider 支撑的 Kernel 状态。★ 本身不是授权                          │
│                                                                     │
│      │ 被引用                                                        │
│      ▼                                                              │
│  CapabilityHandle                                【FROZEN】          │
│  ★ 唯一的授权引用。context 绑定 + ExecutionAuthorityScope 绑定        │
│                                                                     │
│      ├── 绑定到 ──► ExecutionContext              【FROZEN】          │
│      │              调用方执行边界：subject / ExecutionAuthorityScope │
│      │              / lifecycle / 派生的 availability 与绑定能力授权  │
│      │                                                              │
│      └── Scope 必须与 ExecutionContext.Scope 【严格相等】             │
│          不匹配 → fail-closed，且不蕴含其他 Scope 域的相等            │
└─────────────────────────────────────────────────────────────────────┘

  EventRecord                                      【FROZEN】
  不可变审计事实。★ 不是当前状态；User Space 不得发布或改写。
```

### 执行侧对象链

（Gate B 引入；`ExecutionAllocation` 只解决"这次执行占用了一个 Resource 容量"这一件事）

```text
ExecutionRequest                                  【BOUNDED_IMPLEMENTED】
  └─ RootTaskRef / ChildTaskRef 是【不透明】血缘值
     Kernel 不拥有 task 生命周期、优先级、排序、调度、规划或满意度评估

ExecutionDescriptor                               【BOUNDED_IMPLEMENTED】
  └─ 授予 Resource 容量时冻结的规范执行绑定
     内嵌在 ExecutionAllocation 中，★ 不是可单独寻址的对象

ExecutionAllocation                               【BOUNDED_IMPLEMENTED】
  └─ Kernel 管理的运行时对象，证明"一次具体执行已获授 Resource 容量"
     内嵌不可变 ExecutionDescriptor
     Lifecycle 只有 RUNNING / RELEASED
     ★ 持有 CurrentOccupancy（当前占用的权威结论）

ExecutionObservation                              【BOUNDED_IMPLEMENTED】
  └─ 在 Provider 边界捕获的【不可变历史观测】
     分 ExecutionOutcome 与 OccupancyConclusion 两个正交维度
     ★ 不是 Kernel 的当前权威占用结论；后续解析【不能改写它】

CurrentOccupancy                                  【ACCEPTED / BOUNDED_IMPLEMENTED】
  └─ ExecutionAllocation 上的当前权威占用结论
     ★ kernel_object: false（是 allocation 的一个维度，不是独立对象）

ResolutionEvidence                                【ACCEPTED / BOUNDED_IMPLEMENTED】
  └─ 经验证的【状态转移输入】，在授权来源证明占用已 ENDED 时可被准入
     ★ 不是 Kernel Object、不是生命周期、不是 EventRecord、不是 manager/store

OccupancyResolutionAuthority                      【ACCEPTED / BOUNDED_IMPLEMENTED】
OwnershipFence                                    【ACCEPTED / BOUNDED_IMPLEMENTED】
```

> **`kernel_object: false` 是个重要标记**：`CurrentOccupancy`、`ResolutionEvidence`、`OccupancyResolutionAuthority`、`OwnershipFence` 都**不是独立可寻址的 Kernel 对象**，只是 allocation 的状态维度或转移输入。**不要为它们建 manager 或 store。**

---

## 3. 状态词汇表（三套，不要混用）

`INV-LIFECYCLE-AVAILABILITY-SEPARATION`（`CRITICAL`）：**生命周期与可用性是分离的词汇表。**

| 词汇表 | 归属于 | 取值 | 注意 |
|---|---|---|---|
| **LifecycleState** | 对象生命周期 | 见下表 | `UNAVAILABLE` **不是**生命周期状态 |
| **AvailabilityState** | 可用性 | 含 `UNAVAILABLE` | 不是生命周期 |
| **EvaluationState** | User Space / 未来评估边界 | `SATISFIED` / `UNSATISFIED` / `UNKNOWN` | ★ **不是** KernelIntent 的必需生命周期维度 |

### Resource 生命周期是一个严格子集

`INV-RESOURCE-LIFECYCLE-SUBSET`（`HIGH`）：

```text
Resource 接受：CREATED · ACTIVE · SUSPENDED · REMOVED
Resource 不接受：TERMINATED（★ 不是 Resource 的生命周期状态）
```

### 执行占用的三个结论

| `CurrentOccupancy` | 含义 | 精确 claim |
|---|---|---|
| `NOT_ESTABLISHED` | Provider 边界**尚未**跨越 | HELD |
| `UNKNOWN` | 边界已跨越，**无法证明占用已结束** | **仍 HELD** |
| `ENDED` | 有权威证据表明**不可能再占用**该 Resource | NONE（必须已释放） |

> **`UNKNOWN` 不是**"未开始"、**不是**失败、**不是**超时（`INV-UNKNOWN-OCCUPANCY-RETAINS-CLAIM`）。
> 而且**执行结果的确定性**与**占用结论的确定性**是**正交**的两个维度（`INV-OBSERVATION-OUTCOME-OCCUPANCY-SEPARATION`）。

### ExecutionAllocation 生命周期

```text
RUNNING ──► RELEASED        （只有两个值）
```

配套不变量：

- `INV-RELEASED-IMPLIES-ENDED`：`Lifecycle == RELEASED` ⟹ `CurrentOccupancy == ENDED` 且精确 claim 为 NONE
- **一个 RELEASED 的 allocation 不能保留 `UNKNOWN` 占用**

---

## 4. Scope 域：五个名字，只有一个能当授权用

`INV-SCOPE-DOMAIN-SEPARATION`（`CRITICAL`）——**它们之间不蕴含包含、继承、相等或委派关系。**

| # | Scope | 状态 | 含义 | 能当授权吗 |
|---|---|---|---|---|
| 1 | **`ExecutionAuthorityScope`** | `FROZEN` | 当前 K1 的 Handle/Context 授权域 | ✅ **唯一能用**。`CapabilityHandle.Scope` 必须与 `ExecutionContext.Scope` 相等 |
| 2 | `AutonomyScope` | `RESERVED` | 未来自治/参与边界 | ❌ **未映射**到当前 Handle/Context 字段 |
| 3 | `IntentScope` | `RESERVED` | 意图边界 | ❌ 与 `ExecutionAuthorityScope` **不同**，不授予执行授权 |
| 4 | `RoleScope` | `RESERVED` | 未来角色资格/治理 | ❌ 角色选择**不**授予或扩大 Handle 授权 |
| 5 | `MeshOrRegionScope` | `DEFERRED` | Mesh/Region 搜索、信任、故障边界 | ❌ 区域/发现分组**不是**执行授权 |

> ⚠️ **`Resource.Scope` 不自动是 `ExecutionAuthorityScope`。**

---

## 5. 五个必须区分的边界概念

这五个概念最容易被当成"一件事"，但它们**全部是分离的**：

| 概念 | 归属 | 一句话 | 关键不变量 |
|---|---|---|---|
| **`Observation`** | Provider / 外部证据源；Kernel 准入归属 **`OPEN`** | 关于物理/外部世界的**报告** | `INV-OBSERVATION-EVIDENCE-SEPARATION`：准入前是**不可信或有界证据** |
| **`KernelStateTransition`** | Kernel 对象管理器 / 准入边界 | 权威状态**变更** | 观测或 provider 结果**不能**自行成为状态转移 |
| **`EventRecord`** | Kernel / EventManager | 不可变审计**事实** | `INV-EVENT-IMMUTABLE-SEMANTIC`：User Space **不能**发布或改写 |
| **`ProviderDeclaration`** | Provider/Adapter → Resource 准入 | provider 的**声明** | `DEFERRED`：**不**自行建立 Resource 或 Capability 授权 |
| **`RegistrationRequest`** | Provider/Node/Runtime → Resource 准入 | 携带准入**主张** | `DEFERRED`：**不是** Resource 身份，**不**证明 legacy fence 已满足 |

**权威状态转移的唯一合法通路**（`D-R0-OBSERVATION-EVENT-SEPARATION`）：

```text
物理/外部世界
  → ProviderEvidence / Observation        ← 不可信证据
    → 校验 / 准入                           ← ★ 唯一的关卡
      → 权威 Kernel 状态转移
        → 不可变 Kernel EventRecord
```

---

## 6. Intent 三层

（`D-R0.1-INTENT-DUAL-LAYER` + `D-R0.1-INTENT-PERSISTENT-REPRESENTATION`，均 `FROZEN_SEMANTICS_ONLY`）

| 层 | 归属 | 是什么 | 状态 |
|---|---|---|---|
| **`UserIntent`** | User Space | 应用/领域/人/agent 的意图，**可有歧义、可含策略** | `FROZEN_SEMANTICS_ONLY` |
| **`KernelIntentSpec`** | User Space → Kernel 准入边界 | **规范化后的期望态边界数据**，业务语义对 Kernel **不透明** | `FROZEN_SEMANTICS_ONLY` |
| **`KernelIntent`** | Kernel / 未来准入边界 | **未来**被准入的 Kernel 管理持久不透明表示 | `FROZEN_SEMANTICS_ONLY`，运行时 **`NOT_IMPLEMENTED`** |

```text
UserIntent
   │ Intent Formulation —— User Space 的职责：解释、规范化、翻译
   ▼
KernelIntentSpec        ← 边界数据。不是 Kernel Object / 授权引用 / Task / 计划 / Handle
   │ 未来准入边界
   ▼
KernelIntent            ← Kernel 拥有：身份、持久化/生命周期、revision、所有权/scope、
                           结构准入与授权语义
                          Kernel 不拥有：业务语义解释、满意度评估、World Model、
                           reconciliation、Satisfaction Planning、Task 生成、能力授权
```

**三条硬约束**：

| 不变量 | 规则 |
|---|---|
| `INV-R0.1-INTENT-LAYER-SEPARATION` | 两层**不能**互相静默替代 |
| `INV-R0.1-KERNELINTENT-NOT-EXECUTION-AUTHORITY` | KernelIntent **不是** Handle、Instance、Resource、Context、授权证明、Task owner、Planner、Scheduler、评估器、World Model 或隐式执行请求 |
| `INV-R0.1-EVALUATION-REQUIRES-OBSERVATION` | `SATISFIED`/`UNSATISFIED` **需要**经验证的观测或另一个确定性机制；`UNKNOWN` 在证据出现前**始终有效** |

> **`Task` 关系规则**：一个 `KernelIntent` 可产生 **0 / 1 / 多**个 Task；一个 `UserIntent` 可产生 **0 / 1 / 多**个 `KernelIntent`。映射与生成策略归 User Space。
>
> **`ObjectKindIntent` 当前被 Kernel ObjectReference 校验拒绝**（`D-R0-INTENT-REFERENCE-BOUNDARY`）。
> `NOT_IMPLEMENTED` **不等于** `ARCHITECTURALLY_FORBIDDEN`。

---

## 7. Task 与 Capability 的血缘基数

`INV-TASK-EXTERNAL-POLICY-MANAGED`（`CRITICAL`，`FROZEN_SEMANTICS_ONLY`）：

> **Task 由 User Space / Policy Layer 拥有。** 它不是 Kernel Object、不是 Kernel 管理的生命周期、不是 Kernel 调度实体。
> Kernel **不**创建、**不**分解、**不**调度、**不**重试、**不**聚合、**不**完成、**不**评估 Task。
> `RootTaskRef` 和 `ChildTaskRef` 是**不透明血缘引用**——Kernel 只借它们说明"这次执行从哪来"。

**血缘基数规则**（外部 Task/Policy 契约，非 Kernel Object 规则）：

```text
一个 Child Task
  ├─ 至多 ── 1 个具体的 Capability 执行请求
  ├─ 至多 ── 1 个 ExecutionRequest
  └─ 至多 ── 1 次 Capability 调用
```

> 更广的工作流**需要显式分解**，**不能**靠隐式的额外 dispatch 实现。

---

## 8. 授权与分配的四个层次

这四件事**依次发生，互不替代**：

```text
① PermissionSet              （元数据载体）
   D-PERMISSIONSET-METADATA：只做结构校验。不是 RBAC、不是 ACL、
   不是策略决策、不是完整授权证明。

② 授权校验                   （capability authority）
   INV-CAPABILITY-AUTHORITY：需要有效 CapabilityHandle。
   INV-HANDLE-CONTEXT-SCOPE：Handle.Scope 必须 == ExecutionContext.Scope。

③ 容量分配                   （allocation）
   INV-ALLOCATION-IS-NOT-AUTHORITY：★ 有效 Handle + 授权校验
   【不会创建】ExecutionAllocation。容量只被 TryAllocateExecution 消耗。

④ Provider 调用              （dispatch）
   INV-ALLOCATION-DISPATCH-BARRIER：★ 没有有效的 RUNNING
   ExecutionAllocation = 【零次】Provider 调用。
   一个 allocation 至多占用一次正常 dispatch 边界。
```

**三种授权角色互相独立**（`INV-RESOLUTION-AUTHORITY-SEPARATION`）：

| 角色 | 由谁持有 | 不能推出什么 |
|---|---|---|
| **Execution Authority** | `CapabilityHandle` | 不含 Occupancy Resolution Authority |
| **Provider Authority** | Provider 自身契约 | 除非契约明确说明，不含解析授权 |
| **Occupancy Resolution Authority** | Resource 执行所有权权威 | 只能解析**与自身 fence 精确相同**的 allocation |

---

## 9. 术语避坑清单

### `NEEDS_REVIEW` 的术语过载项

这些名字在仓库里**有多个含义或没有冻结的归属**。改代码前**必须**查 `concepts.yaml` 的 `owner` 与 `definition`。

| 术语 | 问题 | 状态 |
|---|---|---|
| **`Capability`** | 历史材料把它当匹配/值概念；当前 Kernel 冻结的是 `CapabilityDeclaration` / `CapabilityInstance` / `CapabilityHandle` **三个不同概念**，**没有**独立的 K0.5 Kernel 对象叫 `Capability` | `NEEDS_REVIEW` |
| **`State`** | 当前 Kernel 材料区分 `LifecycleState` 与 `AvailabilityState`；`EvaluationState` 是 User Space/未来词汇 | `NEEDS_REVIEW` |
| **`Context`** | `ExecutionContext` 是冻结的 Kernel 概念；但通用 `Context` 也指 legacy Runtime 的取消/deadline 传递 | `NEEDS_REVIEW` |
| **`Runtime`** | `risk: concept_collision`。legacy Runtime 执行 mapped plan；K1 材料用 "Kernel Runtime" 指一个**语义边界**，其运行时**未实现**。**不同上下文不必然指同一个概念** | `NEEDS_REVIEW` |
| **`Memory`** | 在已评审材料中**未找到** K0.5/K1 的定义或 Kernel 对象 | `NEEDS_REVIEW` |
| **`Membership`** | 自治材料讨论 Runtime 参与与成员问题，但 K0.5 对象模型**没有** Membership 对象或冻结归属 | `NEEDS_REVIEW` |

### 已被部分取代的历史术语

| 术语 | 状态 | 说明 |
|---|---|---|
| **`Intent`**（无限定） | `FROZEN`（`superseded_umbrella_concept`） | 仅作历史与兼容伞形记录保留。当前架构区分 `UserIntent` / `KernelIntentSpec` / `KernelIntent` |

---

## 10. 概念状态总表

（按 `concepts.yaml` 的 `owner` / `layer` 归类；共 35 个概念）

### Kernel 对象（跨整个对象链）

| 状态 | 概念 |
|---|---|
| `FROZEN` | `Resource`、`CapabilityDeclaration`、`CapabilityInstance`、`CapabilityHandle`、`ExecutionContext`、`EventRecord`、`KernelStateTransition`、`ExecutionAuthorityScope` |
| `BOUNDED_IMPLEMENTED` | `ExecutionRequest`、`ExecutionDescriptor`、`ExecutionAllocation`、`ExecutionObservation` |
| `ACCEPTED`（`BOUNDED_IMPLEMENTED`） | `CurrentOccupancy`、`ResolutionEvidence`、`OccupancyResolutionAuthority`、`OwnershipFence` |

### 核外 / 边界数据 / 未来机制

| 状态 | 概念 |
|---|---|
| `FROZEN_SEMANTICS_ONLY` | `Task`、`UserIntent`、`KernelIntentSpec`、`KernelIntent` |
| `RESERVED` | `AutonomyScope`、`IntentScope`、`RoleScope`、`AutonomousLoop` |
| `DEFERRED` | `ProviderDeclaration`、`RegistrationRequest` |
| `NEEDS_REVIEW` | `Capability`、`Observation`、`State`、`Memory`、`Context`、`Membership`、`Runtime` |

> **完整的 `definition` / `owner` / `sources` 字段以 [`architecture/concepts.yaml`](../architecture/concepts.yaml) 为准。**

---

## 11. 建议阅读顺序

对第一次接触 Kernel 对象模型的人：

| 顺序 | 读什么 | 目的 |
|---|---|---|
| 1 | **本文档第 1–3 节** | 建立"身份 ≠ 授权、观测 ≠ 状态、Task 在核外"三条直觉，搞清状态词汇表 |
| 2 | [`docs/kernel-userspace-boundary.md`](kernel-userspace-boundary.md) | Kernel 与 User Space 的职责划分 |
| 3 | [`architecture/boundaries.yaml`](../architecture/boundaries.yaml) | 10 个 layer 的当前责任与**明确不拥有**什么 |
| 4 | **本文档第 2、4 节**回看 | 对象链 + Scope 域 |
| 5 | [`architecture/invariants.yaml`](../architecture/invariants.yaml) | 48 条不变量，按 `severity` 筛 `CRITICAL` |
| 6 | [`docs/execution-contract.md`](execution-contract.md) | 执行所有权与 same-fence 解析的完整契约 |
| 7 | [`docs/task-model.md`](task-model.md) | Root/Child Task 所有权与血缘 |

> 只想快速了解系统定位的人，读仓库根 `README` 与本目录的 `overview` 即可，**不必**进入本文档。

---

<sub>本文档是概念地图草稿，配合 [`architecture/concepts.yaml`](../architecture/concepts.yaml) 使用。凡与 YAML 权威记录冲突之处，以 YAML 为准。对象链与状态词汇取自 `concepts.yaml`、`invariants.yaml`、`boundaries.yaml` 与 `docs/` 四篇契约文档；`kernel_object: false` 标记取自 `concepts.yaml` 各条目字段。</sub>
