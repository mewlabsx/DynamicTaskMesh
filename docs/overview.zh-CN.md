# 架构概览

[English](overview.md) | 简体中文

DTM 明确区分任务策略、资源身份与执行边界。仓库包含多个演进层次，不能把各自能力合并描述为未经验证的完整生产系统。

## 既有执行参考路径

```text
dtm-submit → Core Task 服务 → Planner → Mapper → Runtime
                                              → gRPC Agent → 模拟 Capability
dtm-query  → Core 查询服务 → SQLite Task/Step/Execution 状态
```

参考路径通过能力/资源发现、节点归属、租约和 fencing 选择有效执行目标。SQLite 存储该路径的状态，迁移只追加，恢复保持保守。提交去重与执行重放契约保护不同边界，均不能单独保证副作用 Exactly-once。

Level 1 Mesh 通过 `internal/mesh` 与 `internal/runtimehost` 增加发现、握手、成员关系、资源视图和确定性协调者选择。v0.6 Invocation 基础提供与传输无关的调用语义及原生 gRPC 参考实现。这些是兼容参考路径，不代表多 Core 高可用或已实现 Binary transport。

## 有限范围的 Kernel 与 User Space

```text
显式 User Space 策略／选定工作
  → taskcreation: CreateRoot / CreateChild / RegisterChild
  → Child Task Runtime → User Space Execution Orchestrator
  → 窄 Kernel 接口 → Logical Execution Facade → Provider 接口

Root Task Runtime ← Child 的权威快照
                  → 显式闭合判定条件
```

Kernel 拥有 Resource、Capability、Execution 和不可变 EventRecord 机制。CapabilityHandle 是能力授权引用；执行授权作用域严格相等，不意味着委派或作用域层级。Resource 和 Capability 是不同概念。

Facade 是进程内组合边界，不是物理 syscall 或 RPC ABI。Gate B Phase 1 提供有限范围的 Resource 执行容量归属和受控派发；Phase 2 仅在相同 ownership fence 下解析跨越 Provider 后的精确占用。Provider 观察、权威状态转换和审计事件分别处理。

User Space 拥有业务解释与策略。US-3 只把已决定的任务具体化，不自动规划、重试、派生工作或评估 Root。Child 执行成功不能证明 Root 目标成功。取消不能证明 Provider 已结束；`UNKNOWN` 必须明确保留，不能变成隐式重试或释放。

## 权威依据与未来范围

[Architecture State](../architecture/README.md)、有效语义治理记录及其纠正文件定义当前事实。[能力状态](status.zh-CN.md)概述边界。历史路线图或已完成设计不能把未来机制提升为已实现能力。

UserIntent 属于 User Space。KernelIntentSpec 和 KernelIntent 描述未来不透明表示的边界，当前 Intent 校验仍禁用。完整自主 Gate B、生产 admission、新 Kernel 持久化、远程 ABI 与最终 Architecture Freeze 均不在当前声明内。


## 参考状态与任务边界

既有 SQLite 路径拥有自己的 Task/Step/Execution 记录、提交键及保守重启行为。提交去重和执行 attempt 重放保护不同边界，均不能保证副作用恰好一次或 Kernel 权威持久化。Resource Invocation 仍是有限范围原生 gRPC 参考，不意味着 Binary transport 或更广泛的冻结实现。

User Space 拥有本地工作与任务投影、显式 Child 选择、Root 闭合条件和已决定任务的创建。Kernel 拥有执行 allocation、authority 和 occupancy 事实。Child 快照用于 Root 评估，Child 成功仍不等于 Root 目标成功。这些新机制位于进程内，不自动继承参考路径的 SQLite 恢复。移除历史实施报告不改变代码边界或验收状态。
