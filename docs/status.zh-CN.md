# 能力状态

[English](status.md) | 简体中文

软件线：v0.7；Kernel 规范：v0.1。本文描述当前源码与已有证据；本轮候选验证另见[验证记录](validation.zh-CN.md)。

| 领域 | 源码／状态 | 边界 |
|---|---|---|
| Core/Agent 任务执行 | 既有参考实现和测试 | 模拟演示、旧执行路径 |
| SQLite 状态、查询和恢复 | 既有参考路径 | 不代表新 Kernel 对象持久化 |
| 提交去重和执行保护 | 各层分别实现 | 没有全面 Exactly-once 保证 |
| Resource 身份、目录和映射 | v0.4 参考基础 | 仍须满足 owner、lease、registration 和 generation |
| 发现、成员关系和协调者 | Level 1 Mesh 参考 | 不宣称多 Core 高可用或跨域自治 |
| Resource Invocation | 原生 gRPC 基础，在 M2-R1 冻结 | 排除 Binary transport |
| Kernel 对象与不变量 | 有限范围实现 | 内部 manager API 不是 User Space ABI |
| 执行归属 | Gate B Phase 1 有限范围验收闭合 | Resource 容量、不可变绑定、受控派发 |
| 占用解析 | Gate B Phase 2 有限范围验收闭合 | 仅同 fence；跨 fence 仍保留／延后 |
| Logical Execution Facade | 已接受的有限范围进程内实现 | 无持久化、远程调用或物理 ABI |
| User Space 编排与 Child/Root runtime | 有限范围实现 | 显式工作与闭合；取消不等于结束 |
| 任务创建 | US-3 有限范围实现 | 仅创建／注册，不自动规划或执行 |
| Goal Loop 与 Workbench | 单独本地实验审查包 | 最终验收待完成，不纳入候选 |

当前语义冻结不等于最终 Architecture Freeze。完整自主 Gate B、KernelIntent runtime、生产 Resource Admission、更广泛授权、新 Kernel 持久化及远程协议仍不在已实现能力声明内。未来范围仍是未来范围，不是永久的架构禁止。

已有历史 PASS 记录分别对应自身的源码版本、环境和有限范围测试。它们引用的原始证据档案未随候选分发，也未针对本候选重新核验；不能把缺失档案表述成近期验证已经完成。

## 已记录状态

这些是选定开发基线中延续的工程验收/治理事实，不是新增审查或本次发行测试结果。详细阶段报告和原始证据保留在本地。早期 DESIGN_ONLY、NOT_IMPLEMENTED、REVIEW_PENDING 阶段标题仍属于历史，不覆盖当前记录。

| 状态边界 | 记录值 | 限制 |
|---|---|---|
| K2-C / K2-D / K2-E | BOUNDED_IMPLEMENTED / CLOSED / CLOSED | 本地机制与有限收敛 |
| Gate B Phase 1 | CLOSED、BOUNDED_IMPLEMENTED | 仅 Resource 执行归属 |
| Gate B Phase 2 | CLOSED；design ACCEPTED；review PASS；implementation BOUNDED_IMPLEMENTED；implementation review PASS | 本地同 fence 占用解析，Full Gate B 未关闭 |
| 预冻结 Logical Facade | design ACCEPTED/PASS；implementation BOUNDED_IMPLEMENTED；acceptance ACCEPTED、verification PASS | runtime_changes BOUNDED_LOGICAL_FACADE_ONLY；internal_manager_dependency NO；milestone_closed NO |
| 语义治理 | FROZEN | 仅已审查语义，不是新增运行行为 |
| Intent 边界 | FROZEN_SEMANTICS_ONLY / NOT_IMPLEMENTED | 持久不透明 KernelIntent 是未来表示 |
| R0 检查点 | FREEZE_CANDIDATE | 延续检查点，不是最终冻结 |
| Full Gate B | RESERVED | 不宣称自主循环完成 |
| 物理 ABI / Scope 治理 | DEFERRED | 不实现传输或委派 |
| 完整授权 / 全局跨 Manager 一致性 / 生产准入 | OPEN | 已有本地机制未关闭这些边界 |
| Final Architecture Freeze | NOT_CLAIMED | 需要独立审查/签署及可复现基线 |

精确范围见[执行](execution-contract.zh-CN.md)、[边界](kernel-userspace-boundary.zh-CN.md)、[权威](resource-authority.zh-CN.md)。机器记录中的 closure_record、implementation_document 等字段现指向本状态汇总，不表示原详细报告公开可下载。原哈希与映射保留在本地发行审计材料中。
