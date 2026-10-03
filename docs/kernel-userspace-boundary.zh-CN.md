# Kernel 与 User Space 边界

## 责任归属

Kernel 负责 Resource/Capability/Context/Handle 结构验证、精确执行分配、受控 Provider 派发、有限占用解析和不可变 EventRecord 事实。Manager 的 Go API 是内部实现 API，不是 User Space syscall 或安全 ABI。User Space 负责业务解释、规划、调度策略、Task 生成、World Model、满足度求值、核对和重试策略。Task 是外部策略管理的数据，不是 Kernel Object。Agent 或 LLM 是确定性 Kernel 机制之外的可选策略。

## Intent 边界

机器记录的 interpretation anchors 指向本节当前规则；保留的 section 标签 `2.3 Intent Boundary`、`5 Frozen Decisions / item 9` 标识原 K1 历史陈述，不是本文章节标题。有范围的替代关系与保留方面均不改变。

UserIntent 是 User Space 所有的应用、领域、人或 Agent 意图。Intent Formulation 指解释、规范化并转换为 KernelIntentSpec，与 Satisfaction Planning 不同。KernelIntentSpec 是未来准入边界的规范化数据，业务语义对 Kernel 不透明；它不是计划、Task、Handle 或权威引用。

KernelIntent 是未来被准入的 Kernel 管理持久、不透明表示：Kernel 届时负责身份、持久化/生命周期、修订、归属/Scope 及结构准入/权威语义。业务满足度求值、规划、核对、Task 生成仍在其外部。KernelIntent 不授予 Capability 权威。当前只是 FROZEN_SEMANTICS_ONLY 的架构语义；运行时 NOT_IMPLEMENTED，当前 ObjectReference 验证仍拒绝 ObjectKindIntent。NOT_IMPLEMENTED 不等于 ARCHITECTURALLY_FORBIDDEN。

EvaluationState 属于 User Space，不是 KernelIntent 的必要生命周期维度。SATISFIED/UNSATISFIED 需要已验证 Observation 或确定性机制；UNKNOWN 合法。EvaluationState UNKNOWN 不等于调用结果 UNKNOWN，满足度不确定不能覆盖执行不确定。相互矛盾的观察及其来源必须可区分，不能静默合并。Kernel 不接受隐式概率或 LLM 求值。

## Scope 与准入

ExecutionAuthorityScope 要求 Context/Handle 严格相等，不授予层级权威或 Delegation。AutonomyScope、IntentScope、RoleScope 仍为 RESERVED；MeshOrRegionScope 为 DEFERRED。Region 或发现分组不构成执行权威。

本地 K2-E bridge 消费已经权威的 ResourceRecordView。投影和发现不会完成生产权威准入，也不会铸造 Handle。更广泛 Resource Admission、完整授权和 K1 全局跨 Manager 一致性仍为 OPEN。安全与权威检查见[资源权威](resource-authority.zh-CN.md)。

## 生效状态与治理

五份当前[架构状态 YAML](../architecture/README.md)记录生效事实及有范围的替代关系。保留的[语义治理签署](../architecture/reviews/kernel-semantic-freeze-governance-signoff-2026-09-06.md)冻结已审查的语义范围。普通新文档、测试、重构或较新日期不能覆盖它。修改冻结边界需要显式限定范围的决策、Boundary Delta、审查和治理批准。

R0 仍是延续的阶段检查点/FREEZE_CANDIDATE。DEFERRED、RESERVED、OPEN、NEEDS_REVIEW 不等于 FORBIDDEN 或 REMOVED。当前语义治理 FROZEN 不等于最终架构冻结；Final Architecture Freeze 仍为 NOT_CLAIMED。Full Gate B 仍为 RESERVED，未来机制需要单独设计和授权。参见[状态](status.zh-CN.md)；历史验收状态不是本次发行的新验证。


[English](kernel-userspace-boundary.md)
