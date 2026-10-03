# 当前关键设计契约

本次发行按主题公开当前设计。逐版本计划、详细实现报告、审查、发现记录及原始证据归档保留在本地/私有范围，不复制为测试 fixture，也不移到另一个公开目录。

| 主题 | 契约 |
|---|---|
| Task、Step、Execution 与 Root/Child/work | [任务模型](task-model.zh-CN.md) |
| 机制/策略、Intent、Scope 与治理 | [Kernel/User Space 边界](kernel-userspace-boundary.zh-CN.md) |
| 逻辑请求/结果、副作用与同 fence 解析 | [执行契约](execution-contract.zh-CN.md) |
| 资源身份、Capability 权威、安全与容量 | [资源权威](resource-authority.zh-CN.md) |
| SQLite 路径、本地身份、不确定性与重启限制 | [恢复与幂等](recovery-idempotency.zh-CN.md) |

五份[架构状态 YAML](../architecture/README.md)保留生效 ID、有范围替代关系、不变量、状态和未来边界。文档引用指向这些当前主题；原私有来源及哈希另行保存。保留的治理签署属于必要语义权威，不是新增验收或本次发行验证。[状态](status.zh-CN.md)说明有限实现，[验证](validation.zh-CN.md)记录实际检查。

收敛改变公开范围与文档检查位置，不改变运行行为、冻结范围或里程碑状态。Core/Agent 与 Kernel/User Space 尚未形成完全整合的运行时。参见[整合方向](integration.zh-CN.md)。


[English](contracts.md)
