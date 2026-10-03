# 运行路径整合方向

[English](integration.md) | 简体中文

当前 Core/Agent 命令使用既有参考执行路径；有限范围的 Kernel 与 User Space 实现仍是独立路径。本预览不宣称已形成整合后的自主运行时。

下一步方向是形成唯一主要执行路径：显式任务输入 → User Space 任务/执行编排 → Kernel logical facade → Provider 适配器 → 既有 Agent。现有 CLI、传输和查询功能，在契约仍成立的地方应复用。

实施前明确稳定的 task/work/invocation/allocation/attempt 绑定、registry 到 Kernel 的授权映射、Provider 结果与占用证据，以及唯一执行 owner。旧重试、重新映射与恢复不能同时控制新路径拥有的工作。发现记录不是执行授权；旧注册 generation 不自动等于 Kernel ownership fence。

Kernel 的权威和占用目前包含内存状态，而既有 Core 从 SQLite 恢复参考状态。整合必须明确重启行为。首个有限范围场景可以采用持久隔离和 fail-closed 恢复，不必立即完成全部 Kernel 持久化；但不能静默重投未解决工作，也不能重启后把资源重新构造为空闲。

首个整合场景验收应展示真实命令到 Agent 的执行、无副作用的授权拒绝、重复请求保护、`UNKNOWN` 可见且不隐式重试/释放，以及崩溃和重启边界的安全行为。既有参考场景须继续通过；仅正常执行成功不够。

仅当公开主要场景、查询/幂等行为和已声明恢复保证均被覆盖，旧调度不存在旁路，且独立审查无阻塞发现时，才能切换默认路径。旧执行归属逻辑随后退出，可复用兼容适配器继续保留。

目标是避免两条并行功能开发路线：新执行功能应面向 Kernel/User Space 边界，旧路径在迁移期间仅接受必要兼容和正确性工作。本文提出方向，不授权实施，不改变冻结里程碑状态。完整自主运行、Intent runtime 和更广泛 admission 不是首个有限范围场景的前提。
