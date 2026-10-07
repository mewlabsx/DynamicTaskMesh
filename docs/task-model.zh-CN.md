# 任务模型

[English](task-model.md) | 简体中文

## 两条执行路径

现有 Core/Agent 路径使用 Task、Step、Execution：Task 承载任务，Step 是可调度的工作单元，Execution 记录选定目标上的一次执行尝试。Planner、Mapper、Runtime 与 gRPC Agent 协作，SQLite 保存这条路径的记录。它尚未被新的 Kernel/User Space 组件替代，也未与后者完成整合。

新组件使用显式的 User Space work 与 Root/Child Task。它们是进程内的策略记录，不是 Kernel Object，也不是自动规划器。软件版本与 Kernel 规范版本是独立维度；软件 v0.7 不表示 Kernel v0.7。

## Root 与 Child 的归属

Root Runtime 保存调用方提供的 Root 身份、不透明的 GoalDescriptor、有限 Child 成员集合和目标级生命周期。成员集合显式处于 OPEN/CLOSED。Root 状态包括 OPEN、ACTIVE、SUCCEEDED、FAILED、UNKNOWN、CANCELLED。调用方提供的确定性闭合谓词根据权威 Child 快照返回 SUCCEEDED/FAILED/UNKNOWN。Child 成功不等于 Root 目标满足；运行时不把目标解释成提示词，也不自动生成计划。

Child Runtime 保存不可变的已选工作绑定和本地生命周期。编排器通过窄 Kernel 端口提交显式工作，再投影返回的执行事实；它不制造 Kernel 权威或占用证据。RootTaskRef、ChildTaskRef 只是谱系引用，不授予权威。当前有限模型中，一个选定 Child work 最多对应一次 Capability 调用；复杂工作流必须显式拆分，不能隐藏额外派发。

## 创建与取消

US-3 的 CreateRoot 只委派 Root 创建；CreateChild 预留具有精确 Root 谱系、权威且初始纯净的 CREATED Child；RegisterChild 重新读取权威 Child 快照，只注册其身份，不能接受调用方自造的快照。创建、成员注册、执行和闭合求值是不同操作。失败不会隐式触发补偿、重试或新增工作。

User Space 的取消请求不证明 Provider 已终止、占用已 ENDED 或 Resource claim 可释放。未解析执行保持 UNKNOWN，必须通过已有权威边界核对。这些本地任务组件不提供自动重试、重映射、重新规划、崩溃恢复或持久身份保证。另行审查的 Goal Loop/Workbench 实验未纳入本次发行。

## 代码与相关契约

参见 [User Space](../internal/userspace/)、[任务创建](../internal/userspace/taskcreation/)、[Root runtime](../internal/userspace/roottask/) 和[执行契约](execution-contract.zh-CN.md)。[整合方向](integration.zh-CN.md)说明何时可将两条路径收敛为统一支持的路径。
