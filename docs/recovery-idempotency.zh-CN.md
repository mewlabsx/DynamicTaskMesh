# 恢复与幂等

## 持久化参考路径

Core/Agent 参考路径在 SQLite 中保存 Task/Step/Execution、提交键与 Resource 记录。Migration 只追加。注册身份/generation、Owner Lease、Endpoint 验证仍必需。Resource 原子持久化先于目录发布；该路径以数据库状态为权威，不以乐观内存视图为权威。

提交去重保护任务准入，Execution/attempt 重放保护另一边界；二者均不构成 Provider 副作用 exactly-once 保证。恢复保持保守：不能因 Agent 消失、Lease 过期、响应丢失、记录不完整就重放不确定副作用。Lease 过期改变归属资格，不证明没有执行。已支持的参考行为由运行代码和 SQLite 回归测试约束。

## 进程内 Kernel 生命周期

新 Kernel allocation、occupancy、invocation binding 与可信解析状态是有限本地内存机制，不继承旧 SQLite 持久化、重放、崩溃恢复保证。Facade InvocationID 是准入的进程内范围中的稳定关联，不是跨重启全局去重存储。ObserveInvocation 的 NOT_KNOWN 没有权威 crossing 事实，不证明安全重放。

InvocationID+精确 InvocationBinding 防止等价提交导致第二次本地 allocation/crossing；输入改变产生冲突。查询/传输关联 RequestID 不建立新调用。结果 UNKNOWN、占用 UNKNOWN 必须显式保留。物理非幂等操作保持保守，除非 Provider/Capability 契约明确证明重复安全；超时或取消不是这种证明。

## 解析与重启限制

只有绑定精确 allocation 和相同 OwnershipFence 的可信 Resource 权威证据才能解析 post-Provider UNKNOWN 占用。原观察不可变；即使已有 ENDED/RELEASED 和精确 claim 不存在，仍须证明等价既往解析，才可返回 ALREADY_RESOLVED。自动精确释放仅属于接受的权威转换，不是普通重试或清理。

跨 fence 解析、归属权威迁移、取消/终止、持久 Kernel 恢复、远程核对均未纳入实现范围。较新 fence 不能推定旧 UNKNOWN 执行已终止。整合两条路径需要明确持久化权威、副作用身份、归属/lease/fence 映射、崩溃边界，再提供故障/重启证据；仅连接包或保留两套日志不足以完成整合。

参见[执行](execution-contract.zh-CN.md)、[权威](resource-authority.zh-CN.md)、[整合](integration.zh-CN.md)、[验证](validation.zh-CN.md)。历史 PASS 描述原基线，不是新 Kernel 当前崩溃恢复证据。


[English](recovery-idempotency.md)
