# 资源身份与权威

[English](resource-authority.md) | 简体中文

## 参考路径

Resource 是自治/提供方归属边界，不等于 Capability、设备或网络地址。Resource 显式声明 CapabilityDeclaration；声明可产生 CapabilityInstance，CapabilityHandle 在 ExecutionContext 中引用实例。身份与 locator、endpoint、拓扑不同。设备/总线映射位于 Owner/Gateway 适配层之后。发现公布候选事实，不构成授权。

现有目录/映射路径要求 Owner Node、有效 Lease、Endpoint、registration_id、generation。注册替换与过期 generation 不能静默保留执行资格。这些检查是参考路径的 fencing 规则，尚未映射为新 Kernel OwnershipFence，也未形成共同的持久权威模型。

## 安全边界

CapabilityHandle 是唯一 Capability 权威引用。CapabilityInstance ID、ResourceID、ProviderID、权限字符串、复制的 Handle ID、可信进程入口或 User Space 声称均不足以授权。必要绑定包括调用者身份与 ExecutionContext、权威 Handle 查找、主体与严格 ExecutionAuthorityScope 相等、生命周期、目标实例与 Resource 关系、声明的操作/参数、当前可用性，以及副作用前执行侧复核。必要证据缺失或无效时 fail-closed。

PermissionSet 当前是经过结构验证的元数据，不是完整授权决策。安全模型定义未来认证/授权证据的边界，不实现凭证发行、密码信任、RBAC/ACL/Policy Engine、完整参数策略、Scope 继承或 Delegation。释放、过期、撤销含义不同；不可用不等于撤销。安全权威正确性不能替代设备功能安全或物理联锁。

LifecycleState 与 AvailabilityState 不同。UNAVAILABLE 属于可用性，不能静默当成生命周期状态。EventRecord 是不可变审计事实，不是可变当前状态。

## 执行容量

权威在 allocation 前验证。Resource 拥有执行容量；其活跃精确 allocation-claim 集合是容量消耗与可用容量的唯一权威来源，不存在并行可变容量计数器。ExecutionAllocation.CurrentOccupancy 是单个 allocation 当前占用结论的独立权威，不拥有容量。二者由不变量关联；仅有 claim 不能区分 NOT_ESTABLISHED 与 UNKNOWN。

Allocation 固定不可变 ExecutionDescriptor/ExecutionAllocation 绑定：Context、Handle、声明、实例、Resource、操作及 Resource 发行的专用 OwnershipFence。Provider 身份在 Dispatch 时经最后一层验证后解析，不把未经验证的 allocation 时路由冻结为执行目标。一次受控 dispatch claim 防止第二次 Provider crossing。Resource.TryAcquireExecution/ReleaseExecution 是内部协调原语；正常执行通过 allocation/release facade，不允许 User Space 直接操作 claim。

## 占用解析权威

Execution Authority、Provider Authority、Occupancy Resolution Authority 相互独立。ResolutionEvidence 是状态转换输入，不是 Kernel Object，也不单独构成证明。独立准入的可信 Resource 侧证据来源必须提供或担保解析权威。User Space 请求不能创建、推断或提升它；facade 不能仅因调用方请求解析就取得 Resource 发行的权威。

权威和证据必须绑定精确 allocation、Resource、相同 OwnershipFence。较新 fence 对旧 UNKNOWN allocation 不授予追溯权威；fence 推进本身不证明终止。普通观察、取消、超时、Resource 不可用不授权释放 claim。参见[执行契约](execution-contract.zh-CN.md)与[恢复及幂等](recovery-idempotency.zh-CN.md)。
