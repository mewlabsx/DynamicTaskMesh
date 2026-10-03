# 执行契约

## 逻辑 facade

已接受的 Logical Execution Facade 在同一本地 Kernel 生命周期内提供 RequestExecution、ObserveInvocation、SubmitOccupancyResolution。它通过窄端口提供不可变值快照；调用方无需内部 Manager 指针、可变对象或锁。这是进程内语义契约，不是物理 syscall、RPC、线协议/认证协议或持久化 ABI。物理 ABI 为 DEFERRED。

RequestExecution 要求调用方持有稳定 InvocationID、ExecutionContext、CapabilityHandle、Operation、不透明 Payload。RootTaskRef/ChildTaskRef 是可选外部谱系；RequestID 是可选消息关联。Resource/Provider/实例 ID 或调用方选择的 fence 不能替代 Handle 权威。必要输入缺失/格式错误在 Provider crossing 前返回 INVALID_REQUEST；无效、过期、外来、终止或 Scope 不匹配的权威 fail-closed。

## 身份绑定

第一次接受调用固定不可变 InvocationBinding：Context、Handle、Operation、精确 Kernel 可见不透明 Payload、RootTaskRef、ChildTaskRef。RequestID 从不参与 InvocationID 身份。调用方/Context 权威事实经验证；Resource、实例、声明、OwnershipFence 是 Kernel 派生的 allocation 事实。

相同 InvocationID 与完全相同绑定返回/查询已有调用，不进行第二次 allocation 或 Provider 调用。任何绑定字段不同均返回 INVOCATION_CONFLICT，原调用不变。Payload 比较为精确不透明值相等；本地 []byte 实现采用精确字节序列相等。相同字节确定 Kernel 可见身份，不证明业务等价；不同字节不证明业务含义不同。保守冲突可以接受。Kernel 不做 schema 感知规范化；所需业务/编码规范化必须在进入边界前完成。

一个接受的 InvocationID 最多关联一个精确 ExecutionAllocationID。Allocation ID 只是只读关联，不是权威或释放令牌。ObserveInvocation 只读，可核对可选精确 allocation；不会创建权威、释放 claim 或改写身份。

## 结果与 crossing

结果包含 InvocationID、可选 ExecutionAllocationID、RequestStatus、ProviderCrossingAvailable/ProviderCrossing、不可变 ExecutionObservation、CurrentOccupancy、ReconciliationRequired、RetrySafetyFact、结构化 ErrorInfo 及可选不可变 EventReference。ErrorInfo 区分 Category、Phase、crossing 有效性、关联与核对事实；不能仅凭诊断字符串判断安全性。

RequestStatus 在语义模型下包含 REJECTED、ACCEPTED、DISPATCHED、RUNNING、COMPLETED、FAILED、UNKNOWN。观察区分 NOT_KNOWN、ACCEPTED_NOT_ALLOCATED、ALLOCATED_NOT_CROSSED、DEFINITIVE_OBSERVATION、UNRESOLVED_UNKNOWN、ENDED_RELEASED、INVARIANT_UNKNOWN。进程内记录不存在，不证明没有执行过。

当前本地 crossing 事实只有 NOT_CROSSED_DEFINITE、CROSSED。ProviderCrossingAvailable=false 表示没有可解释的 crossing 事实；NOT_KNOWN 不会因此变成可安全重放。CROSSING_UNCERTAIN/PROVIDER_CROSSING_UNCERTAIN 是未来/保留状态，不是当前本地实现状态。CROSSED 后若 Provider 仅返回错误而无确定占用证据，结果/占用保持 UNKNOWN，精确 claim 保留。Kernel 不能编造确定的 pre-provider 失败。

RetrySafetyFact 为 NOT_DECLARED、EXPLICITLY_SAFE、UNSAFE_OR_UNKNOWN，不是 SHOULD_RETRY。错误阶段区分 REQUEST_VALIDATION、AUTHORITY_VALIDATION、ALLOCATION、PRE_PROVIDER、PROVIDER_CROSSING、POST_PROVIDER、OBSERVATION、RESOLUTION、RELEASE。类别区分 INVALID_REQUEST、INVALID_OR_STALE_AUTHORITY、CAPACITY_UNAVAILABLE、INVOCATION_CONFLICT、PROVIDER_NOT_AVAILABLE、EXECUTION_UNRESOLVED、RESOLUTION_NOT_ALLOWED、ALREADY_RELEASED、ALREADY_RESOLVED、INVARIANT_VIOLATION、INTERNAL_FAILURE。AUTHORIZATION_DENIED 表示另行可用的明确拒绝，不代表完整授权求值器存在。必要证据缺失绝不表示批准。

## 归属与同 fence 解析

权威先于精确 allocation。最后一层验证与 Dispatch 时 Provider 解析先于一次受控 crossing。ExecutionObservation 是不可变历史，后续解析不能改写它。CurrentOccupancy 是单个 allocation 的当前状态：NOT_ESTABLISHED、UNKNOWN、ENDED。Resource 精确 claim 独立决定容量记账。

UNKNOWN 保留精确 claim。Cancellation request != execution ended。同 fence 的权威 ENDED 解析原子移除精确 claim，建立 CurrentOccupancy=ENDED/Lifecycle=RELEASED，并产生恰好一条接受的不可变解析 EventRecord。ResolutionEvidence 不是 Kernel Object。ResolutionAuthorityFence 必须等于 ExecutionAllocation.OwnershipFence；E18 权威不能解析 E17 allocation。跨 fence 解析、fence 转换运行时、追溯权威仍属于未来/保留范围。

SubmitOccupancyResolution 要求独立准入的可信 Resource 侧权威来源及精确绑定，不能从普通请求派生权威。RESOLVED 建立精确释放；UNRESOLVED 保留不确定性。ALREADY_RESOLVED 要求原始不可变 UNKNOWN 观察、等价解析绑定、精确 ENDED/RELEASED 终态及精确 claim 不存在；仅有终态不是解析历史。验证或事件提交失败不能部分改变 claim/状态/事件边界。

## 有限一致性

结构验证可以读取某一时点快照；crossing 前执行最后一层复核。精确 claim 存在/不存在、CurrentOccupancy、allocation 生命周期组成有限本地归属边界；容量 claim 与占用状态不能相互替代。

强制 EventRecord 原子耦合仅限接受的 Phase 2 延迟同 fence 解析。DispatchExecution 中即时确定 ENDED、allocation、普通派发/观察/释放不承诺相同事件耦合。不提供通用全转换事件原子性、通用跨 Manager 事务、全局线性化或全局快照隔离。外部 Phase 2 解析必须对 facade 可见；facade 本地标记不是占用权威，也不能从 allocation 存在推断 crossing。

预冻结逻辑 facade 实现在有限范围内已接受/验证，但记录的 milestone_closed 仍为 NO。Gate B Phase 1/2 在有限范围内 CLOSED；Full Gate B 为 RESERVED；KernelIntent runtime 为 NOT_IMPLEMENTED；Final Architecture Freeze 为 NOT_CLAIMED。这次文档收敛不改变这些不同状态。参见[状态](status.zh-CN.md)、[权威](resource-authority.zh-CN.md)、[恢复](recovery-idempotency.zh-CN.md)。


[English](execution-contract.md)
