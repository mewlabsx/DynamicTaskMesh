# DTM Development Guide

## 当前项目状态

软件线 v0.7 / Kernel 规范 v0.1。公开契约在 docs/contracts.md，中文在 docs/contracts.zh-CN.md。architecture/ 的五份 YAML 是当前控制面；其选定的 semantic governance sign-off 是冻结语义权威。

Gate B Phase 1/2 仅在已有有限范围 CLOSED；逻辑 facade 已接受，但其 milestone_closed 仍 NO。Full Gate B RESERVED，KernelIntent runtime NOT_IMPLEMENTED，Final Architecture Freeze NOT_CLAIMED。不得从历史设计、测试通过或较新日期扩大实现声明。

## Architecture Principles

1. **复杂度必须守卫真实边界**

   任何新的计算、验证、协议或协调机制，都必须说明它守卫的安全、正确性或一致性边界；不引入它会发生什么错误；以及是否存在更简单的确定性、本地化或已有机制可以替代。没有明确收益就不增加复杂度；不要仅因“典型分布式系统通常这样做”而引入共识、PKI、额外服务、全局目录或其他基础设施。

2. **Task-oriented，而不是 Device-oriented**

   系统围绕 Task 与 Resource/Capability 建模。节点只是 Resource 的 Owner、运行位置和生命周期边界，不是业务能力模型本身；Task、Resource/Node、Execution 模型保持解耦。

3. **Resource 是自治边界，Capability 是其显式声明的能力**

   Resource 不等于 Capability、设备、主机或网络节点。Resource 标识描述自治边界；Resource 通过 `CapabilityDeclaration` 显式声明能力，物理设备、总线、适配器和实际执行对象之间的映射尽量留在 Owner/Gateway 本地，不泄漏进 Mesh 全局模型。

4. **递归自治**

   长期设计应允许同一套自治机制在不同规模上递归重放：discovery、handshake、membership、role selection、resource view、scheduling/execution。网络分区首先理解为自治边界变化，而不仅仅是“节点离线”；只有不同自治单元之间才引入 reconciliation、merge 或 governance 等额外语义。

5. **优先确定性收敛，而不是默认共识**

   能够由相同输入通过纯确定性函数得到唯一结果的问题，应优先使用确定性裁决，不引入共识或时序依赖。这不是“DTM 永远禁止共识”的绝对规则：只有当安全性或正确性确实要求多个参与者对同一状态建立唯一承诺，而 Epoch、Fencing、Lease 或确定性规则无法守卫该边界时，才可考虑更强协调机制，并由对应设计和 milestone 明确授权。

6. **Identity 与 Locator 分离**

   Identity 必须稳定，用于认证、授权、Owner 关系和长期引用；Locator/Path 用于寻址，可以随拓扑、挂载关系、自治层级或网络位置变化。长期密钥、授权语义和持久身份不得绑定到可变路径。

7. **优先局部命名空间**

   如果一个标识只需要在 Owner、父域或局部自治单元内唯一，就不要为了方便强行要求全局唯一；局部语义优先于全局协调。

8. **Authorization Invariant**

   任何具有副作用的执行都必须满足明确授权条件。真正执行副作用的一侧至少要能验证 actor/identity 是否匹配、action 与 parameters 的授权范围、authorization window、integrity/authentication proof，以及 trigger/attempt 是否违反重复执行约束；任一条件失败都必须 fail-closed，安全责任不能只落在调用方。K0.5-R1 历史基线只冻结 `PermissionSet` 的授权元数据载体、Handle 绑定和基本结构校验，不实现完整授权求值；授权来源、Operation Enforcement 和跨 Scope 授权由 K1 定义。

9. **未知执行结果不得盲目重试**

   对于可能已经产生副作用但结果未知的执行，默认不得自动重试。物理副作用动作默认按 `NON_IDEMPOTENT` 处理，除非 Resource/Capability 契约显式声明并能证明其他幂等语义；继续保留 fencing、attempt、persistence、recovery 和 idempotency 相关设计。

10. **渐进兼容**

    协议、Schema、Protobuf、Resource 模型和网络机制优先采用追加式、双栈或阶段迁移，避免大爆炸式替换已经工作的 reference profile。

11. **明确系统边界**

    后续设计不得扩大术语含义：DTM autonomy 主要指发现、调度和执行自治，不等于节点可以自行改变治理规则；DTM security 当前保证授权正确性，不替代 functional safety；不运行完整 DTM Runtime 的极弱设备属于 managed leaf，而不是完整 Mesh member；具体设备驱动、总线协议和硬件细节应尽量由 adapter/gateway 层处理。


## Change and review scope

保持当前接口、Append-only migration 与已有参考路径兼容。引用目录调整不能修改 decision/invariant/boundary 的冻结值、作用范围或 runtime 状态。历史详细记录仅本地保存，不复制进公开 fixture 或发行包。

Authority、Kernel/User Space、Object、Scope、Lifecycle、Invocation 或冻结决策变化应先做独立 read-only 审查，再由主执行者裁决和进行有限修复；普通局部修改不要求机械委派。审查者不能替代架构状态或用户授权。文档整理不启动新 milestone。

必要核验：go test -count=1 -timeout=15m ./...、go build ./cmd/...、go vet ./...。保留失败和未运行结果；历史 PASS 不能当作本次验证。公开文档检查必须读取当前契约和有效架构状态，不能因文件缺失跳过检查。
