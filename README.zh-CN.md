# Dynamic Task Mesh

[English](README.md) | 简体中文

Dynamic Task Mesh（DTM）是以任务为中心的实验性资源编排项目。它包含 Core/Agent 执行参考实现、Level 1 Mesh 和 Resource Invocation 基础，以及通过进程内组合提供的、有明确实现范围的 Kernel 与 User Space 任务机制。

软件线为 **v0.7**，Kernel 规范为 **v0.1**，两者是独立的版本轴。首个公开源码预览版为 **v0.7.0-alpha.1**，采用 AGPL-3.0-only，公开仓库为 [mewlabsx/DynamicTaskMesh](https://github.com/mewlabsx/DynamicTaskMesh)。详见[发布范围](docs/release-readiness.md)。Core/Agent 当前运行既有参考路径，尚未接入新 Kernel/User Space；参见[运行路径整合方向](docs/integration.md)。

## 从这里开始

- [快速开始](docs/quickstart.md)：运行 Core 和两个模拟 Agent，提交任务并查询结果。
- [架构概览](docs/overview.md)：了解参考执行路径，以及 Kernel 与 User Space 的职责边界。
- [能力状态](docs/status.md)：区分已实现机制、待验收实验和未来范围。
- [示例](docs/examples.md)：体验提交去重、异步查询和能力执行节点替换。
- [文档与精选历史](docs/README.md)。
- [贡献说明](CONTRIBUTING.md)。

上述主要说明目前以英文提供；历史正文保留原语言。

使用 Go 1.24 或兼容工具链，在本目录执行构建和检查：

```text
go build ./cmd/...
go test -count=1 -timeout=15m ./...
go vet ./...
```

候选版本实际执行的验证结果和限制见[验证记录](docs/validation.md)。最短运行步骤及配置位置见[快速开始](docs/quickstart.md)。

## 实现范围与限制

既有 Core/Agent 参考实现支持按能力执行任务、基于 SQLite 的 Task/Step/Execution 状态、查询、提交去重、fencing 和保守恢复。Level 1 Mesh 与原生 gRPC Resource Invocation 基础继续作为兼容参考。这些持久化与恢复能力不自动适用于新的 Kernel 机制。

Kernel 通过进程内逻辑执行门面提供有明确范围的对象、执行所有权和同一所有权 fence 下的占用解析机制。User Space 在其上提供有限范围的执行编排器、Child/Root Task 机制和任务创建边界。Gate B Phase 1 与 Phase 2 的局部阶段关闭，不等于完整 Kernel 自治闭环已经完成。

DTM 当前不宣称已具备生产可用性、最终 Architecture Freeze、KernelIntent runtime、生产 Resource Admission、新 Kernel 的持久化或远程 ABI、多 Core 高可用、Binary transport、无限制任务派生或功能安全保证。可能已产生副作用但结果未知的执行，不得触发隐式重试。参考实现与 Kernel 机制的具体区别见[能力状态](docs/status.md)。

Goal Loop 和本地 Workbench 保留在独立的本地审阅目录，等待最终验收。它们未纳入本候选的默认命令或能力声明。

## 架构依据

[Architecture State](architecture/README.md) 及其引用的治理记录描述当前有效架构。历史设计、实施和审查记录保留各自原有的范围与状态；较晚的时间戳不能覆盖已冻结决策。

[R0.1 UserIntent / KernelIntent Boundary 审查](docs/kernel-userspace-boundary.md#effective-state-and-governance) 记录了 Intent 的职责区分：UserIntent 属于 User Space；KernelIntent 是未来由 Kernel 管理、业务语义保持不透明的表示，不授予 Capability 执行权限。当前校验未启用 Intent runtime。[AGENTS.md](AGENTS.md) 保留开发规则和历史边界要求。

## 源码来源

[第三方声明](THIRD_PARTY_NOTICES.md) 列出了依赖许可证并保留上游原文。

## 许可证与维护

Copyright (c) 2026 Zhao Tao (赵涛)。

除另有标识外，本候选中的 DTM 自有代码和文档采用 GNU Affero General Public License 第三版，仅此版本（`AGPL-3.0-only`），详见 [LICENSE](LICENSE)。第三方组件及其声明继续适用各自许可证。

允许按许可证商业使用和修改。分发时须履行对应源码提供义务；如果修改程序，并让用户通过计算机网络与修改版远程交互，须按第 13 条向这些用户显著提供免费获取对应源码的机会。这不要求向本仓库提交修改，也不要求自动公开所有私人修改。具体权利与义务以许可证原文为准，软件在法律允许范围内不提供担保。

这是个人维护项目，当前以代码和技术文档公开为主，不承诺响应时间、长期支持或接受 PR。分发具有网络交互能力的修改版前，应提供适当的源码获取机制；本说明本身不构成运行时的源码获取机制。

本候选于 2026 年 10 月 3 日从本地 HEAD `0684db339c854fee7b86210c92da16d133189f29` 及选定工作区内容整理而来。公开候选范围不包含原始证据归档、本地运行数据、申请材料和工具分发。部分历史用户目录路径已替换为占位符，历史验证结果未被升级为当前结论。[开发历史](docs/history.md) 说明了时间线与证据限制。

中英文 README 对应同一候选范围。后续更新应同步项目定位、版本、能力限制、示例入口和来源信息。


[中文文档导航](docs/README.zh-CN.md)
