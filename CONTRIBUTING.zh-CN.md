# 贡献说明

[English](CONTRIBUTING.md) | 简体中文

这是个人维护的 AGPL-3.0-only 源码预览。维护者不承诺接受 pull request，也不承诺响应时间。以下说明供查看或修改源码的人了解开发边界。

改动契约前，请先阅读[架构依据](architecture/README.md)、[能力状态](docs/status.md)与 [AGENTS.md](AGENTS.md)。把任务策略留在 User Space，保持 Resource/Capability 身份边界，迁移只做追加，并保留兼容 profile。一个已完成的本地里程碑不构成对下一个里程碑的授权。

在候选根目录执行：

```text
go build ./cmd/...
go test -count=1 -timeout=15m ./...
go vet ./...
```

行为变更要有有意义的测试。竞态与 fuzz 检查是额外的定向门禁，取决于可用的编译器与工具链。PowerShell 协议再生成见[协议代码生成](docs/protocol-generation.md)。

Kernel 架构测试也会读取文档。编辑 README、AGENTS、YAML 或被引用的记录时，须保留已冻结的边界文本与历史状态；不要为了让某次文档编辑通过而删除断言。应新增一段简短的公开说明，而不是替换历史事实。

请说明问题、产生的行为、影响范围、兼容性影响与实际验证情况。缓存、运行时数据库、原始日志、源码快照、凭据与审查包都不得进入已发布的源码。每次变更只暂存本次涉及的文件。
