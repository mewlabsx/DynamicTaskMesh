# 参考执行示例

[English](examples.md) | 简体中文

先完成[快速入门](quickstart.zh-CN.md)。所有示例使用 Core/Agent 参考路径与模拟能力；Windows 命令从候选根目录运行。

## 提交去重

使用客户端生成的键异步提交：

```powershell
./bin/dtm-submit.exe -core 127.0.0.1:50051 -async -idempotency-key example-001 -target-temperature 26
./bin/dtm-submit.exe -core 127.0.0.1:50051 -async -idempotency-key example-001 -target-temperature 26
```

重复提交返回原任务 ID，并有 `deduplicated=true`。查询该 ID 可检查进度。同一键对应不同业务请求时，应因冲突被拒绝。提交去重不意味着允许重复执行结果不明、非幂等的物理动作。

## 查询异步提交的任务

```powershell
./bin/dtm-query.exe get --task-id <returned-task-id> --core-address 127.0.0.1:50051
./bin/dtm-query.exe executions --task-id <returned-task-id> --core-address 127.0.0.1:50051
```

运行前替换占位符。任务可能在第一次查询前已完成，示例不要求必须观察到运行中状态。

## 替换制冷 Agent

用 Ctrl+C 停止 Cooling 001，然后启动备用配置：

```powershell
./bin/dtm-agent.exe -config ./configs/demo/cooling-agent-002.yaml
```

等待注册后提交另一任务，制冷结果应标识 `cooling-node-002`。这演示按能力替换执行目标，不是 Coordinator/Core 故障切换。

## 能力缺失

使用已停止并备份原状态后的全新演示状态，只启动 Core 和温度 Agent，然后提交新任务。缺少有效制冷能力时，规划/映射应拒绝任务，不能虚构执行者。已有集成测试覆盖该路径；报告结果时保留实际错误。

## Kernel 与实验内容

可阅读 `internal/kernel` 和 `internal/userspace` 测试，了解同 fence 解析、`UNKNOWN` 保留与显式任务闭合。这些是本地机制测试。单独保存的 Goal Loop 和 Workbench 需要最终验收后，才能进入公开示例集合。
