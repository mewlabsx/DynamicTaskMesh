# 快速入门：提交和查询本地任务

[English](quickstart.md) | 简体中文

本教程使用既有 Core/Agent 参考执行路径，模拟温度传感与制冷能力，不调用新的 Kernel Goal Loop，也不连接真实设备。所有命令从候选根目录运行。

## 环境要求与构建

使用 Go 1.24 或兼容工具链；本地核对过的版本见[验证记录](validation.zh-CN.md)。依赖在 `go.mod` 和 `go.sum` 中声明，首次构建需要已有 Go 模块缓存或能下载依赖。已包含生成的 Protobuf 文件，普通构建不需要 protoc。

Windows PowerShell：

```powershell
New-Item -ItemType Directory -Force bin | Out-Null
go build -o ./bin/ ./cmd/...
```

POSIX shell：

```sh
mkdir -p bin
go build -o ./bin/ ./cmd/...
```

下列 Windows 命令使用 `.exe`，其他平台省略后缀。本教程仅在 Windows 本地验证，不能据此认定 Linux/macOS 已受支持。

## 启动三个服务

打开三个终端，都位于候选根目录。第一个启动 Core：

```powershell
./bin/dtm-core.exe -config ./configs/demo/core.yaml
```

第二个启动温度 Agent：

```powershell
./bin/dtm-agent.exe -config ./configs/demo/sensor-agent.yaml
```

第三个启动制冷 Agent：

```powershell
./bin/dtm-agent.exe -config ./configs/demo/cooling-agent-001.yaml
```

等待 Core 输出 `dtm-core listening`，每个 Agent 输出 `register success`。Core 使用 `127.0.0.1:50051`；温度和制冷 Agent 分别使用 `50061`、`50062`。配置中的数据库路径相对配置文件目录解析，因此演示状态位于候选根目录的 `data/`。启动前确保端口未被占用。

## 提交与查询

第四个终端运行：

```powershell
./bin/dtm-submit.exe -core 127.0.0.1:50051 -target-temperature 26
```

预期响应报告任务成功，并包含 `sensor-node-001` 和 `cooling-node-001` 的结果。模拟传感器报告温度 30，制冷结果包含 `cooling_started`。这是执行成功，不是实际温度降低的测量结果，也不能证明新 Root Task 的目标判定成立。

复制返回的任务 ID，然后运行：

```powershell
./bin/dtm-query.exe get --task-id <returned-task-id> --core-address 127.0.0.1:50051
./bin/dtm-query.exe executions --task-id <returned-task-id> --core-address 127.0.0.1:50051
./bin/dtm-query.exe list --limit 10 --core-address 127.0.0.1:50051
```

运行前把 `<returned-task-id>` 替换为实际值。其他受控场景见[示例](examples.zh-CN.md)。

## 停止与重新开始

先在各 Agent 终端按 Ctrl+C，再停止 Core。若希望重启后查询已有任务，保留状态。若要全新演示，先停止全部进程，再把候选的 `data/` 移到你选择的备份位置。不要删除仍在使用的数据库，也不要假定重启会自动重试结果不明的副作用。

服务无法绑定时，检查端口是否被其他进程占用；注册未完成时，确认三份配置使用相同 Core 地址。存储启动错误应在提交前排查，不能修改历史迁移来绕过错误。
