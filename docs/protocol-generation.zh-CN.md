# 协议代码生成

[English](protocol-generation.md) | 简体中文

普通构建使用已纳入源码的生成 Go 文件。若修改协议，安装 `scripts/generate-proto.ps1` 强制要求的版本：protoc 30.2、protoc-gen-go v1.36.6、protoc-gen-go-grpc 1.5.1。本次本地候选准备不包含工具安装。

在根目录的 PowerShell 中运行：

```powershell
./scripts/generate-proto.ps1
go test ./api/...
```

候选脚本应同时生成 `api/proto/dtm/v1/dtm.proto` 和 `invocation.proto`，再格式化生成的 Go 输出。验收前检查生成差异与兼容性。只有所需工具实际存在、命令实际成功，才能认定协议重生成已验证；见[验证记录](validation.zh-CN.md)。
