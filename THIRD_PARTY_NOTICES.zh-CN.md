# 第三方授权说明

[English](THIRD_PARTY_NOTICES.md) | 简体中文

本文件记录本候选于 2026 年 10 月 3 日声明的 Go 依赖。DTM 自身的许可声明见 [README](README.md) 与 [LICENSE](LICENSE)；本清单不把第三方权利转移给 DTM 版权持有人。

当前 Windows 包与测试选型使用 16 个外部模块。第 17 个已声明模块 github.com/google/uuid 未被该选型使用；其许可证单独保留在下文。仅出现在传递依赖图中的模块不视为随附组件。此处不内置任何依赖源代码。

## 依赖许可证

许可证标签是对已审阅的根文本与补充文本的概括，并非文件级 SPDX 表达式。下方链接的未修改上游原文仍为权威。

| 模块 | 版本 | 范围 | 许可证概要 | 原文 |
|---|---|---|---|---|
| github.com/dustin/go-humanize | v1.0.1 | 当前包与测试 | MIT | [LICENSE](third_party/licenses/github.com_dustin_go-humanize@v1.0.1/LICENSE) |
| github.com/google/uuid | v1.6.0 | 已声明；当前选型未使用 | BSD-3-Clause | [LICENSE](third_party/licenses/github.com_google_uuid@v1.6.0/LICENSE) |
| github.com/mattn/go-isatty | v0.0.20 | 当前包与测试 | MIT | [LICENSE](third_party/licenses/github.com_mattn_go-isatty@v0.0.20/LICENSE) |
| github.com/ncruces/go-strftime | v0.1.9 | 当前包与测试 | MIT | [LICENSE](third_party/licenses/github.com_ncruces_go-strftime@v0.1.9/LICENSE) |
| github.com/remyoudompheng/bigfft | v0.0.0-20230129092748-24d4a6f8daec | 当前包与测试 | BSD-3-Clause | [LICENSE](third_party/licenses/github.com_remyoudompheng_bigfft@v0.0.0-20230129092748-24d4a6f8daec/LICENSE) |
| golang.org/x/exp | v0.0.0-20250620022241-b7579e27df2b | 当前包与测试 | BSD-3-Clause | [LICENSE](third_party/licenses/golang.org_x_exp@v0.0.0-20250620022241-b7579e27df2b/LICENSE) |
| golang.org/x/net | v0.41.0 | 当前包与测试 | BSD-3-Clause | [LICENSE](third_party/licenses/golang.org_x_net@v0.41.0/LICENSE) |
| golang.org/x/sys | v0.36.0 | 当前包与测试 | BSD-3-Clause | [LICENSE](third_party/licenses/golang.org_x_sys@v0.36.0/LICENSE) |
| golang.org/x/text | v0.26.0 | 当前包与测试 | BSD-3-Clause | [LICENSE](third_party/licenses/golang.org_x_text@v0.26.0/LICENSE) |
| google.golang.org/genproto/googleapis/rpc | v0.0.0-20250707201910-8d1bb00bc6a7 | 当前包与测试 | Apache-2.0 | [LICENSE](third_party/licenses/google.golang.org_genproto_googleapis_rpc@v0.0.0-20250707201910-8d1bb00bc6a7/LICENSE) |
| google.golang.org/grpc | v1.75.0 | 当前包与测试 | Apache-2.0 | [LICENSE](third_party/licenses/google.golang.org_grpc@v1.75.0/LICENSE)、[NOTICE.txt](third_party/licenses/google.golang.org_grpc@v1.75.0/NOTICE.txt) |
| google.golang.org/protobuf | v1.36.6 | 当前包与测试 | BSD-3-Clause | [LICENSE](third_party/licenses/google.golang.org_protobuf@v1.36.6/LICENSE) |
| gopkg.in/yaml.v3 | v3.0.1 | 当前包与测试 | MIT + Apache-2.0（按文件区分） | [LICENSE](third_party/licenses/gopkg.in_yaml.v3@v3.0.1/LICENSE)、[NOTICE](third_party/licenses/gopkg.in_yaml.v3@v3.0.1/NOTICE) |
| modernc.org/libc | v1.66.10 | 当前包与测试 | BSD-3-Clause + MIT + 补充声明 | [COPYRIGHT-MUSL](third_party/licenses/modernc.org_libc@v1.66.10/COPYRIGHT-MUSL)、[LICENSE](third_party/licenses/modernc.org_libc@v1.66.10/LICENSE)、[LICENSE-GO](third_party/licenses/modernc.org_libc@v1.66.10/LICENSE-GO)、[LICENSE](third_party/licenses/modernc.org_libc@v1.66.10/honnef.co/go/netdb/LICENSE) |
| modernc.org/mathutil | v1.7.1 | 当前包与测试 | BSD-3-Clause | [LICENSE](third_party/licenses/modernc.org_mathutil@v1.7.1/LICENSE) |
| modernc.org/memory | v1.11.0 | 当前包与测试 | BSD-3-Clause；Go/mmap 补充许可证；logo 引用 | [LICENSE](third_party/licenses/modernc.org_memory@v1.11.0/LICENSE)、[LICENSE-GO](third_party/licenses/modernc.org_memory@v1.11.0/LICENSE-GO)、[LICENSE-LOGO](third_party/licenses/modernc.org_memory@v1.11.0/LICENSE-LOGO)、[LICENSE-MMAP-GO](third_party/licenses/modernc.org_memory@v1.11.0/LICENSE-MMAP-GO) |
| modernc.org/sqlite | v1.39.1 | 当前包与测试 | BSD-3-Clause 封装；SQLite 公有领域 | [LICENSE](third_party/licenses/modernc.org_sqlite@v1.39.1/LICENSE)、[SQLITE-LICENSE](third_party/licenses/modernc.org_sqlite@v1.39.1/SQLITE-LICENSE) |

## 各组件特有边界

- YAML v3 对不同文件分别适用 MIT 与 Apache-2.0。二者不是可互换的许可选择；须保留完整的 LICENSE 与 NOTICE。
- modernc SQLite 的 Go 封装有自己的 BSD 许可证。单独的 SQLite 公有领域声明适用于上游 SQLite 内容，不自动适用于整个 Go 模块。
- modernc libc 包含额外的 musl、Go 与 netdb 声明。须在根许可证之外一并保留这些补充文本。
- modernc memory 包含 Go 与 mmap 声明。LICENSE-LOGO 只包含一个引用 URL，不构成肯定性的许可授予。本候选不包含该 logo 资源。
- gRPC 在其 Apache 许可证之外还包含 NOTICE.txt。

## 分发范围

本源码候选不包含本地生成的二进制。分发二进制前，应检查每一次实际构建并保留适用的 Go 工具链/运行时声明。分发到其他平台前，应重新检查平台相关依赖。内置或新增第三方源码/资源需要对新增内容做文件级审查。

上游许可证文本中保留的版权人名称与联系方式属于署名信息，不是 DTM 维护者或漏洞报告的联系方式。公开安全报告渠道仍待确定。
