# Third-party notices

This file records the Go dependencies declared by this local candidate on October 3, 2026. DTM's own licensing statement is in [README](README.md) and [LICENSE](LICENSE); this inventory does not transfer third-party rights to the DTM copyright holder.

The current Windows package and test selection uses 16 external modules. The 17th declared module, github.com/google/uuid, is not used by that selection; its license is retained separately below. Modules present only in the transitive module graph are not represented as shipped components. No dependency source code is vendored here.

## Dependency licenses

License labels summarize the reviewed root and supplemental texts; they are not file-level SPDX expressions. The unmodified upstream texts linked below remain authoritative.

| Module | Version | Scope | License summary | Original texts |
|---|---|---|---|---|
| github.com/dustin/go-humanize | v1.0.1 | Current packages and tests | MIT | [LICENSE](third_party/licenses/github.com_dustin_go-humanize@v1.0.1/LICENSE) |
| github.com/google/uuid | v1.6.0 | Declared; unused in current selection | BSD-3-Clause | [LICENSE](third_party/licenses/github.com_google_uuid@v1.6.0/LICENSE) |
| github.com/mattn/go-isatty | v0.0.20 | Current packages and tests | MIT | [LICENSE](third_party/licenses/github.com_mattn_go-isatty@v0.0.20/LICENSE) |
| github.com/ncruces/go-strftime | v0.1.9 | Current packages and tests | MIT | [LICENSE](third_party/licenses/github.com_ncruces_go-strftime@v0.1.9/LICENSE) |
| github.com/remyoudompheng/bigfft | v0.0.0-20230129092748-24d4a6f8daec | Current packages and tests | BSD-3-Clause | [LICENSE](third_party/licenses/github.com_remyoudompheng_bigfft@v0.0.0-20230129092748-24d4a6f8daec/LICENSE) |
| golang.org/x/exp | v0.0.0-20250620022241-b7579e27df2b | Current packages and tests | BSD-3-Clause | [LICENSE](third_party/licenses/golang.org_x_exp@v0.0.0-20250620022241-b7579e27df2b/LICENSE) |
| golang.org/x/net | v0.41.0 | Current packages and tests | BSD-3-Clause | [LICENSE](third_party/licenses/golang.org_x_net@v0.41.0/LICENSE) |
| golang.org/x/sys | v0.36.0 | Current packages and tests | BSD-3-Clause | [LICENSE](third_party/licenses/golang.org_x_sys@v0.36.0/LICENSE) |
| golang.org/x/text | v0.26.0 | Current packages and tests | BSD-3-Clause | [LICENSE](third_party/licenses/golang.org_x_text@v0.26.0/LICENSE) |
| google.golang.org/genproto/googleapis/rpc | v0.0.0-20250707201910-8d1bb00bc6a7 | Current packages and tests | Apache-2.0 | [LICENSE](third_party/licenses/google.golang.org_genproto_googleapis_rpc@v0.0.0-20250707201910-8d1bb00bc6a7/LICENSE) |
| google.golang.org/grpc | v1.75.0 | Current packages and tests | Apache-2.0 | [LICENSE](third_party/licenses/google.golang.org_grpc@v1.75.0/LICENSE), [NOTICE.txt](third_party/licenses/google.golang.org_grpc@v1.75.0/NOTICE.txt) |
| google.golang.org/protobuf | v1.36.6 | Current packages and tests | BSD-3-Clause | [LICENSE](third_party/licenses/google.golang.org_protobuf@v1.36.6/LICENSE) |
| gopkg.in/yaml.v3 | v3.0.1 | Current packages and tests | MIT + Apache-2.0 (file-specific) | [LICENSE](third_party/licenses/gopkg.in_yaml.v3@v3.0.1/LICENSE), [NOTICE](third_party/licenses/gopkg.in_yaml.v3@v3.0.1/NOTICE) |
| modernc.org/libc | v1.66.10 | Current packages and tests | BSD-3-Clause + MIT + supplemental notices | [COPYRIGHT-MUSL](third_party/licenses/modernc.org_libc@v1.66.10/COPYRIGHT-MUSL), [LICENSE](third_party/licenses/modernc.org_libc@v1.66.10/LICENSE), [LICENSE-GO](third_party/licenses/modernc.org_libc@v1.66.10/LICENSE-GO), [LICENSE](third_party/licenses/modernc.org_libc@v1.66.10/honnef.co/go/netdb/LICENSE) |
| modernc.org/mathutil | v1.7.1 | Current packages and tests | BSD-3-Clause | [LICENSE](third_party/licenses/modernc.org_mathutil@v1.7.1/LICENSE) |
| modernc.org/memory | v1.11.0 | Current packages and tests | BSD-3-Clause; supplemental Go/mmap licenses; logo reference | [LICENSE](third_party/licenses/modernc.org_memory@v1.11.0/LICENSE), [LICENSE-GO](third_party/licenses/modernc.org_memory@v1.11.0/LICENSE-GO), [LICENSE-LOGO](third_party/licenses/modernc.org_memory@v1.11.0/LICENSE-LOGO), [LICENSE-MMAP-GO](third_party/licenses/modernc.org_memory@v1.11.0/LICENSE-MMAP-GO) |
| modernc.org/sqlite | v1.39.1 | Current packages and tests | BSD-3-Clause wrapper; SQLite public domain | [LICENSE](third_party/licenses/modernc.org_sqlite@v1.39.1/LICENSE), [SQLITE-LICENSE](third_party/licenses/modernc.org_sqlite@v1.39.1/SQLITE-LICENSE) |

## Component-specific boundaries

- YAML v3 applies MIT and Apache-2.0 to different files. These are not interchangeable license choices; retain the complete LICENSE and NOTICE.
- The modernc SQLite Go wrapper has its own BSD license. The separate SQLite public-domain declaration applies to upstream SQLite content, not automatically to the whole Go module.
- modernc libc includes additional musl, Go and netdb notices. Retain the supplemental texts alongside the root license.
- modernc memory includes Go and mmap notices. LICENSE-LOGO contains only a reference URL, not an affirmative license grant. The logo asset is not included in this candidate.
- gRPC includes NOTICE.txt alongside its Apache license.

## Distribution scope

This source candidate excludes locally generated binaries. Before distributing binaries, inspect each actual build and retain applicable Go toolchain/runtime notices. Recheck platform-specific dependencies before distributing other platforms. Vendoring or adding third-party source/assets requires a file-level review of the added content.

Preserved copyright holder names and contacts in upstream license texts are attribution, not DTM maintainer or vulnerability-reporting contacts. A public security reporting channel remains pending.
