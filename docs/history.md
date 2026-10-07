# Selected development history

[简体中文](history.zh-CN.md)

The locally reachable development history contains 176 commits from July 26 to September 11, 2026, spanning about 47 days. This is the span of available records, not proof of the original project start date or uninterrupted daily work.

Dates below are original committer dates in UTC+08:00. IDs identify the original development repository; this snapshot excludes that development Git history and starts with independent history, so these IDs may not resolve in the public repository.

| Period in 2026 | Development | Representative original commit IDs |
|---|---|---|
| July 26 | Task model, mapping and an independent Core/Agent gRPC demo | `3db2c38`, `095571b`, `06907d8`, `5a235dc` |
| July 27–31 | Async submission/query, stateful execution, recovery and stabilization | `a1fb608`, `146640b`, `c314a28` |
| July 31–August 4 | SQLite persistence, query/recovery, idempotency and three-VM validation records | `0ba425c`, `9937601`, `1ba4b43`, `d80c332` |
| August 5–11 | Resource identity, directory, persistence and mapping | `b1edfbb`, `c2930dc`, `25c6a0b`, `38dcbe4` |
| August 11–19 | Level 1 self-organizing mesh | `c9ab595`, `d1fb564`, `8a8d7bc`, `aaf5291` |
| August 20–21 | Resource Invocation Foundation | `76d9462`, `c2b8a62` |
| August 21–27 | Autonomous Kernel concept and R0 checkpoint | `3821ab7`, `7b08017` |
| August 31–September 4 | Bounded execution ownership and same-fence resolution | `081d1b5`, `6cd055c` |
| September 6–8 | Logical facade, semantic governance and bounded refactoring | `c40d14a`, `25d136a`, `614b172` |
| September 8–11 | User Space orchestrator, Child/Root Tasks and task creation | `b8498c8`, `3440999`, `73d3b0a`, `0684db3` |

Working-tree documents dated September 30 separately describe the experimental Goal Loop and local Workbench. They have no corresponding commits in this reachable baseline and remain pending final acceptance; their source is outside this candidate.

Detailed historical reports, raw archives and repeated process records are not shipped with this summary. The local archive preserves the 79 records excluded after review, plus backups of retained material. The original development repository also retains its source documents. No historical commits or acceptance dates were rewritten to create this timeline.

Author time, committer time, document date and acceptance date are distinct. Historical PASS applies to its recorded source, environment and scope; current checks are listed in [validation](validation.md). Current architecture is governed by [Architecture State](../architecture/README.md), with necessary references in [contracts](contracts.md).
