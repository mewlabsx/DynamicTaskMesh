# Candidate validation

[简体中文](validation.zh-CN.md)

Date: October 3, 2026 (Asia/Shanghai). Windows/amd64, Go 1.24.12. This record applies to the compact public tree with 18 bilingual documentation topics, assembled from the selected development baseline plus publication-only documentation and document-check migrations.

## Scope revision

The `concept-map` and `code-map` topics were added after the checks below were recorded. This revision updates the scope count only: no product code, contract, schema, migration or Architecture State YAML changed, and no new validation was performed. Every result below still describes the source it was recorded against, not the two added topics.

## Current convergence checks

| Check | Result | Scope |
|---|---|---|
| `go test -count=1 -timeout=15m ./...` | PASS | All included packages, existing process/integration tests, current contract and architecture assertions |
| `go build ./cmd/...` | PASS | All included command packages; output kept outside the public tree |
| `go vet ./...` | PASS | Current package set |
| Architecture semantic equivalence | PASS | Five YAML files, 1,776 scalar values; 281 document-reference changes, zero other scalar/ID/status/list/structure changes |
| Production source SHA-256 comparison | PASS | All included non-test Go/proto/SQL files unchanged from the prior snapshot; independent review checked 155 files |
| Current local document references | PASS | Reader-facing Markdown file/heading targets and current architecture source fragments; remote URLs and legacy references inside code literals are outside this check |
| Public scope inventory | PASS | 18 topics / 36 docs files, no version/history subdirectories, no original Git history, local archives or unaccepted experiments |
| Independent read-only review | PASS after clarification | Five bilingual key designs and three architecture-test migrations; effective governance, status boundaries and production checks preserved |

Historical document-header and phase-report assertions were migrated to required current topic contracts, unchanged machine-readable semantics/status checks and retained production-boundary scans. The historical drift-findings YAML parse-only check is local archive material; all five current YAML parse checks remain required. No production behavior or test bypass was introduced. The first targeted migration check found stale headings and a case-sensitive contract phrase; these were repaired before the full suite passed.

The old 147-to-69 and later 55-original-document preparations were intermediate scopes. Their byte-identical YAML/test statements applied only to those earlier stages. Current YAML and three document-dependent tests have changed reference/check locations; their architecture semantics, runtime assertions and complete status expectation manifest were preserved. Historical acceptance remains historical, not fresh review of an integrated runtime.

## Earlier reference smoke and limits

Earlier preparation ran the Core/two simulated Agents quickstart, synchronous submission and get/executions/list queries, plus repeated keyed async admission with the same Task ID and deduplicated=true. Those process checks passed on the same production source and were not repeated for this documentation convergence. They do not prove graceful shutdown, crash recovery, multi-VM acceptance or new Kernel persistence.

Protocol regeneration was NOT RUN because protoc and the Go generators were unavailable. No new race/fuzz gate, Linux/macOS run, performance study, complete secret audit or raw historical evidence revalidation was performed here. Logs, source/reference mappings and private-source hashes are retained locally outside the publication manifest. Dependency licenses are documented in [third-party notices](../THIRD_PARTY_NOTICES.md). See [release scope](release-readiness.md).
