# Contributing

This is a personally maintained AGPL-3.0-only source preview. The maintainer does not commit to accepting pull requests or to response times. The following describes development boundaries for anyone inspecting or modifying the source.

Read [architecture authority](architecture/README.md), [status](docs/status.md) and [AGENTS.md](AGENTS.md) before changing contracts. Keep task policy in User Space, preserve Resource/Capability identity boundaries, append migrations, and retain compatibility profiles. A completed local milestone does not authorize a future one.

From the candidate root, run:

```text
go build ./cmd/...
go test -count=1 -timeout=15m ./...
go vet ./...
```

Use meaningful tests for behavior changes. Race and fuzz checks are additional targeted gates, subject to supported compiler/toolchain availability. PowerShell protocol regeneration is documented in [protocol generation](docs/protocol-generation.md).

Kernel architecture tests also read documentation. Preserve frozen boundary text and historical statuses when editing README, AGENTS, YAML or referenced records; do not remove assertions to make a documentation edit pass. Add a short public explanation instead of replacing historical truth.

Describe the problem, resulting behavior, scope, compatibility implications and actual validation. Keep caches, runtime databases, raw logs, source snapshots, credentials and review bundles outside published source. Check only the files intended for each change.
