# Dynamic Task Mesh

English | [简体中文](README.zh-CN.md)

Dynamic Task Mesh (DTM) is an experimental, task-oriented resource orchestration project. It includes a Core/Agent execution reference, Level 1 mesh and Resource Invocation foundations, and a bounded in-process Kernel with explicit User Space task mechanisms.

The software line is **v0.7**. The Kernel specification is **v0.1**. These are separate version axes. The initial public source preview is **v0.7.0-alpha.1**, under AGPL-3.0-only, at [mewlabsx/DynamicTaskMesh](https://github.com/mewlabsx/DynamicTaskMesh). See [release scope](docs/release-readiness.md). Core/Agent commands use the existing reference path and do not yet run through Kernel/User Space; see [integration direction](docs/integration.md).

## Start here

- [Quickstart](docs/quickstart.md): run a Core, two simulated Agents, submit a task and query its result.
- [Architecture overview](docs/overview.md): understand the reference execution path and the Kernel/User Space boundary.
- [Capability status](docs/status.md): distinguish implemented mechanisms, experimental candidates and future scope.
- [Examples](docs/examples.md): submission deduplication, asynchronous queries and capability replacement.
- [Documentation and selected history](docs/README.md).
- [Contributing](CONTRIBUTING.md).

Build and check from this directory with Go 1.24 or a compatible toolchain:

```text
go build ./cmd/...
go test -count=1 -timeout=15m ./...
go vet ./...
```

These commands are intended checks; the candidate's recorded results belong in [validation](docs/validation.md).

## Implementation boundaries

The existing Core/Agent reference supports capability-based execution, SQLite-backed Task/Step/Execution state, querying, submission deduplication, fencing and conservative recovery. The Level 1 mesh and native gRPC Resource Invocation foundations remain compatibility references.

The Kernel provides bounded object, execution ownership and same-fence occupancy resolution mechanisms through an in-process logical facade. User Space adds a bounded execution orchestrator, Child/Root Task mechanisms and a task creation boundary. Gate B Phase 1 and Phase 2 closure do not establish a complete autonomous Kernel loop.

DTM does not currently claim production readiness, final Architecture Freeze, KernelIntent runtime, production Resource Admission, new-Kernel persistence or remote ABI, multi-Core HA, Binary transport, unrestricted derivation, or functional safety. Unknown side-effect outcomes must not trigger implicit retries. See [status](docs/status.md) for the distinction between the reference profile and Kernel mechanisms.

The Goal Loop and local Workbench are held in a separate local review directory pending final acceptance. They are not part of this candidate's default commands or capability claims.

## Architecture authority

[Architecture State](architecture/README.md) and its referenced governance records describe the effective architecture. Historical design, implementation and review records retain their original scope and status; a later timestamp does not override a frozen decision.

The [R0.1 UserIntent / KernelIntent Boundary review](docs/kernel-userspace-boundary.md#effective-state-and-governance) records the Intent distinction: UserIntent belongs to User Space; KernelIntent is a prospective opaque Kernel-managed representation and grants no capability authority. Current validation does not enable Intent runtime. [AGENTS.md](AGENTS.md) preserves the development and historical boundary rules.

## Source provenance

[Third-party notices](THIRD_PARTY_NOTICES.md) records dependency licenses and preserved upstream texts.

## License and maintenance

Copyright (c) 2026 Zhao Tao (赵涛).

Unless otherwise identified, DTM's own code and documentation in this candidate are licensed under the GNU Affero General Public License, version 3 only (`AGPL-3.0-only`); see [LICENSE](LICENSE). Third-party components and their notices retain their respective licenses.

Commercial use and modification are permitted under the license. Distribution requires compliance with its corresponding-source obligations. If you modify the program and users interact with your version remotely over a computer network, you must prominently offer those users an opportunity to obtain the corresponding source free of charge, as required by section 13. This does not require submitting changes to this repository or automatically publishing every private modification. The license text governs; the software is provided without warranty to the extent permitted by law.

This is a personally maintained project focused on sharing code and technical documentation. There is no commitment to response times, long-term support or acceptance of pull requests. Before distributing a modified network-enabled version, provide an appropriate source access mechanism; this notice is not such a runtime mechanism.

The candidate was assembled on October 3, 2026 from local HEAD `0684db339c854fee7b86210c92da16d133189f29` and selected working-tree content. It omits raw archives, local runtime data, application materials and bundled tooling. Some historical home paths were replaced with placeholders; historical results were not upgraded. [Development history](docs/history.md) explains the evidence limits.
