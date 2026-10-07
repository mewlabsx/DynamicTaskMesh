# Current design contracts

[简体中文](contracts.zh-CN.md)

This release publishes current design by topic. Version-by-version plans, detailed implementation reports, reviews, findings and raw evidence archives remain private/local; they are not copied into fixtures or another public directory.

| Topic | Contract |
|---|---|
| Task, Step, Execution and Root/Child/work | [Task model](task-model.md) |
| Mechanism/policy, Intent, Scope and governance | [Kernel/User Space boundary](kernel-userspace-boundary.md) |
| Logical request/result, side effects and same-fence resolution | [Execution](execution-contract.md) |
| Resource identity, capability authority, security and capacity | [Resource authority](resource-authority.md) |
| SQLite profile, local identity, uncertainty and restart limitations | [Recovery and idempotency](recovery-idempotency.md) |

The five [Architecture State YAML files](../architecture/README.md) preserve effective IDs, scoped supersession, invariants, states and future boundaries. Document references point to these current topics; original private provenance is retained separately with hashes. Retained governance sign-off remains necessary semantic authority and is not a new acceptance or release validation. [Status](status.md) describes bounded implementation, and [validation](validation.md) records actual checks.

The consolidation changes publication and document-check locations, not runtime behavior, frozen scope or milestone state. Core/Agent and Kernel/User Space are not yet one fully integrated runtime. See [integration](integration.md).
