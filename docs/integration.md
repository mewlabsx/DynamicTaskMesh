# Runtime integration direction

The current Core/Agent commands use the existing reference execution profile. The bounded Kernel and User Space implementation is separate. This preview does not claim an integrated autonomous runtime.

The next integration direction is one primary execution path: explicit task input -> User Space task/execution orchestration -> Kernel logical facade -> a Provider adapter -> existing Agent execution. Existing CLI, transport and query functionality should be reused where their contracts remain valid.

Before implementation, define stable task/work/invocation/allocation/attempt bindings, registry-to-Kernel authority mappings, Provider outcome and occupancy evidence, and a single execution owner. Existing retries, remapping and recovery must not also control work owned by the new path. A discovery record is not execution authority; an old registration generation is not automatically a Kernel ownership fence.

Kernel authority and occupancy currently include in-memory state, while the existing Core recovers reference state from SQLite. Integration must explicitly address restart behavior. A first bounded profile may use durable isolation and fail-closed recovery instead of complete Kernel persistence; it must not silently redispatch unresolved work or reconstruct an empty resource as free after restart.

The first accepted integrated profile must demonstrate real command-to-Agent execution, authorization rejection without side effects, duplicate-request protection, UNKNOWN visibility without implicit retry/release, and safe behavior at crash/restart boundaries. Existing reference scenarios must continue to pass. Normal successful execution alone is insufficient.

The default path can switch only when public primary scenarios, query/idempotency behavior and the stated recovery guarantees are covered, no old scheduler bypass remains, and independent review has no blocking findings. Old execution ownership logic can then retire; reusable compatibility adapters may remain.

The intent is to avoid two parallel feature-development tracks: new execution features should target the Kernel/User Space boundary, with the old profile limited to necessary compatibility and correctness work during migration. This document proposes direction; it does not authorize implementation or change frozen milestone statuses. Full autonomous operation, Intent runtime and broader admission are not prerequisites for the first bounded profile.


[简体中文](integration.zh-CN.md)
