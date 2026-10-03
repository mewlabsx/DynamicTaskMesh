# Reference execution examples

First complete the [quickstart](quickstart.md). All examples use the Core/Agent reference profile and simulated capabilities. Windows commands run from the candidate root.

## Submission deduplication

Submit asynchronously with a client-generated key:

```powershell
./bin/dtm-submit.exe -core 127.0.0.1:50051 -async -idempotency-key example-001 -target-temperature 26
./bin/dtm-submit.exe -core 127.0.0.1:50051 -async -idempotency-key example-001 -target-temperature 26
```

The repeat returns the original task ID with `deduplicated=true`. Query that ID to inspect progress. Reusing the key with a different business request is rejected as a conflict. Submission deduplication does not grant permission to repeat an uncertain non-idempotent physical action.

## Query asynchronously submitted work

```powershell
./bin/dtm-query.exe get --task-id <returned-task-id> --core-address 127.0.0.1:50051
./bin/dtm-query.exe executions --task-id <returned-task-id> --core-address 127.0.0.1:50051
```

Replace the placeholder before running. A task may complete before the first query; the example does not require a visible running state.

## Replace the cooling Agent

Stop Cooling 001 with Ctrl+C. Start the alternate configuration:

```powershell
./bin/dtm-agent.exe -config ./configs/demo/cooling-agent-002.yaml
```

Wait for registration, then submit another task. Its cooling result should identify `cooling-node-002`. This demonstrates capability-based target replacement, not Coordinator/Core failover.

## Missing capability

Use a fresh stopped-and-backed-up demo state, start only Core and the temperature Agent, and submit a new task. Without a valid cooling capability, planning/mapping should reject the task rather than invent an executor. Existing integration tests cover this path. Preserve actual errors when reporting results.

## Kernel and experimental work

Inspect `internal/kernel` and `internal/userspace` tests to explore same-fence resolution, UNKNOWN preservation and explicit task closure. These are local mechanism tests. The separately held Goal Loop and Workbench need final acceptance before joining a public example set.


[简体中文](examples.zh-CN.md)
