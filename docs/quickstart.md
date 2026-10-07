# Quickstart: submit and query a local task

[简体中文](quickstart.zh-CN.md)

This walkthrough uses the existing Core/Agent execution reference with simulated temperature and cooling capabilities. It does not invoke the new Kernel Goal Loop or real equipment. Run every command from the candidate root.

## Requirements and build

Use Go 1.24 or a compatible toolchain. The locally checked toolchain is recorded in [validation](validation.md). Dependencies are declared in `go.mod` and `go.sum`; first-time builds need access to the Go module cache or dependency downloads. Protobuf generated files are included, so protoc is not required for an ordinary build.

For Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force bin | Out-Null
go build -o ./bin/ ./cmd/...
```

For a POSIX shell:

```sh
mkdir -p bin
go build -o ./bin/ ./cmd/...
```

Windows commands below use `.exe`; on other platforms omit that suffix. This walkthrough's local verification covers Windows only; it does not establish Linux or macOS support.

## Start three services

Use three terminals, all at the candidate root. Start the Core:

```powershell
./bin/dtm-core.exe -config ./configs/demo/core.yaml
```

Start the temperature Agent in the second terminal:

```powershell
./bin/dtm-agent.exe -config ./configs/demo/sensor-agent.yaml
```

Start the cooling Agent in the third terminal:

```powershell
./bin/dtm-agent.exe -config ./configs/demo/cooling-agent-001.yaml
```

Wait for `dtm-core listening` and each Agent's `register success` message. The Core uses `127.0.0.1:50051`; temperature and cooling Agents use ports `50061` and `50062`. Database paths in these files resolve relative to the configuration file directory, placing demo state under `data/` in the candidate root. Ports must be unused before starting.

## Submit and query

In a fourth terminal:

```powershell
./bin/dtm-submit.exe -core 127.0.0.1:50051 -target-temperature 26
```

The expected response reports a succeeded task with `sensor-node-001` and `cooling-node-001` results. The simulated sensor reports temperature 30, and the cooling result includes `cooling_started`. This is execution success, not measured physical cooling or proof of a new Root Task goal predicate.

Copy the returned task ID, then run:

```powershell
./bin/dtm-query.exe get --task-id <returned-task-id> --core-address 127.0.0.1:50051
./bin/dtm-query.exe executions --task-id <returned-task-id> --core-address 127.0.0.1:50051
./bin/dtm-query.exe list --limit 10 --core-address 127.0.0.1:50051
```

Replace `<returned-task-id>` with the actual value before running. Additional controlled scenarios are in [examples](examples.md).

## Stop and start over

Press Ctrl+C in each Agent terminal and then in the Core terminal. Keep state if you want to inspect existing tasks after restart. For a fresh demonstration, first stop all processes, then move the candidate's `data/` directory to a backup location of your choice. Do not remove a live database or assume restart automatically retries uncertain side effects.

If a service cannot bind, check for another process using its port. If registration does not complete, confirm that all three configurations use the same Core address. Storage startup errors should be investigated before submitting work; do not edit migration history to bypass them.
