# Protocol generation

Normal builds use checked-in generated Go files. To modify protocols, install the generator versions enforced by `scripts/generate-proto.ps1`: protoc 30.2, protoc-gen-go v1.36.6 and protoc-gen-go-grpc 1.5.1. Tool installation is outside this local candidate preparation.

Run from the root in PowerShell:

```powershell
./scripts/generate-proto.ps1
go test ./api/...
```

The candidate script must generate both `api/proto/dtm/v1/dtm.proto` and `invocation.proto`, then format their generated Go outputs. Review generated diffs and compatibility before acceptance. Regeneration is not considered verified unless the required tools are present and the command actually succeeds; see [validation](validation.md).


[简体中文](protocol-generation.zh-CN.md)
