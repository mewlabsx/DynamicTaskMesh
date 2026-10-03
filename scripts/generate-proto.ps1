[CmdletBinding()]
param()

$ErrorActionPreference = "Stop"
$repositoryRoot = Split-Path -Parent $PSScriptRoot
$protoRoot = Join-Path $repositoryRoot "api\proto"
$protoFiles = @("api\proto\dtm\v1\dtm.proto", "api\proto\dtm\v1\invocation.proto")

$expectedVersions = [ordered]@{
    "protoc" = "libprotoc 30.2"
    "protoc-gen-go" = "protoc-gen-go v1.36.6"
    "protoc-gen-go-grpc" = "protoc-gen-go-grpc 1.5.1"
}
$resolvedTools = @{}
foreach ($tool in $expectedVersions.Keys) {
    $command = Get-Command $tool -ErrorAction SilentlyContinue
    if (-not $command) {
        throw "$tool is required on PATH; expected $($expectedVersions[$tool])"
    }
    $resolvedTools[$tool] = $command.Source
}

foreach ($tool in $expectedVersions.Keys) {
    $actual = ((& $resolvedTools[$tool] --version 2>&1) | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "$tool --version failed with exit code $LASTEXITCODE"
    }
    $normalized = $actual -replace '^([A-Za-z0-9-]+)\.exe(\s+)', '$1$2'
    if ($normalized -ne $expectedVersions[$tool]) {
        throw "$tool version mismatch: got '$actual' (normalized '$normalized'), expected '$($expectedVersions[$tool])'"
    }
    Write-Host "$tool=$normalized"
}

Push-Location $repositoryRoot
try {
    & $resolvedTools["protoc"] `
        "--plugin=protoc-gen-go=$($resolvedTools['protoc-gen-go'])" `
        "--plugin=protoc-gen-go-grpc=$($resolvedTools['protoc-gen-go-grpc'])" `
        "--proto_path=." `
        "--go_out=$repositoryRoot" `
        "--go_opt=paths=source_relative" `
        "--go-grpc_out=$repositoryRoot" `
        "--go-grpc_opt=paths=source_relative" `
        $protoFiles
    if ($LASTEXITCODE -ne 0) {
        throw "protoc failed with exit code $LASTEXITCODE"
    }
}
finally {
    Pop-Location
}

& gofmt -w `
    (Join-Path $protoRoot "dtm\v1\invocation.pb.go") `
    (Join-Path $protoRoot "dtm\v1\invocation_grpc.pb.go") `
    (Join-Path $protoRoot "dtm\v1\dtm.pb.go") `
    (Join-Path $protoRoot "dtm\v1\dtm_grpc.pb.go")
if ($LASTEXITCODE -ne 0) {
    throw "gofmt failed with exit code $LASTEXITCODE"
}
