param(
    [string]$RepositoryRoot = (Resolve-Path "$PSScriptRoot\\..")
)

$ErrorActionPreference = 'Stop'
$modulePath = 'github.com/cc-auto-agent/harness-core'
$work = Join-Path ([System.IO.Path]::GetTempPath()) ("harness-module-smoke-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $work | Out-Null
try {
    Push-Location $work
    go mod init example.com/harness-smoke
    if ($LASTEXITCODE -ne 0) { throw 'go mod init failed' }
    go mod edit "-replace=$modulePath=$RepositoryRoot"
    if ($LASTEXITCODE -ne 0) { throw 'go mod edit failed' }
    @"
package smoke

import core "$modulePath/pkg/core"

var _ = core.DefaultMaxSteps
"@ | Set-Content -NoNewline smoke.go
    go get "$modulePath/pkg/core"
    if ($LASTEXITCODE -ne 0) { throw 'go get through local replace failed' }
    go test .
    if ($LASTEXITCODE -ne 0) { throw 'external module compile failed' }
}
finally {
    Pop-Location
    Remove-Item -LiteralPath $work -Recurse -Force
}
