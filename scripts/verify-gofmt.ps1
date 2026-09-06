$ErrorActionPreference = 'Stop'
$failed = $false

$repoRoot = Split-Path -Parent $PSScriptRoot
$sourceRoots = @('cmd', 'examples', 'internal', 'pkg', 'scripts') |
  ForEach-Object { Join-Path $repoRoot $_ } |
  Where-Object { Test-Path -LiteralPath $_ }
$files = Get-ChildItem -Path $sourceRoots -Recurse -File -Filter '*.go' |
  Where-Object {
    $_.FullName -notmatch '[\\/]vendor[\\/]' -and
    $_.FullName -notmatch '[\\/]\.[^\\/]+[\\/]'
  }
foreach ($file in $files) {
  $diff = (& gofmt -d $file.FullName 2>&1 | Out-String)
  if (-not [string]::IsNullOrWhiteSpace($diff)) {
    $diff.TrimEnd()
    $failed = $true
  }
}

if ($failed) {
  Write-Error 'gofmt check failed; run gofmt -w on the files above.'
  exit 1
}
