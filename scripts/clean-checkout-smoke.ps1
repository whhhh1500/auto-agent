$ErrorActionPreference = 'Stop'
$root = (Get-Location).Path
$temp = Join-Path ([IO.Path]::GetTempPath()) ("harness-clean-" + [guid]::NewGuid())
New-Item -ItemType Directory -Path $temp | Out-Null
try {
  $paths = @('LICENSE', 'CONTRIBUTING.md', 'SECURITY.md', 'CODE_OF_CONDUCT.md', 'CHANGELOG.md', 'SUPPORT.md', 'RELEASE.md', 'README.md', 'go.mod', 'go.sum', 'Dockerfile', 'docker-compose.yml', '.dockerignore', 'cmd', 'examples', 'internal', 'pkg', 'scripts', 'docs', 'openapi', '.github')
  foreach ($path in $paths) { Copy-Item -LiteralPath (Join-Path $root $path) -Destination $temp -Recurse -Force }
  Push-Location $temp
  go list ./...
  go test -count=1 -timeout 600s ./...
  go build ./...
  Pop-Location
} finally {
  if ((Get-Location).Path -ne $root) { Pop-Location }
  Remove-Item -LiteralPath $temp -Recurse -Force
}
