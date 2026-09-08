param([string]$Image = 'auto-agent:smoke')

$ErrorActionPreference = 'Stop'
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'docker is required for container smoke' }
docker image inspect $Image *> $null
if ($LASTEXITCODE -ne 0) { docker build --tag $Image . }
$name = "auto-agent-smoke-$([guid]::NewGuid().ToString('N'))"
try {
  docker run --detach --name $name --publish 18080:8080 `
    --env HARNESS_MODE=dev `
    --env HARNESS_DATABASE_TYPE=sqlite `
    --env HARNESS_SQLITE_PATH=/data/core.db `
    --env HARNESS_DEV_HEADER_AUTH=true $Image | Out-Null
  $ready = $false
  for ($i = 0; $i -lt 60; $i++) {
    try { Invoke-WebRequest http://127.0.0.1:18080/healthz -UseBasicParsing | Out-Null; $ready = $true; break } catch { Start-Sleep -Seconds 1 }
  }
  if (-not $ready) { throw 'container did not become healthy' }
  $health = Invoke-WebRequest http://127.0.0.1:18080/healthz -UseBasicParsing
  if ($health.StatusCode -ne 200 -or $health.Content -notmatch '"status":"ok"') { throw 'health check failed' }
  $readiness = Invoke-WebRequest http://127.0.0.1:18080/readyz -UseBasicParsing
  if ($readiness.StatusCode -ne 200 -or $readiness.Content -notmatch '"status":"ready"') { throw 'readiness check failed' }
  $headers = @{ Accept = 'application/json'; 'Content-Type' = 'application/json'; 'X-Harness-Tenant' = 'smoke'; 'X-Harness-Subject' = 'smoke' }
  $scope = @(
    @{ kind = 'global'; id = 'global' }, @{ kind = 'deployment'; id = 'default' },
    @{ kind = 'product'; id = 'default' }, @{ kind = 'tenant'; id = 'smoke' }, @{ kind = 'user'; id = 'smoke' }
  )
  $publish = @{ scope = $scope; layer = @{ profile_id = 'container.smoke'; name = 'Container smoke'; model = @{ provider = 'mock'; model = 'mock' } } } | ConvertTo-Json -Depth 8
  $published = Invoke-WebRequest http://127.0.0.1:18080/v1/profiles/container.smoke/publish -Method Post -Headers $headers -Body $publish -UseBasicParsing
  if ($published.StatusCode -ne 201) { throw "profile publish failed: $($published.StatusCode)" }
  $session = Invoke-WebRequest http://127.0.0.1:18080/v1/sessions -Method Post -Headers $headers -Body '{"profile_id":"container.smoke"}' -UseBasicParsing
  if ($session.StatusCode -ne 201) { throw "session create failed: $($session.StatusCode)" }
  $sessionID = ($session.Content | ConvertFrom-Json).id
  if ([string]::IsNullOrWhiteSpace($sessionID)) { throw 'session id missing' }
  $headers.Accept = 'text/event-stream'
  $sse = Invoke-WebRequest "http://127.0.0.1:18080/v1/sessions/$sessionID/runs" -Method Post -Headers $headers -Body '{"message":"container smoke"}' -TimeoutSec 15 -UseBasicParsing
  if ($sse.StatusCode -ne 200 -or $sse.Headers['Content-Type'] -notmatch 'text/event-stream' -or $sse.Content -notmatch '(?m)^event: ' -or $sse.Content -notmatch '(?m)^data: ') { throw 'SSE response did not contain valid event/data frames' }
  Write-Output 'container HTTP/readiness/real-session/SSE smoke passed'
} finally {
  docker rm --force $name *> $null
}
