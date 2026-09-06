param(
  [Parameter(Mandatory = $true)][string]$ServerBinary,
  [int]$Port = 18081,
  [ValidateSet('sqlite', 'postgres')][string]$DatabaseType = 'sqlite',
  [string]$PostgresDSN = ''
)

$ErrorActionPreference = 'Stop'

$binary = (Resolve-Path -LiteralPath $ServerBinary).Path
$tempBase = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$smokeRoot = [IO.Path]::GetFullPath((Join-Path $tempBase ("harness-bootstrap-smoke-" + [guid]::NewGuid().ToString('N'))))
if (-not $smokeRoot.StartsWith($tempBase, [StringComparison]::OrdinalIgnoreCase)) {
  throw 'temporary smoke directory escaped the system temporary directory'
}

$tempRootCreated = $false
$originalPostgresDsn = $env:HARNESS_POSTGRES_DSN
$postgresDsnSet = $false

$environment = @"
HARNESS_MODE=dev
HARNESS_DATA_DIR=./data
HARNESS_SERVER_PORT=$Port
"@

if ($DatabaseType -eq 'postgres') {
  $effectivePostgresDsn = if ([string]::IsNullOrWhiteSpace($env:HARNESS_POSTGRES_DSN)) {
    $PostgresDSN
  } else {
    $env:HARNESS_POSTGRES_DSN
  }

  if ([string]::IsNullOrWhiteSpace($effectivePostgresDsn) -or $effectivePostgresDsn -match "[`r`n]") {
    throw 'PostgreSQL requires one-line HARNESS_POSTGRES_DSN or PostgresDSN parameter'
  }

  $env:HARNESS_POSTGRES_DSN = $effectivePostgresDsn
  $postgresDsnSet = $true
  $environment += "`nHARNESS_DATABASE_TYPE=postgres`nHARNESS_POSTGRES_DSN=$effectivePostgresDsn`n"
} else {
  $environment += "`nHARNESS_DATABASE_TYPE=sqlite`nHARNESS_SQLITE_PATH=./runtime.db`n"
}

$base = "http://127.0.0.1:$Port"
$first = $null
$second = $null
$scriptError = $null
$cleanupErrors = New-Object System.Collections.Generic.List[string]
$result = $null

function Add-CleanupError {
  param([System.Collections.Generic.List[string]]$Errors, [string]$Message)
  $Errors.Add($Message) | Out-Null
}

function Stop-SmokeProcess {
  param([Diagnostics.Process]$Process)
  try {
    if ($null -eq $Process -or $Process.HasExited) {
      return
    }
    Stop-Process -Id $Process.Id -ErrorAction Stop
    $null = $Process.WaitForExit(30000)
  } catch {
    if ($_.Exception -is [System.InvalidOperationException]) {
      return
    }
    throw
  }
}

function Wait-SmokeReady([Diagnostics.Process]$Process) {
  for ($attempt = 0; $attempt -lt 100; $attempt++) {
    if ($Process.HasExited) {
      throw "server exited before readiness with code $($Process.ExitCode)"
    }
    try {
      $response = Invoke-WebRequest "$base/readyz" -UseBasicParsing -TimeoutSec 1
      if ($response.StatusCode -eq 200) { return }
    } catch {}
    Start-Sleep -Milliseconds 100
  }
  throw 'server readiness timeout'
}

function Start-SmokeServer([string]$Prefix) {
  Start-Process -FilePath $binary -WorkingDirectory $smokeRoot -WindowStyle Hidden `
    -RedirectStandardOutput (Join-Path $smokeRoot "$Prefix.out.log") `
    -RedirectStandardError (Join-Path $smokeRoot "$Prefix.err.log") -PassThru
}

function Read-SmokeLog([string]$Prefix) {
  (Get-Content -Raw (Join-Path $smokeRoot "$Prefix.out.log") -ErrorAction SilentlyContinue) +
    (Get-Content -Raw (Join-Path $smokeRoot "$Prefix.err.log") -ErrorAction SilentlyContinue)
}

try {
  New-Item -ItemType Directory -Path $smokeRoot | Out-Null
  $tempRootCreated = $true
  [IO.File]::WriteAllText((Join-Path $smokeRoot '.env'), $environment)

  $first = Start-SmokeServer 'first'
  Wait-SmokeReady $first
  $firstLog = Read-SmokeLog 'first'
  $accountMatch = [regex]::Match($firstLog, 'ONE-TIME INITIAL ADMIN ACCOUNT: (admin_\d{5})')
  $passwordMatch = [regex]::Match($firstLog, 'ONE-TIME INITIAL ADMIN PASSWORD: (\S+)')
  if (-not $accountMatch.Success -or -not $passwordMatch.Success) {
    throw 'one-time credentials were not printed on first start'
  }
  $account = $accountMatch.Groups[1].Value
  $initialPassword = $passwordMatch.Groups[1].Value

  $loginBody = @{ account = $account; password = $initialPassword } | ConvertTo-Json -Compress
  $login = Invoke-RestMethod "$base/v1/auth/login" -Method Post -ContentType 'application/json' -Body $loginBody
  if (-not $login.must_change_password -or [string]::IsNullOrWhiteSpace($login.token)) {
    throw 'initial login was not restricted'
  }
  $restrictedHeaders = @{ Authorization = "Bearer $($login.token)" }
  $deniedStatus = 0
  try {
    $deniedStatus = (Invoke-WebRequest "$base/v1/admin/overview" -Headers $restrictedHeaders -UseBasicParsing).StatusCode
  } catch {
    if ($null -ne $_.Exception.Response) {
      $deniedStatus = [int]$_.Exception.Response.StatusCode
    } else {
      throw
    }
  }
  if ($deniedStatus -ne 403) {
    throw "pending account overview returned $deniedStatus"
  }

  $passwordBytes = New-Object byte[] 24
  $passwordGenerator = [Security.Cryptography.RandomNumberGenerator]::Create()
  try {
    $passwordGenerator.GetBytes($passwordBytes)
  } finally {
    $passwordGenerator.Dispose()
  }
  $newPassword = [Convert]::ToBase64String($passwordBytes)
  $activateBody = @{ password = $newPassword; confirm = $newPassword } | ConvertTo-Json -Compress
  $activated = Invoke-RestMethod "$base/v1/auth/activate" -Method Post -Headers $restrictedHeaders -ContentType 'application/json' -Body $activateBody
  if ($activated.must_change_password -or [string]::IsNullOrWhiteSpace($activated.token)) {
    throw 'activation did not return a normal token'
  }
  $activeHeaders = @{ Authorization = "Bearer $($activated.token)" }
  $overview = Invoke-WebRequest "$base/v1/admin/overview" -Headers $activeHeaders -UseBasicParsing
  if ($overview.StatusCode -ne 200) {
    throw 'activated account could not access overview'
  }

  Stop-SmokeProcess -Process $first
  $first = $null
  $second = Start-SmokeServer 'second'
  Wait-SmokeReady $second
  $secondLog = Read-SmokeLog 'second'
  if ($secondLog -match 'ONE-TIME INITIAL ADMIN (ACCOUNT|PASSWORD)') {
    throw 'credentials were printed again after restart'
  }
  $reloginBody = @{ account = $account; password = $newPassword } | ConvertTo-Json -Compress
  $relogin = Invoke-RestMethod "$base/v1/auth/login" -Method Post -ContentType 'application/json' -Body $reloginBody
  if ($relogin.must_change_password -or [string]::IsNullOrWhiteSpace($relogin.token)) {
    throw 'relogin after restart failed'
  }

  $result = [PSCustomObject]@{
    first_start_credentials_printed = $true
    pending_access_status = $deniedStatus
    activation_overview_status = $overview.StatusCode
    restart_credentials_reprinted = $false
    restart_login_active = $true
    database_ready = if ($DatabaseType -eq 'sqlite') { Test-Path (Join-Path $smokeRoot 'runtime.db') } else { $true }
    database_type = $DatabaseType
  }
} catch {
  $scriptError = $_
} finally {
  try {
    Stop-SmokeProcess -Process $first
  } catch {
    Add-CleanupError -Errors $cleanupErrors -Message ('could not stop first process: ' + $_.Exception.Message)
  }
  try {
    Stop-SmokeProcess -Process $second
  } catch {
    Add-CleanupError -Errors $cleanupErrors -Message ('could not stop second process: ' + $_.Exception.Message)
  }

  if ($postgresDsnSet) {
    try {
      if ($null -eq $originalPostgresDsn) {
        Remove-Item Env:\HARNESS_POSTGRES_DSN -ErrorAction Stop
      } else {
        $env:HARNESS_POSTGRES_DSN = $originalPostgresDsn
      }
    } catch {
      Add-CleanupError -Errors $cleanupErrors -Message ('could not restore HARNESS_POSTGRES_DSN: ' + $_.Exception.Message)
    }
  }

  if ($tempRootCreated -and (Test-Path -LiteralPath $smokeRoot)) {
    $resolved = [IO.Path]::GetFullPath($smokeRoot)
    if ($resolved.StartsWith($tempBase, [StringComparison]::OrdinalIgnoreCase)) {
      try {
        Remove-Item -LiteralPath $resolved -Recurse -Force -ErrorAction Stop
      } catch {
        Add-CleanupError -Errors $cleanupErrors -Message ('could not remove temporary smoke dir: ' + $_.Exception.Message)
      }
    }
  }

  if ($cleanupErrors.Count -gt 0) {
    Write-Verbose ($cleanupErrors -join [Environment]::NewLine)
  }
}

if ($null -ne $scriptError) {
  throw $scriptError
}
if ($null -ne $result) {
  $result
}
