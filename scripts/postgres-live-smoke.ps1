param(
  [Parameter(Mandatory = $true)][string]$PostgresBin,
  [int]$PostgresPort = 55432,
  [int]$ServerPort = 18082
)

$ErrorActionPreference = 'Stop'

if ($PostgresPort -eq 5432) {
  throw 'PostgreSQL smoke must not use the default host instance port 5432'
}

$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$bin = (Resolve-Path -LiteralPath $PostgresBin).Path
$requiredPrograms = @('initdb.exe', 'pg_ctl.exe', 'createdb.exe', 'pg_isready.exe', 'psql.exe')
foreach ($program in $requiredPrograms) {
  if (-not (Test-Path -LiteralPath (Join-Path $bin $program))) {
    throw "missing PostgreSQL program: $program"
  }
}

function Assert-PortAvailable {
  param([int]$Port, [string]$Label)
  $listener = $null
  try {
    $listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Loopback, $Port)
    $listener.Start()
  } catch {
    throw "$Label port $Port is already in use or cannot be bound"
  } finally {
    if ($null -ne $listener) {
      $listener.Stop()
    }
  }
}

function Quote-CmdArg {
  param([Parameter(Mandatory = $true)][string]$Argument)
  return '"' + ($Argument -replace '"', '\"') + '"'
}

function Invoke-PgCtl {
  param(
    [Parameter(Mandatory = $true)][string]$Mode,
    [Parameter(Mandatory = $true)][string]$DataDir,
    [string]$LogPath,
    [string]$Options,
    [string]$ExtraArgs
  )

  $pgCtlPath = Join-Path $bin 'pg_ctl.exe'
  $argParts = @("-D $(Quote-CmdArg $DataDir)")

  if ($Mode -eq 'start') {
    if ($LogPath) {
      $argParts += "-l $(Quote-CmdArg $LogPath)"
    }
    if ($Options) {
      $argParts += "-o $(Quote-CmdArg $Options)"
    }
  } elseif ($Mode -eq 'stop') {
    if ($ExtraArgs) {
      $argParts += $ExtraArgs
    }
  } else {
    throw "unsupported pg_ctl mode: $Mode"
  }

  $argParts += '-w'
  $argParts += $Mode
  $arguments = $argParts -join ' '

  $psi = New-Object System.Diagnostics.ProcessStartInfo
  $psi.FileName = $pgCtlPath
  $psi.Arguments = $arguments
  $psi.UseShellExecute = $true
  $psi.WindowStyle = [System.Diagnostics.ProcessWindowStyle]::Hidden

  $pgCtlProcess = [System.Diagnostics.Process]::Start($psi)
  if ($null -eq $pgCtlProcess) {
    throw "failed to start pg_ctl process for mode '$Mode'"
  }

  $pgCtlProcess.WaitForExit()
  return $pgCtlProcess.ExitCode
}

Assert-PortAvailable -Port $PostgresPort -Label 'PostgreSQL'
Assert-PortAvailable -Port $ServerPort -Label 'server'

function Wait-ForPostmasterExit {
  param([string]$DataDir)
  $pidPath = Join-Path $DataDir 'postmaster.pid'
  for ($attempt = 0; $attempt -lt 120; $attempt++) {
    if (-not (Test-Path -LiteralPath $pidPath)) {
      return $true
    }
    [System.Threading.Thread]::Sleep(100)
  }
  return $false
}

function Wait-PostgresAcceptingConnections {
  param(
    [int]$Port,
    [string]$Role,
    [int]$Attempts = 100,
    [int]$SleepMilliseconds = 100
  )
  for ($attempt = 0; $attempt -lt $Attempts; $attempt++) {
    & (Join-Path $bin 'pg_isready.exe') -h 127.0.0.1 -p $Port -d postgres -U $Role *> $null
    if ($LASTEXITCODE -eq 0) {
      return $true
    }
    [System.Threading.Thread]::Sleep($SleepMilliseconds)
  }
  return $false
}

$tempBase = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$smokeRoot = [IO.Path]::GetFullPath((Join-Path $tempBase ("harness-postgres-smoke-" + [guid]::NewGuid().ToString('N'))))
if (-not $smokeRoot.StartsWith($tempBase, [StringComparison]::OrdinalIgnoreCase)) {
  throw 'temporary PostgreSQL directory escaped the system temporary directory'
}

$tempRootCreated = $false
$role = 'harness_test_admin'
$clusterStartIssued = $false
$clusterReady = $false
$clusterStopConfirmed = $false
$cleanupErrors = New-Object System.Collections.Generic.List[string]
$previousTestDSN = $env:HARNESS_TEST_PG_DSN
$previousBootstrapDSN = $env:HARNESS_POSTGRES_DSN
$testPgDb = 'postgres'
$locationPushed = $false
$scriptError = $null
$result = $null

function Add-CleanupError {
  param([System.Collections.Generic.List[string]]$Errors, [string]$Message)
  $Errors.Add($Message) | Out-Null
}

try {
  New-Item -ItemType Directory -Path $smokeRoot | Out-Null
  $tempRootCreated = $true
  $dataDir = Join-Path $smokeRoot 'data'
  $logPath = Join-Path $smokeRoot 'postgres.log'
  $serverBinary = Join-Path $smokeRoot 'harness-server.exe'

  & (Join-Path $bin 'initdb.exe') -D $dataDir -U $role -A trust --encoding=UTF8 --no-locale *> $null
  if ($LASTEXITCODE -ne 0) { throw 'initdb failed' }

  $pgCtlStartExitCode = Invoke-PgCtl -Mode 'start' -DataDir $dataDir -LogPath $logPath -Options "-p $PostgresPort -h 127.0.0.1"
  if ($pgCtlStartExitCode -ne 0) { throw "temporary PostgreSQL start failed (exit code: $pgCtlStartExitCode)" }
  $clusterStartIssued = $true

  $clusterReady = Wait-PostgresAcceptingConnections -Port $PostgresPort -Role $role
  if (-not $clusterReady) {
    throw 'PostgreSQL did not report ready to accept connections in time'
  }

  & (Join-Path $bin 'createdb.exe') -h 127.0.0.1 -p $PostgresPort -U $role harness_server_smoke *> $null
  if ($LASTEXITCODE -ne 0) { throw 'create smoke database failed' }

  $testDSN = "postgres://${role}@127.0.0.1:$PostgresPort/postgres?sslmode=disable"
  $serverDSN = "postgres://${role}@127.0.0.1:$PostgresPort/harness_server_smoke?sslmode=disable"
  if ($testDSN -match '://[^/]+/([^?]+)') {
    $testPgDb = $matches[1]
  }
  $env:HARNESS_TEST_PG_DSN = $testDSN
  $activeTestDsn = $testDSN
  $env:HARNESS_POSTGRES_DSN = $serverDSN

  try {
    Push-Location $projectRoot
    $locationPushed = $true
    try {
      & go run ./scripts/test-postgres -log (Join-Path $smokeRoot 'postgres-test.jsonl')
      if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL integration gate failed' }

      & go build -o $serverBinary ./cmd/server
      if ($LASTEXITCODE -ne 0) { throw 'server build failed' }

      & (Join-Path $PSScriptRoot 'bootstrap-smoke.ps1') -ServerBinary $serverBinary -Port $ServerPort -DatabaseType postgres
      if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL server lifecycle smoke failed' }

      $schemaCountOutput = & (Join-Path $bin 'psql.exe') -h 127.0.0.1 -p $PostgresPort -U $role -d $testPgDb -Atq -c "SELECT COUNT(*) FROM pg_namespace WHERE nspname LIKE 'harness_test_%';"
      $schemaCount = 0
      if ([int]::TryParse($schemaCountOutput.Trim(), [ref]$schemaCount)) {
        if ($schemaCount -ne 0) {
          throw "residual harness_test_* schemas detected: $schemaCount"
        }
      } else {
        throw "residue schema check returned non-numeric result: '$schemaCountOutput'"
      }

      $result = [PSCustomObject]@{
        postgres_storage_tests = $true
        postgres_server_lifecycle = $true
        residual_harness_test_schema_count = $schemaCount
        isolated_port = $PostgresPort
        persistent_instance_touched = $false
      }
    } finally {
      if ($locationPushed) {
        Pop-Location
        $locationPushed = $false
      }
    }
  } finally {
    if ($null -eq $previousTestDSN) {
      Remove-Item Env:\HARNESS_TEST_PG_DSN -ErrorAction SilentlyContinue
    } else {
      $env:HARNESS_TEST_PG_DSN = $previousTestDSN
    }

    if ($null -eq $previousBootstrapDSN) {
      Remove-Item Env:\HARNESS_POSTGRES_DSN -ErrorAction SilentlyContinue
    } else {
      $env:HARNESS_POSTGRES_DSN = $previousBootstrapDSN
    }
  }
} catch {
  $scriptError = $_
} finally {
  if ($clusterStartIssued -and -not $clusterReady) {
    $startupWaitCompleted = Wait-PostgresAcceptingConnections -Port $PostgresPort -Role $role -Attempts 30 -SleepMilliseconds 100
    if (-not $startupWaitCompleted) {
      Add-CleanupError -Errors $cleanupErrors -Message 'PostgreSQL startup confirmation timed out during cleanup pre-stop wait'
    }
  }

  if ($clusterStartIssued) {
    $stopExitCode = $null
    try {
      $stopExitCode = Invoke-PgCtl -Mode 'stop' -DataDir $dataDir -ExtraArgs '-m fast'
      if ($stopExitCode -eq 0 -and (Wait-ForPostmasterExit -DataDir $dataDir)) {
        $clusterStopConfirmed = $true
      } else {
        Add-CleanupError -Errors $cleanupErrors -Message "PostgreSQL stop verification failed (exit code: $stopExitCode)"
      }
    } catch {
      Add-CleanupError -Errors $cleanupErrors -Message ('PostgreSQL stop failed: ' + $_.Exception.Message)
    }
  }

  if ($clusterStopConfirmed -and $tempRootCreated -and (Test-Path -LiteralPath $smokeRoot)) {
    $resolved = [IO.Path]::GetFullPath($smokeRoot)
    if ($resolved.StartsWith($tempBase, [StringComparison]::OrdinalIgnoreCase)) {
      try {
        Remove-Item -LiteralPath $resolved -Recurse -Force -ErrorAction Stop
      } catch {
        Add-CleanupError -Errors $cleanupErrors -Message ('remove temp dir failed: ' + $_.Exception.Message)
      }
    }
  }

  if ($cleanupErrors.Count -gt 0) {
    Write-Verbose ($cleanupErrors -join [Environment]::NewLine)
  }
}

if ($null -ne $scriptError) {
  if ($cleanupErrors.Count -gt 0) {
    Write-Warning ('Cleanup warnings: ' + ($cleanupErrors -join [Environment]::NewLine))
  }
  throw $scriptError
}

if ($cleanupErrors.Count -gt 0) {
  throw "cleanup failed despite successful test flow: $($cleanupErrors -join '; ')"
}

if ($null -ne $result) {
  $result
}
