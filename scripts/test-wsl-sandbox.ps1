param(
    [string]$Distro = 'Ubuntu-24.04',
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$User,
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$Ext4Root,
    [Parameter(Mandatory = $true)]
    [ValidateNotNullOrEmpty()]
    [string]$WindowsRoot
)

$ErrorActionPreference = 'Stop'

function Require-DDrivePath([string]$Path, [string]$Label) {
    $candidate = $Path
    if ($candidate.StartsWith('\\?\')) {
        $candidate = $candidate.Substring(4)
    }
    $resolved = (Resolve-Path -LiteralPath $candidate).Path
    if ([System.IO.Path]::GetPathRoot($resolved) -ne 'D:\') {
        throw "$Label must resolve on D:, got $resolved"
    }
    return $resolved
}

function Convert-ToWslPath([string]$Path) {
    # wsl.exe treats backslashes in its command tail as escapes.  Normalize
    # only the command argument to portable drive-slash form before asking the
    # selected distribution to convert it.
    $wslInput = $Path.Replace('\', '/')
    $converted = & wsl.exe -d $Distro -- /usr/bin/wslpath -u -- $wslInput
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($converted)) {
        throw "failed to convert path for WSL"
    }
    return $converted.Trim()
}

if (-not (Get-Command wsl.exe -ErrorAction SilentlyContinue)) {
    throw 'wsl.exe is required'
}

$lxssRoot = 'HKCU:\Software\Microsoft\Windows\CurrentVersion\Lxss'
$distribution = Get-ChildItem -LiteralPath $lxssRoot | ForEach-Object {
    $properties = Get-ItemProperty -LiteralPath $_.PSPath
    if ($properties.DistributionName -eq $Distro) { $properties }
} | Select-Object -First 1
if ($null -eq $distribution) {
    throw "WSL distribution $Distro is not registered"
}
if ($distribution.Version -ne 2) {
    throw "WSL distribution $Distro must use WSL2"
}

$basePath = Require-DDrivePath $distribution.BasePath 'WSL BasePath'
$vhdx = Join-Path $basePath 'ext4.vhdx'
if (-not (Test-Path -LiteralPath $vhdx -PathType Leaf)) {
    throw "WSL VHDX is missing at the registered D: BasePath"
}

$windowsParent = Require-DDrivePath $WindowsRoot 'WindowsRoot'
$runRoot = Join-Path $windowsParent ('harness-wsl-sandbox-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $runRoot | Out-Null

try {
    $scriptPath = Convert-ToWslPath (Join-Path $PSScriptRoot 'test-wsl-sandbox.sh')
    $wslRunRoot = Convert-ToWslPath $runRoot
    $userID = & wsl.exe -d $Distro -u $User -- /usr/bin/id -u
    if ($LASTEXITCODE -ne 0) {
        throw "WSL user $User is unavailable"
    }
    if ($userID.Trim() -eq '0') {
        throw 'WSL sandbox acceptance refuses root; provide a non-root -User'
    }

    & wsl.exe -d $Distro -u $User -- /bin/bash $scriptPath `
        --distro $Distro --user $User --ext4-root $Ext4Root --windows-root $wslRunRoot
    if ($LASTEXITCODE -ne 0) {
        throw 'WSL sandbox acceptance failed'
    }

    $evidence = Get-ChildItem -LiteralPath $runRoot -Filter '*.json' -File -ErrorAction SilentlyContinue
    if ($evidence.Count -eq 0) {
        throw 'WSL sandbox acceptance produced no JSON evidence'
    }
    $evidence | ForEach-Object {
        Write-Host ("evidence: " + $_.FullName)
        Get-Content -LiteralPath $_.FullName
    }
    Write-Host ("WSL BasePath verified on D: " + $basePath)
    Write-Host ("WSL VHDX verified on D: " + $vhdx)
}
catch {
    $failureEvidence = [ordered]@{
        schema = 'harness-wsl-sandbox-v1'
        status = 'fail'
        stage  = 'preflight_or_runner'
    } | ConvertTo-Json -Compress
    Set-Content -LiteralPath (Join-Path $runRoot 'failure.json') -Value $failureEvidence -NoNewline
    # With $ErrorActionPreference=Stop, Write-Error would itself terminate this
    # catch block before the retained-evidence location is printed.
    [Console]::Error.WriteLine('WSL sandbox acceptance failed: ' + $_.Exception.Message)
    Write-Host ("failed evidence root retained for inspection: " + $runRoot)
    exit 1
}
