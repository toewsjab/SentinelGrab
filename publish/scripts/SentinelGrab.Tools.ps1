Set-StrictMode -Version Latest

$script:SentinelGrabToolsRoot = Split-Path -Parent $PSCommandPath

function Get-SentinelGrabProjectRoot {
    [CmdletBinding()]
    param(
        [string]$ProjectRoot
    )

    if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
        $candidate = Resolve-Path -LiteralPath (Join-Path $script:SentinelGrabToolsRoot "..")
    }
    else {
        $candidate = Resolve-Path -LiteralPath $ProjectRoot
    }

    $root = $candidate.ProviderPath
    $projectFile = Join-Path $root "SentinelGrab.csproj"
    if (-not (Test-Path -LiteralPath $projectFile -PathType Leaf)) {
        throw "SentinelGrab.csproj was not found under '$root'. Supply -ProjectRoot."
    }

    return $root
}

function Add-SentinelGrabArgument {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)][ref]$Arguments,
        [Parameter(Mandatory=$true)][string]$Name,
        [object]$Value
    )

    if ($null -eq $Value) {
        return
    }

    if ($Value -is [string] -and $Value.Length -eq 0) {
        return
    }

    if ($Value -is [array] -and $Value.Count -eq 0) {
        return
    }

    if ($Value -is [array]) {
        $Arguments.Value = @($Arguments.Value) + @($Name, ($Value -join ","))
        return
    }

    if ($Value -is [datetime]) {
        $Arguments.Value = @($Arguments.Value) + @($Name, $Value.ToString("yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture))
        return
    }

    if ($Value -is [IFormattable]) {
        $Arguments.Value = @($Arguments.Value) + @($Name, $Value.ToString($null, [Globalization.CultureInfo]::InvariantCulture))
        return
    }

    $Arguments.Value = @($Arguments.Value) + @($Name, [string]$Value)
}

function Add-SentinelGrabSwitch {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)][ref]$Arguments,
        [Parameter(Mandatory=$true)][string]$Name,
        [Parameter(Mandatory=$true)][bool]$Enabled
    )

    if ($Enabled) {
        $Arguments.Value = @($Arguments.Value) + $Name
    }
}

function Invoke-SentinelGrabProgram {
    [CmdletBinding()]
    param(
        [string]$ProjectRoot,
        [string]$DotNetPath = "dotnet",
        [ValidateSet("Debug", "Release")]
        [string]$Configuration = "Release",
        [switch]$NoRestore,
        [switch]$DryRun,
        [string[]]$ProgramArgs = @()
    )

    $root = Get-SentinelGrabProjectRoot -ProjectRoot $ProjectRoot
    $projectFile = Join-Path $root "SentinelGrab.csproj"

    $dotnetArgs = @(
        "run",
        "--project", $projectFile,
        "--configuration", $Configuration
    )

    if ($NoRestore) {
        $dotnetArgs += "--no-restore"
    }

    $dotnetArgs += "--"
    $dotnetArgs += $ProgramArgs

    Write-Host ("& {0} {1}" -f $DotNetPath, ($dotnetArgs -join " "))

    if ($DryRun) {
        return
    }

    & $DotNetPath @dotnetArgs
    if ($LASTEXITCODE -ne 0) {
        throw "SentinelGrab exited with code $LASTEXITCODE."
    }
}
