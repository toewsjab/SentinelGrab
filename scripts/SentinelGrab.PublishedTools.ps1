Set-StrictMode -Version Latest

$script:SentinelGrabPublishedToolsRoot = Split-Path -Parent $PSCommandPath

function Get-SentinelGrabPublishedRoot {
    [CmdletBinding()]
    param(
        [string]$PublishedRoot
    )

    if (-not [string]::IsNullOrWhiteSpace($PublishedRoot)) {
        $root = (Resolve-Path -LiteralPath $PublishedRoot).ProviderPath
    }
    else {
        $scriptParent = Resolve-Path -LiteralPath (Join-Path $script:SentinelGrabPublishedToolsRoot "..")
        $root = $scriptParent.ProviderPath

        $appInParent = (Test-Path -LiteralPath (Join-Path $root "SentinelGrab.exe") -PathType Leaf) -or
            (Test-Path -LiteralPath (Join-Path $root "SentinelGrab.dll") -PathType Leaf)
        if (-not $appInParent) {
            $repoPublish = Join-Path $root "publish"
            if (Test-Path -LiteralPath $repoPublish -PathType Container) {
                $root = (Resolve-Path -LiteralPath $repoPublish).ProviderPath
            }
        }
    }

    $exePath = Join-Path $root "SentinelGrab.exe"
    $dllPath = Join-Path $root "SentinelGrab.dll"
    if (-not (Test-Path -LiteralPath $exePath -PathType Leaf) -and
        -not (Test-Path -LiteralPath $dllPath -PathType Leaf)) {
        throw "SentinelGrab.exe or SentinelGrab.dll was not found under '$root'. Supply -PublishedRoot."
    }

    return $root
}

function Add-SentinelGrabPublishedArgument {
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

function Invoke-PublishedSentinelGrabProgram {
    [CmdletBinding()]
    param(
        [string]$PublishedRoot,
        [string]$DotNetPath = "dotnet",
        [switch]$UseDll,
        [switch]$DryRun,
        [string[]]$ProgramArgs = @()
    )

    $root = Get-SentinelGrabPublishedRoot -PublishedRoot $PublishedRoot
    $exePath = Join-Path $root "SentinelGrab.exe"
    $dllPath = Join-Path $root "SentinelGrab.dll"

    Push-Location $root
    try {
        if ((Test-Path -LiteralPath $exePath -PathType Leaf) -and -not $UseDll) {
            Write-Host ("& {0} {1}" -f $exePath, ($ProgramArgs -join " "))
            if (-not $DryRun) {
                & $exePath @ProgramArgs
            }
        }
        else {
            Write-Host ("& {0} {1} {2}" -f $DotNetPath, $dllPath, ($ProgramArgs -join " "))
            if (-not $DryRun) {
                & $DotNetPath $dllPath @ProgramArgs
            }
        }

        if (-not $DryRun -and $LASTEXITCODE -ne 0) {
            throw "SentinelGrab exited with code $LASTEXITCODE."
        }
    }
    finally {
        Pop-Location
    }
}
