[CmdletBinding()]
param(
    [int]$Lookback,
    [int]$Lag,
    [string[]]$Products,
    [int]$Cloud,
    [string]$Bbox,

    [string]$ProjectRoot,
    [string]$DotNetPath = "dotnet",
    [ValidateSet("Debug", "Release")]
    [string]$Configuration = "Release",
    [switch]$NoRestore,
    [switch]$DryRun,

    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$ExtraArgs = @()
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "SentinelGrab.Tools.ps1")

$programArgs = @("--mode", "daily")
if ($PSBoundParameters.ContainsKey("Lookback")) { Add-SentinelGrabArgument ([ref]$programArgs) "--lookback" $Lookback }
if ($PSBoundParameters.ContainsKey("Lag")) { Add-SentinelGrabArgument ([ref]$programArgs) "--lag" $Lag }
Add-SentinelGrabArgument ([ref]$programArgs) "--products" $Products
if ($PSBoundParameters.ContainsKey("Cloud")) { Add-SentinelGrabArgument ([ref]$programArgs) "--cloud" $Cloud }
Add-SentinelGrabArgument ([ref]$programArgs) "--bbox" $Bbox
$programArgs += $ExtraArgs

Invoke-SentinelGrabProgram `
    -ProjectRoot $ProjectRoot `
    -DotNetPath $DotNetPath `
    -Configuration $Configuration `
    -NoRestore:$NoRestore `
    -DryRun:$DryRun `
    -ProgramArgs $programArgs
