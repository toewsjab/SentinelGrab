[CmdletBinding()]
param(
    [int]$Year,
    [int]$Month,
    [int]$Day,
    [int]$Cloud,
    [double]$FarmCloud,
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

$programArgs = @("--mode", "cli")
if ($PSBoundParameters.ContainsKey("Year")) { Add-SentinelGrabArgument ([ref]$programArgs) "--year" $Year }
if ($PSBoundParameters.ContainsKey("Month")) { Add-SentinelGrabArgument ([ref]$programArgs) "--month" $Month }
if ($PSBoundParameters.ContainsKey("Day")) { Add-SentinelGrabArgument ([ref]$programArgs) "--day" $Day }
if ($PSBoundParameters.ContainsKey("Cloud")) { Add-SentinelGrabArgument ([ref]$programArgs) "--cloud" $Cloud }
if ($PSBoundParameters.ContainsKey("FarmCloud")) { Add-SentinelGrabArgument ([ref]$programArgs) "--farm-cloud" $FarmCloud }
Add-SentinelGrabArgument ([ref]$programArgs) "--bbox" $Bbox
$programArgs += $ExtraArgs

Invoke-SentinelGrabProgram `
    -ProjectRoot $ProjectRoot `
    -DotNetPath $DotNetPath `
    -Configuration $Configuration `
    -NoRestore:$NoRestore `
    -DryRun:$DryRun `
    -ProgramArgs $programArgs
