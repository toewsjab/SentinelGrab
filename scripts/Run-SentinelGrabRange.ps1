[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [datetime]$From,

    [Parameter(Mandatory=$true)]
    [datetime]$To,

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

$programArgs = @("--mode", "range")
Add-SentinelGrabArgument ([ref]$programArgs) "--from" $From
Add-SentinelGrabArgument ([ref]$programArgs) "--to" $To
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
