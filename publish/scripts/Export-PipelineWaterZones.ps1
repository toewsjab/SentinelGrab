[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [long]$RunId,

    [Parameter(Mandatory=$true)]
    [string]$Output,

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

$programArgs = @("--export-pipeline-water-zones")
Add-SentinelGrabArgument ([ref]$programArgs) "--run-id" $RunId
Add-SentinelGrabArgument ([ref]$programArgs) "--output" $Output
$programArgs += $ExtraArgs

Invoke-SentinelGrabProgram `
    -ProjectRoot $ProjectRoot `
    -DotNetPath $DotNetPath `
    -Configuration $Configuration `
    -NoRestore:$NoRestore `
    -DryRun:$DryRun `
    -ProgramArgs $programArgs
