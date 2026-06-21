[CmdletBinding()]
param(
    [ValidateSet("cli", "db", "daily", "range")]
    [string]$Mode = "cli",

    [string]$ProjectRoot,
    [string]$DotNetPath = "dotnet",
    [ValidateSet("Debug", "Release")]
    [string]$Configuration = "Release",
    [switch]$NoRestore,
    [switch]$DryRun,

    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$ProgramArgs = @()
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "SentinelGrab.Tools.ps1")

$argsToPass = @("--mode", $Mode) + $ProgramArgs
Invoke-SentinelGrabProgram `
    -ProjectRoot $ProjectRoot `
    -DotNetPath $DotNetPath `
    -Configuration $Configuration `
    -NoRestore:$NoRestore `
    -DryRun:$DryRun `
    -ProgramArgs $argsToPass
