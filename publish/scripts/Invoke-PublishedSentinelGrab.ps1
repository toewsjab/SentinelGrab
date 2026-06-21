[CmdletBinding()]
param(
    [ValidateSet("cli", "db", "daily", "range")]
    [string]$Mode = "cli",

    [string]$PublishedRoot,
    [string]$DotNetPath = "dotnet",
    [switch]$UseDll,
    [switch]$DryRun,

    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$ProgramArgs = @()
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "SentinelGrab.PublishedTools.ps1")

$argsToPass = @("--mode", $Mode) + $ProgramArgs
Invoke-PublishedSentinelGrabProgram `
    -PublishedRoot $PublishedRoot `
    -DotNetPath $DotNetPath `
    -UseDll:$UseDll `
    -DryRun:$DryRun `
    -ProgramArgs $argsToPass
