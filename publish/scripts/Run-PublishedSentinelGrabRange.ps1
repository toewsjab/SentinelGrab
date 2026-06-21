[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [datetime]$From,

    [Parameter(Mandatory=$true)]
    [datetime]$To,

    [string[]]$Products,
    [int]$Cloud,
    [string]$Bbox,

    [string]$PublishedRoot,
    [string]$DotNetPath = "dotnet",
    [switch]$UseDll,
    [switch]$DryRun,

    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$ExtraArgs = @()
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "SentinelGrab.PublishedTools.ps1")

$programArgs = @("--mode", "range")
Add-SentinelGrabPublishedArgument ([ref]$programArgs) "--from" $From
Add-SentinelGrabPublishedArgument ([ref]$programArgs) "--to" $To
Add-SentinelGrabPublishedArgument ([ref]$programArgs) "--products" $Products
if ($PSBoundParameters.ContainsKey("Cloud")) { Add-SentinelGrabPublishedArgument ([ref]$programArgs) "--cloud" $Cloud }
Add-SentinelGrabPublishedArgument ([ref]$programArgs) "--bbox" $Bbox
$programArgs += $ExtraArgs

Invoke-PublishedSentinelGrabProgram `
    -PublishedRoot $PublishedRoot `
    -DotNetPath $DotNetPath `
    -UseDll:$UseDll `
    -DryRun:$DryRun `
    -ProgramArgs $programArgs
