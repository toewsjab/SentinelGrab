[CmdletBinding()]
param(
    [int]$Lookback,
    [int]$Lag,
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

$programArgs = @("--mode", "daily")
if ($PSBoundParameters.ContainsKey("Lookback")) { Add-SentinelGrabPublishedArgument ([ref]$programArgs) "--lookback" $Lookback }
if ($PSBoundParameters.ContainsKey("Lag")) { Add-SentinelGrabPublishedArgument ([ref]$programArgs) "--lag" $Lag }
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
