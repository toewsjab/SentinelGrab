[CmdletBinding(DefaultParameterSetName="GeoJson")]
param(
    [Parameter(Mandatory=$true)]
    [datetime]$DateFrom,

    [Parameter(Mandatory=$true)]
    [datetime]$DateTo,

    [Parameter(Mandatory=$true, ParameterSetName="StoredPath")]
    [long]$PipelinePathId,

    [Parameter(Mandatory=$true, ParameterSetName="GeoJson")]
    [string]$PipelineGeoJson,

    [string]$PipelineName,
    [string]$DirectionDescription,
    [decimal]$ChainageOriginM,
    [double]$CorridorHalfWidthM,
    [double]$BinLengthM,
    [string]$IncludedMonths,
    [int]$Cloud,
    [switch]$SaveToDb,

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

$programArgs = @("--mode", "cli", "--product", "PIPELINE_WATER")
Add-SentinelGrabArgument ([ref]$programArgs) "--date-from" $DateFrom
Add-SentinelGrabArgument ([ref]$programArgs) "--date-to" $DateTo

if ($PSCmdlet.ParameterSetName -eq "StoredPath") {
    Add-SentinelGrabArgument ([ref]$programArgs) "--pipeline-path-id" $PipelinePathId
}
else {
    Add-SentinelGrabArgument ([ref]$programArgs) "--pipeline-geojson" $PipelineGeoJson
}

Add-SentinelGrabArgument ([ref]$programArgs) "--pipeline-name" $PipelineName
Add-SentinelGrabArgument ([ref]$programArgs) "--direction-description" $DirectionDescription
if ($PSBoundParameters.ContainsKey("ChainageOriginM")) { Add-SentinelGrabArgument ([ref]$programArgs) "--chainage-origin-m" $ChainageOriginM }
if ($PSBoundParameters.ContainsKey("CorridorHalfWidthM")) { Add-SentinelGrabArgument ([ref]$programArgs) "--corridor-half-width-m" $CorridorHalfWidthM }
if ($PSBoundParameters.ContainsKey("BinLengthM")) { Add-SentinelGrabArgument ([ref]$programArgs) "--bin-length-m" $BinLengthM }
Add-SentinelGrabArgument ([ref]$programArgs) "--included-months" $IncludedMonths
if ($PSBoundParameters.ContainsKey("Cloud")) { Add-SentinelGrabArgument ([ref]$programArgs) "--cloud" $Cloud }
Add-SentinelGrabSwitch ([ref]$programArgs) "--save-pipeline-water-db" $SaveToDb.IsPresent
$programArgs += $ExtraArgs

Invoke-SentinelGrabProgram `
    -ProjectRoot $ProjectRoot `
    -DotNetPath $DotNetPath `
    -Configuration $Configuration `
    -NoRestore:$NoRestore `
    -DryRun:$DryRun `
    -ProgramArgs $programArgs
