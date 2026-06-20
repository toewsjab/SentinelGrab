[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)]
    [string]$Path,

    [string]$PipelineName,
    [string]$DirectionDescription,
    [string]$SourceReference,
    [decimal]$ChainageOriginM,
    [double]$EndpointToleranceM,
    [double]$DensifyMaxSegmentM,
    [double]$MaxSectionLengthM,

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

$programArgs = @("--import-pipeline", $Path)
Add-SentinelGrabArgument ([ref]$programArgs) "--pipeline-name" $PipelineName
Add-SentinelGrabArgument ([ref]$programArgs) "--direction-description" $DirectionDescription
Add-SentinelGrabArgument ([ref]$programArgs) "--source-reference" $SourceReference
if ($PSBoundParameters.ContainsKey("ChainageOriginM")) { Add-SentinelGrabArgument ([ref]$programArgs) "--chainage-origin-m" $ChainageOriginM }
if ($PSBoundParameters.ContainsKey("EndpointToleranceM")) { Add-SentinelGrabArgument ([ref]$programArgs) "--endpoint-tolerance-m" $EndpointToleranceM }
if ($PSBoundParameters.ContainsKey("DensifyMaxSegmentM")) { Add-SentinelGrabArgument ([ref]$programArgs) "--densify-max-segment-m" $DensifyMaxSegmentM }
if ($PSBoundParameters.ContainsKey("MaxSectionLengthM")) { Add-SentinelGrabArgument ([ref]$programArgs) "--max-section-length-m" $MaxSectionLengthM }
$programArgs += $ExtraArgs

Invoke-SentinelGrabProgram `
    -ProjectRoot $ProjectRoot `
    -DotNetPath $DotNetPath `
    -Configuration $Configuration `
    -NoRestore:$NoRestore `
    -DryRun:$DryRun `
    -ProgramArgs $programArgs
