# BuildTiles_NDMI.ps1
# NDMI = (B08 - B11) / (B08 + B11)
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][long]$JobId,
  [Parameter(Mandatory=$true)][string]$DateKey,
  [Parameter(Mandatory=$true)][string]$InputDir,

  [Parameter(Mandatory=$true)][string]$OutputRootPath,
  [Parameter(Mandatory=$true)][string]$OsgeoRoot,

  [int]$ZoomMin = 12,
  [int]$ZoomMax = 16,
  [string]$ProductSubPath = "ndmi",

  [double]$IndexMin = -0.2,
  [double]$IndexMax = 0.6,

  [int]$Processes = 1,
  [switch]$CleanOutput
)

$ErrorActionPreference = "Stop"

function Fail($msg) { throw "[NDMI] $msg" }
function Ensure-Dir([string]$p) { if (-not (Test-Path $p)) { New-Item -ItemType Directory -Force -Path $p | Out-Null } }

function Resolve-PythonExe([string]$root) {
  $p = Join-Path $root "apps\Python312\python.exe"
  if (Test-Path $p) { return $p }
  $py = Get-ChildItem (Join-Path $root "apps") -Directory -Filter "Python*" -ErrorAction SilentlyContinue |
        Sort-Object Name -Descending | Select-Object -First 1
  if ($null -eq $py) { Fail "Could not find Python under $root\apps" }
  $p2 = Join-Path $py.FullName "python.exe"
  if (-not (Test-Path $p2)) { Fail "python.exe not found at $p2" }
  return $p2
}

function Resolve-GdalData([string]$root) {
  $cand = @((Join-Path $root "share\gdal"), (Join-Path $root "apps\gdal\share\gdal"))
  foreach ($c in $cand) { if (Test-Path $c) { return $c } }
  $hit = Get-ChildItem $root -Recurse -Filter "gcs.csv" -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($hit) { return (Split-Path $hit.FullName -Parent) }
  return ""
}

$gdalBin = Join-Path $OsgeoRoot "bin"
if (-not (Test-Path $gdalBin)) { Fail "GDAL bin not found: $gdalBin (check -OsgeoRoot)" }
$python = Resolve-PythonExe $OsgeoRoot
$gdalData = Resolve-GdalData $OsgeoRoot

$env:PATH = "$gdalBin;$env:PATH"
if ($gdalData -ne "") { $env:GDAL_DATA = $gdalData } else { Write-Warning "[NDMI] GDAL_DATA not found under $OsgeoRoot (may warn, usually still works)." }

$outDir = Join-Path (Join-Path $OutputRootPath $ProductSubPath) $DateKey
if ($CleanOutput -and (Test-Path $outDir)) { Remove-Item $outDir -Recurse -Force }
Ensure-Dir $outDir

$workDir = Join-Path $InputDir "_work\ndmi"
Ensure-Dir $workDir
Push-Location $workDir

try {
  $b08 = Get-ChildItem -Path $InputDir -Recurse -File -Filter "B08.tif" -ErrorAction SilentlyContinue
  $b11 = Get-ChildItem -Path $InputDir -Recurse -File -Filter "B11.tif" -ErrorAction SilentlyContinue
  if ($b08.Count -eq 0 -or $b11.Count -eq 0) { Fail "Missing bands. Need B08.tif and B11.tif under InputDir." }

  & gdalbuildvrt -overwrite B08.vrt @($b08 | % FullName) | Out-Null
  & gdalbuildvrt -overwrite B11.vrt @($b11 | % FullName) | Out-Null

  & gdalbuildvrt -overwrite -separate idx.vrt B08.vrt B11.vrt | Out-Null
  & gdalwarp -overwrite -t_srs EPSG:3857 -r bilinear -multi -wo NUM_THREADS=ALL_CPUS `
    idx.vrt idx_3857.tif -co TILED=YES -co COMPRESS=DEFLATE | Out-Null

  & $python -m osgeo_utils.gdal_calc --quiet `
    -A idx_3857.tif --A_band=1 `
    -B idx_3857.tif --B_band=2 `
    --calc="((A-B)/(A+B+1e-6))" `
    --type=Float32 --NoDataValue=-9999 `
    --overwrite --outfile=ndmi_f32.tif

  & gdal_translate -of GTiff ndmi_f32.tif ndmi_8bit.tif -ot Byte `
    -scale $IndexMin $IndexMax 0 255 `
    -a_nodata 0 `
    -co TILED=YES -co COMPRESS=DEFLATE | Out-Null

  & $python -m osgeo_utils.gdal2tiles --xyz -z "$ZoomMin-$ZoomMax" -w none -r bilinear -e --resume `
    --processes=$Processes ndmi_8bit.tif $outDir

  Write-Host "[NDMI] DONE -> $outDir"
}
finally {
  Pop-Location
}
