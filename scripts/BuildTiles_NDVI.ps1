# BuildTiles_NDVI.ps1
# NDVI = (B08 - B04) / (B08 + B04)
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][long]$JobId,
  [Parameter(Mandatory=$true)][string]$DateKey,
  [Parameter(Mandatory=$true)][string]$InputDir,

  [Parameter(Mandatory=$true)][string]$OutputRootPath,
  [Parameter(Mandatory=$true)][string]$OsgeoRoot,

  [int]$ZoomMin = 8,
  [int]$ZoomMax = 16,
  [string]$ProductSubPath = "ndvi",

  # NDVI scaling to 8-bit
  [double]$IndexMin = -0.2,
  [double]$IndexMax = 0.9,

  [int]$Processes = 1,
  [switch]$CleanOutput
)

$ErrorActionPreference = "Stop"

function Fail($msg) { throw "[NDVI] $msg" }
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
if ($gdalData -ne "") { $env:GDAL_DATA = $gdalData } else { Write-Warning "[NDVI] GDAL_DATA not found under $OsgeoRoot (may warn, usually still works)." }

$outDir = Join-Path (Join-Path $OutputRootPath $ProductSubPath) $DateKey
if ($CleanOutput -and (Test-Path $outDir)) { Remove-Item $outDir -Recurse -Force }
Ensure-Dir $outDir

$workDir = Join-Path $InputDir "_work\ndvi"
Ensure-Dir $workDir
Push-Location $workDir

try {
  $b08 = Get-ChildItem -Path $InputDir -Recurse -File -Filter "B08.tif" -ErrorAction SilentlyContinue
  $b04 = Get-ChildItem -Path $InputDir -Recurse -File -Filter "B04.tif" -ErrorAction SilentlyContinue
  if ($b08.Count -eq 0 -or $b04.Count -eq 0) { Fail "Missing bands. Need B08.tif and B04.tif under InputDir." }

  & gdalbuildvrt -overwrite B08.vrt @($b08 | % FullName) | Out-Null
  & gdalbuildvrt -overwrite B04.vrt @($b04 | % FullName) | Out-Null

  # Stack then warp once so bands align
  & gdalbuildvrt -overwrite -separate idx.vrt B08.vrt B04.vrt | Out-Null
  & gdalwarp -overwrite -t_srs EPSG:3857 -r bilinear -multi -wo NUM_THREADS=ALL_CPUS `
    idx.vrt idx_3857.tif -co TILED=YES -co COMPRESS=DEFLATE | Out-Null

  # NDVI float
  & $python -m osgeo_utils.gdal_calc --quiet `
    -A idx_3857.tif --A_band=1 `
    -B idx_3857.tif --B_band=2 `
    --calc="((A-B)/(A+B+1e-6))" `
    --type=Float32 --NoDataValue=-9999 `
    --overwrite --outfile=ndvi_f32.tif

  # Scale to 8-bit for tiling
  & gdal_translate -of GTiff ndvi_f32.tif ndvi_8bit.tif -ot Byte `
    -scale $IndexMin $IndexMax 0 255 `
    -a_nodata 0 `
    -co TILED=YES -co COMPRESS=DEFLATE | Out-Null

  & $python -m osgeo_utils.gdal2tiles --xyz -z "$ZoomMin-$ZoomMax" -w none -r bilinear -e --resume `
    --processes=$Processes ndvi_8bit.tif $outDir

  Write-Host "[NDVI] DONE -> $outDir"
}
finally {
  Pop-Location
}
