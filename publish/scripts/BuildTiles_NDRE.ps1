# BuildTiles_NDRE.ps1
# NDRE = (B8A - B05) / (B8A + B05)
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][long]$JobId,
  [Parameter(Mandatory=$true)][string]$DateKey,
  [Parameter(Mandatory=$true)][string]$InputDir,

  [Parameter(Mandatory=$true)][string]$OutputRootPath,
  [Parameter(Mandatory=$true)][string]$OsgeoRoot,

  [int]$ZoomMin = 8,
  [int]$ZoomMax = 16,
  [string]$ProductSubPath = "ndre",

  [double]$IndexMin = 0.0,
  [double]$IndexMax = 0.4,

  [int]$Processes = 1,
  [switch]$CleanOutput
)

$ErrorActionPreference = "Stop"

function Fail($msg) { throw "[NDRE] $msg" }
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
if ($gdalData -ne "") { $env:GDAL_DATA = $gdalData } else { Write-Warning "[NDRE] GDAL_DATA not found under $OsgeoRoot (may warn, usually still works)." }

$outDir = Join-Path (Join-Path $OutputRootPath $ProductSubPath) $DateKey
if ($CleanOutput -and (Test-Path $outDir)) { Remove-Item $outDir -Recurse -Force }
Ensure-Dir $outDir

$workDir = Join-Path $InputDir "_work\ndre"
Ensure-Dir $workDir
Push-Location $workDir

try {
  $b8a = Get-ChildItem -Path $InputDir -Recurse -File -Filter "B8A.tif" -ErrorAction SilentlyContinue
  $b05 = Get-ChildItem -Path $InputDir -Recurse -File -Filter "B05.tif" -ErrorAction SilentlyContinue
  if ($b8a.Count -eq 0 -or $b05.Count -eq 0) { Fail "Missing bands. Need B8A.tif and B05.tif under InputDir." }

  & gdalbuildvrt -overwrite B8A.vrt @($b8a | % FullName) | Out-Null
  & gdalbuildvrt -overwrite B05.vrt @($b05 | % FullName) | Out-Null

  & gdalbuildvrt -overwrite -separate idx.vrt B8A.vrt B05.vrt | Out-Null
  & gdalwarp -overwrite -t_srs EPSG:3857 -r bilinear -multi -wo NUM_THREADS=ALL_CPUS `
    idx.vrt idx_3857.tif -co TILED=YES -co COMPRESS=DEFLATE | Out-Null

  & $python -m osgeo_utils.gdal_calc --quiet `
    -A idx_3857.tif --A_band=1 `
    -B idx_3857.tif --B_band=2 `
    --calc="((A-B)/(A+B+1e-6))" `
    --type=Float32 --NoDataValue=-9999 `
    --overwrite --outfile=ndre_f32.tif

  & gdal_translate -of GTiff ndre_f32.tif ndre_8bit.tif -ot Byte `
    -scale $IndexMin $IndexMax 0 255 `
    -a_nodata 0 `
    -co TILED=YES -co COMPRESS=DEFLATE | Out-Null

  & $python -m osgeo_utils.gdal2tiles --xyz -z "$ZoomMin-$ZoomMax" -w none -r bilinear -e --resume `
    --processes=$Processes ndre_8bit.tif $outDir

  Write-Host "[NDRE] DONE -> $outDir"
}
finally {
  Pop-Location
}
