# BuildTiles_RGB.ps1
# RGB = (B04,B03,B02) -> warp EPSG:3857 -> 8-bit stretch -> XYZ tiles
[CmdletBinding()]
param(
  [Parameter(Mandatory=$true)][long]$JobId,
  [Parameter(Mandatory=$true)][string]$DateKey,
  [Parameter(Mandatory=$true)][string]$InputDir,

  [Parameter(Mandatory=$true)][string]$OutputRootPath,
  [Parameter(Mandatory=$true)][string]$OsgeoRoot,

  [int]$ZoomMin = 8,
  [int]$ZoomMax = 16,
  [string]$ProductSubPath = "rgb",

  # 0..ScaleMaxRGB -> 0..255 (tune 3000..6000)
  [int]$ScaleMaxRGB = 4000,

  # Keep 1 on Windows for stability
  [int]$Processes = 1,

  [switch]$CleanOutput
)

$ErrorActionPreference = "Stop"

function Fail($msg) { throw "[RGB] $msg" }
function Ensure-Dir([string]$p) { if (-not (Test-Path $p)) { New-Item -ItemType Directory -Force -Path $p | Out-Null } }

function Resolve-PythonExe([string]$root) {
  $p = Join-Path $root "apps\Python312\python.exe"
  if (Test-Path $p) { return $p }

  $py = Get-ChildItem (Join-Path $root "apps") -Directory -Filter "Python*" -ErrorAction SilentlyContinue |
        Sort-Object Name -Descending |
        Select-Object -First 1
  if ($null -eq $py) { Fail "Could not find Python under $root\apps" }
  $p2 = Join-Path $py.FullName "python.exe"
  if (-not (Test-Path $p2)) { Fail "python.exe not found at $p2" }
  return $p2
}

function Resolve-GdalData([string]$root) {
  $cand = @(
    (Join-Path $root "share\gdal"),
    (Join-Path $root "apps\gdal\share\gdal")
  )
  foreach ($c in $cand) { if (Test-Path $c) { return $c } }

  # Deep search fallback: find a known GDAL data file like gcs.csv
  $hit = Get-ChildItem $root -Recurse -Filter "gcs.csv" -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($hit) { return (Split-Path $hit.FullName -Parent) }

  # If not found, return empty and we proceed (tools may still run but may warn)
  return ""
}

$gdalBin = Join-Path $OsgeoRoot "bin"
if (-not (Test-Path $gdalBin)) { Fail "GDAL bin not found: $gdalBin (check -OsgeoRoot)" }

$python = Resolve-PythonExe $OsgeoRoot
$gdalData = Resolve-GdalData $OsgeoRoot

$env:PATH = "$gdalBin;$env:PATH"
if ($gdalData -ne "") { $env:GDAL_DATA = $gdalData } else { Write-Warning "[RGB] GDAL_DATA not found under $OsgeoRoot (may warn, usually still works)." }

$outDir = Join-Path (Join-Path $OutputRootPath $ProductSubPath) $DateKey
if ($CleanOutput -and (Test-Path $outDir)) { Remove-Item $outDir -Recurse -Force }
Ensure-Dir $outDir

$workDir = Join-Path $InputDir "_work\rgb"
Ensure-Dir $workDir
Push-Location $workDir

try {
  $b02 = Get-ChildItem -Path $InputDir -Recurse -File -Filter "B02.tif" -ErrorAction SilentlyContinue
  $b03 = Get-ChildItem -Path $InputDir -Recurse -File -Filter "B03.tif" -ErrorAction SilentlyContinue
  $b04 = Get-ChildItem -Path $InputDir -Recurse -File -Filter "B04.tif" -ErrorAction SilentlyContinue
  if ($b02.Count -eq 0 -or $b03.Count -eq 0 -or $b04.Count -eq 0) {
    Fail "Missing bands under InputDir. Need B02.tif, B03.tif, B04.tif."
  }

  # Mosaic each band (handles multi-scene)
  & gdalbuildvrt -overwrite B02.vrt @($b02 | % FullName) | Out-Null
  & gdalbuildvrt -overwrite B03.vrt @($b03 | % FullName) | Out-Null
  & gdalbuildvrt -overwrite B04.vrt @($b04 | % FullName) | Out-Null

  # Stack RGB: R,G,B = B04,B03,B02
  & gdalbuildvrt -overwrite -separate rgb.vrt B04.vrt B03.vrt B02.vrt | Out-Null

  & gdalwarp -overwrite -t_srs EPSG:3857 -r bilinear -multi -wo NUM_THREADS=ALL_CPUS `
    rgb.vrt rgb_3857.tif -co TILED=YES -co COMPRESS=DEFLATE | Out-Null

  # 8-bit stretch (fixes dark tiles)
  & gdal_translate -of GTiff rgb_3857.tif rgb_3857_8bit.tif -ot Byte `
    -scale_1 0 $ScaleMaxRGB 0 255 `
    -scale_2 0 $ScaleMaxRGB 0 255 `
    -scale_3 0 $ScaleMaxRGB 0 255 `
    -co TILED=YES -co COMPRESS=DEFLATE | Out-Null

  & $python -m osgeo_utils.gdal2tiles --xyz -z "$ZoomMin-$ZoomMax" -w none -r bilinear -e --resume `
    --processes=$Processes rgb_3857_8bit.tif $outDir

  Write-Host "[RGB] DONE -> $outDir"
}
finally {
  Pop-Location
}
