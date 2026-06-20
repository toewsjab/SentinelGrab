# Farm-level Sentinel-2 cloud screening

This version screens Sentinel-2 Level-2A acquisitions using the per-pixel `SCL` band before downloading the larger RGB or vegetation-index bands.

## Pipeline

1. Search STAC using the existing date range and bbox.
2. Group STAC items by `s2:datatake_id`, so adjacent MGRS tiles from the same satellite pass are evaluated together.
3. Download only `SCL.tif` for each candidate acquisition.
4. Use GDAL to crop/mosaic the SCL rasters to the configured bbox.
5. Count SCL classes inside that bbox.
6. Reject acquisitions that do not cover the bbox or exceed the local cloud/shadow threshold.
7. Download the requested spectral bands only for the selected acquisition.
8. Build the existing RGB/NDVI/NDMI/NDRE tiles from that selected acquisition.

## Configuration

```json
"FarmCloudScreening": {
  "Enabled": true,
  "MaxCloudOrShadowPercent": 1,
  "MinDataCoveragePercent": 99,
  "MaxAcquisitionsToCheck": 0
}
```

`MaxAcquisitionsToCheck = 0` evaluates every acquisition returned by the STAC request.

The existing `Cli.CloudCoverMax` is still the whole Sentinel tile metadata filter. Set it to `100` when farm-level screening is enabled, otherwise a tile reported as 90% cloudy is discarded before its farm pixels can be checked. In DB mode, this version automatically searches with 100% when farm screening is enabled; the job's `CloudCoverMax` remains in the schema but is not used as the local farm threshold.

CLI override:

```powershell
dotnet run -- --year 2025 --month 5 --cloud 100 --farm-cloud 1
```

Scheduled daily processing:

```powershell
dotnet SentinelGrab.dll --mode daily
```

Daily mode searches the configured recent window, farm-screens candidate acquisitions, skips products already present in `SentinelGrabAvailableLayers`, queues a normal SentinelGrab job for missing products, and processes it immediately.

Manual date-range processing:

```powershell
dotnet SentinelGrab.dll --mode range --from 2025-05-01 --to 2025-05-31
```

Range mode checks each calendar day in the range and processes configured products for dates with acceptable imagery.

## SCL interpretation

The selector counts SCL classes 8, 9, and 10 as cloud and class 3 as cloud shadow. Snow (11), unclassified/low-probability cloud (7), and saturated/defective pixels (1) are reported separately and do not currently fail the cloud threshold.

## Current boundary limitation

The current job model contains only a bbox. Cloud scoring and output clipping therefore use that rectangle, not the exact farm parcel geometry. To process only the legal/field polygons, add a GeoJSON or WKT boundary to the job and use it as a GDAL cutline for both `FarmCloudScorer` and `GdalProductTileBuilder`.

## Mosaic limitation

The previous `PreferMosaic` behavior selected several low-metadata-cloud items and stacked them in a VRT. That is not a cloud-aware mosaic and can retain clouds. With farm screening enabled, this version selects one acquisition pass and all MGRS tiles needed to cover the bbox. A true multi-date mosaic requires masking each source band with its matching SCL raster before compositing clear pixels.
