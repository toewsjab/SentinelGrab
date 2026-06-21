# SentinelGrab PowerShell entry points

These scripts are wrappers around `SentinelGrab.csproj`. They keep the C# program as the single implementation of the Sentinel/STAC, database, GDAL, and pipeline-water logic while exposing each current mode as a PowerShell command.

Common parameters:

- `-ProjectRoot`: repository root. Defaults to the parent of this `scripts` folder.
- `-Configuration`: `Release` by default.
- `-NoRestore`: passes `--no-restore` to `dotnet run`.
- `-DryRun`: prints the exact `dotnet run` command without executing it.
- trailing unbound arguments are forwarded to the program.

Examples:

```powershell
.\scripts\Run-SentinelGrabCli.ps1 -Year 2025 -Month 5 -DryRun
.\scripts\Run-SentinelGrabCli.ps1 -Year 2025 -Month 5 -Day 15 -Cloud 40 -Bbox "-103.76,50.52,-103.08,50.95"

.\scripts\Run-SentinelGrabDb.ps1

.\scripts\Run-SentinelGrabDaily.ps1 -Lookback 14 -Lag 1 -Products RGB,NDVI,NDMI,NDRE
.\scripts\Run-SentinelGrabRange.ps1 -From 2025-05-01 -To 2025-05-31 -Products RGB,NDVI

.\scripts\Import-PipelinePath.ps1 -Path .\data\pipeline.geojson -PipelineName "Main line"

.\scripts\Run-PipelineWater.ps1 -DateFrom 2025-04-01 -DateTo 2025-10-31 -PipelinePathId 1 -SaveToDb
.\scripts\Run-PipelineWater.ps1 -DateFrom 2025-04-01 -DateTo 2025-10-31 -PipelineGeoJson .\data\pipeline.geojson -CorridorHalfWidthM 50 -BinLengthM 20

.\scripts\Export-PipelineWaterZones.ps1 -RunId 1 -Output .\out\pipeline-water-zones.geojson
.\scripts\Export-PipelineWaterObservations.ps1 -RunId 1 -Output .\out\pipeline-water-observations.geojson
.\scripts\Export-PipelineWaterCsv.ps1 -RunId 1 -Output .\out\pipeline-water-zones.csv
```

Use `Invoke-SentinelGrab.ps1` when you want the raw mode wrapper:

```powershell
.\scripts\Invoke-SentinelGrab.ps1 -Mode range -- --from 2025-05-01 --to 2025-05-31 --products RGB,NDVI
```

## Published server scripts

Use these on the server from a published folder. They do not build the project and do not call `dotnet run`; they launch the already published `SentinelGrab.exe`, or `SentinelGrab.dll` with `-UseDll`.

```powershell
.\scripts\Run-PublishedSentinelGrabDaily.ps1
.\scripts\Run-PublishedSentinelGrabRange.ps1 -From 2025-05-01 -To 2025-05-31
```

If the scripts are somewhere other than the published app's `scripts` folder, point them at the published app:

```powershell
.\scripts\Run-PublishedSentinelGrabDaily.ps1 -PublishedRoot D:\jtprograms\SentinelGrab
.\scripts\Run-PublishedSentinelGrabRange.ps1 -PublishedRoot D:\jtprograms\SentinelGrab -From 2025-05-01 -To 2025-05-31
```

For Task Scheduler, use `powershell.exe` with arguments like:

```text
-NoProfile -ExecutionPolicy Bypass -File "D:\jtprograms\SentinelGrab\scripts\Run-PublishedSentinelGrabDaily.ps1"
```

If the published scripts are not copied to the server folder, run them from wherever they are and supply the published app folder:

```text
-NoProfile -ExecutionPolicy Bypass -File "C:\code\JTF\SentinelGrab\scripts\Run-PublishedSentinelGrabDaily.ps1" -PublishedRoot "D:\jtprograms\SentinelGrab"
```
