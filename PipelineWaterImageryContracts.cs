public sealed record PipelineSectionCorridor
{
    public PipelineSection Section { get; init; } = new();
    public int ProjectedSrid { get; init; }
    public string CorridorWgs84GeoJson { get; init; } = "";
    public string CorridorWgs84Wkt { get; init; } = "";
    public string CorridorProjectedWkt { get; init; } = "";
}

public sealed record PipelineWaterSectionSearchPlan
{
    public PipelineSectionCorridor Corridor { get; init; } = new();
    public IReadOnlyList<StacAcquisitionGroup> AcquisitionGroups { get; init; } = Array.Empty<StacAcquisitionGroup>();
}

public sealed record PipelineChainageBinPart
{
    public PipelineChainageBin Bin { get; init; } = new();
    public int ProjectedSrid { get; init; }
    public string RouteBinProjectedWkt { get; init; } = "";
    public string RouteBinWgs84Wkt { get; init; } = "";
    public string CorridorProjectedWkt { get; init; } = "";
    public string CorridorWgs84Wkt { get; init; } = "";
}

public sealed record PipelineChainageBinGeometry
{
    public PipelineChainageBin Bin { get; init; } = new();
    public IReadOnlyList<PipelineChainageBinPart> Parts { get; init; } = Array.Empty<PipelineChainageBinPart>();
    public string RouteBinWgs84Wkt { get; init; } = "";
}

public sealed record PipelineWaterSectionDetection
{
    public string AcquisitionKey { get; init; } = "";
    public DateTimeOffset AcquiredAt { get; init; }
    public int SectionOrdinal { get; init; }
    public int ProjectedSrid { get; init; }
    public IReadOnlyList<string> WaterPolygonProjectedWkts { get; init; } = Array.Empty<string>();
    public string? ClearAreaProjectedWkt { get; init; }
}

public sealed record PipelineWaterAcquisitionDetection
{
    public string AcquisitionKey { get; init; } = "";
    public DateTimeOffset AcquiredAt { get; init; }
    public IReadOnlyList<PipelineWaterSectionDetection> Sections { get; init; } = Array.Empty<PipelineWaterSectionDetection>();
    public string? WaterGeoJsonPath { get; init; }
}

public sealed record PipelineWaterSectionDetectionRequest
{
    public StacAcquisitionGroup Acquisition { get; init; } = new("", DateOnly.MinValue, DateTimeOffset.MinValue, Array.Empty<StacItem>());
    public PipelineSectionCorridor Corridor { get; init; } = new();
    public string WorkDirectory { get; init; } = "";
    public WaterDetectionConfig WaterDetection { get; init; } = new();
}

public delegate Task<PipelineWaterSectionDetection> PipelineWaterSectionDetector(
    PipelineWaterSectionDetectionRequest request,
    CancellationToken cancellationToken);

public sealed record PipelineWaterBinTemporalStats
{
    public int BinIndex { get; init; }
    public decimal StartChainageM { get; init; }
    public decimal EndChainageM { get; init; }
    public int WaterObservationCount { get; init; }
    public int DryObservationCount { get; init; }
    public int UnknownObservationCount { get; init; }
    public int ClearObservationCount => WaterObservationCount + DryObservationCount;
    public decimal? WaterFrequency { get; init; }
    public string PersistenceClass { get; init; } = "";
    public DateTimeOffset? FirstWaterObservedAt { get; init; }
    public DateTimeOffset? LastWaterObservedAt { get; init; }
    public decimal? MaximumWaterAreaM2 { get; init; }
    public decimal? MinimumWaterDistanceM { get; init; }
    public bool HasCentrelineCrossing { get; init; }
    public string RouteBinWkt { get; init; } = "";
}

public sealed record PipelineWaterRunSaveRequest
{
    public long JobId { get; init; }
    public long JobProductId { get; init; }
    public long PipelinePathId { get; init; }
    public DateTime DateFrom { get; init; }
    public DateTime DateTo { get; init; }
    public string Method { get; init; } = "";
    public string AlgorithmVersion { get; init; } = "";
    public decimal CorridorHalfWidthM { get; init; }
    public decimal AnalysisBinLengthM { get; init; }
    public int AcquisitionCount { get; init; }
    public int ClearAcquisitionCount { get; init; }
    public string OutputDirectory { get; init; } = "";
    public string? ObservationsGeoJsonPath { get; init; }
    public string? ZonesGeoJsonPath { get; init; }
    public IReadOnlyList<PipelineWaterBinObservation> Observations { get; init; } = Array.Empty<PipelineWaterBinObservation>();
    public IReadOnlyList<PipelineWaterZoneResult> Zones { get; init; } = Array.Empty<PipelineWaterZoneResult>();
}

public sealed record PipelineWaterRunSaveResult(
    long PipelineWaterRunId,
    IReadOnlyList<PipelineWaterZoneResult> Zones);

public sealed record StacAssetCropRequest
{
    public string AssetKey { get; init; } = "";
    public string AssetHref { get; init; } = "";
    public string AcquisitionKey { get; init; } = "";
    public int SectionOrdinal { get; init; }
    public string SectionCorridorGeoJson { get; init; } = "";
    public string OutputDirectory { get; init; } = "";
    public string OsgeoRoot { get; init; } = "";
    public long MaximumLocalDiskBytes { get; init; } = 2147483648L;
    public long MaximumRasterPixels { get; init; } = 500000000L;
}

public sealed record StacAssetCropCommand
{
    public string ToolName { get; init; } = "gdalwarp";
    public IReadOnlyList<string> Arguments { get; init; } = Array.Empty<string>();
    public IReadOnlyList<string> RedactedArguments { get; init; } = Array.Empty<string>();
    public string WorkingDirectory { get; init; } = "";
    public string CutlinePath { get; init; } = "";
    public string OutputPath { get; init; } = "";
    public bool UsesRemoteCog { get; init; }
}

public sealed record StacAssetCropResult
{
    public string OutputPath { get; init; } = "";
    public bool UsedRemoteCog { get; init; }
    public bool UsedFallbackDownload { get; init; }
    public string Log { get; init; } = "";
}
