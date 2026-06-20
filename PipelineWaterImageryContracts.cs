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
