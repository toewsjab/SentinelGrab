public sealed record SentinelGrabJob
{
    public long JobId { get; init; }
    public string Status { get; init; } = "";
    public int? Priority { get; init; }
    public DateTime? CreatedAt { get; init; }
    public DateTime? DateFrom { get; init; }
    public DateTime? DateTo { get; init; }
    public string? DateKey { get; init; }
    public double? BboxMinLon { get; init; }
    public double? BboxMinLat { get; init; }
    public double? BboxMaxLon { get; init; }
    public double? BboxMaxLat { get; init; }
    public string? Bbox { get; init; }
    public int? CloudCoverMax { get; init; }
    public bool PreferMosaic { get; init; }
    public int? MaxScenes { get; init; }
    public int? ZoomMin { get; init; }
    public int? ZoomMax { get; init; }
    public string? OutputRootPath { get; init; }
    public string? SceneId { get; init; }
}

public sealed record SentinelGrabJobProduct
{
    public long JobProductId { get; init; }
    public long JobId { get; init; }
    public string ProductCode { get; init; } = "";
    public string? OutputSubPath { get; init; }
    public string Status { get; init; } = "";
}

public static class SentinelGrabProductCodes
{
    public const string PipelineWater = "PIPELINE_WATER";
}
public sealed record AvailableLayer
{
    public long JobId { get; init; }
    public long JobProductId { get; init; }
    public string ProductCode { get; init; } = "";
    public string DateKey { get; init; } = "";
    public DateTime? DateFrom { get; init; }
    public DateTime? DateTo { get; init; }
    public double? BboxMinLon { get; init; }
    public double? BboxMinLat { get; init; }
    public double? BboxMaxLon { get; init; }
    public double? BboxMaxLat { get; init; }
    public string OutputRootPath { get; init; } = "";
    public string ProductSubPath { get; init; } = "";
    public string OutputDir { get; init; } = "";
}

public sealed record SentinelPipelinePathRecord
{
    public long PipelinePathId { get; init; }
    public string PathName { get; init; } = "";
    public string RouteGeometry { get; init; } = "";
    public decimal RouteLengthM { get; init; }
    public decimal ChainageOriginM { get; init; }
    public string? DirectionDescription { get; init; }
    public string? SourceReference { get; init; }
    public string SourceHash { get; init; } = "";
    public bool IsActive { get; init; } = true;
    public DateTime? CreatedAt { get; init; }
    public DateTime? ModifiedAt { get; init; }
}

public sealed record PipelinePathImportRequest
{
    public string SourceText { get; init; } = "";
    public string PathName { get; init; } = "";
    public string? DirectionDescription { get; init; }
    public string? SourceReference { get; init; }
    public decimal ChainageOriginM { get; init; }
    public double EndpointToleranceM { get; init; } = 0.5d;
    public double DensifyMaxSegmentLengthM { get; init; } = 100d;
    public double MaxProjectedSectionLengthM { get; init; } = 25000d;
}

public sealed record PipelinePathImportResult
{
    public SentinelPipelinePathRecord Path { get; init; } = new();
    public IReadOnlyList<PipelinePathSectionRecord> Sections { get; init; } = Array.Empty<PipelinePathSectionRecord>();
    public IReadOnlyList<PipelinePathEndpointGap> EndpointGaps { get; init; } = Array.Empty<PipelinePathEndpointGap>();
    public double StartLongitude { get; init; }
    public double StartLatitude { get; init; }
    public double EndLongitude { get; init; }
    public double EndLatitude { get; init; }
    public IReadOnlyList<int> CrossedUtmZones { get; init; } = Array.Empty<int>();
}

public sealed record PipelinePathSectionRecord
{
    public int SectionOrdinal { get; init; }
    public int UtmZone { get; init; }
    public bool NorthernHemisphere { get; init; }
    public decimal StartChainageM { get; init; }
    public decimal EndChainageM { get; init; }
    public string SectionGeometryWkt { get; init; } = "";
}

public sealed record PipelinePathEndpointGap
{
    public int FromComponentIndex { get; init; }
    public int ToComponentIndex { get; init; }
    public double GapMetres { get; init; }
}
public sealed record SentinelPipelineWaterRequestRecord
{
    public long JobProductId { get; init; }
    public long PipelinePathId { get; init; }
    public decimal CorridorHalfWidthM { get; init; }
    public decimal AnalysisBinLengthM { get; init; }
    public int MinimumClearObservations { get; init; }
    public decimal PersistentFrequencyThreshold { get; init; }
    public decimal SeasonalFrequencyThreshold { get; init; }
    public string? IncludedMonthsCsv { get; init; }
    public DateTime? CreatedAt { get; init; }
}

public sealed record SentinelPipelineWaterRunRecord
{
    public long PipelineWaterRunId { get; init; }
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
    public DateTime? CreatedAt { get; init; }
    public DateTime? ModifiedAt { get; init; }
}

public sealed record SentinelPipelineWaterBinObservationRecord
{
    public long PipelineWaterObservationId { get; init; }
    public long PipelineWaterRunId { get; init; }
    public string AcquisitionKey { get; init; } = "";
    public DateTimeOffset AcquiredAt { get; init; }
    public int BinIndex { get; init; }
    public decimal StartChainageM { get; init; }
    public decimal EndChainageM { get; init; }
    public string ObservationState { get; init; } = "";
    public string? ExposureType { get; init; }
    public decimal? WaterAreaInCorridorM2 { get; init; }
    public decimal? LengthOnWaterM { get; init; }
    public decimal? NearestWaterDistanceM { get; init; }
    public string RouteBinGeometry { get; init; } = "";
    public string? WaterIntersectionGeometry { get; init; }
}

public sealed record SentinelPipelineWaterZoneRecord
{
    public long PipelineWaterZoneId { get; init; }
    public long PipelineWaterRunId { get; init; }
    public int ZoneOrdinal { get; init; }
    public decimal StartChainageM { get; init; }
    public decimal EndChainageM { get; init; }
    public decimal LengthM { get; init; }
    public int WaterObservationCount { get; init; }
    public int DryObservationCount { get; init; }
    public int UnknownObservationCount { get; init; }
    public int ClearObservationCount { get; init; }
    public decimal? WaterFrequency { get; init; }
    public string PersistenceClass { get; init; } = "";
    public DateTimeOffset? FirstWaterObservedAt { get; init; }
    public DateTimeOffset? LastWaterObservedAt { get; init; }
    public decimal? MaximumWaterAreaM2 { get; init; }
    public decimal? MinimumWaterDistanceM { get; init; }
    public string RouteZoneGeometry { get; init; } = "";
}
public readonly record struct Bbox(double MinLon, double MinLat, double MaxLon, double MaxLat)
{
    public double[] ToArray() => new[] { MinLon, MinLat, MaxLon, MaxLat };
}
