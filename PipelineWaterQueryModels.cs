public sealed record PipelineWaterRunQueryRecord
{
    public long PipelineWaterRunId { get; init; }
    public long JobId { get; init; }
    public long JobProductId { get; init; }
    public long PipelinePathId { get; init; }
    public string PathName { get; init; } = "";
    public DateTime DateFrom { get; init; }
    public DateTime DateTo { get; init; }
    public string Method { get; init; } = "";
    public string AlgorithmVersion { get; init; } = "";
    public int AcquisitionCount { get; init; }
    public int ClearAcquisitionCount { get; init; }
}

public sealed record PipelineWaterZoneQueryRecord
{
    public long PipelineWaterZoneId { get; init; }
    public long PipelineWaterRunId { get; init; }
    public long PipelinePathId { get; init; }
    public string PathName { get; init; } = "";
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
    public bool HasCentrelineCrossing { get; init; }
    public string RouteZoneWkt { get; init; } = "";
}

public sealed record PipelineWaterObservationQueryRecord
{
    public long PipelineWaterObservationId { get; init; }
    public long PipelineWaterRunId { get; init; }
    public long PipelinePathId { get; init; }
    public string PathName { get; init; } = "";
    public long? PipelineWaterZoneId { get; init; }
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
    public string RouteBinWkt { get; init; } = "";
    public string? WaterIntersectionWkt { get; init; }
}

public sealed record PipelineWaterInsufficientClearLocationRecord
{
    public long PipelineWaterRunId { get; init; }
    public long PipelinePathId { get; init; }
    public string PathName { get; init; } = "";
    public int BinIndex { get; init; }
    public decimal StartChainageM { get; init; }
    public decimal EndChainageM { get; init; }
    public int ClearObservationCount { get; init; }
    public int UnknownObservationCount { get; init; }
    public int MinimumClearObservations { get; init; }
    public string RouteBinWkt { get; init; } = "";
}
