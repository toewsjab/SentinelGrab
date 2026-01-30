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

public readonly record struct Bbox(double MinLon, double MinLat, double MaxLon, double MaxLat)
{
    public double[] ToArray() => new[] { MinLon, MinLat, MaxLon, MaxLat };
}
