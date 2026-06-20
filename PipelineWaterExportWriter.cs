using System.Globalization;
using System.Text;
using System.Text.Json;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

public sealed record PipelineWaterExportResult(
    string RouteGeoJsonPath,
    string ObservationsGeoJsonPath,
    string ZonesGeoJsonPath,
    string SummaryJsonPath);

public sealed class PipelineWaterExportWriter
{
    private readonly WKTReader _wktReader = new(NetTopologySuite.NtsGeometryServices.Instance);
    private readonly GeoJsonWriter _geoJsonWriter = new();

    public async Task<PipelineWaterExportResult> WriteAsync(
        SentinelPipelinePathRecord pipelinePath,
        PipelineWaterBuildResult result,
        IReadOnlyList<PipelineWaterBinObservation> observations,
        string outputDirectory,
        CancellationToken cancellationToken = default)
    {
        Directory.CreateDirectory(outputDirectory);

        var routePath = Path.Combine(outputDirectory, "pipeline-route.geojson");
        var observationsPath = Path.Combine(outputDirectory, "pipeline-water-observations.geojson");
        var zonesPath = Path.Combine(outputDirectory, "pipeline-water-zones.geojson");
        var summaryPath = Path.Combine(outputDirectory, "pipeline-water-summary.json");

        await File.WriteAllTextAsync(routePath, BuildRouteGeoJson(pipelinePath), cancellationToken);
        await File.WriteAllTextAsync(observationsPath, BuildObservationsGeoJson(observations), cancellationToken);
        await File.WriteAllTextAsync(zonesPath, BuildZonesGeoJson(result.Zones), cancellationToken);
        await File.WriteAllTextAsync(summaryPath, BuildSummaryJson(pipelinePath, result, observations), cancellationToken);

        return new PipelineWaterExportResult(routePath, observationsPath, zonesPath, summaryPath);
    }

    public string BuildRouteGeoJson(SentinelPipelinePathRecord pipelinePath)
    {
        var properties = new Dictionary<string, object?>
        {
            ["pipelinePathId"] = pipelinePath.PipelinePathId,
            ["pathName"] = pipelinePath.PathName,
            ["routeLengthM"] = pipelinePath.RouteLengthM,
            ["chainageOriginM"] = pipelinePath.ChainageOriginM,
            ["directionDescription"] = pipelinePath.DirectionDescription,
            ["sourceReference"] = pipelinePath.SourceReference
        };
        return BuildFeatureCollection(new[] { BuildFeature(pipelinePath.RouteGeometry, properties) });
    }

    public string BuildObservationsGeoJson(IReadOnlyList<PipelineWaterBinObservation> observations)
    {
        var features = observations
            .OrderBy(observation => observation.BinIndex)
            .ThenBy(observation => observation.AcquiredAt)
            .ThenBy(observation => observation.AcquisitionKey, StringComparer.OrdinalIgnoreCase)
            .Select(observation => BuildFeature(
                observation.WaterIntersectionWkt ?? observation.RouteBinWkt,
                new Dictionary<string, object?>
                {
                    ["acquisitionKey"] = observation.AcquisitionKey,
                    ["acquiredAt"] = observation.AcquiredAt.ToString("O", CultureInfo.InvariantCulture),
                    ["binIndex"] = observation.BinIndex,
                    ["startChainageM"] = observation.StartChainageM,
                    ["endChainageM"] = observation.EndChainageM,
                    ["observationState"] = observation.ObservationState,
                    ["exposureType"] = observation.ExposureType,
                    ["waterAreaInCorridorM2"] = observation.WaterAreaInCorridorM2,
                    ["lengthOnWaterM"] = observation.LengthOnWaterM,
                    ["nearestWaterDistanceM"] = observation.NearestWaterDistanceM
                }))
            .ToList();
        return BuildFeatureCollection(features);
    }

    public string BuildZonesGeoJson(IReadOnlyList<PipelineWaterZoneResult> zones)
    {
        var features = zones
            .OrderBy(zone => zone.ZoneOrdinal)
            .Select(zone => BuildFeature(
                zone.RouteZoneWkt,
                new Dictionary<string, object?>
                {
                    ["pipelineWaterZoneId"] = zone.PipelineWaterZoneId,
                    ["startChainageM"] = zone.StartChainageM,
                    ["endChainageM"] = zone.EndChainageM,
                    ["lengthM"] = zone.LengthM,
                    ["persistenceClass"] = zone.PersistenceClass,
                    ["waterFrequency"] = zone.WaterFrequency,
                    ["waterObservationCount"] = zone.WaterObservationCount,
                    ["clearObservationCount"] = zone.ClearObservationCount,
                    ["firstWaterObservedAt"] = zone.FirstWaterObservedAt?.ToString("O", CultureInfo.InvariantCulture),
                    ["lastWaterObservedAt"] = zone.LastWaterObservedAt?.ToString("O", CultureInfo.InvariantCulture),
                    ["minimumWaterDistanceM"] = zone.MinimumWaterDistanceM,
                    ["hasCentrelineCrossing"] = zone.HasCentrelineCrossing
                }))
            .ToList();
        return BuildFeatureCollection(features);
    }

    public string BuildSummaryJson(
        SentinelPipelinePathRecord pipelinePath,
        PipelineWaterBuildResult result,
        IReadOnlyList<PipelineWaterBinObservation> observations)
    {
        var summary = new
        {
            result.JobId,
            result.JobProductId,
            result.PipelinePathId,
            pipelinePath.PathName,
            pipelinePath.DirectionDescription,
            result.Method,
            result.AlgorithmVersion,
            result.AcquisitionCount,
            result.ClearAcquisitionCount,
            ObservationCount = observations.Count,
            ZoneCount = result.Zones.Count,
            Classes = result.Zones
                .GroupBy(zone => zone.PersistenceClass, StringComparer.Ordinal)
                .OrderBy(group => group.Key, StringComparer.Ordinal)
                .Select(group => new
                {
                    PersistenceClass = group.Key,
                    ZoneCount = group.Count(),
                    TotalLengthM = group.Sum(zone => zone.LengthM)
                }),
            GeneratedAtUtc = DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture)
        };

        return JsonSerializer.Serialize(summary, JsonOptions);
    }

    private string BuildFeature(string wkt, IReadOnlyDictionary<string, object?> properties)
    {
        var geometry = _wktReader.Read(wkt);
        geometry.SRID = 4326;
        return $$"""
{"type":"Feature","geometry":{{_geoJsonWriter.Write(geometry)}},"properties":{{JsonSerializer.Serialize(properties, JsonOptions)}}}
""";
    }

    private static string BuildFeatureCollection(IEnumerable<string> features)
    {
        var builder = new StringBuilder();
        builder.Append("{\"type\":\"FeatureCollection\",\"features\":[");
        builder.Append(string.Join(",", features));
        builder.Append("]}");
        return builder.ToString();
    }

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };
}
