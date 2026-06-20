using System.Globalization;
using System.Text;
using System.Text.Json;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

public sealed class PipelineWaterMapExportWriter
{
    private readonly WKTReader _wktReader = new(NetTopologySuite.NtsGeometryServices.Instance);
    private readonly GeoJsonWriter _geoJsonWriter = new();

    public string BuildZonesGeoJson(IReadOnlyList<PipelineWaterZoneQueryRecord> zones)
    {
        var features = zones
            .OrderBy(zone => zone.StartChainageM)
            .ThenBy(zone => zone.ZoneOrdinal)
            .Select(zone => BuildFeature(
                zone.RouteZoneWkt,
                new Dictionary<string, object?>
                {
                    ["pipelineWaterRunId"] = zone.PipelineWaterRunId,
                    ["pipelinePathId"] = zone.PipelinePathId,
                    ["routeName"] = zone.PathName,
                    ["pipelineWaterZoneId"] = zone.PipelineWaterZoneId,
                    ["zoneOrdinal"] = zone.ZoneOrdinal,
                    ["startChainageM"] = zone.StartChainageM,
                    ["endChainageM"] = zone.EndChainageM,
                    ["lengthM"] = zone.LengthM,
                    ["persistenceClass"] = zone.PersistenceClass,
                    ["waterFrequency"] = zone.WaterFrequency,
                    ["waterObservationCount"] = zone.WaterObservationCount,
                    ["dryObservationCount"] = zone.DryObservationCount,
                    ["unknownObservationCount"] = zone.UnknownObservationCount,
                    ["clearObservationCount"] = zone.ClearObservationCount,
                    ["firstWaterObservedAt"] = zone.FirstWaterObservedAt?.ToString("O", CultureInfo.InvariantCulture),
                    ["lastWaterObservedAt"] = zone.LastWaterObservedAt?.ToString("O", CultureInfo.InvariantCulture),
                    ["minimumWaterDistanceM"] = zone.MinimumWaterDistanceM,
                    ["hasCentrelineCrossing"] = zone.HasCentrelineCrossing,
                    ["styleKey"] = GetZoneStyleKey(zone.PersistenceClass),
                    ["crossingStyleKey"] = zone.HasCentrelineCrossing ? "Crossing" : null
                }))
            .ToList();
        return BuildFeatureCollection(features);
    }

    public string BuildObservationsGeoJson(IReadOnlyList<PipelineWaterObservationQueryRecord> observations)
    {
        var features = observations
            .OrderBy(observation => observation.AcquiredAt)
            .ThenBy(observation => observation.BinIndex)
            .ThenBy(observation => observation.AcquisitionKey, StringComparer.OrdinalIgnoreCase)
            .Select(observation => BuildFeature(
                observation.WaterIntersectionWkt ?? observation.RouteBinWkt,
                new Dictionary<string, object?>
                {
                    ["pipelineWaterObservationId"] = observation.PipelineWaterObservationId,
                    ["pipelineWaterRunId"] = observation.PipelineWaterRunId,
                    ["pipelinePathId"] = observation.PipelinePathId,
                    ["routeName"] = observation.PathName,
                    ["pipelineWaterZoneId"] = observation.PipelineWaterZoneId,
                    ["acquisitionKey"] = observation.AcquisitionKey,
                    ["acquiredAt"] = observation.AcquiredAt.ToString("O", CultureInfo.InvariantCulture),
                    ["binIndex"] = observation.BinIndex,
                    ["startChainageM"] = observation.StartChainageM,
                    ["endChainageM"] = observation.EndChainageM,
                    ["observationState"] = observation.ObservationState,
                    ["exposureType"] = observation.ExposureType,
                    ["waterAreaInCorridorM2"] = observation.WaterAreaInCorridorM2,
                    ["lengthOnWaterM"] = observation.LengthOnWaterM,
                    ["nearestWaterDistanceM"] = observation.NearestWaterDistanceM,
                    ["styleKey"] = observation.ExposureType == "Crossing" ? "Crossing" : observation.ObservationState
                }))
            .ToList();
        return BuildFeatureCollection(features);
    }

    public string BuildZonesCsv(IReadOnlyList<PipelineWaterZoneQueryRecord> zones)
    {
        var builder = new StringBuilder();
        builder.AppendLine("route name,zone ordinal,start chainage,end chainage,length,persistence class,water frequency,water observation count,clear observation count,unknown observation count,first water date,last water date,minimum water distance,centreline crossing flag");

        foreach (var zone in zones.OrderBy(zone => zone.StartChainageM).ThenBy(zone => zone.ZoneOrdinal))
        {
            builder.AppendCsv(zone.PathName);
            builder.AppendCsv(zone.ZoneOrdinal);
            builder.AppendCsv(zone.StartChainageM);
            builder.AppendCsv(zone.EndChainageM);
            builder.AppendCsv(zone.LengthM);
            builder.AppendCsv(zone.PersistenceClass);
            builder.AppendCsv(zone.WaterFrequency);
            builder.AppendCsv(zone.WaterObservationCount);
            builder.AppendCsv(zone.ClearObservationCount);
            builder.AppendCsv(zone.UnknownObservationCount);
            builder.AppendCsv(zone.FirstWaterObservedAt?.ToString("O", CultureInfo.InvariantCulture));
            builder.AppendCsv(zone.LastWaterObservedAt?.ToString("O", CultureInfo.InvariantCulture));
            builder.AppendCsv(zone.MinimumWaterDistanceM);
            builder.AppendCsv(zone.HasCentrelineCrossing);
            builder.Length--;
            builder.AppendLine();
        }

        return builder.ToString();
    }

    public string BuildIntegrityScreeningSummaryJson(
        PipelineWaterRunQueryRecord run,
        IReadOnlyList<PipelineWaterZoneQueryRecord> zones)
    {
        var summary = new
        {
            run.PipelineWaterRunId,
            run.PipelinePathId,
            RouteName = run.PathName,
            run.DateFrom,
            run.DateTo,
            Description = "This is an observed surface-water exposure prioritization/screening layer.",
            InterpretationLimit = "It is not a direct measurement of coating condition, groundwater at pipe depth, cathodic protection effectiveness or external corrosion.",
            CorrosionRiskScore = "No corrosion-risk score is calculated. Add validated coating, CP, soil resistivity, drainage, ILI/CIS/DCVG/ACVG or inspection data before corrosion-risk scoring.",
            ZoneCount = zones.Count,
            CentrelineCrossingZoneCount = zones.Count(zone => zone.HasCentrelineCrossing),
            PersistenceClassCounts = zones
                .GroupBy(zone => zone.PersistenceClass, StringComparer.Ordinal)
                .OrderBy(group => group.Key, StringComparer.Ordinal)
                .Select(group => new
                {
                    PersistenceClass = group.Key,
                    Count = group.Count(),
                    LengthM = group.Sum(zone => zone.LengthM)
                }),
            GenericGeoJsonStyling = new
            {
                Persistent = "darkest line",
                Seasonal = "medium line",
                Intermittent = "light line",
                InsufficientData = "dashed/hatched",
                Crossing = "separate point/segment symbol"
            }
        };

        return JsonSerializer.Serialize(summary, JsonOptions);
    }

    private string BuildFeature(string wkt, IReadOnlyDictionary<string, object?> properties)
    {
        var geometry = _wktReader.Read(wkt);
        geometry.SRID = 4326;
        ValidateCoordinateBounds(geometry);
        return $$"""
{"type":"Feature","geometry":{{_geoJsonWriter.Write(geometry)}},"properties":{{JsonSerializer.Serialize(properties, JsonOptions)}}}
""";
    }

    private static void ValidateCoordinateBounds(Geometry geometry)
    {
        foreach (var coordinate in geometry.Coordinates)
        {
            if (coordinate.X < -180d || coordinate.X > 180d || coordinate.Y < -90d || coordinate.Y > 90d)
            {
                throw new InvalidOperationException("Pipeline water export geometry contains coordinates outside EPSG:4326 bounds.");
            }
        }
    }

    private static string BuildFeatureCollection(IEnumerable<string> features)
    {
        var builder = new StringBuilder();
        builder.Append("{\"type\":\"FeatureCollection\",\"features\":[");
        builder.Append(string.Join(",", features));
        builder.Append("]}");
        return builder.ToString();
    }

    private static string GetZoneStyleKey(string persistenceClass)
    {
        return persistenceClass switch
        {
            "Persistent" => "PersistentDarkestLine",
            "Seasonal" => "SeasonalMediumLine",
            "Intermittent" => "IntermittentLightLine",
            "InsufficientData" => "InsufficientDataDashedHatched",
            _ => persistenceClass
        };
    }

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };
}

internal static class PipelineWaterCsvExtensions
{
    public static void AppendCsv(this StringBuilder builder, object? value)
    {
        builder.Append(EscapeCsv(value));
        builder.Append(',');
    }

    private static string EscapeCsv(object? value)
    {
        if (value is null)
        {
            return string.Empty;
        }

        var text = value switch
        {
            decimal decimalValue => decimalValue.ToString(CultureInfo.InvariantCulture),
            double doubleValue => doubleValue.ToString("G17", CultureInfo.InvariantCulture),
            float floatValue => floatValue.ToString("G9", CultureInfo.InvariantCulture),
            bool boolValue => boolValue ? "true" : "false",
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty
        };

        return text.Contains('"') || text.Contains(',') || text.Contains('\r') || text.Contains('\n')
            ? "\"" + text.Replace("\"", "\"\"", StringComparison.Ordinal) + "\""
            : text;
    }
}
