using System.Text.Json;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Xunit;

public sealed class PipelineWaterMapExportWriterTests
{
    [Fact]
    public void ZonesGeoJsonIsValidAndFeatureCountMatchesQueryRows()
    {
        var zones = SampleZones();
        var geoJson = new PipelineWaterMapExportWriter().BuildZonesGeoJson(zones);

        using var document = JsonDocument.Parse(geoJson);
        var features = document.RootElement.GetProperty("features");

        Assert.Equal(zones.Count, features.GetArrayLength());
        Assert.Equal("FeatureCollection", document.RootElement.GetProperty("type").GetString());
        foreach (var feature in features.EnumerateArray())
        {
            var geometry = new GeoJsonReader().Read<Geometry>(feature.GetProperty("geometry").GetRawText());
            Assert.False(geometry.IsEmpty);
            Assert.All(geometry.Coordinates, coordinate =>
            {
                Assert.InRange(coordinate.X, -180d, 180d);
                Assert.InRange(coordinate.Y, -90d, 90d);
            });
        }
    }

    [Fact]
    public void ZonesGeoJsonIsOrderedByChainage()
    {
        var zones = SampleZones().Reverse().ToList();
        var geoJson = new PipelineWaterMapExportWriter().BuildZonesGeoJson(zones);

        using var document = JsonDocument.Parse(geoJson);
        var features = document.RootElement.GetProperty("features").EnumerateArray().ToList();

        Assert.Equal(10m, features[0].GetProperty("properties").GetProperty("startChainageM").GetDecimal());
        Assert.Equal(30m, features[1].GetProperty("properties").GetProperty("startChainageM").GetDecimal());
    }

    [Fact]
    public void ObservationsGeoJsonIsValidAndFeatureCountMatchesQueryRows()
    {
        var observations = SampleObservations();
        var geoJson = new PipelineWaterMapExportWriter().BuildObservationsGeoJson(observations);

        using var document = JsonDocument.Parse(geoJson);
        var features = document.RootElement.GetProperty("features");

        Assert.Equal(observations.Count, features.GetArrayLength());
        Assert.Equal("Crossing", features[0].GetProperty("properties").GetProperty("styleKey").GetString());
        foreach (var feature in features.EnumerateArray())
        {
            var geometry = new GeoJsonReader().Read<Geometry>(feature.GetProperty("geometry").GetRawText());
            Assert.False(geometry.IsEmpty);
        }
    }

    [Fact]
    public void ExportRejectsCoordinatesOutsideWgs84Bounds()
    {
        var zones = SampleZones()
            .Select(zone => zone with { RouteZoneWkt = "LINESTRING (181 50.1, 181.1 50.1)" })
            .ToList();

        var ex = Assert.Throws<InvalidOperationException>(() => new PipelineWaterMapExportWriter().BuildZonesGeoJson(zones));

        Assert.Contains("EPSG:4326", ex.Message);
    }

    [Fact]
    public void CsvContainsRequestedColumnsAndTotals()
    {
        var zones = SampleZones();
        var csv = new PipelineWaterMapExportWriter().BuildZonesCsv(zones);
        var lines = csv.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);

        Assert.StartsWith("route name,zone ordinal,start chainage,end chainage,length,persistence class", lines[0]);
        Assert.Equal(zones.Count + 1, lines.Length);

        var waterTotal = 0;
        var clearTotal = 0;
        var unknownTotal = 0;
        foreach (var line in lines.Skip(1))
        {
            var columns = line.Split(',');
            waterTotal += int.Parse(columns[7]);
            clearTotal += int.Parse(columns[8]);
            unknownTotal += int.Parse(columns[9]);
        }

        Assert.Equal(zones.Sum(zone => zone.WaterObservationCount), waterTotal);
        Assert.Equal(zones.Sum(zone => zone.ClearObservationCount), clearTotal);
        Assert.Equal(zones.Sum(zone => zone.UnknownObservationCount), unknownTotal);
    }

    [Fact]
    public void IntegritySummaryUsesScreeningLanguageAndNoCorrosionRiskScore()
    {
        var run = new PipelineWaterRunQueryRecord
        {
            PipelineWaterRunId = 7,
            PipelinePathId = 2,
            PathName = "Route A",
            DateFrom = new DateTime(2023, 4, 1),
            DateTo = new DateTime(2026, 10, 31)
        };

        var summary = new PipelineWaterMapExportWriter().BuildIntegrityScreeningSummaryJson(run, SampleZones());

        Assert.Contains("observed surface-water exposure", summary);
        Assert.Contains("prioritization/screening layer", summary);
        Assert.Contains("not a direct measurement of coating condition, groundwater at pipe depth, cathodic protection effectiveness or external corrosion", summary);
        Assert.Contains("No corrosion-risk score is calculated", summary);
        Assert.Contains("dashed/hatched", summary);
        Assert.Contains("separate point/segment symbol", summary);
    }

    private static IReadOnlyList<PipelineWaterZoneQueryRecord> SampleZones()
    {
        return new[]
        {
            new PipelineWaterZoneQueryRecord
            {
                PipelineWaterZoneId = 101,
                PipelineWaterRunId = 7,
                PipelinePathId = 2,
                PathName = "Route A",
                ZoneOrdinal = 1,
                StartChainageM = 10,
                EndChainageM = 20,
                LengthM = 10,
                WaterObservationCount = 4,
                DryObservationCount = 1,
                UnknownObservationCount = 2,
                ClearObservationCount = 5,
                WaterFrequency = 0.800000m,
                PersistenceClass = "Persistent",
                FirstWaterObservedAt = DateTimeOffset.Parse("2024-05-01T12:00:00Z"),
                LastWaterObservedAt = DateTimeOffset.Parse("2024-05-03T12:00:00Z"),
                MinimumWaterDistanceM = 0,
                HasCentrelineCrossing = true,
                RouteZoneWkt = "LINESTRING (-103.2 50.1, -103.19 50.1)"
            },
            new PipelineWaterZoneQueryRecord
            {
                PipelineWaterZoneId = 102,
                PipelineWaterRunId = 7,
                PipelinePathId = 2,
                PathName = "Route A",
                ZoneOrdinal = 2,
                StartChainageM = 30,
                EndChainageM = 40,
                LengthM = 10,
                WaterObservationCount = 1,
                DryObservationCount = 4,
                UnknownObservationCount = 3,
                ClearObservationCount = 5,
                WaterFrequency = 0.200000m,
                PersistenceClass = "Seasonal",
                FirstWaterObservedAt = DateTimeOffset.Parse("2024-06-01T12:00:00Z"),
                LastWaterObservedAt = DateTimeOffset.Parse("2024-06-01T12:00:00Z"),
                MinimumWaterDistanceM = 12,
                HasCentrelineCrossing = false,
                RouteZoneWkt = "LINESTRING (-103.18 50.1, -103.17 50.1)"
            }
        };
    }

    private static IReadOnlyList<PipelineWaterObservationQueryRecord> SampleObservations()
    {
        return new[]
        {
            new PipelineWaterObservationQueryRecord
            {
                PipelineWaterObservationId = 501,
                PipelineWaterRunId = 7,
                PipelinePathId = 2,
                PathName = "Route A",
                PipelineWaterZoneId = 101,
                AcquisitionKey = "20240501_T1",
                AcquiredAt = DateTimeOffset.Parse("2024-05-01T12:00:00Z"),
                BinIndex = 0,
                StartChainageM = 10,
                EndChainageM = 20,
                ObservationState = "Water",
                ExposureType = "Crossing",
                WaterAreaInCorridorM2 = 25,
                LengthOnWaterM = 3,
                NearestWaterDistanceM = 0,
                RouteBinWkt = "LINESTRING (-103.2 50.1, -103.19 50.1)",
                WaterIntersectionWkt = "POLYGON ((-103.2 50.1, -103.199 50.1, -103.199 50.101, -103.2 50.101, -103.2 50.1))"
            },
            new PipelineWaterObservationQueryRecord
            {
                PipelineWaterObservationId = 502,
                PipelineWaterRunId = 7,
                PipelinePathId = 2,
                PathName = "Route A",
                PipelineWaterZoneId = null,
                AcquisitionKey = "20240502_T1",
                AcquiredAt = DateTimeOffset.Parse("2024-05-02T12:00:00Z"),
                BinIndex = 1,
                StartChainageM = 20,
                EndChainageM = 30,
                ObservationState = "Dry",
                RouteBinWkt = "LINESTRING (-103.19 50.1, -103.18 50.1)"
            }
        };
    }
}
