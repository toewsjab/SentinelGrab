using System.Text.Json;
using Xunit;

public sealed class PipelineWaterZoneAggregatorTests
{
    [Fact]
    public void UnknownObservationsAreExcludedFromWaterFrequencyDenominator()
    {
        var observations = new List<PipelineWaterBinObservation>();
        observations.Add(Observation(0, 0, 10, "Water", day: 1));
        observations.Add(Observation(0, 0, 10, "Dry", day: 2));
        for (var day = 3; day <= 10; day++)
        {
            observations.Add(Observation(0, 0, 10, "Unknown", day: day));
        }

        var stats = new PipelineWaterZoneAggregator().BuildBinStats(observations, Config(minimumClearObservations: 1));

        Assert.Equal(1, stats[0].WaterObservationCount);
        Assert.Equal(1, stats[0].DryObservationCount);
        Assert.Equal(8, stats[0].UnknownObservationCount);
        Assert.Equal(2, stats[0].ClearObservationCount);
        Assert.Equal(0.500000m, stats[0].WaterFrequency);
        Assert.Equal("Seasonal", stats[0].PersistenceClass);
    }

    [Fact]
    public void ThresholdBoundariesClassifyObservedWaterFrequency()
    {
        var observations = new List<PipelineWaterBinObservation>();
        AddBin(observations, 0, 0, 10, water: 4, dry: 1);
        AddBin(observations, 1, 10, 20, water: 1, dry: 4);
        AddBin(observations, 2, 20, 30, water: 1, dry: 9);

        var stats = new PipelineWaterZoneAggregator().BuildBinStats(observations, Config(minimumClearObservations: 1));

        Assert.Equal("Persistent", stats[0].PersistenceClass);
        Assert.Equal(0.800000m, stats[0].WaterFrequency);
        Assert.Equal("Seasonal", stats[1].PersistenceClass);
        Assert.Equal(0.200000m, stats[1].WaterFrequency);
        Assert.Equal("Intermittent", stats[2].PersistenceClass);
        Assert.Equal(0.100000m, stats[2].WaterFrequency);
    }

    [Fact]
    public void MergeGapAllowsAdjacentMatchingZonesAcrossSmallDryGap()
    {
        var observations = new List<PipelineWaterBinObservation>();
        AddBin(observations, 0, 0, 10, water: 2, dry: 0);
        AddBin(observations, 1, 10, 15, water: 0, dry: 2);
        AddBin(observations, 2, 15, 25, water: 2, dry: 0);

        var zones = new PipelineWaterZoneAggregator().BuildZones(observations, Config(mergeGapM: 5, minimumClearObservations: 1));

        Assert.Single(zones);
        Assert.Equal(0m, zones[0].StartChainageM);
        Assert.Equal(25m, zones[0].EndChainageM);
        Assert.Equal(4, zones[0].WaterObservationCount);
        Assert.Equal(4, zones[0].ClearObservationCount);
    }

    [Fact]
    public void MergeGapSplitsMatchingZonesWhenGapIsTooLarge()
    {
        var observations = new List<PipelineWaterBinObservation>();
        AddBin(observations, 0, 0, 10, water: 2, dry: 0);
        AddBin(observations, 1, 10, 15, water: 0, dry: 2);
        AddBin(observations, 2, 15, 25, water: 2, dry: 0);

        var zones = new PipelineWaterZoneAggregator().BuildZones(observations, Config(mergeGapM: 4, minimumClearObservations: 1));

        Assert.Equal(2, zones.Count);
        Assert.Equal(0m, zones[0].StartChainageM);
        Assert.Equal(15m, zones[1].StartChainageM);
    }

    [Fact]
    public void InsufficientDataBinBlocksMergeEvenWhenGapAllowsIt()
    {
        var observations = new List<PipelineWaterBinObservation>();
        AddBin(observations, 0, 0, 10, water: 2, dry: 0);
        observations.Add(Observation(1, 10, 15, "Unknown", day: 1));
        AddBin(observations, 2, 15, 25, water: 2, dry: 0);

        var zones = new PipelineWaterZoneAggregator().BuildZones(observations, Config(mergeGapM: 10, minimumClearObservations: 1));

        Assert.Equal(2, zones.Count);
        Assert.Equal("Persistent", zones[0].PersistenceClass);
        Assert.Equal("Persistent", zones[1].PersistenceClass);
    }

    [Fact]
    public void InsufficientDataWithObservedWaterIsItsOwnZone()
    {
        var observations = new List<PipelineWaterBinObservation>();
        observations.Add(Observation(0, 0, 10, "Water", day: 1));

        var zones = new PipelineWaterZoneAggregator().BuildZones(observations, Config(minimumClearObservations: 2));

        Assert.Single(zones);
        Assert.Equal("InsufficientData", zones[0].PersistenceClass);
        Assert.Equal(1, zones[0].WaterObservationCount);
        Assert.Equal(1, zones[0].ClearObservationCount);
    }

    [Fact]
    public void ZeroFrequencyBinsAreOmittedFromExposureZones()
    {
        var observations = new List<PipelineWaterBinObservation>();
        AddBin(observations, 0, 0, 10, water: 0, dry: 3);

        var zones = new PipelineWaterZoneAggregator().BuildZones(observations, Config(minimumClearObservations: 1));

        Assert.Empty(zones);
    }

    [Fact]
    public void AggregationIsDeterministicAcrossReruns()
    {
        var observations = new List<PipelineWaterBinObservation>();
        AddBin(observations, 0, 0, 10, water: 2, dry: 1);
        AddBin(observations, 1, 10, 20, water: 1, dry: 2);

        var aggregator = new PipelineWaterZoneAggregator();
        var first = aggregator.BuildZones(observations, Config(minimumClearObservations: 1));
        var second = aggregator.BuildZones(observations.AsEnumerable().Reverse().ToList(), Config(minimumClearObservations: 1));

        Assert.Equal(first.Select(zone => zone.ZoneOrdinal), second.Select(zone => zone.ZoneOrdinal));
        Assert.Equal(first.Select(zone => zone.StartChainageM), second.Select(zone => zone.StartChainageM));
        Assert.Equal(first.Select(zone => zone.EndChainageM), second.Select(zone => zone.EndChainageM));
        Assert.Equal(first.Select(zone => zone.PersistenceClass), second.Select(zone => zone.PersistenceClass));
        Assert.Equal(first.Select(zone => zone.WaterFrequency), second.Select(zone => zone.WaterFrequency));
    }

    [Fact]
    public void ZoneGeoJsonContainsRequiredProperties()
    {
        var zones = new[]
        {
            new PipelineWaterZoneResult
            {
                PipelineWaterZoneId = 44,
                ZoneOrdinal = 1,
                StartChainageM = 0,
                EndChainageM = 10,
                LengthM = 10,
                PersistenceClass = "Persistent",
                WaterFrequency = 1m,
                WaterObservationCount = 2,
                ClearObservationCount = 2,
                FirstWaterObservedAt = DateTimeOffset.Parse("2026-05-01T00:00:00Z"),
                LastWaterObservedAt = DateTimeOffset.Parse("2026-05-02T00:00:00Z"),
                MinimumWaterDistanceM = 0,
                HasCentrelineCrossing = true,
                RouteZoneWkt = "LINESTRING (-103.2 50.1, -103.1 50.1)"
            }
        };

        var geoJson = new PipelineWaterExportWriter().BuildZonesGeoJson(zones);
        using var document = JsonDocument.Parse(geoJson);
        var properties = document.RootElement.GetProperty("features")[0].GetProperty("properties");

        Assert.Equal(44, properties.GetProperty("pipelineWaterZoneId").GetInt64());
        Assert.Equal("Persistent", properties.GetProperty("persistenceClass").GetString());
        Assert.True(properties.GetProperty("hasCentrelineCrossing").GetBoolean());
        Assert.Equal(2, properties.GetProperty("clearObservationCount").GetInt32());
    }

    private static PipelineWaterConfig Config(
        double mergeGapM = 20,
        int minimumClearObservations = 2)
    {
        var config = new PipelineWaterConfig
        {
            MinimumClearObservations = minimumClearObservations,
            PersistentFrequencyThreshold = 0.80d,
            SeasonalFrequencyThreshold = 0.20d,
            MergeGapM = mergeGapM
        };
        config.Validate();
        return config;
    }

    private static void AddBin(
        List<PipelineWaterBinObservation> observations,
        int binIndex,
        decimal start,
        decimal end,
        int water,
        int dry)
    {
        var day = 1;
        for (var i = 0; i < water; i++)
        {
            observations.Add(Observation(binIndex, start, end, "Water", day++));
        }

        for (var i = 0; i < dry; i++)
        {
            observations.Add(Observation(binIndex, start, end, "Dry", day++));
        }
    }

    private static PipelineWaterBinObservation Observation(
        int binIndex,
        decimal start,
        decimal end,
        string state,
        int day)
    {
        return new PipelineWaterBinObservation
        {
            AcquisitionKey = $"acq-{day:00}",
            AcquiredAt = new DateTimeOffset(2026, 5, day, 12, 0, 0, TimeSpan.Zero),
            BinIndex = binIndex,
            StartChainageM = start,
            EndChainageM = end,
            ObservationState = state,
            ExposureType = state == "Water" ? "Crossing" : null,
            WaterAreaInCorridorM2 = state == "Water" ? 10 : null,
            LengthOnWaterM = state == "Water" ? 5 : null,
            NearestWaterDistanceM = state == "Water" ? 0 : null,
            RouteBinWkt = $"LINESTRING ({-103.2 + (double)start / 1000d} 50.1, {-103.2 + (double)end / 1000d} 50.1)"
        };
    }
}
