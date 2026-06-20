using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

public sealed class PipelineWaterZoneAggregator
{
    private readonly WKTReader _wktReader = new(NetTopologySuite.NtsGeometryServices.Instance);
    private readonly WKTWriter _wktWriter = new();

    public IReadOnlyList<PipelineWaterBinTemporalStats> BuildBinStats(
        IReadOnlyList<PipelineWaterBinObservation> observations,
        PipelineWaterConfig config)
    {
        config.Validate();
        return observations
            .GroupBy(observation => observation.BinIndex)
            .Select(group => BuildBinStats(group, config))
            .OrderBy(stats => stats.StartChainageM)
            .ThenBy(stats => stats.BinIndex)
            .ToList();
    }

    public IReadOnlyList<PipelineWaterZoneResult> BuildZones(
        IReadOnlyList<PipelineWaterBinObservation> observations,
        PipelineWaterConfig config)
    {
        var stats = BuildBinStats(observations, config);
        return BuildZones(stats, config);
    }

    public IReadOnlyList<PipelineWaterZoneResult> BuildZones(
        IReadOnlyList<PipelineWaterBinTemporalStats> stats,
        PipelineWaterConfig config)
    {
        config.Validate();
        var statsByIndex = stats.ToDictionary(item => item.BinIndex);
        var zoneCandidates = stats
            .Where(item => item.WaterFrequency.HasValue && item.WaterFrequency.Value > 0m)
            .OrderBy(item => item.StartChainageM)
            .ThenBy(item => item.BinIndex)
            .ToList();

        var zones = new List<PipelineWaterZoneResult>();
        List<PipelineWaterBinTemporalStats>? active = null;

        foreach (var candidate in zoneCandidates)
        {
            if (active is null)
            {
                active = new List<PipelineWaterBinTemporalStats> { candidate };
                continue;
            }

            var previous = active[^1];
            if (CanMerge(previous, candidate, active[0].PersistenceClass, statsByIndex, config.MergeGapM))
            {
                active.Add(candidate);
                continue;
            }

            zones.Add(BuildZone(active, zones.Count + 1));
            active = new List<PipelineWaterBinTemporalStats> { candidate };
        }

        if (active is not null)
        {
            zones.Add(BuildZone(active, zones.Count + 1));
        }

        return zones;
    }

    private PipelineWaterBinTemporalStats BuildBinStats(
        IEnumerable<PipelineWaterBinObservation> observations,
        PipelineWaterConfig config)
    {
        var ordered = observations
            .OrderBy(observation => observation.AcquiredAt)
            .ThenBy(observation => observation.AcquisitionKey, StringComparer.OrdinalIgnoreCase)
            .ToList();
        if (ordered.Count == 0)
        {
            throw new InvalidOperationException("Cannot aggregate an empty observation group.");
        }

        var water = ordered.Count(observation => string.Equals(observation.ObservationState, "Water", StringComparison.Ordinal));
        var dry = ordered.Count(observation => string.Equals(observation.ObservationState, "Dry", StringComparison.Ordinal));
        var unknown = ordered.Count(observation => string.Equals(observation.ObservationState, "Unknown", StringComparison.Ordinal));
        var clear = water + dry;
        var waterFrequency = clear > 0
            ? Math.Round((decimal)water / clear, 6, MidpointRounding.AwayFromZero)
            : (decimal?)null;

        var waterObservations = ordered
            .Where(observation => string.Equals(observation.ObservationState, "Water", StringComparison.Ordinal))
            .ToList();
        return new PipelineWaterBinTemporalStats
        {
            BinIndex = ordered[0].BinIndex,
            StartChainageM = ordered.Min(observation => observation.StartChainageM),
            EndChainageM = ordered.Max(observation => observation.EndChainageM),
            WaterObservationCount = water,
            DryObservationCount = dry,
            UnknownObservationCount = unknown,
            WaterFrequency = waterFrequency,
            PersistenceClass = Classify(clear, waterFrequency, config),
            FirstWaterObservedAt = waterObservations.Count == 0 ? null : waterObservations.Min(observation => observation.AcquiredAt),
            LastWaterObservedAt = waterObservations.Count == 0 ? null : waterObservations.Max(observation => observation.AcquiredAt),
            MaximumWaterAreaM2 = waterObservations
                .Where(observation => observation.WaterAreaInCorridorM2.HasValue)
                .Select(observation => observation.WaterAreaInCorridorM2!.Value)
                .DefaultIfEmpty()
                .Max(),
            MinimumWaterDistanceM = waterObservations
                .Where(observation => observation.NearestWaterDistanceM.HasValue)
                .Select(observation => observation.NearestWaterDistanceM!.Value)
                .DefaultIfEmpty()
                .Min(),
            HasCentrelineCrossing = waterObservations.Any(observation => string.Equals(observation.ExposureType, "Crossing", StringComparison.Ordinal)),
            RouteBinWkt = ordered[0].RouteBinWkt
        };
    }

    private static string Classify(int clearObservationCount, decimal? waterFrequency, PipelineWaterConfig config)
    {
        if (clearObservationCount < config.MinimumClearObservations)
        {
            return "InsufficientData";
        }

        if (!waterFrequency.HasValue || waterFrequency.Value == 0m)
        {
            return "Intermittent";
        }

        if (waterFrequency.Value >= (decimal)config.PersistentFrequencyThreshold)
        {
            return "Persistent";
        }

        if (waterFrequency.Value >= (decimal)config.SeasonalFrequencyThreshold)
        {
            return "Seasonal";
        }

        return "Intermittent";
    }

    private static bool CanMerge(
        PipelineWaterBinTemporalStats previous,
        PipelineWaterBinTemporalStats candidate,
        string activeClass,
        IReadOnlyDictionary<int, PipelineWaterBinTemporalStats> statsByIndex,
        double mergeGapM)
    {
        if (!string.Equals(candidate.PersistenceClass, activeClass, StringComparison.Ordinal))
        {
            return false;
        }

        var gap = candidate.StartChainageM - previous.EndChainageM;
        if (gap < 0m || gap > (decimal)mergeGapM)
        {
            return false;
        }

        for (var index = previous.BinIndex + 1; index < candidate.BinIndex; index++)
        {
            if (statsByIndex.TryGetValue(index, out var between)
                && string.Equals(between.PersistenceClass, "InsufficientData", StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private PipelineWaterZoneResult BuildZone(IReadOnlyList<PipelineWaterBinTemporalStats> bins, int zoneOrdinal)
    {
        var water = bins.Sum(bin => bin.WaterObservationCount);
        var dry = bins.Sum(bin => bin.DryObservationCount);
        var unknown = bins.Sum(bin => bin.UnknownObservationCount);
        var clear = water + dry;
        var frequency = clear > 0
            ? Math.Round((decimal)water / clear, 6, MidpointRounding.AwayFromZero)
            : (decimal?)null;

        var firstWater = bins
            .Where(bin => bin.FirstWaterObservedAt.HasValue)
            .Select(bin => bin.FirstWaterObservedAt!.Value)
            .DefaultIfEmpty()
            .Min();
        var lastWater = bins
            .Where(bin => bin.LastWaterObservedAt.HasValue)
            .Select(bin => bin.LastWaterObservedAt!.Value)
            .DefaultIfEmpty()
            .Max();

        return new PipelineWaterZoneResult
        {
            ZoneOrdinal = zoneOrdinal,
            StartChainageM = bins.Min(bin => bin.StartChainageM),
            EndChainageM = bins.Max(bin => bin.EndChainageM),
            LengthM = bins.Max(bin => bin.EndChainageM) - bins.Min(bin => bin.StartChainageM),
            WaterObservationCount = water,
            DryObservationCount = dry,
            UnknownObservationCount = unknown,
            ClearObservationCount = clear,
            WaterFrequency = frequency,
            PersistenceClass = bins[0].PersistenceClass,
            FirstWaterObservedAt = firstWater == default ? null : firstWater,
            LastWaterObservedAt = lastWater == default ? null : lastWater,
            MaximumWaterAreaM2 = bins
                .Where(bin => bin.MaximumWaterAreaM2.HasValue)
                .Select(bin => bin.MaximumWaterAreaM2!.Value)
                .DefaultIfEmpty()
                .Max(),
            MinimumWaterDistanceM = bins
                .Where(bin => bin.MinimumWaterDistanceM.HasValue)
                .Select(bin => bin.MinimumWaterDistanceM!.Value)
                .DefaultIfEmpty()
                .Min(),
            HasCentrelineCrossing = bins.Any(bin => bin.HasCentrelineCrossing),
            RouteZoneWkt = BuildRouteZoneWkt(bins)
        };
    }

    private string BuildRouteZoneWkt(IReadOnlyList<PipelineWaterBinTemporalStats> bins)
    {
        var geometries = bins
            .Select(bin => _wktReader.Read(bin.RouteBinWkt))
            .ToList();
        if (geometries.Count == 1)
        {
            return _wktWriter.Write(geometries[0]);
        }

        var lines = geometries
            .SelectMany(ExplodeLineStrings)
            .ToArray();
        return _wktWriter.Write(NetTopologySuite.NtsGeometryServices.Instance.CreateGeometryFactory(srid: 4326).CreateMultiLineString(lines));
    }

    private static IEnumerable<LineString> ExplodeLineStrings(Geometry geometry)
    {
        if (geometry is LineString line)
        {
            yield return line;
            yield break;
        }

        if (geometry is MultiLineString multiLine)
        {
            for (var i = 0; i < multiLine.NumGeometries; i++)
            {
                if (multiLine.GetGeometryN(i) is LineString component)
                {
                    yield return component;
                }
            }
        }
    }
}
