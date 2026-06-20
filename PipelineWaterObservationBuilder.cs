using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using NetTopologySuite.Operation.Union;

public sealed class PipelineWaterObservationBuilder
{
    private const double GeometryEpsilon = 0.000001d;
    private readonly WKTReader _wktReader = new(NetTopologySuite.NtsGeometryServices.Instance);
    private readonly WKTWriter _wktWriter = new();

    public PipelineWaterAcquisitionResult Build(
        PipelineWaterAcquisitionDetection detection,
        IReadOnlyList<PipelineChainageBinGeometry> bins,
        double minimumClearFractionPerBin)
    {
        if (string.IsNullOrWhiteSpace(detection.AcquisitionKey))
        {
            throw new InvalidOperationException("Pipeline water detection acquisition key is required.");
        }

        if (!double.IsFinite(minimumClearFractionPerBin) || minimumClearFractionPerBin < 0d || minimumClearFractionPerBin > 1d)
        {
            throw new InvalidOperationException("Minimum clear fraction per bin must be between 0 and 1.");
        }

        var sectionDetections = detection.Sections.ToDictionary(section => section.SectionOrdinal);
        var observations = new List<PipelineWaterBinObservation>();
        var clearFractions = new List<double>();

        foreach (var bin in bins.OrderBy(bin => bin.Bin.BinIndex))
        {
            var observation = BuildBinObservation(detection, bin, sectionDetections, minimumClearFractionPerBin, out var clearFraction);
            clearFractions.Add(clearFraction);
            observations.Add(observation);
        }

        var clearObservationCount = observations.Count(observation =>
            string.Equals(observation.ObservationState, "Water", StringComparison.Ordinal)
            || string.Equals(observation.ObservationState, "Dry", StringComparison.Ordinal));

        return new PipelineWaterAcquisitionResult
        {
            AcquisitionKey = detection.AcquisitionKey,
            AcquiredAt = detection.AcquiredAt,
            IsClear = clearObservationCount == observations.Count,
            ClearFraction = clearFractions.Count == 0 ? 0d : clearFractions.Average(),
            WaterGeoJsonPath = detection.WaterGeoJsonPath,
            BinObservations = observations
        };
    }

    private PipelineWaterBinObservation BuildBinObservation(
        PipelineWaterAcquisitionDetection detection,
        PipelineChainageBinGeometry bin,
        IReadOnlyDictionary<int, PipelineWaterSectionDetection> sectionDetections,
        double minimumClearFractionPerBin,
        out double clearFraction)
    {
        var totalCorridorArea = 0d;
        var clearArea = 0d;
        var waterArea = 0d;
        var lengthOnWater = 0d;
        var minimumWaterDistance = double.PositiveInfinity;
        var crossesCentreline = false;
        var intersectionWgs84Parts = new List<Geometry>();

        foreach (var part in bin.Parts)
        {
            var routeLine = _wktReader.Read(part.RouteBinProjectedWkt);
            var corridor = _wktReader.Read(part.CorridorProjectedWkt);
            totalCorridorArea += corridor.Area;

            if (!sectionDetections.TryGetValue(part.Bin.SectionOrdinal, out var sectionDetection))
            {
                continue;
            }

            if (!string.IsNullOrWhiteSpace(sectionDetection.ClearAreaProjectedWkt))
            {
                var clearGeometry = _wktReader.Read(sectionDetection.ClearAreaProjectedWkt);
                clearArea += corridor.Intersection(clearGeometry).Area;
            }

            var waterGeometry = UnionWaterGeometry(sectionDetection.WaterPolygonProjectedWkts);
            if (waterGeometry is null || waterGeometry.IsEmpty)
            {
                continue;
            }

            var waterInCorridor = corridor.Intersection(waterGeometry);
            if (waterInCorridor.IsEmpty || waterInCorridor.Area <= GeometryEpsilon)
            {
                continue;
            }

            waterArea += waterInCorridor.Area;
            var lineWater = routeLine.Intersection(waterGeometry);
            if (!lineWater.IsEmpty && lineWater.Length > GeometryEpsilon)
            {
                crossesCentreline = true;
                lengthOnWater += lineWater.Length;
                minimumWaterDistance = 0d;
            }
            else
            {
                minimumWaterDistance = Math.Min(minimumWaterDistance, routeLine.Distance(waterInCorridor));
            }

            var (zone, northernHemisphere) = DecodeUtmSrid(part.ProjectedSrid);
            var wgs84Intersection = PipelineGeometryProjector.ProjectUtmToWgs84(waterInCorridor, zone, northernHemisphere);
            if (!wgs84Intersection.IsEmpty)
            {
                intersectionWgs84Parts.Add(wgs84Intersection);
            }
        }

        clearFraction = totalCorridorArea <= GeometryEpsilon
            ? 0d
            : Math.Clamp(clearArea / totalCorridorArea, 0d, 1d);

        if (waterArea > GeometryEpsilon)
        {
            var waterIntersectionWkt = BuildWaterIntersectionWkt(intersectionWgs84Parts);
            return new PipelineWaterBinObservation
            {
                AcquisitionKey = detection.AcquisitionKey,
                AcquiredAt = detection.AcquiredAt,
                BinIndex = bin.Bin.BinIndex,
                StartChainageM = bin.Bin.StartChainageM,
                EndChainageM = bin.Bin.EndChainageM,
                ObservationState = "Water",
                ExposureType = crossesCentreline ? "Crossing" : "Proximity",
                WaterAreaInCorridorM2 = ToDecimalArea(waterArea),
                LengthOnWaterM = ToDecimalMetres(crossesCentreline ? lengthOnWater : 0d),
                NearestWaterDistanceM = ToDecimalMetres(crossesCentreline ? 0d : minimumWaterDistance),
                RouteBinWkt = bin.RouteBinWgs84Wkt,
                WaterIntersectionWkt = waterIntersectionWkt
            };
        }

        if (clearFraction < minimumClearFractionPerBin)
        {
            return BuildStateOnlyObservation(detection, bin, "Unknown");
        }

        return BuildStateOnlyObservation(detection, bin, "Dry");
    }

    private PipelineWaterBinObservation BuildStateOnlyObservation(
        PipelineWaterAcquisitionDetection detection,
        PipelineChainageBinGeometry bin,
        string state)
    {
        return new PipelineWaterBinObservation
        {
            AcquisitionKey = detection.AcquisitionKey,
            AcquiredAt = detection.AcquiredAt,
            BinIndex = bin.Bin.BinIndex,
            StartChainageM = bin.Bin.StartChainageM,
            EndChainageM = bin.Bin.EndChainageM,
            ObservationState = state,
            RouteBinWkt = bin.RouteBinWgs84Wkt
        };
    }

    private Geometry? UnionWaterGeometry(IReadOnlyList<string> waterPolygonProjectedWkts)
    {
        if (waterPolygonProjectedWkts.Count == 0)
        {
            return null;
        }

        var geometries = waterPolygonProjectedWkts
            .Select(wkt => _wktReader.Read(wkt))
            .Where(geometry => !geometry.IsEmpty)
            .ToList();
        return geometries.Count == 0 ? null : UnaryUnionOp.Union(geometries);
    }

    private string? BuildWaterIntersectionWkt(IReadOnlyList<Geometry> intersectionWgs84Parts)
    {
        if (intersectionWgs84Parts.Count == 0)
        {
            return null;
        }

        var merged = UnaryUnionOp.Union(intersectionWgs84Parts);
        return merged.IsEmpty ? null : _wktWriter.Write(merged);
    }

    private static (int Zone, bool NorthernHemisphere) DecodeUtmSrid(int srid)
    {
        if (srid is >= 32601 and <= 32660)
        {
            return (srid - 32600, true);
        }

        if (srid is >= 32701 and <= 32760)
        {
            return (srid - 32700, false);
        }

        throw new InvalidOperationException("Pipeline projected SRID must be an EPSG UTM SRID.");
    }

    private static decimal ToDecimalMetres(double value)
    {
        if (!double.IsFinite(value))
        {
            value = 0d;
        }

        return Math.Round((decimal)value, 3, MidpointRounding.AwayFromZero);
    }

    private static decimal ToDecimalArea(double value) => Math.Round((decimal)value, 2, MidpointRounding.AwayFromZero);
}
