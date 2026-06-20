using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using NetTopologySuite.Operation.Union;
using Xunit;

public sealed class PipelineWaterObservationBuilderTests
{
    private readonly WKTReader _reader = new(NetTopologySuite.NtsGeometryServices.Instance);
    private readonly WKTWriter _writer = new();

    [Fact]
    public void CentrelineCrossingIsWaterCrossing()
    {
        var bins = BuildSingleSectionBins();
        var water = SquareAround(((LineString)_reader.Read(bins[0].Parts[0].RouteBinProjectedWkt)).InteriorPoint.Coordinate, 10);
        var result = BuildResult(bins, new[] { water }, clearWkt: UnionCorridors(bins));

        var observation = result.BinObservations[0];
        Assert.Equal("Water", observation.ObservationState);
        Assert.Equal("Crossing", observation.ExposureType);
        Assert.True(observation.WaterAreaInCorridorM2 > 0);
        Assert.True(observation.LengthOnWaterM > 0);
        Assert.Equal(0m, observation.NearestWaterDistanceM);
        Assert.NotNull(observation.WaterIntersectionWkt);
    }

    [Fact]
    public void WaterInCorridorButNotCentrelineIsProximity()
    {
        var bins = BuildSingleSectionBins(corridorHalfWidthM: 30);
        var route = (LineString)_reader.Read(bins[0].Parts[0].RouteBinProjectedWkt);
        var coordinate = route.InteriorPoint.Coordinate;
        var water = SquareAround(new Coordinate(coordinate.X, coordinate.Y + 20), 4);
        var result = BuildResult(bins, new[] { water }, clearWkt: UnionCorridors(bins));

        var observation = result.BinObservations[0];
        Assert.Equal("Water", observation.ObservationState);
        Assert.Equal("Proximity", observation.ExposureType);
        Assert.Equal(0m, observation.LengthOnWaterM);
        Assert.True(observation.NearestWaterDistanceM > 0);
    }

    [Fact]
    public void ClearNoWaterBinIsDry()
    {
        var bins = BuildSingleSectionBins();
        var result = BuildResult(bins, Array.Empty<Geometry>(), clearWkt: UnionCorridors(bins));

        Assert.All(result.BinObservations, observation =>
        {
            Assert.Equal("Dry", observation.ObservationState);
            Assert.Null(observation.ExposureType);
            Assert.Null(observation.WaterAreaInCorridorM2);
        });
    }

    [Fact]
    public void LowClearFractionIsUnknownAndNotDry()
    {
        var bins = BuildSingleSectionBins();
        var result = BuildResult(bins, Array.Empty<Geometry>(), clearWkt: null, minimumClearFraction: 0.80d);

        Assert.All(result.BinObservations, observation => Assert.Equal("Unknown", observation.ObservationState));
    }

    [Fact]
    public void WaterSpanningTwoBinsCreatesTwoWaterObservations()
    {
        var bins = BuildSingleSectionBins(routeLengthM: 200, analysisBinLengthM: 100);
        var firstRoute = (LineString)_reader.Read(bins[0].Parts[0].RouteBinProjectedWkt);
        var boundary = firstRoute.EndPoint.Coordinate;
        var water = SquareAround(boundary, 12);
        var result = BuildResult(bins, new[] { water }, clearWkt: UnionCorridors(bins));

        Assert.Equal(2, result.BinObservations.Count);
        Assert.All(result.BinObservations, observation => Assert.Equal("Water", observation.ObservationState));
        Assert.Equal(new[] { 0, 1 }, result.BinObservations.Select(observation => observation.BinIndex).ToArray());
    }

    [Fact]
    public void OverlappingMgrsWaterPolygonsDoNotDuplicateAreaOrObservations()
    {
        var bins = BuildSingleSectionBins();
        var water = SquareAround(((LineString)_reader.Read(bins[0].Parts[0].RouteBinProjectedWkt)).InteriorPoint.Coordinate, 10);
        var single = BuildResult(bins, new[] { water }, clearWkt: UnionCorridors(bins)).BinObservations[0];
        var duplicate = BuildResult(bins, new[] { water, water }, clearWkt: UnionCorridors(bins)).BinObservations[0];

        Assert.Single(duplicate.WaterAreaInCorridorM2 is null ? Array.Empty<decimal>() : new[] { duplicate.WaterAreaInCorridorM2.Value });
        Assert.Equal(single.WaterAreaInCorridorM2, duplicate.WaterAreaInCorridorM2);
    }

    [Fact]
    public void ChainageRemainsContinuousAcrossSectionAndUtmBoundary()
    {
        var sections = new[]
        {
            new PipelineSection
            {
                SectionOrdinal = 1,
                UtmZone = 13,
                NorthernHemisphere = true,
                StartChainageM = 0,
                EndChainageM = 100,
                RouteSectionWkt = "LINESTRING (-102.2 50.1, -102 50.1)"
            },
            new PipelineSection
            {
                SectionOrdinal = 2,
                UtmZone = 14,
                NorthernHemisphere = true,
                StartChainageM = 100,
                EndChainageM = 200,
                RouteSectionWkt = "LINESTRING (-102 50.1, -101.8 50.1)"
            }
        };

        var bins = new PipelineChainageBinBuilder().Build(sections, 75, 20);

        Assert.Equal(new[] { 0m, 75m, 150m }, bins.Select(bin => bin.Bin.StartChainageM).ToArray());
        Assert.Equal(new[] { 75m, 150m, 200m }, bins.Select(bin => bin.Bin.EndChainageM).ToArray());
        Assert.Equal(2, bins[1].Parts.Count);
        Assert.Equal(bins[0].Bin.EndChainageM, bins[1].Bin.StartChainageM);
        Assert.Equal(bins[1].Bin.EndChainageM, bins[2].Bin.StartChainageM);
    }

    private IReadOnlyList<PipelineChainageBinGeometry> BuildSingleSectionBins(
        double routeLengthM = 100,
        double analysisBinLengthM = 100,
        double corridorHalfWidthM = 25)
    {
        var section = new PipelineSection
        {
            SectionOrdinal = 1,
            UtmZone = 13,
            NorthernHemisphere = true,
            StartChainageM = 0,
            EndChainageM = (decimal)routeLengthM,
            RouteSectionWkt = "LINESTRING (-103.2 50.1, -103.197 50.1)"
        };

        return new PipelineChainageBinBuilder().Build(new[] { section }, analysisBinLengthM, corridorHalfWidthM);
    }

    private PipelineWaterAcquisitionResult BuildResult(
        IReadOnlyList<PipelineChainageBinGeometry> bins,
        IReadOnlyList<Geometry> waterPolygons,
        string? clearWkt,
        double minimumClearFraction = 0.80d)
    {
        var detection = new PipelineWaterAcquisitionDetection
        {
            AcquisitionKey = "take-1",
            AcquiredAt = DateTimeOffset.Parse("2026-05-01T18:00:00Z"),
            Sections = new[]
            {
                new PipelineWaterSectionDetection
                {
                    SectionOrdinal = 1,
                    ProjectedSrid = bins[0].Parts[0].ProjectedSrid,
                    WaterPolygonProjectedWkts = waterPolygons.Select(geometry => _writer.Write(geometry)).ToArray(),
                    ClearAreaProjectedWkt = clearWkt
                }
            }
        };

        return new PipelineWaterObservationBuilder().Build(detection, bins, minimumClearFraction);
    }

    private string UnionCorridors(IReadOnlyList<PipelineChainageBinGeometry> bins)
    {
        var corridors = bins
            .SelectMany(bin => bin.Parts)
            .Select(part => _reader.Read(part.CorridorProjectedWkt))
            .ToList();
        return _writer.Write(UnaryUnionOp.Union(corridors));
    }

    private static Polygon SquareAround(Coordinate center, double halfSizeM)
    {
        var factory = NetTopologySuite.NtsGeometryServices.Instance.CreateGeometryFactory();
        return factory.CreatePolygon(new[]
        {
            new Coordinate(center.X - halfSizeM, center.Y - halfSizeM),
            new Coordinate(center.X + halfSizeM, center.Y - halfSizeM),
            new Coordinate(center.X + halfSizeM, center.Y + halfSizeM),
            new Coordinate(center.X - halfSizeM, center.Y + halfSizeM),
            new Coordinate(center.X - halfSizeM, center.Y - halfSizeM)
        });
    }
}
