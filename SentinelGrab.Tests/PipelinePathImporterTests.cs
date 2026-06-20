using Xunit;

public sealed class PipelinePathImporterTests
{
    [Fact]
    public void OrderedLineStringImports()
    {
        var result = Import("LINESTRING(-103.2 50.1, -103.1 50.11, -103.0 50.12)");

        Assert.True(result.Path.RouteLengthM > 0);
        Assert.Equal(-103.2d, result.StartLongitude, 6);
        Assert.Equal(50.12d, result.EndLatitude, 6);
        Assert.NotEmpty(result.Sections);
        Assert.Equal("LINESTRING (-103.2 50.1, -103.1 50.11, -103 50.12)", result.Path.RouteGeometry);
    }

    [Fact]
    public void ContinuousOrderedMultiLineStringImports()
    {
        var result = Import("MULTILINESTRING((-103.2 50.1, -103.1 50.1), (-103.100001 50.1, -103.0 50.1))", endpointToleranceM: 1d);

        Assert.Single(result.EndpointGaps);
        Assert.True(result.EndpointGaps[0].GapMetres <= 1d);
        Assert.Contains("-103", result.Path.RouteGeometry);
    }

    [Fact]
    public void DisconnectedComponentsAreRejected()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            Import("MULTILINESTRING((-103.2 50.1, -103.1 50.1), (-103.0 50.1, -102.9 50.1))", endpointToleranceM: 1d));

        Assert.Contains("endpoint gap", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BranchingPathIsRejected()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            Import("LINESTRING(-103.2 50.1, -103.0 50.1, -103.1 50.1, -103.1 50.2)"));

        Assert.Contains("non-branching", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ReversedInputProducesDifferentHashAndDirection()
    {
        var forward = Import("LINESTRING(-103.2 50.1, -103.1 50.1, -103.0 50.1)");
        var reversed = Import("LINESTRING(-103.0 50.1, -103.1 50.1, -103.2 50.1)");

        Assert.NotEqual(forward.Path.SourceHash, reversed.Path.SourceHash);
        Assert.Equal(-103.2d, forward.StartLongitude, 6);
        Assert.Equal(-103.0d, reversed.StartLongitude, 6);
    }

    [Fact]
    public void UtmBoundaryCrossingCreatesContinuousSections()
    {
        var result = Import(
            "LINESTRING(-102.2 50.1, -101.8 50.1)",
            densifyMaxSegmentLengthM: 500d,
            maxProjectedSectionLengthM: 25000d);

        Assert.True(result.Sections.Count >= 2);
        Assert.Contains(13, result.CrossedUtmZones);
        Assert.Contains(14, result.CrossedUtmZones);

        for (var i = 1; i < result.Sections.Count; i++)
        {
            Assert.Equal(result.Sections[i - 1].EndChainageM, result.Sections[i].StartChainageM);
        }
    }

    [Fact]
    public void GeoJsonFeatureCollectionImports()
    {
        const string geoJson = """
        {
          "type": "FeatureCollection",
          "features": [
            {
              "type": "Feature",
              "properties": {},
              "geometry": {
                "type": "LineString",
                "coordinates": [[-103.2, 50.1], [-103.1, 50.1]]
              }
            }
          ]
        }
        """;

        var result = Import(geoJson);

        Assert.True(result.Path.RouteLengthM > 0);
        Assert.Equal(-103.2d, result.StartLongitude, 6);
    }

    private static PipelinePathImportResult Import(
        string source,
        double endpointToleranceM = 0.5d,
        double densifyMaxSegmentLengthM = 100d,
        double maxProjectedSectionLengthM = 25000d)
    {
        return new PipelinePathImporter().Import(new PipelinePathImportRequest
        {
            SourceText = source,
            PathName = "Test pipeline",
            ChainageOriginM = 0m,
            EndpointToleranceM = endpointToleranceM,
            DensifyMaxSegmentLengthM = densifyMaxSegmentLengthM,
            MaxProjectedSectionLengthM = maxProjectedSectionLengthM
        });
    }
}
