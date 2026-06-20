using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Xunit;

public sealed class PipelineCorridorBuilderTests
{
    [Fact]
    public void BuildsRoundEndedWgs84CorridorFromProjectedSection()
    {
        var section = new PipelineSection
        {
            SectionOrdinal = 1,
            UtmZone = 13,
            NorthernHemisphere = true,
            StartChainageM = 0,
            EndChainageM = 1000,
            RouteSectionWkt = "LINESTRING (-103.2 50.1, -103.1 50.1)"
        };

        var corridor = new PipelineCorridorBuilder().Build(section, 50);
        var geometry = new GeoJsonReader().Read<Geometry>(corridor.CorridorWgs84GeoJson);

        Assert.Equal(32613, corridor.ProjectedSrid);
        Assert.True(geometry is Polygon or MultiPolygon);
        Assert.True(geometry.IsValid);
        Assert.Contains("\"Polygon\"", corridor.CorridorWgs84GeoJson);
        Assert.Contains("POLYGON", corridor.CorridorProjectedWkt);
    }
}
