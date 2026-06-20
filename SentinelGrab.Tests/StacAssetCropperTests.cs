using Xunit;

public sealed class StacAssetCropperTests
{
    [Fact]
    public void RemoteCommandUsesVsicurlCutlineAndDeterministicOutput()
    {
        var request = Request("B03");

        var command = StacAssetCropper.CreateRemoteCropCommand(
            request,
            "https://example.test/B03.tif?sig=secret-token");

        Assert.True(command.UsesRemoteCog);
        Assert.Contains("/vsicurl/https://example.test/B03.tif?sig=secret-token", command.Arguments);
        Assert.Contains("-cutline", command.Arguments);
        Assert.Contains("-crop_to_cutline", command.Arguments);
        Assert.Contains("section-0003_take_1_B03.tif", command.OutputPath);
        Assert.Contains("bilinear", command.Arguments);
        Assert.DoesNotContain(command.RedactedArguments, arg => arg.Contains("secret-token", StringComparison.Ordinal));
    }

    [Fact]
    public void SclUsesNearestNeighbourResampling()
    {
        var command = StacAssetCropper.CreateRemoteCropCommand(
            Request("SCL"),
            "https://example.test/SCL.tif?sig=secret-token");

        Assert.Contains("near", command.Arguments);
        Assert.DoesNotContain("bilinear", command.Arguments);
    }

    private static StacAssetCropRequest Request(string assetKey)
    {
        return new StacAssetCropRequest
        {
            AssetKey = assetKey,
            AssetHref = $"https://example.test/{assetKey}.tif",
            AcquisitionKey = "take/1",
            SectionOrdinal = 3,
            SectionCorridorGeoJson = """{"type":"Polygon","coordinates":[[[-103,50],[-102.9,50],[-102.9,50.1],[-103,50.1],[-103,50]]]}""",
            OutputDirectory = Path.Combine(Path.GetTempPath(), "SentinelGrabCropperTests"),
            OsgeoRoot = "C:\\OSGeo4W"
        };
    }
}
