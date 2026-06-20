using Xunit;

public sealed class WorkImageCleanerTests
{
    [Fact]
    public void CleanDeletesDownloadedAndGdalIntermediateImages()
    {
        var root = CreateTempRoot();
        try
        {
            var scene = Path.Combine(root, "single");
            Directory.CreateDirectory(scene);
            File.WriteAllText(Path.Combine(scene, "B04.tif"), "band");
            File.WriteAllText(Path.Combine(scene, "B04.tif.aux.xml"), "aux");
            File.WriteAllText(Path.Combine(scene, "B04.tif.part"), "partial");
            File.WriteAllText(Path.Combine(scene, "item.json"), "{}");

            var work = Path.Combine(scene, "_work", "rgb");
            Directory.CreateDirectory(work);
            File.WriteAllText(Path.Combine(work, "rgb.vrt"), "vrt");
            File.WriteAllText(Path.Combine(work, "rgb_3857_8bit.tif"), "tif");

            var result = WorkImageCleaner.Clean(root);

            Assert.Equal(3, result.FilesDeleted);
            Assert.Equal(1, result.DirectoriesDeleted);
            Assert.Empty(result.Errors);
            Assert.False(File.Exists(Path.Combine(scene, "B04.tif")));
            Assert.False(File.Exists(Path.Combine(scene, "B04.tif.aux.xml")));
            Assert.False(File.Exists(Path.Combine(scene, "B04.tif.part")));
            Assert.False(Directory.Exists(Path.Combine(scene, "_work")));
            Assert.True(File.Exists(Path.Combine(scene, "item.json")));
        }
        finally
        {
            DeleteTempRoot(root);
        }
    }

    [Fact]
    public void CleanPreservesPublishedAndMetadataFiles()
    {
        var root = CreateTempRoot();
        try
        {
            var output = Path.Combine(root, "pipeline-water");
            Directory.CreateDirectory(output);
            File.WriteAllText(Path.Combine(output, "pipeline-water-zones.geojson"), "{}");
            File.WriteAllText(Path.Combine(output, "summary.json"), "{}");
            File.WriteAllText(Path.Combine(output, "tile.png"), "png");

            var result = WorkImageCleaner.Clean(root);

            Assert.False(result.HasWork);
            Assert.True(File.Exists(Path.Combine(output, "pipeline-water-zones.geojson")));
            Assert.True(File.Exists(Path.Combine(output, "summary.json")));
            Assert.True(File.Exists(Path.Combine(output, "tile.png")));
        }
        finally
        {
            DeleteTempRoot(root);
        }
    }

    private static string CreateTempRoot()
    {
        var path = Path.Combine(Path.GetTempPath(), "SentinelGrabCleanerTests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void DeleteTempRoot(string root)
    {
        if (Directory.Exists(root))
        {
            Directory.Delete(root, recursive: true);
        }
    }
}
