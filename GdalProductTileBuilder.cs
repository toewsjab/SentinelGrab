using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Security;
using System.Text;
using System.Text.Json;

public sealed record TileBuildRequest
{
    public long JobId { get; init; }
    public string ProductCode { get; init; } = "";
    public string DateKey { get; init; } = "";
    public string InputDir { get; init; } = "";
    public string OutputRootPath { get; init; } = "";
    public string ProductSubPath { get; init; } = "";
    public string OsgeoRoot { get; init; } = "";
    public Bbox? ClipBbox { get; init; }
    public int ZoomMin { get; init; } = 8;
    public int ZoomMax { get; init; } = 14;
    public int Processes { get; init; } = 1;
    public int ScaleMaxRgb { get; init; } = 4000;
    public double IndexMin { get; init; } = -0.2;
    public double IndexMax { get; init; } = 0.9;
}

public sealed record TileBuildResult(string OutputDir, string Log);

public sealed class GdalProductTileBuilder
{
    public async Task<TileBuildResult> BuildAsync(TileBuildRequest request)
    {
        if (request.ZoomMax < request.ZoomMin)
        {
            throw new InvalidOperationException("ZoomMax must be greater than or equal to ZoomMin.");
        }

        var productCode = request.ProductCode.Trim().ToUpperInvariant();
        var outputDir = Path.Combine(request.OutputRootPath, request.ProductSubPath, request.DateKey);
        Directory.CreateDirectory(outputDir);

        var gdal = new GdalToolRunner(request.OsgeoRoot);
        var log = new StringBuilder();
        log.AppendLine($"Building {productCode} tiles for JobId={request.JobId}.");
        log.AppendLine($"OutputDir={outputDir}");
        if (request.ClipBbox is { } clipBbox)
        {
            log.AppendLine(
                string.Create(
                    CultureInfo.InvariantCulture,
                    $"ClipBbox={clipBbox.MinLon},{clipBbox.MinLat},{clipBbox.MaxLon},{clipBbox.MaxLat}"));
        }

        switch (productCode)
        {
            case "RGB":
                await BuildRgbAsync(request, gdal, outputDir, log);
                break;
            case "NDVI":
                await BuildIndexAsync(request, gdal, outputDir, log, "ndvi", "B08", "B04");
                break;
            case "NDMI":
                await BuildIndexAsync(request, gdal, outputDir, log, "ndmi", "B08", "B11");
                break;
            case "NDRE":
                await BuildIndexAsync(request, gdal, outputDir, log, "ndre", "B8A", "B05");
                break;
            default:
                throw new InvalidOperationException($"Unsupported Sentinel product '{request.ProductCode}'.");
        }

        return new TileBuildResult(outputDir, log.ToString());
    }

    private static async Task BuildRgbAsync(TileBuildRequest request, GdalToolRunner gdal, string outputDir, StringBuilder log)
    {
        var workDir = Path.Combine(request.InputDir, "_work", "rgb");
        Directory.CreateDirectory(workDir);

        try
        {
            await BuildBandVrtAsync(gdal, workDir, request.InputDir, "B02", log);
            await BuildBandVrtAsync(gdal, workDir, request.InputDir, "B03", log);
            await BuildBandVrtAsync(gdal, workDir, request.InputDir, "B04", log);

            await gdal.RunCheckedAsync(
                "gdalbuildvrt",
                new[] { "-overwrite", "-separate", "rgb.vrt", "B04.vrt", "B03.vrt", "B02.vrt" },
                workDir,
                log);

            await gdal.RunCheckedAsync(
                "gdalwarp",
                BuildWarpToWebMercatorArgs(request, "rgb.vrt", "rgb_3857.tif"),
                workDir,
                log);

            await gdal.RunCheckedAsync(
                "gdal_translate",
                new[]
                {
                    "-of", "GTiff",
                    "rgb_3857.tif",
                    "rgb_3857_8bit.tif",
                    "-ot", "Byte",
                    "-scale_1", "0", request.ScaleMaxRgb.ToString(CultureInfo.InvariantCulture), "0", "255",
                    "-scale_2", "0", request.ScaleMaxRgb.ToString(CultureInfo.InvariantCulture), "0", "255",
                    "-scale_3", "0", request.ScaleMaxRgb.ToString(CultureInfo.InvariantCulture), "0", "255",
                    "-co", "TILED=YES",
                    "-co", "COMPRESS=DEFLATE"
                },
                workDir,
                log);

            var tileGenerator = new GdalXyzTileGenerator();
            await tileGenerator.GenerateAsync(
                Path.Combine(workDir, "rgb_3857_8bit.tif"),
                outputDir,
                request.ZoomMin,
                request.ZoomMax,
                request.Processes,
                gdal,
                log);
        }
        finally
        {
            TryDeleteDirectory(workDir, log);
        }
    }

    private static async Task BuildIndexAsync(
        TileBuildRequest request,
        GdalToolRunner gdal,
        string outputDir,
        StringBuilder log,
        string productName,
        string numeratorBand,
        string denominatorBand)
    {
        var workDir = Path.Combine(request.InputDir, "_work", productName);
        Directory.CreateDirectory(workDir);

        try
        {
            await BuildBandVrtAsync(gdal, workDir, request.InputDir, numeratorBand, log);
            await BuildBandVrtAsync(gdal, workDir, request.InputDir, denominatorBand, log);

            await gdal.RunCheckedAsync(
                "gdalbuildvrt",
                new[] { "-overwrite", "-separate", "idx.vrt", $"{numeratorBand}.vrt", $"{denominatorBand}.vrt" },
                workDir,
                log);

            await gdal.RunCheckedAsync(
                "gdalwarp",
                BuildWarpToWebMercatorArgs(request, "idx.vrt", "idx_3857.tif"),
                workDir,
                log);

            var sourceInfo = await RasterInfo.ReadAsync(gdal, Path.Combine(workDir, "idx_3857.tif"), workDir);
            var bandAPath = Path.Combine(workDir, "band_a_f32.bin");
            var bandBPath = Path.Combine(workDir, "band_b_f32.bin");
            var outputRawPath = Path.Combine(workDir, $"{productName}_8bit.raw");
            var outputVrtPath = Path.Combine(workDir, $"{productName}_8bit.vrt");
            var outputTifPath = Path.Combine(workDir, $"{productName}_8bit.tif");

            DeleteIfExists(bandAPath, Path.ChangeExtension(bandAPath, ".hdr"));
            DeleteIfExists(bandBPath, Path.ChangeExtension(bandBPath, ".hdr"));
            DeleteIfExists(outputRawPath, outputVrtPath, outputTifPath);

            await gdal.RunCheckedAsync(
                "gdal_translate",
                new[] { "-of", "ENVI", "-ot", "Float32", "-b", "1", "idx_3857.tif", Path.GetFileName(bandAPath) },
                workDir,
                log);

            await gdal.RunCheckedAsync(
                "gdal_translate",
                new[] { "-of", "ENVI", "-ot", "Float32", "-b", "2", "idx_3857.tif", Path.GetFileName(bandBPath) },
                workDir,
                log);

            ComputeScaledIndexRaster(
                bandAPath,
                bandBPath,
                outputRawPath,
                sourceInfo.Width,
                sourceInfo.Height,
                request.IndexMin,
                request.IndexMax);

            await WriteRawByteVrtAsync(outputVrtPath, outputRawPath, sourceInfo);

            await gdal.RunCheckedAsync(
                "gdal_translate",
                new[]
                {
                    "-of", "GTiff",
                    "-a_nodata", "0",
                    Path.GetFileName(outputVrtPath),
                    Path.GetFileName(outputTifPath),
                    "-co", "TILED=YES",
                    "-co", "COMPRESS=DEFLATE"
                },
                workDir,
                log);

            var tileGenerator = new GdalXyzTileGenerator();
            await tileGenerator.GenerateAsync(
                outputTifPath,
                outputDir,
                request.ZoomMin,
                request.ZoomMax,
                request.Processes,
                gdal,
                log);
        }
        finally
        {
            TryDeleteDirectory(workDir, log);
        }
    }

    private static IReadOnlyList<string> BuildWarpToWebMercatorArgs(TileBuildRequest request, string source, string destination)
    {
        var args = new List<string>
        {
            "-overwrite",
            "-t_srs", "EPSG:3857",
            "-r", "bilinear",
            "-multi",
            "-wo", "NUM_THREADS=ALL_CPUS"
        };

        if (request.ClipBbox is { } bbox)
        {
            args.AddRange(new[]
            {
                "-te_srs", "EPSG:4326",
                "-te",
                bbox.MinLon.ToString("G17", CultureInfo.InvariantCulture),
                bbox.MinLat.ToString("G17", CultureInfo.InvariantCulture),
                bbox.MaxLon.ToString("G17", CultureInfo.InvariantCulture),
                bbox.MaxLat.ToString("G17", CultureInfo.InvariantCulture)
            });
        }

        args.AddRange(new[]
        {
            source,
            destination,
            "-co", "TILED=YES",
            "-co", "COMPRESS=DEFLATE"
        });

        return args;
    }

    private static async Task BuildBandVrtAsync(
        GdalToolRunner gdal,
        string workDir,
        string inputDir,
        string bandName,
        StringBuilder log)
    {
        var files = FindBandFiles(inputDir, bandName);
        if (files.Count == 0)
        {
            throw new InvalidOperationException($"Missing band {bandName}. Expected {bandName}.tif under {inputDir}.");
        }

        var args = new List<string> { "-overwrite", $"{bandName}.vrt" };
        args.AddRange(files);
        await gdal.RunCheckedAsync("gdalbuildvrt", args, workDir, log);
    }

    private static List<string> FindBandFiles(string inputDir, string bandName)
    {
        var workSegment = $"{Path.DirectorySeparatorChar}_work{Path.DirectorySeparatorChar}";
        return Directory
            .EnumerateFiles(inputDir, $"{bandName}.tif", SearchOption.AllDirectories)
            .Where(path => !path.Contains(workSegment, StringComparison.OrdinalIgnoreCase))
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static void ComputeScaledIndexRaster(
        string bandAPath,
        string bandBPath,
        string outputRawPath,
        int width,
        int height,
        double indexMin,
        double indexMax)
    {
        if (indexMax <= indexMin)
        {
            throw new InvalidOperationException("IndexMax must be greater than IndexMin.");
        }

        var inputRowBytes = checked(width * sizeof(float));
        var bandARow = new byte[inputRowBytes];
        var bandBRow = new byte[inputRowBytes];
        var outputRow = new byte[width];
        var scale = 255.0 / (indexMax - indexMin);

        using var bandA = File.OpenRead(bandAPath);
        using var bandB = File.OpenRead(bandBPath);
        using var output = File.Create(outputRawPath);

        for (var y = 0; y < height; y++)
        {
            bandA.ReadExactly(bandARow);
            bandB.ReadExactly(bandBRow);

            for (var x = 0; x < width; x++)
            {
                var offset = x * sizeof(float);
                var a = BinaryPrimitives.ReadSingleLittleEndian(bandARow.AsSpan(offset, sizeof(float)));
                var b = BinaryPrimitives.ReadSingleLittleEndian(bandBRow.AsSpan(offset, sizeof(float)));
                var sum = a + b;

                if (!float.IsFinite(a) || !float.IsFinite(b) || Math.Abs(sum) <= 0.000001f)
                {
                    outputRow[x] = 0;
                    continue;
                }

                var index = (a - b) / (sum + 0.000001f);
                var scaled = (index - indexMin) * scale;
                outputRow[x] = ToByte(scaled);
            }

            output.Write(outputRow);
        }
    }

    private static byte ToByte(double value)
    {
        if (double.IsNaN(value) || double.IsInfinity(value) || value <= 0)
        {
            return 0;
        }

        if (value >= 255)
        {
            return 255;
        }

        return (byte)Math.Round(value, MidpointRounding.AwayFromZero);
    }

    private static async Task WriteRawByteVrtAsync(string vrtPath, string rawPath, RasterInfo info)
    {
        var rawFileName = SecurityElement.Escape(Path.GetFileName(rawPath)) ?? Path.GetFileName(rawPath);
        var srs = SecurityElement.Escape(info.Wkt) ?? "EPSG:3857";
        var geoTransform = string.Join(
            ", ",
            info.GeoTransform.Select(value => value.ToString("G17", CultureInfo.InvariantCulture)));

        var xml = $"""
<VRTDataset rasterXSize="{info.Width}" rasterYSize="{info.Height}">
  <SRS>{srs}</SRS>
  <GeoTransform>{geoTransform}</GeoTransform>
  <VRTRasterBand dataType="Byte" band="1" subClass="VRTRawRasterBand">
    <NoDataValue>0</NoDataValue>
    <SourceFilename relativeToVRT="1">{rawFileName}</SourceFilename>
    <ImageOffset>0</ImageOffset>
    <PixelOffset>1</PixelOffset>
    <LineOffset>{info.Width}</LineOffset>
    <ByteOrder>LSB</ByteOrder>
  </VRTRasterBand>
</VRTDataset>
""";

        await File.WriteAllTextAsync(vrtPath, xml, Encoding.UTF8);
    }

    private static void DeleteIfExists(params string[] paths)
    {
        foreach (var path in paths)
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
    }

    private static void TryDeleteDirectory(string path, StringBuilder log)
    {
        try
        {
            if (Directory.Exists(path))
            {
                Directory.Delete(path, recursive: true);
                log.AppendLine($"Deleted scratch folder: {path}");
            }
        }
        catch (Exception ex)
        {
            log.AppendLine($"Unable to delete scratch folder {path}: {ex.Message}");
        }
    }
}

public sealed class GdalXyzTileGenerator
{
    private const int TileSize = 256;
    private const double OriginShift = 20037508.342789244;

    public async Task GenerateAsync(
        string sourcePath,
        string outputDir,
        int zoomMin,
        int zoomMax,
        int processes,
        GdalToolRunner gdal,
        StringBuilder log)
    {
        var info = await RasterInfo.ReadAsync(gdal, sourcePath, Path.GetDirectoryName(sourcePath) ?? Environment.CurrentDirectory);
        var parallelism = Math.Max(1, processes);
        var totalTiles = 0;

        for (var zoom = zoomMin; zoom <= zoomMax; zoom++)
        {
            var range = TileRange.FromBounds(info.MinX, info.MinY, info.MaxX, info.MaxY, zoom);
            if (range.IsEmpty)
            {
                log.AppendLine($"Zoom {zoom}: no intersecting Web Mercator tiles.");
                continue;
            }

            var zoomTiles = ((range.MaxX - range.MinX) + 1) * ((range.MaxY - range.MinY) + 1);
            totalTiles += zoomTiles;
            log.AppendLine($"Zoom {zoom}: generating {zoomTiles} tile(s), x={range.MinX}-{range.MaxX}, y={range.MinY}-{range.MaxY}.");

            await GenerateZoomAsync(sourcePath, outputDir, zoom, range, parallelism, gdal);
        }

        log.AppendLine($"XYZ tile generation complete. Tiles attempted: {totalTiles}.");
    }

    private static async Task GenerateZoomAsync(
        string sourcePath,
        string outputDir,
        int zoom,
        TileRange range,
        int parallelism,
        GdalToolRunner gdal)
    {
        var tasks = new List<Task>();
        using var throttle = new SemaphoreSlim(parallelism);

        for (var x = range.MinX; x <= range.MaxX; x++)
        {
            var xDir = Path.Combine(outputDir, zoom.ToString(CultureInfo.InvariantCulture), x.ToString(CultureInfo.InvariantCulture));
            Directory.CreateDirectory(xDir);

            for (var y = range.MinY; y <= range.MaxY; y++)
            {
                await throttle.WaitAsync();
                var tileX = x;
                var tileY = y;
                var outPath = Path.Combine(xDir, $"{tileY.ToString(CultureInfo.InvariantCulture)}.png");

                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        await GenerateTileAsync(sourcePath, outPath, zoom, tileX, tileY, gdal);
                    }
                    finally
                    {
                        throttle.Release();
                    }
                }));

                if (tasks.Count >= parallelism * 4)
                {
                    var completed = await Task.WhenAny(tasks);
                    tasks.Remove(completed);
                    await completed;
                }
            }
        }

        await Task.WhenAll(tasks);
    }

    private static Task GenerateTileAsync(string sourcePath, string outPath, int zoom, int x, int y, GdalToolRunner gdal)
    {
        var bounds = TileBounds.For(zoom, x, y);
        var args = new[]
        {
            "-q",
            "-overwrite",
            "-t_srs", "EPSG:3857",
            "-te",
            bounds.MinX.ToString("G17", CultureInfo.InvariantCulture),
            bounds.MinY.ToString("G17", CultureInfo.InvariantCulture),
            bounds.MaxX.ToString("G17", CultureInfo.InvariantCulture),
            bounds.MaxY.ToString("G17", CultureInfo.InvariantCulture),
            "-ts", TileSize.ToString(CultureInfo.InvariantCulture), TileSize.ToString(CultureInfo.InvariantCulture),
            "-r", "bilinear",
            "-dstalpha",
            "-of", "PNG",
            sourcePath,
            outPath
        };

        return gdal.RunCheckedAsync("gdalwarp", args, Path.GetDirectoryName(outPath) ?? Environment.CurrentDirectory, null);
    }

    private readonly record struct TileBounds(double MinX, double MinY, double MaxX, double MaxY)
    {
        public static TileBounds For(int zoom, int x, int y)
        {
            var resolution = 2 * OriginShift / (TileSize * Math.Pow(2, zoom));
            var minX = x * TileSize * resolution - OriginShift;
            var maxX = (x + 1) * TileSize * resolution - OriginShift;
            var maxY = OriginShift - y * TileSize * resolution;
            var minY = OriginShift - (y + 1) * TileSize * resolution;
            return new TileBounds(minX, minY, maxX, maxY);
        }
    }

    private readonly record struct TileRange(int MinX, int MaxX, int MinY, int MaxY)
    {
        public bool IsEmpty => MaxX < MinX || MaxY < MinY;

        public static TileRange FromBounds(double minX, double minY, double maxX, double maxY, int zoom)
        {
            var n = 1 << zoom;
            var worldSpan = 2 * OriginShift;
            const double epsilon = 0.000000001;

            var tileMinX = Clamp((int)Math.Floor((minX + OriginShift) / worldSpan * n), 0, n - 1);
            var tileMaxX = Clamp((int)Math.Floor(((maxX + OriginShift) / worldSpan * n) - epsilon), 0, n - 1);
            var tileMinY = Clamp((int)Math.Floor((OriginShift - maxY) / worldSpan * n), 0, n - 1);
            var tileMaxY = Clamp((int)Math.Floor(((OriginShift - minY) / worldSpan * n) - epsilon), 0, n - 1);

            return new TileRange(tileMinX, tileMaxX, tileMinY, tileMaxY);
        }

        private static int Clamp(int value, int min, int max)
        {
            if (value < min)
            {
                return min;
            }

            if (value > max)
            {
                return max;
            }

            return value;
        }
    }
}

public sealed class GdalToolRunner
{
    private readonly string _binPath;
    private readonly string? _gdalDataPath;

    public GdalToolRunner(string osgeoRoot)
    {
        if (string.IsNullOrWhiteSpace(osgeoRoot))
        {
            throw new InvalidOperationException("OsgeoRoot is not configured.");
        }

        var root = Path.GetFullPath(osgeoRoot);
        _binPath = Path.Combine(root, "bin");
        if (!Directory.Exists(_binPath))
        {
            throw new DirectoryNotFoundException($"GDAL bin not found: {_binPath}.");
        }

        _gdalDataPath = ResolveGdalDataPath(root);
    }

    public async Task<CommandResult> RunAsync(string toolName, IEnumerable<string> arguments, string workingDirectory)
    {
        var exeName = toolName.EndsWith(".exe", StringComparison.OrdinalIgnoreCase) ? toolName : $"{toolName}.exe";
        var exePath = Path.Combine(_binPath, exeName);
        if (!File.Exists(exePath))
        {
            throw new FileNotFoundException($"GDAL executable not found: {exePath}", exePath);
        }

        var psi = new ProcessStartInfo
        {
            FileName = exePath,
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true
        };

        foreach (var arg in arguments)
        {
            psi.ArgumentList.Add(arg);
        }

        psi.Environment.TryGetValue("PATH", out var currentPath);
        psi.Environment["PATH"] = string.IsNullOrWhiteSpace(currentPath)
            ? _binPath
            : $"{_binPath};{currentPath}";
        if (!string.IsNullOrWhiteSpace(_gdalDataPath))
        {
            psi.Environment["GDAL_DATA"] = _gdalDataPath;
        }

        using var process = new Process { StartInfo = psi };
        var start = DateTime.UtcNow;
        process.Start();

        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();
        await Task.WhenAll(stdoutTask, stderrTask, process.WaitForExitAsync());

        return new CommandResult(
            exePath,
            psi.ArgumentList.ToArray(),
            process.ExitCode,
            stdoutTask.Result,
            stderrTask.Result,
            DateTime.UtcNow - start);
    }

    public async Task<CommandResult> RunCheckedAsync(
        string toolName,
        IEnumerable<string> arguments,
        string workingDirectory,
        StringBuilder? log)
    {
        var result = await RunAsync(toolName, arguments, workingDirectory);
        if (log is not null)
        {
            log.AppendLine($"{Path.GetFileName(result.FileName)} completed in {result.Duration} with ExitCode={result.ExitCode}.");
        }

        if (result.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"{Path.GetFileName(result.FileName)} failed with ExitCode={result.ExitCode}.\nSTDOUT:\n{Truncate(result.Stdout)}\nSTDERR:\n{Truncate(result.Stderr)}");
        }

        return result;
    }

    private static string? ResolveGdalDataPath(string osgeoRoot)
    {
        var candidates = new[]
        {
            Path.Combine(osgeoRoot, "share", "gdal"),
            Path.Combine(osgeoRoot, "apps", "gdal", "share", "gdal")
        };

        return candidates.FirstOrDefault(Directory.Exists);
    }

    private static string Truncate(string value, int maxLength = 8000)
    {
        return value.Length <= maxLength ? value : value.Substring(0, maxLength);
    }
}

public sealed record CommandResult(
    string FileName,
    IReadOnlyList<string> Arguments,
    int ExitCode,
    string Stdout,
    string Stderr,
    TimeSpan Duration);

public sealed record RasterInfo(
    int Width,
    int Height,
    double MinX,
    double MinY,
    double MaxX,
    double MaxY,
    double[] GeoTransform,
    string Wkt)
{
    public static async Task<RasterInfo> ReadAsync(GdalToolRunner gdal, string sourcePath, string workingDirectory)
    {
        var result = await gdal.RunCheckedAsync("gdalinfo", new[] { "-json", sourcePath }, workingDirectory, null);
        using var doc = JsonDocument.Parse(result.Stdout);
        var root = doc.RootElement;

        var size = root.GetProperty("size");
        var width = size[0].GetInt32();
        var height = size[1].GetInt32();

        var geoTransform = ReadGeoTransform(root, width, height);
        var bounds = ReadBounds(root, width, height, geoTransform);
        var wkt = "EPSG:3857";
        if (root.TryGetProperty("coordinateSystem", out var coordinateSystem)
            && coordinateSystem.TryGetProperty("wkt", out var wktElement)
            && wktElement.ValueKind == JsonValueKind.String)
        {
            wkt = wktElement.GetString() ?? wkt;
        }

        return new RasterInfo(width, height, bounds.MinX, bounds.MinY, bounds.MaxX, bounds.MaxY, geoTransform, wkt);
    }

    private static double[] ReadGeoTransform(JsonElement root, int width, int height)
    {
        if (root.TryGetProperty("geoTransform", out var geoTransformElement)
            && geoTransformElement.ValueKind == JsonValueKind.Array
            && geoTransformElement.GetArrayLength() >= 6)
        {
            return geoTransformElement.EnumerateArray().Take(6).Select(e => e.GetDouble()).ToArray();
        }

        if (root.TryGetProperty("cornerCoordinates", out var corners)
            && corners.TryGetProperty("upperLeft", out var upperLeft)
            && corners.TryGetProperty("lowerRight", out var lowerRight))
        {
            var minX = upperLeft[0].GetDouble();
            var maxY = upperLeft[1].GetDouble();
            var maxX = lowerRight[0].GetDouble();
            var minY = lowerRight[1].GetDouble();
            return new[]
            {
                minX,
                (maxX - minX) / width,
                0,
                maxY,
                0,
                (minY - maxY) / height
            };
        }

        throw new InvalidOperationException("GDAL raster metadata does not include geoTransform or cornerCoordinates.");
    }

    private static (double MinX, double MinY, double MaxX, double MaxY) ReadBounds(
        JsonElement root,
        int width,
        int height,
        double[] geoTransform)
    {
        if (root.TryGetProperty("cornerCoordinates", out var corners)
            && corners.TryGetProperty("upperLeft", out var upperLeft)
            && corners.TryGetProperty("lowerRight", out var lowerRight))
        {
            var x1 = upperLeft[0].GetDouble();
            var y1 = upperLeft[1].GetDouble();
            var x2 = lowerRight[0].GetDouble();
            var y2 = lowerRight[1].GetDouble();
            return (Math.Min(x1, x2), Math.Min(y1, y2), Math.Max(x1, x2), Math.Max(y1, y2));
        }

        var originX = geoTransform[0];
        var pixelWidth = geoTransform[1];
        var rotationX = geoTransform[2];
        var originY = geoTransform[3];
        var rotationY = geoTransform[4];
        var pixelHeight = geoTransform[5];

        var xA = originX;
        var yA = originY;
        var xB = originX + width * pixelWidth + height * rotationX;
        var yB = originY + width * rotationY + height * pixelHeight;

        return (Math.Min(xA, xB), Math.Min(yA, yB), Math.Max(xA, xB), Math.Max(yA, yB));
    }
}
