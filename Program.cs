using System.Globalization;
using System.Net;

var projectRoot = FindProjectRoot(Environment.CurrentDirectory)
    ?? FindProjectRoot(AppContext.BaseDirectory)
    ?? Environment.CurrentDirectory;

var config = AppConfig.Load(projectRoot);
var mode = ResolveMode(args, config);

Console.WriteLine($"SentinelGrab starting in '{mode}' mode.");

if (string.Equals(mode, "db", StringComparison.OrdinalIgnoreCase))
{
    await RunDbModeAsync(config, projectRoot);
}
else
{
    await RunCliModeAsync(config, projectRoot, args);
}

static async Task RunCliModeAsync(AppConfig config, string projectRoot, string[] args)
{
    var bboxText = GetArgValue(args, "--bbox") ?? config.Cli.Bbox ?? "-103.86731513843112,50.5123611,-102.9133333,50.99259736981789";
    var bbox = ParseBbox(bboxText);

    var year = GetArgValue(args, "--year") is { } y && int.TryParse(y, out var yv) ? yv : config.Cli.Year;
    var month = GetArgValue(args, "--month") is { } m && int.TryParse(m, out var mv) ? mv : config.Cli.Month;
    var day = GetArgValue(args, "--day") is { } d && int.TryParse(d, out var dv) ? dv : (int?)null;
    var cloudMax = GetArgValue(args, "--cloud") is { } c && int.TryParse(c, out var cv) ? cv : config.Cli.CloudCoverMax;

    DateTime rangeStart;
    DateTime rangeEnd;
    string outputFolder;
    if (day.HasValue)
    {
        var date = new DateTime(year, month, day.Value);
        rangeStart = date.Date;
        rangeEnd = date.Date;
        outputFolder = $"{year:D4}-{month:D2}-{day.Value:D2}";
    }
    else
    {
        var monthStart = new DateTime(year, month, 1);
        rangeStart = monthStart;
        rangeEnd = monthStart.AddMonths(1).AddDays(-1);
        outputFolder = $"{year:D4}-{month:D2}";
    }

    var workRoot = ResolveRootPath(config.WorkRoot ?? "data", projectRoot);
    var outDir = Path.Combine(workRoot, outputFolder);
    Directory.CreateDirectory(outDir);

    using var http = CreateHttpClient();
    var stac = new StacClient(http);
    var items = await stac.SearchAsync(bbox, rangeStart, rangeEnd, cloudMax, 100);

    if (items.Count == 0)
    {
        Console.WriteLine(day.HasValue ? "No scenes found for that date/window." : "No scenes found for that month/window.");
        return;
    }

    var best = items.OrderBy(i => i.CloudCover ?? double.MaxValue).First();
    Console.WriteLine($"Best scene: {best.Id} cloud={(best.CloudCover ?? 0):0.0}%");

    await File.WriteAllTextAsync(Path.Combine(outDir, "item.json"), best.RawJson);

    var bands = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "B02", "B03", "B04", "SCL" };
    var downloader = new BandDownloader(http, stac);
    await downloader.DownloadBandsAsync(best, bands, outDir);

    if (string.IsNullOrWhiteSpace(config.OutputRootPath))
    {
        Console.WriteLine("OutputRootPath is not configured; skipping tile generation.");
        return;
    }

    if (string.IsNullOrWhiteSpace(config.OsgeoRoot))
    {
        Console.WriteLine("OsgeoRoot is not configured; skipping tile generation.");
        return;
    }

    var outputRootPath = ResolveRootPath(config.OutputRootPath, projectRoot);
    var scriptPath = Path.Combine(projectRoot, "scripts", "BuildTiles_RGB.ps1");
    if (!File.Exists(scriptPath))
    {
        Console.WriteLine($"Script not found: {scriptPath}. Skipping tile generation.");
        return;
    }

    var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
    {
        ["JobId"] = "0",
        ["DateKey"] = outputFolder,
        ["InputDir"] = outDir,
        ["OutputRootPath"] = outputRootPath,
        ["OsgeoRoot"] = config.OsgeoRoot,
        ["ScaleMaxRGB"] = config.DefaultScaleMaxRGB.ToString(CultureInfo.InvariantCulture),
        ["Processes"] = Math.Max(1, config.DefaultProcesses).ToString(CultureInfo.InvariantCulture)
    };

    var scriptRunner = new PowerShellScriptRunner();
    var result = await scriptRunner.RunAsync(scriptPath, parameters);
    var status = result.ExitCode == 0 ? "Tile generation completed." : "Tile generation failed.";
    Console.WriteLine($"{status} ExitCode={result.ExitCode}; Duration={result.Duration}.");
    if (!string.IsNullOrWhiteSpace(result.Stdout))
    {
        Console.WriteLine(Truncate(result.Stdout));
    }

    if (!string.IsNullOrWhiteSpace(result.Stderr))
    {
        Console.WriteLine(Truncate(result.Stderr));
    }
}

static async Task RunDbModeAsync(AppConfig config, string projectRoot)
{
    if (string.IsNullOrWhiteSpace(config.SqlConnectionString))
    {
        Console.WriteLine("SqlConnectionString is not configured.");
        return;
    }

    var repo = new JobRepository(config.SqlConnectionString);
    var job = await repo.ClaimNextJobAsync();

    if (job is null)
    {
        Console.WriteLine("No queued jobs found.");
        return;
    }

    Console.WriteLine($"Claimed JobId {job.JobId} (Status={job.Status}).");

    try
    {
        await ProcessJobAsync(job, repo, config, projectRoot);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"Job {job.JobId} failed: {ex.Message}");
        await repo.MarkJobFailedAsync(job.JobId);
    }
}

static async Task ProcessJobAsync(SentinelGrabJob job, JobRepository repo, AppConfig config, string projectRoot)
{
    var products = await repo.GetPendingProductsAsync(job.JobId);
    if (products.Count == 0)
    {
        Console.WriteLine("No queued/failed products found; inserting default RGB product.");
        products.Add(await repo.InsertDefaultProductAsync(job.JobId));
    }

    var (dateFrom, dateTo, dateKey) = ResolveDateRange(job);
    var bbox = ResolveBbox(job);

    var cloudMax = job.CloudCoverMax ?? 100;
    var maxScenes = Math.Max(1, job.MaxScenes ?? 1);
    var wantMultiple = job.PreferMosaic || maxScenes > 1;
    if (job.PreferMosaic && maxScenes < 2)
    {
        Console.WriteLine("PreferMosaic=1 but MaxScenes < 2; only one scene will be downloaded.");
    }

    var bandSet = ComputeRequiredBands(products);
    bandSet.Add("SCL");

    using var http = CreateHttpClient();
    var stac = new StacClient(http);
    List<StacItem> items;
    var sceneId = job.SceneId?.Trim();
    if (!string.IsNullOrWhiteSpace(sceneId))
    {
        Console.WriteLine($"Using SceneId {sceneId}.");
        var item = await stac.GetByIdAsync(sceneId);
        if (item is null)
        {
            Console.WriteLine($"No scene found for SceneId {sceneId}.");
            await repo.MarkJobFailedAsync(job.JobId);
            return;
        }

        items = new List<StacItem> { item };
        wantMultiple = false;
        maxScenes = 1;
    }
    else
    {
        items = await stac.SearchAsync(bbox, dateFrom, dateTo, cloudMax, 100);
        if (items.Count == 0)
        {
            Console.WriteLine("No scenes found for job.");
            await repo.MarkJobFailedAsync(job.JobId);
            return;
        }
    }

    var selected = items
        .OrderBy(i => i.CloudCover ?? double.MaxValue)
        .Take(wantMultiple ? maxScenes : 1)
        .ToList();

    Console.WriteLine($"Selected {selected.Count} scene(s). Bands: {string.Join(", ", bandSet)}");

    var workRoot = ResolveRootPath(config.WorkRoot ?? "data", projectRoot);
    var jobRoot = Path.Combine(workRoot, job.JobId.ToString(CultureInfo.InvariantCulture));
    Directory.CreateDirectory(jobRoot);

    var downloader = new BandDownloader(http, stac);
    foreach (var item in selected)
    {
        var sceneFolder = selected.Count == 1 && !wantMultiple
            ? Path.Combine(jobRoot, "single")
            : Path.Combine(jobRoot, SanitizePathSegment(item.Id));

        Directory.CreateDirectory(sceneFolder);
        await File.WriteAllTextAsync(Path.Combine(sceneFolder, "item.json"), item.RawJson);
        await downloader.DownloadBandsAsync(item, bandSet, sceneFolder);
    }

    var outputRoot = job.OutputRootPath ?? config.OutputRootPath;
    if (string.IsNullOrWhiteSpace(outputRoot))
    {
        throw new InvalidOperationException("OutputRootPath is not configured on job or appsettings.");
    }

    if (string.IsNullOrWhiteSpace(config.OsgeoRoot))
    {
        throw new InvalidOperationException("OsgeoRoot is not configured.");
    }

    var outputRootPath = ResolveRootPath(outputRoot, projectRoot);
    var scriptRunner = new PowerShellScriptRunner();

    foreach (var product in products)
    {
        var productCode = product.ProductCode.Trim().ToUpperInvariant();
        var productSubPath = string.IsNullOrWhiteSpace(product.OutputSubPath)
            ? product.ProductCode.Trim().ToLowerInvariant()
            : product.OutputSubPath.Trim();

        await repo.UpdateJobProductStatusAsync(product.JobProductId, "Running", null, "Starting script.");

        var scriptPath = Path.Combine(projectRoot, "scripts", $"BuildTiles_{productCode}.ps1");
        if (!File.Exists(scriptPath))
        {
            await repo.UpdateJobProductStatusAsync(product.JobProductId, "Failed", "Script not found.", scriptPath);
            continue;
        }

        var zoomMin = job.ZoomMin ?? 8;
        var zoomMax = job.ZoomMax ?? 14;
        var processes = Math.Max(1, config.DefaultProcesses);

        var parameters = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["JobId"] = job.JobId.ToString(CultureInfo.InvariantCulture),
            ["DateKey"] = dateKey,
            ["InputDir"] = jobRoot,
            ["OutputRootPath"] = outputRootPath,
            ["ProductSubPath"] = productSubPath,
            ["ZoomMin"] = zoomMin.ToString(CultureInfo.InvariantCulture),
            ["ZoomMax"] = zoomMax.ToString(CultureInfo.InvariantCulture),
            ["OsgeoRoot"] = config.OsgeoRoot,
            ["Processes"] = processes.ToString(CultureInfo.InvariantCulture)
        };

        if (productCode == "RGB")
        {
            parameters["ScaleMaxRGB"] = config.DefaultScaleMaxRGB.ToString(CultureInfo.InvariantCulture);
        }
        else
        {
            parameters["IndexMin"] = config.DefaultNdviMin.ToString(CultureInfo.InvariantCulture);
            parameters["IndexMax"] = config.DefaultNdviMax.ToString(CultureInfo.InvariantCulture);
        }

        try
        {
            var result = await scriptRunner.RunAsync(scriptPath, parameters);
            var combinedLog = $"ExitCode={result.ExitCode}; Duration={result.Duration};\nSTDOUT:\n{result.Stdout}\nSTDERR:\n{result.Stderr}";

            if (result.ExitCode == 0)
            {
                var outputDir = Path.Combine(outputRootPath, productSubPath, dateKey);
                var log = $"Tiles created at: {outputDir}. Template: {outputDir}\\{{z}}\\{{x}}\\{{y}}.png\n{combinedLog}";
                await repo.UpdateJobProductStatusAsync(product.JobProductId, "Succeeded", null, Truncate(log));

                var available = new AvailableLayer
                {
                    JobId = job.JobId,
                    JobProductId = product.JobProductId,
                    ProductCode = productCode,
                    DateKey = dateKey,
                    DateFrom = dateFrom.Date,
                    DateTo = dateTo.Date,
                    BboxMinLon = bbox.MinLon,
                    BboxMinLat = bbox.MinLat,
                    BboxMaxLon = bbox.MaxLon,
                    BboxMaxLat = bbox.MaxLat,
                    OutputRootPath = outputRootPath,
                    ProductSubPath = productSubPath,
                    OutputDir = outputDir
                };

                await repo.UpsertAvailableLayerAsync(available);
            }
            else
            {
                await repo.UpdateJobProductStatusAsync(product.JobProductId, "Failed", Truncate(result.Stderr), Truncate(combinedLog));
            }
        }
        catch (Exception ex)
        {
            await repo.UpdateJobProductStatusAsync(product.JobProductId, "Failed", Truncate(ex.Message), Truncate(ex.ToString()));
        }
    }

    var counts = await repo.GetJobProductStatusCountsAsync(job.JobId);
    if (counts.Total == 0)
    {
        await repo.MarkJobFailedAsync(job.JobId);
        Console.WriteLine("No products found to evaluate job status.");
        return;
    }

    if (counts.Failed == 0 && counts.Succeeded == counts.Total)
    {
        await repo.MarkJobSucceededAsync(job.JobId);
        Console.WriteLine($"Job {job.JobId} succeeded.");
    }
    else
    {
        await repo.MarkJobFailedAsync(job.JobId);
        Console.WriteLine($"Job {job.JobId} failed. Succeeded={counts.Succeeded}, Failed={counts.Failed}, Total={counts.Total}.");
    }
}

static HashSet<string> ComputeRequiredBands(IEnumerable<SentinelGrabJobProduct> products)
{
    var bands = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

    foreach (var product in products)
    {
        var code = product.ProductCode.Trim().ToUpperInvariant();
        switch (code)
        {
            case "RGB":
                bands.Add("B02");
                bands.Add("B03");
                bands.Add("B04");
                break;
            case "NDVI":
                bands.Add("B08");
                bands.Add("B04");
                break;
            case "NDMI":
                bands.Add("B08");
                bands.Add("B11");
                break;
            case "NDRE":
                bands.Add("B8A");
                bands.Add("B05");
                break;
        }
    }

    return bands;
}

static (DateTime From, DateTime To, string DateKey) ResolveDateRange(SentinelGrabJob job)
{
    if (job.DateFrom.HasValue && job.DateTo.HasValue)
    {
        var key = job.DateKey ?? job.DateFrom.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        return (job.DateFrom.Value.Date, job.DateTo.Value.Date, key);
    }

    if (!string.IsNullOrWhiteSpace(job.DateKey))
    {
        var dateKey = job.DateKey.Trim();
        if (DateTime.TryParseExact(dateKey, "yyyy-MM", CultureInfo.InvariantCulture, DateTimeStyles.None, out var month))
        {
            var start = new DateTime(month.Year, month.Month, 1);
            var end = start.AddMonths(1).AddDays(-1);
            return (start, end, dateKey);
        }

        if (DateTime.TryParseExact(dateKey, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var day))
        {
            return (day.Date, day.Date, dateKey);
        }

        if (DateTime.TryParseExact(dateKey, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var compact))
        {
            var dk = compact.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            return (compact.Date, compact.Date, dk);
        }
    }

    throw new InvalidOperationException("Job DateFrom/DateTo or DateKey is required.");
}

static Bbox ResolveBbox(SentinelGrabJob job)
{
    if (job.BboxMinLon.HasValue && job.BboxMinLat.HasValue && job.BboxMaxLon.HasValue && job.BboxMaxLat.HasValue)
    {
        return new Bbox(job.BboxMinLon.Value, job.BboxMinLat.Value, job.BboxMaxLon.Value, job.BboxMaxLat.Value);
    }

    if (!string.IsNullOrWhiteSpace(job.Bbox))
    {
        return ParseBbox(job.Bbox);
    }

    throw new InvalidOperationException("Job bbox is required (BboxMinLon/Lat/MaxLon/MaxLat or Bbox string).");
}

static Bbox ParseBbox(string bboxText)
{
    var parts = bboxText.Split(new[] { ',', ' ' }, StringSplitOptions.RemoveEmptyEntries);
    if (parts.Length != 4)
    {
        throw new FormatException("Bbox must have 4 numbers: minLon,minLat,maxLon,maxLat.");
    }

    return new Bbox(
        double.Parse(parts[0], CultureInfo.InvariantCulture),
        double.Parse(parts[1], CultureInfo.InvariantCulture),
        double.Parse(parts[2], CultureInfo.InvariantCulture),
        double.Parse(parts[3], CultureInfo.InvariantCulture)
    );
}

static string ResolveMode(string[] args, AppConfig config)
{
    if (args.Any(a => string.Equals(a, "--db", StringComparison.OrdinalIgnoreCase)))
    {
        return "db";
    }

    var modeArg = GetArgValue(args, "--mode");
    if (!string.IsNullOrWhiteSpace(modeArg))
    {
        return modeArg.Trim();
    }

    return string.IsNullOrWhiteSpace(config.Mode) ? "cli" : config.Mode;
}

static string? GetArgValue(string[] args, string name)
{
    for (var i = 0; i < args.Length; i++)
    {
        if (string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length)
        {
            return args[i + 1];
        }
    }

    return null;
}

static HttpClient CreateHttpClient()
{
    var http = new HttpClient(new HttpClientHandler
    {
        AutomaticDecompression = DecompressionMethods.All
    })
    {
        Timeout = TimeSpan.FromMinutes(10)
    };

    return http;
}

static string ResolveRootPath(string path, string projectRoot)
{
    if (Path.IsPathRooted(path))
    {
        return Path.GetFullPath(path);
    }

    return Path.GetFullPath(Path.Combine(projectRoot, path));
}

static string SanitizePathSegment(string value)
{
    var invalid = Path.GetInvalidFileNameChars();
    var buffer = value.ToCharArray();
    for (var i = 0; i < buffer.Length; i++)
    {
        if (invalid.Contains(buffer[i]))
        {
            buffer[i] = '_';
        }
    }

    return new string(buffer);
}

static string Truncate(string? value, int maxLen = 20000)
{
    if (string.IsNullOrEmpty(value))
    {
        return string.Empty;
    }

    return value.Length <= maxLen ? value : value.Substring(0, maxLen);
}

static string? FindProjectRoot(string startDir)
{
    var dir = new DirectoryInfo(startDir);
    while (dir is not null)
    {
        if (dir.EnumerateFiles("*.csproj", SearchOption.TopDirectoryOnly).Any())
        {
            return dir.FullName;
        }
        dir = dir.Parent;
    }

    return null;
}
