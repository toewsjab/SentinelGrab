using System.Globalization;
using System.Net;

var projectRoot = FindProjectRoot(Environment.CurrentDirectory)
    ?? FindProjectRoot(AppContext.BaseDirectory)
    ?? Environment.CurrentDirectory;

var config = AppConfig.Load(projectRoot);

if (PipelineWaterExportCli.IsExportCommand(args))
{
    await PipelineWaterExportCli.RunAsync(config, projectRoot, args);
    return;
}

if (GetArgValue(args, "--import-pipeline") is { } pipelineImportPath)
{
    await RunPipelineImportAsync(config, projectRoot, args, pipelineImportPath);
    return;
}

var mode = ResolveMode(args, config);

Console.WriteLine($"SentinelGrab starting in '{mode}' mode.");

if (string.Equals(mode, "db", StringComparison.OrdinalIgnoreCase))
{
    await RunDbModeAsync(config, projectRoot);
}
else if (string.Equals(mode, "daily", StringComparison.OrdinalIgnoreCase))
{
    await RunDailyModeAsync(config, projectRoot, args);
}
else if (string.Equals(mode, "range", StringComparison.OrdinalIgnoreCase))
{
    await RunRangeModeAsync(config, projectRoot, args);
}
else
{
    await RunCliModeAsync(config, projectRoot, args);
}

static async Task RunPipelineImportAsync(AppConfig config, string projectRoot, string[] args, string pipelineImportPath)
{
    if (string.IsNullOrWhiteSpace(config.SqlConnectionString))
    {
        throw new InvalidOperationException("SqlConnectionString is required for --import-pipeline.");
    }

    var sourcePath = ResolveRootPath(pipelineImportPath, projectRoot);
    var sourceText = await File.ReadAllTextAsync(sourcePath);
    var chainageOriginM = GetDecimalArg(args, "--chainage-origin-m") ?? 0m;
    var importRequest = new PipelinePathImportRequest
    {
        SourceText = sourceText,
        PathName = GetArgValue(args, "--pipeline-name") ?? Path.GetFileNameWithoutExtension(sourcePath),
        DirectionDescription = GetArgValue(args, "--direction-description"),
        SourceReference = GetArgValue(args, "--source-reference") ?? sourcePath,
        ChainageOriginM = chainageOriginM,
        EndpointToleranceM = GetDoubleArg(args, "--endpoint-tolerance-m") ?? 0.5d,
        DensifyMaxSegmentLengthM = GetDoubleArg(args, "--densify-max-segment-m") ?? 100d,
        MaxProjectedSectionLengthM = GetDoubleArg(args, "--max-section-length-m") ?? 25000d
    };

    var importer = new PipelinePathImporter();
    var result = importer.Import(importRequest);
    var repo = new JobRepository(config.SqlConnectionString);
    var existing = await repo.GetPipelinePathBySourceHashAsync(result.Path.SourceHash);
    var path = existing ?? await repo.InsertPipelinePathAsync(result.Path);

    Console.WriteLine(existing is null
        ? $"Imported pipeline route {path.PipelinePathId}."
        : $"Pipeline route already exists as {path.PipelinePathId}; import matched SourceHash.");
    Console.WriteLine($"Route ID: {path.PipelinePathId}");
    Console.WriteLine($"Total length m: {result.Path.RouteLengthM:0.###}");
    Console.WriteLine($"Start: {result.StartLongitude.ToString("G17", CultureInfo.InvariantCulture)},{result.StartLatitude.ToString("G17", CultureInfo.InvariantCulture)}");
    Console.WriteLine($"End: {result.EndLongitude.ToString("G17", CultureInfo.InvariantCulture)},{result.EndLatitude.ToString("G17", CultureInfo.InvariantCulture)}");
    Console.WriteLine($"Section count: {result.Sections.Count}");
    Console.WriteLine($"Crossed UTM zones: {string.Join(", ", result.CrossedUtmZones)}");
    Console.WriteLine($"Source hash: {result.Path.SourceHash}");

    if (result.EndpointGaps.Count == 0)
    {
        Console.WriteLine("Endpoint gaps: none");
    }
    else
    {
        Console.WriteLine("Endpoint gaps:");
        foreach (var gap in result.EndpointGaps)
        {
            Console.WriteLine(
                $"  component {gap.FromComponentIndex} -> {gap.ToComponentIndex}: {gap.GapMetres.ToString("0.###", CultureInfo.InvariantCulture)} m");
        }
    }
}

static async Task RunDailyModeAsync(AppConfig config, string projectRoot, string[] args)
{
    ValidateAutomaticModeConfig(config, "daily");
    var bbox = ResolveConfiguredBbox(config, args);
    var lookbackDays = GetIntArg(args, "--lookback") ?? config.DailyCheck.LookbackDays;
    var lagDays = GetIntArg(args, "--lag") ?? config.DailyCheck.LagDays;
    if (lookbackDays <= 0)
    {
        throw new InvalidOperationException("DailyCheck LookbackDays must be greater than zero.");
    }

    if (lagDays < 0)
    {
        throw new InvalidOperationException("DailyCheck LagDays cannot be negative.");
    }

    var productCodes = ResolveProductCodes(GetArgValue(args, "--products"), config.DailyCheck.ProductCodes);
    if (productCodes.Count == 0)
    {
        throw new InvalidOperationException("Daily mode needs at least one product code.");
    }

    var rangeEnd = DateTime.Today.AddDays(-lagDays);
    var rangeStart = rangeEnd.AddDays(1 - lookbackDays);

    await RunAutomaticImageryWindowAsync(
        config,
        projectRoot,
        args,
        bbox,
        rangeStart,
        rangeEnd,
        productCodes,
        "daily");
}

static async Task RunRangeModeAsync(AppConfig config, string projectRoot, string[] args)
{
    ValidateAutomaticModeConfig(config, "range");
    var bbox = ResolveConfiguredBbox(config, args);
    var productCodes = ResolveProductCodes(GetArgValue(args, "--products"), config.DailyCheck.ProductCodes);
    if (productCodes.Count == 0)
    {
        throw new InvalidOperationException("Range mode needs at least one product code.");
    }

    var fromText = GetArgValue(args, "--from") ?? GetArgValue(args, "--start");
    var toText = GetArgValue(args, "--to") ?? GetArgValue(args, "--end");
    if (!TryParseDate(fromText, out var fromDate) || !TryParseDate(toText, out var toDate))
    {
        throw new InvalidOperationException("Range mode requires --from yyyy-MM-dd and --to yyyy-MM-dd.");
    }

    if (toDate < fromDate)
    {
        throw new InvalidOperationException("--to must be greater than or equal to --from.");
    }

    Console.WriteLine(
        $"Range check processing {fromDate:yyyy-MM-dd} through {toDate:yyyy-MM-dd}; " +
        $"products={string.Join(", ", productCodes)}.");

    for (var date = fromDate; date <= toDate; date = date.AddDays(1))
    {
        await RunAutomaticImageryWindowAsync(
            config,
            projectRoot,
            args,
            bbox,
            date,
            date,
            productCodes,
            "range");
    }
}

static async Task RunAutomaticImageryWindowAsync(
    AppConfig config,
    string projectRoot,
    string[] args,
    Bbox bbox,
    DateTime rangeStart,
    DateTime rangeEnd,
    IReadOnlyList<string> productCodes,
    string modeName)
{
    var osgeoRoot = config.OsgeoRoot ?? throw new InvalidOperationException($"OsgeoRoot is required for {modeName} mode.");
    var sqlConnectionString = config.SqlConnectionString ?? throw new InvalidOperationException($"SqlConnectionString is required for {modeName} mode.");
    var outputRoot = config.OutputRootPath ?? throw new InvalidOperationException($"OutputRootPath is required for {modeName} mode.");
    var cloudMax = config.FarmCloudScreening.Enabled
        ? 100
        : GetIntArg(args, "--cloud") ?? config.Cli.CloudCoverMax;

    Console.WriteLine($"Searching {rangeStart:yyyy-MM-dd} through {rangeEnd:yyyy-MM-dd}.");

    using var http = CreateHttpClient();
    var stac = CreateStacClient(http, config);
    List<StacItem> items;
    try
    {
        items = await stac.SearchAsync(bbox, rangeStart, rangeEnd, cloudMax, 100);
    }
    catch (HttpRequestException ex)
    {
        Console.WriteLine(
            $"STAC search failed for {rangeStart:yyyy-MM-dd} through {rangeEnd:yyyy-MM-dd}; skipping this window. {ex.Message}");
        return;
    }
    catch (TaskCanceledException ex)
    {
        Console.WriteLine(
            $"STAC search timed out for {rangeStart:yyyy-MM-dd} through {rangeEnd:yyyy-MM-dd}; skipping this window. {ex.Message}");
        return;
    }

    if (items.Count == 0)
    {
        Console.WriteLine($"No Sentinel scenes found for {rangeStart:yyyy-MM-dd} through {rangeEnd:yyyy-MM-dd}.");
        return;
    }

    var workRoot = ResolveRootPath(config.WorkRoot ?? "data", projectRoot);
    var runRoot = Path.Combine(
        workRoot,
        modeName,
        $"{rangeStart:yyyy-MM-dd}_{rangeEnd:yyyy-MM-dd}");
    Directory.CreateDirectory(runRoot);

    var downloader = new BandDownloader(http, stac);
    FarmSceneCandidate selectedCandidate;
    if (config.FarmCloudScreening.Enabled)
    {
        var selector = new FarmSceneSelector(downloader, new GdalToolRunner(osgeoRoot));
        try
        {
            var selection = await selector.SelectAsync(
                items,
                bbox,
                Path.Combine(runRoot, "screening"),
                config.FarmCloudScreening);
            selectedCandidate = selection.Selected;
        }
        catch (InvalidOperationException ex)
        {
            Console.WriteLine(
                $"No acceptable Sentinel acquisition for {rangeStart:yyyy-MM-dd} through {rangeEnd:yyyy-MM-dd}: {ex.Message}");
            return;
        }
    }
    else
    {
        var selectedItem = items.OrderBy(item => item.CloudCover ?? double.MaxValue).First();
        selectedCandidate = new FarmSceneCandidate(
            selectedItem.AcquisitionKey,
            selectedItem.AcquiredAt,
            new[] { selectedItem },
            new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase),
            runRoot,
            new FarmCloudScore(0, 0, 0, 0, 0, 0, 0, 0, 0, selectedItem.CloudCover ?? 0, 0, selectedItem.CloudCover ?? 0, 0, 0, 0));
    }

    var selectedDate = selectedCandidate.AcquiredAt?.UtcDateTime.Date ?? rangeEnd.Date;
    var dateKey = selectedDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    var repo = new JobRepository(sqlConnectionString);
    var availableProducts = await repo.GetAvailableProductCodesAsync(dateKey, productCodes);
    var missingProducts = productCodes
        .Where(productCode => !availableProducts.Contains(productCode))
        .ToList();

    if (missingProducts.Count == 0)
    {
        Console.WriteLine($"All configured products are already registered for {dateKey}; nothing to process.");
        CleanWorkImages(runRoot);
        return;
    }

    var outputRootPath = ResolveRootPath(outputRoot, projectRoot);
    var jobId = await repo.InsertDailyJobAsync(
        new DailySentinelGrabJobRequest
        {
            JobName = $"{modeName} Sentinel imagery {dateKey}",
            Layer = config.DailyCheck.Layer,
            DateKey = dateKey,
            DateFrom = selectedDate,
            DateTo = selectedDate,
            CloudCoverMax = cloudMax,
            Bbox = bbox,
            OutputRootPath = outputRootPath,
            ZoomMin = config.DailyCheck.ZoomMin,
            ZoomMax = config.DailyCheck.ZoomMax,
            Priority = config.DailyCheck.Priority,
            CreatedBy = config.DailyCheck.CreatedBy
        },
        missingProducts);

    Console.WriteLine($"Queued {modeName} SentinelGrab job {jobId} for {dateKey}; products={string.Join(", ", missingProducts)}.");

    var job = new SentinelGrabJob
    {
        JobId = jobId,
        Status = "Queued",
        DateFrom = selectedDate,
        DateTo = selectedDate,
        DateKey = dateKey,
        BboxMinLon = bbox.MinLon,
        BboxMinLat = bbox.MinLat,
        BboxMaxLon = bbox.MaxLon,
        BboxMaxLat = bbox.MaxLat,
        CloudCoverMax = cloudMax,
        PreferMosaic = false,
        MaxScenes = 1,
        ZoomMin = config.DailyCheck.ZoomMin,
        ZoomMax = config.DailyCheck.ZoomMax,
        OutputRootPath = outputRootPath
    };

    try
    {
        await ProcessJobAsync(job, repo, config, projectRoot, selectedCandidate);
    }
    catch (Exception ex)
    {
        Console.WriteLine($"{modeName} job {jobId} failed: {ex.Message}");
        await repo.MarkJobFailedAsync(jobId);
        throw;
    }
    finally
    {
        var workRootPath = ResolveRootPath(config.WorkRoot ?? "data", projectRoot);
        CleanWorkImages(Path.Combine(workRootPath, jobId.ToString(CultureInfo.InvariantCulture)));
        CleanWorkImages(runRoot);
    }
}

static async Task RunCliModeAsync(AppConfig config, string projectRoot, string[] args)
{
    if (PipelineWaterOperations.IsPipelineWaterProduct(GetArgValue(args, "--product")))
    {
        await PipelineWaterOperations.RunCliModeAsync(config, projectRoot, args);
        return;
    }

    var bbox = ResolveConfiguredBbox(config, args);

    var year = GetArgValue(args, "--year") is { } y && int.TryParse(y, out var yv) ? yv : config.Cli.Year;
    var month = GetArgValue(args, "--month") is { } m && int.TryParse(m, out var mv) ? mv : config.Cli.Month;
    var day = GetArgValue(args, "--day") is { } d && int.TryParse(d, out var dv) ? dv : (int?)null;
    var cloudMax = GetArgValue(args, "--cloud") is { } c && int.TryParse(c, out var cv) ? cv : config.Cli.CloudCoverMax;
    var farmCloudMax = GetArgValue(args, "--farm-cloud") is { } fc
        && double.TryParse(fc, NumberStyles.Float, CultureInfo.InvariantCulture, out var fcv)
            ? fcv
            : config.FarmCloudScreening.MaxCloudOrShadowPercent;

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
    var stac = CreateStacClient(http, config);
    var items = await stac.SearchAsync(bbox, rangeStart, rangeEnd, cloudMax, 100);

    if (items.Count == 0)
    {
        Console.WriteLine(day.HasValue ? "No scenes found for that date/window." : "No scenes found for that month/window.");
        return;
    }

    var bands = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "B02", "B03", "B04", "SCL" };
    var downloader = new BandDownloader(http, stac);
    string tileInputDir;

    if (config.FarmCloudScreening.Enabled)
    {
        if (string.IsNullOrWhiteSpace(config.OsgeoRoot))
        {
            throw new InvalidOperationException("OsgeoRoot is required when FarmCloudScreening is enabled.");
        }

        var selector = new FarmSceneSelector(downloader, new GdalToolRunner(config.OsgeoRoot));
        var selection = await selector.SelectAsync(
            items,
            bbox,
            Path.Combine(outDir, "screening"),
            config.FarmCloudScreening,
            farmCloudMax);

        tileInputDir = selection.Selected.InputDir;
        foreach (var item in selection.Selected.Items)
        {
            var sceneFolder = selection.Selected.SceneFolders[item.Id];
            await downloader.DownloadBandsAsync(item, bands, sceneFolder);
        }
    }
    else
    {
        var best = items.OrderBy(i => i.CloudCover ?? double.MaxValue).First();
        Console.WriteLine($"Best scene by whole-tile cloud metadata: {best.Id} cloud={(best.CloudCover ?? 0):0.0}%");

        await File.WriteAllTextAsync(Path.Combine(outDir, "item.json"), best.RawJson);
        await downloader.DownloadBandsAsync(best, bands, outDir);
        tileInputDir = outDir;
    }

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
    var tileBuilder = new GdalProductTileBuilder();
    try
    {
        var result = await tileBuilder.BuildAsync(new TileBuildRequest
        {
            JobId = 0,
            ProductCode = "RGB",
            DateKey = outputFolder,
            InputDir = tileInputDir,
            OutputRootPath = outputRootPath,
            ProductSubPath = "rgb",
            ZoomMin = 8,
            ZoomMax = 16,
            OsgeoRoot = config.OsgeoRoot,
            ClipBbox = bbox,
            Processes = Math.Max(1, config.DefaultProcesses),
            ScaleMaxRgb = config.DefaultScaleMaxRGB
        });

        Console.WriteLine($"Tile generation completed: {result.OutputDir}");
        Console.WriteLine(Truncate(result.Log));
    }
    finally
    {
        CleanWorkImages(outDir);
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
    finally
    {
        var workRoot = ResolveRootPath(config.WorkRoot ?? "data", projectRoot);
        CleanWorkImages(Path.Combine(workRoot, job.JobId.ToString(CultureInfo.InvariantCulture)));
    }
}

static async Task ProcessJobAsync(
    SentinelGrabJob job,
    JobRepository repo,
    AppConfig config,
    string projectRoot,
    FarmSceneCandidate? preselectedCandidate = null)
{
    var products = await repo.GetPendingProductsAsync(job.JobId);
    if (products.Count == 0)
    {
        Console.WriteLine("No queued/failed products found; inserting default RGB product.");
        products.Add(await repo.InsertDefaultProductAsync(job.JobId));
    }

    var (dateFrom, dateTo, dateKey) = ResolveDateRange(job);
    var pipelineProducts = products
        .Where(product => PipelineWaterOperations.IsPipelineWaterProduct(product.ProductCode))
        .ToList();
    var rasterProducts = products
        .Where(product => !PipelineWaterOperations.IsPipelineWaterProduct(product.ProductCode))
        .ToList();

    if (rasterProducts.Count == 0)
    {
        var pipelineWorkRoot = ResolveRootPath(config.WorkRoot ?? "data", projectRoot);
        var pipelineJobRoot = Path.Combine(pipelineWorkRoot, job.JobId.ToString(CultureInfo.InvariantCulture));
        Directory.CreateDirectory(pipelineJobRoot);

        using var pipelineHttp = CreateHttpClient();
        var pipelineStac = CreateStacClient(pipelineHttp, config);
        var pipelineCloudMax = job.CloudCoverMax ?? 100;
        foreach (var product in pipelineProducts)
        {
            await PipelineWaterOperations.ProcessDbProductAsync(
                job,
                product,
                repo,
                config,
                projectRoot,
                pipelineStac,
                dateFrom,
                dateTo,
                pipelineJobRoot,
                pipelineCloudMax);
        }

        var pipelineCounts = await repo.GetJobProductStatusCountsAsync(job.JobId);
        if (pipelineCounts.Total == 0)
        {
            await repo.MarkJobFailedAsync(job.JobId);
            Console.WriteLine("No products found to evaluate job status.");
            return;
        }

        if (pipelineCounts.Failed == 0 && pipelineCounts.Succeeded == pipelineCounts.Total)
        {
            await repo.MarkJobSucceededAsync(job.JobId);
            Console.WriteLine($"Job {job.JobId} succeeded.");
        }
        else
        {
            await repo.MarkJobFailedAsync(job.JobId);
            Console.WriteLine($"Job {job.JobId} failed. Succeeded={pipelineCounts.Succeeded}, Failed={pipelineCounts.Failed}, Total={pipelineCounts.Total}.");
        }

        return;
    }

    var requestedBbox = ResolveBbox(job);
    var areaLimitBbox = BuildAreaLimitBbox(config.AreaLimit);
    var bbox = ApplyAreaLimit(requestedBbox, areaLimitBbox);
    if (bbox != requestedBbox)
    {
        Console.WriteLine($"Job bbox capped to configured AreaLimit: {FormatBbox(bbox)}.");
    }

    var configuredTileCloudMax = job.CloudCoverMax ?? 100;
    var cloudMax = config.FarmCloudScreening.Enabled ? 100 : configuredTileCloudMax;
    if (config.FarmCloudScreening.Enabled && configuredTileCloudMax < 100)
    {
        Console.WriteLine(
            $"Ignoring job CloudCoverMax={configuredTileCloudMax} during STAC search because it is whole-tile metadata. " +
            "FarmCloudScreening must inspect the local SCL pixels, including tiles reported as highly cloudy.");
    }

    var maxScenes = Math.Max(1, job.MaxScenes ?? 1);
    var wantMultiple = job.PreferMosaic || maxScenes > 1;
    if (job.PreferMosaic && maxScenes < 2)
    {
        Console.WriteLine("PreferMosaic=1 but MaxScenes < 2; only one scene will be downloaded.");
    }

    var bandSet = ComputeRequiredBands(rasterProducts, config.WaterDetection);
    bandSet.Add("SCL");

    var workRoot = ResolveRootPath(config.WorkRoot ?? "data", projectRoot);
    var jobRoot = Path.Combine(workRoot, job.JobId.ToString(CultureInfo.InvariantCulture));
    Directory.CreateDirectory(jobRoot);

    using var http = CreateHttpClient();
    var stac = CreateStacClient(http, config);
    List<StacItem> items;
    var sceneId = job.SceneId?.Trim();
    if (preselectedCandidate is not null)
    {
        Console.WriteLine($"Using preselected acquisition {preselectedCandidate.AcquisitionKey}.");
        items = preselectedCandidate.Items.ToList();
        wantMultiple = items.Count > 1;
        maxScenes = items.Count;
    }
    else if (!string.IsNullOrWhiteSpace(sceneId))
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

    var downloader = new BandDownloader(http, stac);
    IReadOnlyList<StacItem> selected;
    IReadOnlyDictionary<string, string>? selectedSceneFolders = null;
    string tileInputDir;
    string sceneRoot = jobRoot;

    if (preselectedCandidate is not null)
    {
        selected = preselectedCandidate.Items;
        selectedSceneFolders = preselectedCandidate.SceneFolders.Count > 0
            ? preselectedCandidate.SceneFolders
            : null;
        tileInputDir = preselectedCandidate.InputDir;
        sceneRoot = tileInputDir;
    }
    else if (string.IsNullOrWhiteSpace(sceneId) && config.FarmCloudScreening.Enabled)
    {
        if (string.IsNullOrWhiteSpace(config.OsgeoRoot))
        {
            throw new InvalidOperationException("OsgeoRoot is required when FarmCloudScreening is enabled.");
        }

        if (wantMultiple)
        {
            Console.WriteLine(
                "FarmCloudScreening selects one acquisition pass and all intersecting MGRS tiles. " +
                "PreferMosaic/MaxScenes is not used for a multi-date cloud mosaic.");
        }

        var selector = new FarmSceneSelector(downloader, new GdalToolRunner(config.OsgeoRoot));
        var selection = await selector.SelectAsync(
            items,
            bbox,
            Path.Combine(jobRoot, "screening"),
            config.FarmCloudScreening);

        selected = selection.Selected.Items;
        selectedSceneFolders = selection.Selected.SceneFolders;
        tileInputDir = selection.Selected.InputDir;
    }
    else
    {
        selected = items
            .OrderBy(i => i.CloudCover ?? double.MaxValue)
            .Take(wantMultiple ? maxScenes : 1)
            .ToList();

        tileInputDir = jobRoot;
    }

    Console.WriteLine($"Selected {selected.Count} scene(s). Bands: {string.Join(", ", bandSet)}");

    foreach (var item in selected)
    {
        string sceneFolder;
        if (selectedSceneFolders is not null && selectedSceneFolders.TryGetValue(item.Id, out var screenedFolder))
        {
            sceneFolder = screenedFolder;
        }
        else
        {
            sceneFolder = selected.Count == 1 && !wantMultiple
                ? Path.Combine(sceneRoot, "single")
                : Path.Combine(sceneRoot, SanitizePathSegment(item.Id));
        }

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
    var tileBuilder = new GdalProductTileBuilder();

    foreach (var product in rasterProducts)
    {
        var productCode = product.ProductCode.Trim().ToUpperInvariant();
        var productSubPath = string.IsNullOrWhiteSpace(product.OutputSubPath)
            ? product.ProductCode.Trim().ToLowerInvariant()
            : product.OutputSubPath.Trim();

        await repo.UpdateJobProductStatusAsync(product.JobProductId, "Running", null, "Starting C# tile builder.");

        var zoomMin = job.ZoomMin ?? 8;
        var zoomMax = job.ZoomMax ?? 14;
        var processes = Math.Max(1, config.DefaultProcesses);

        try
        {
            var result = await tileBuilder.BuildAsync(new TileBuildRequest
            {
                JobId = job.JobId,
                ProductCode = productCode,
                DateKey = dateKey,
                InputDir = tileInputDir,
                OutputRootPath = outputRootPath,
                ProductSubPath = productSubPath,
                ZoomMin = zoomMin,
                ZoomMax = zoomMax,
                OsgeoRoot = config.OsgeoRoot,
                ClipBbox = bbox,
                Processes = processes,
                ScaleMaxRgb = config.DefaultScaleMaxRGB,
                IndexMin = config.DefaultNdviMin,
                IndexMax = config.DefaultNdviMax
            });

            var log = $"Tiles created at: {result.OutputDir}. Template: {result.OutputDir}\\{{z}}\\{{x}}\\{{y}}.png\n{result.Log}";
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
                OutputDir = result.OutputDir
            };

            await repo.UpsertAvailableLayerAsync(available);
        }
        catch (Exception ex)
        {
            await repo.UpdateJobProductStatusAsync(product.JobProductId, "Failed", Truncate(ex.Message), Truncate(ex.ToString()));
        }
    }

    foreach (var product in pipelineProducts)
    {
        await PipelineWaterOperations.ProcessDbProductAsync(
            job,
            product,
            repo,
            config,
            projectRoot,
            stac,
            dateFrom,
            dateTo,
            jobRoot,
            cloudMax);
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

static HashSet<string> ComputeRequiredBands(IEnumerable<SentinelGrabJobProduct> products, WaterDetectionConfig waterDetection)
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
            case SentinelGrabProductCodes.PipelineWater:
                AddWaterDetectorBands(bands, waterDetection);
                break;
        }
    }

    return bands;
}

static void AddWaterDetectorBands(HashSet<string> bands, WaterDetectionConfig waterDetection)
{
    bands.Add("SCL");

    if (string.Equals(waterDetection.Method, "Hybrid", StringComparison.OrdinalIgnoreCase))
    {
        bands.Add("B03");
        bands.Add("B08");
        bands.Add("B11");
    }
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

static void ValidateAutomaticModeConfig(AppConfig config, string modeName)
{
    if (string.IsNullOrWhiteSpace(config.SqlConnectionString))
    {
        throw new InvalidOperationException($"SqlConnectionString is required for {modeName} mode so processed layers are registered.");
    }

    if (string.IsNullOrWhiteSpace(config.OutputRootPath))
    {
        throw new InvalidOperationException($"OutputRootPath is required for {modeName} mode.");
    }

    if (string.IsNullOrWhiteSpace(config.OsgeoRoot))
    {
        throw new InvalidOperationException($"OsgeoRoot is required for {modeName} mode.");
    }

    if (!config.FarmCloudScreening.Enabled)
    {
        Console.WriteLine($"FarmCloudScreening is disabled; {modeName} mode will select by whole-tile cloud metadata only.");
    }
}

static Bbox ResolveConfiguredBbox(AppConfig config, string[] args)
{
    var areaLimitBbox = BuildAreaLimitBbox(config.AreaLimit);
    var bboxText = GetArgValue(args, "--bbox") ?? config.Cli.Bbox ?? FormatBbox(areaLimitBbox);
    var requestedBbox = ParseBbox(bboxText);
    var bbox = ApplyAreaLimit(requestedBbox, areaLimitBbox);
    if (bbox != requestedBbox)
    {
        Console.WriteLine($"Using area-limited bbox {FormatBbox(bbox)}.");
    }

    return bbox;
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

static Bbox BuildAreaLimitBbox(AreaLimitConfig areaLimit)
{
    if (areaLimit.RadiusMiles <= 0)
    {
        throw new InvalidOperationException("AreaLimit RadiusMiles must be greater than zero.");
    }

    const double kilometersPerMile = 1.609344d;
    const double kilometersPerDegree = 111.32d;

    var radiusKm = areaLimit.RadiusMiles * kilometersPerMile;
    var deltaLat = radiusKm / kilometersPerDegree;
    var deltaLon = radiusKm / (kilometersPerDegree * Math.Cos(areaLimit.CenterLat * Math.PI / 180d));

    return new Bbox(
        areaLimit.CenterLon - deltaLon,
        areaLimit.CenterLat - deltaLat,
        areaLimit.CenterLon + deltaLon,
        areaLimit.CenterLat + deltaLat);
}

static Bbox ApplyAreaLimit(Bbox requestedBbox, Bbox areaLimitBbox)
{
    var minLon = Math.Max(requestedBbox.MinLon, areaLimitBbox.MinLon);
    var minLat = Math.Max(requestedBbox.MinLat, areaLimitBbox.MinLat);
    var maxLon = Math.Min(requestedBbox.MaxLon, areaLimitBbox.MaxLon);
    var maxLat = Math.Min(requestedBbox.MaxLat, areaLimitBbox.MaxLat);

    if (minLon >= maxLon || minLat >= maxLat)
    {
        throw new InvalidOperationException(
            $"Requested bbox {FormatBbox(requestedBbox)} does not overlap configured AreaLimit {FormatBbox(areaLimitBbox)}.");
    }

    return new Bbox(minLon, minLat, maxLon, maxLat);
}

static string FormatBbox(Bbox bbox)
{
    return string.Join(
        ",",
        bbox.MinLon.ToString("G17", CultureInfo.InvariantCulture),
        bbox.MinLat.ToString("G17", CultureInfo.InvariantCulture),
        bbox.MaxLon.ToString("G17", CultureInfo.InvariantCulture),
        bbox.MaxLat.ToString("G17", CultureInfo.InvariantCulture));
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
    var alternateName = name.StartsWith("--", StringComparison.Ordinal)
        ? "-" + name[2..]
        : null;

    for (var i = 0; i < args.Length; i++)
    {
        if ((string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase)
                || (alternateName is not null && string.Equals(args[i], alternateName, StringComparison.OrdinalIgnoreCase)))
            && i + 1 < args.Length)
        {
            return args[i + 1];
        }
    }

    return null;
}

static bool TryParseDate(string? value, out DateTime date)
{
    if (!string.IsNullOrWhiteSpace(value)
        && DateTime.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
    {
        date = parsed.Date;
        return true;
    }

    date = default;
    return false;
}

static int? GetIntArg(string[] args, string name)
{
    return GetArgValue(args, name) is { } value && int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
        ? parsed
        : null;
}

static double? GetDoubleArg(string[] args, string name)
{
    return GetArgValue(args, name) is { } value && double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
        ? parsed
        : null;
}

static decimal? GetDecimalArg(string[] args, string name)
{
    return GetArgValue(args, name) is { } value && decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsed)
        ? parsed
        : null;
}

static IReadOnlyList<string> ResolveProductCodes(string? productsArg, IEnumerable<string> configuredProducts)
{
    var source = !string.IsNullOrWhiteSpace(productsArg)
        ? productsArg.Split(new[] { ',', ';', ' ' }, StringSplitOptions.RemoveEmptyEntries)
        : configuredProducts;

    return source
        .Select(product => product.Trim().ToUpperInvariant())
        .Where(product => product.Length > 0)
        .Distinct(StringComparer.OrdinalIgnoreCase)
        .ToList();
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

static StacClient CreateStacClient(HttpClient http, AppConfig config)
{
    return new StacClient(http, config.PlanetaryComputer.ToStacClientOptions());
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

static void CleanWorkImages(string root)
{
    WorkImageCleaner.LogResult(root, WorkImageCleaner.Clean(root));
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
