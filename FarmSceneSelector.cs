using System.Globalization;
using System.Text;
using System.Text.Json;

public sealed record FarmCloudScore(
    long TotalPixels,
    long CoveredPixels,
    long ClearPixels,
    long CloudPixels,
    long CloudShadowPixels,
    long UnclassifiedPixels,
    long SnowPixels,
    long SaturatedOrDefectivePixels,
    double DataCoveragePercent,
    double CloudPercent,
    double CloudShadowPercent,
    double CloudOrShadowPercent,
    double UnclassifiedPercent,
    double SnowPercent,
    double SaturatedOrDefectivePercent);

public sealed record FarmSceneCandidate(
    string AcquisitionKey,
    DateTimeOffset? AcquiredAt,
    IReadOnlyList<StacItem> Items,
    IReadOnlyDictionary<string, string> SceneFolders,
    string InputDir,
    FarmCloudScore CloudScore);

public sealed record FarmSceneSelection(
    FarmSceneCandidate Selected,
    IReadOnlyList<FarmSceneCandidate> Candidates);

public sealed class FarmSceneSelector
{
    private readonly BandDownloader _downloader;
    private readonly FarmCloudScorer _cloudScorer;

    public FarmSceneSelector(BandDownloader downloader, GdalToolRunner gdal)
    {
        _downloader = downloader;
        _cloudScorer = new FarmCloudScorer(gdal);
    }

    public async Task<FarmSceneSelection> SelectAsync(
        IReadOnlyCollection<StacItem> items,
        Bbox farmBbox,
        string screeningRoot,
        FarmCloudScreeningConfig config,
        double? maxCloudOrShadowPercentOverride = null)
    {
        if (items.Count == 0)
        {
            throw new InvalidOperationException("No Sentinel items were supplied for farm cloud screening.");
        }

        var maxCloudOrShadowPercent = maxCloudOrShadowPercentOverride
            ?? config.MaxCloudOrShadowPercent;

        ValidatePercent(maxCloudOrShadowPercent, nameof(config.MaxCloudOrShadowPercent));
        ValidatePercent(config.MinDataCoveragePercent, nameof(config.MinDataCoveragePercent));
        if (config.MaxAcquisitionsToCheck < 0)
        {
            throw new InvalidOperationException($"{nameof(config.MaxAcquisitionsToCheck)} cannot be negative.");
        }

        Directory.CreateDirectory(screeningRoot);

        var acquisitionGroups = items
            .GroupBy(item => item.AcquisitionKey, StringComparer.OrdinalIgnoreCase)
            .Select(group => new
            {
                Key = group.Key,
                AcquiredAt = group.Max(item => item.AcquiredAt),
                Items = group
                    .OrderBy(item => item.MgrsTile, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(item => item.Id, StringComparer.OrdinalIgnoreCase)
                    .ToList()
            })
            .OrderByDescending(group => group.AcquiredAt)
            .ThenBy(group => group.Key, StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (config.MaxAcquisitionsToCheck > 0)
        {
            acquisitionGroups = acquisitionGroups
                .Take(config.MaxAcquisitionsToCheck)
                .ToList();
        }

        Console.WriteLine(
            $"Farm cloud screening {acquisitionGroups.Count} acquisition(s); " +
            $"required coverage >= {config.MinDataCoveragePercent:0.##}%, " +
            $"cloud/shadow <= {maxCloudOrShadowPercent:0.##}%.");

        var candidates = new List<FarmSceneCandidate>();

        foreach (var acquisition in acquisitionGroups)
        {
            var acquisitionDir = Path.Combine(screeningRoot, SanitizePathSegment(acquisition.Key));
            Directory.CreateDirectory(acquisitionDir);

            try
            {
                var sceneFolders = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                var sclPaths = new List<string>();

                foreach (var item in acquisition.Items)
                {
                    var sceneDir = Path.Combine(acquisitionDir, SanitizePathSegment(item.Id));
                    Directory.CreateDirectory(sceneDir);
                    sceneFolders[item.Id] = sceneDir;

                    await File.WriteAllTextAsync(Path.Combine(sceneDir, "item.json"), item.RawJson);
                    var sclPath = await _downloader.DownloadBandAsync(item, "SCL", sceneDir);
                    if (!string.IsNullOrWhiteSpace(sclPath))
                    {
                        sclPaths.Add(sclPath);
                    }
                }

                if (sclPaths.Count == 0)
                {
                    Console.WriteLine($"Acquisition {acquisition.Key}: no SCL assets; skipped.");
                    continue;
                }

                var score = await _cloudScorer.ScoreAsync(sclPaths, farmBbox, acquisitionDir);
                var candidate = new FarmSceneCandidate(
                    acquisition.Key,
                    acquisition.AcquiredAt,
                    acquisition.Items,
                    sceneFolders,
                    acquisitionDir,
                    score);

                candidates.Add(candidate);
                await WriteCandidateSummaryAsync(candidate);

                Console.WriteLine(
                    $"Acquisition {acquisition.Key}: " +
                    $"coverage={score.DataCoveragePercent:0.00}%, " +
                    $"cloud/shadow={score.CloudOrShadowPercent:0.00}% " +
                    $"(cloud={score.CloudPercent:0.00}%, shadow={score.CloudShadowPercent:0.00}%), " +
                    $"unclassified={score.UnclassifiedPercent:0.00}%, snow={score.SnowPercent:0.00}%.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Acquisition {acquisition.Key}: cloud screening failed: {ex.Message}");
                await File.WriteAllTextAsync(
                    Path.Combine(acquisitionDir, "cloud-screening-error.txt"),
                    ex.ToString());
            }
        }

        if (candidates.Count == 0)
        {
            throw new InvalidOperationException("Cloud screening failed for every Sentinel acquisition.");
        }

        var ranked = candidates
            .OrderBy(candidate => candidate.CloudScore.CloudOrShadowPercent)
            .ThenBy(candidate => candidate.CloudScore.UnclassifiedPercent)
            .ThenByDescending(candidate => candidate.CloudScore.DataCoveragePercent)
            .ThenByDescending(candidate => candidate.AcquiredAt)
            .ToList();

        var selected = ranked.FirstOrDefault(candidate =>
            candidate.CloudScore.DataCoveragePercent >= config.MinDataCoveragePercent
            && candidate.CloudScore.CloudOrShadowPercent <= maxCloudOrShadowPercent);

        if (selected is null)
        {
            var best = ranked[0];
            throw new InvalidOperationException(
                $"No acquisition met the farm cloud threshold. " +
                $"Best was {best.AcquisitionKey}: coverage={best.CloudScore.DataCoveragePercent:0.00}%, " +
                $"cloud/shadow={best.CloudScore.CloudOrShadowPercent:0.00}%. " +
                $"Required coverage >= {config.MinDataCoveragePercent:0.00}% and " +
                $"cloud/shadow <= {maxCloudOrShadowPercent:0.00}%.");
        }

        var selection = new FarmSceneSelection(selected, ranked);
        await WriteSelectionSummaryAsync(screeningRoot, selection, maxCloudOrShadowPercent, config.MinDataCoveragePercent);

        Console.WriteLine(
            $"Selected acquisition {selected.AcquisitionKey}: " +
            $"coverage={selected.CloudScore.DataCoveragePercent:0.00}%, " +
            $"cloud/shadow={selected.CloudScore.CloudOrShadowPercent:0.00}%, " +
            $"tiles={selected.Items.Count}.");

        return selection;
    }

    private static async Task WriteCandidateSummaryAsync(FarmSceneCandidate candidate)
    {
        var summary = new
        {
            candidate.AcquisitionKey,
            AcquiredAt = candidate.AcquiredAt?.ToString("O", CultureInfo.InvariantCulture),
            Items = candidate.Items.Select(item => new
            {
                item.Id,
                item.MgrsTile,
                item.CloudCover
            }),
            candidate.CloudScore
        };

        await File.WriteAllTextAsync(
            Path.Combine(candidate.InputDir, "farm-cloud-score.json"),
            JsonSerializer.Serialize(summary, JsonOptions));
    }

    private static async Task WriteSelectionSummaryAsync(
        string screeningRoot,
        FarmSceneSelection selection,
        double maxCloudOrShadowPercent,
        double minDataCoveragePercent)
    {
        var summary = new
        {
            Thresholds = new
            {
                MaxCloudOrShadowPercent = maxCloudOrShadowPercent,
                MinDataCoveragePercent = minDataCoveragePercent
            },
            SelectedAcquisitionKey = selection.Selected.AcquisitionKey,
            Candidates = selection.Candidates.Select(candidate => new
            {
                candidate.AcquisitionKey,
                AcquiredAt = candidate.AcquiredAt?.ToString("O", CultureInfo.InvariantCulture),
                TileCount = candidate.Items.Count,
                candidate.CloudScore
            })
        };

        await File.WriteAllTextAsync(
            Path.Combine(screeningRoot, "farm-cloud-selection.json"),
            JsonSerializer.Serialize(summary, JsonOptions));
    }

    private static void ValidatePercent(double value, string name)
    {
        if (!double.IsFinite(value) || value < 0 || value > 100)
        {
            throw new InvalidOperationException($"{name} must be a finite value between 0 and 100.");
        }
    }

    private static string SanitizePathSegment(string value)
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

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true
    };
}

public sealed class FarmCloudScorer
{
    private readonly GdalToolRunner _gdal;

    public FarmCloudScorer(GdalToolRunner gdal)
    {
        _gdal = gdal;
    }

    public async Task<FarmCloudScore> ScoreAsync(
        IReadOnlyCollection<string> sclPaths,
        Bbox farmBbox,
        string workingDirectory)
    {
        if (sclPaths.Count == 0)
        {
            throw new InvalidOperationException("At least one SCL raster is required.");
        }

        Directory.CreateDirectory(workingDirectory);

        var rawPath = Path.Combine(workingDirectory, "farm-scl.dat");
        var headerPath = Path.ChangeExtension(rawPath, ".hdr");
        DeleteIfExists(rawPath, headerPath, rawPath + ".aux.xml", headerPath + ".aux.xml");

        try
        {
            var args = new List<string>
            {
                "-overwrite",
                "-of", "ENVI",
                "-ot", "Byte",
                "-r", "near",
                "-srcnodata", "0",
                "-dstnodata", "0",
                "-multi",
                "-wo", "NUM_THREADS=ALL_CPUS",
                "-wo", "INIT_DEST=NO_DATA",
                "-te_srs", "EPSG:4326",
                "-te",
                farmBbox.MinLon.ToString("G17", CultureInfo.InvariantCulture),
                farmBbox.MinLat.ToString("G17", CultureInfo.InvariantCulture),
                farmBbox.MaxLon.ToString("G17", CultureInfo.InvariantCulture),
                farmBbox.MaxLat.ToString("G17", CultureInfo.InvariantCulture),
                "-co", "INTERLEAVE=BSQ"
            };

            args.AddRange(sclPaths);
            args.Add(Path.GetFileName(rawPath));

            var log = new StringBuilder();
            await _gdal.RunCheckedAsync("gdalwarp", args, workingDirectory, log);

            if (!File.Exists(rawPath) || new FileInfo(rawPath).Length == 0)
            {
                throw new InvalidOperationException("GDAL produced an empty farm SCL raster.");
            }

            return CountClasses(rawPath);
        }
        finally
        {
            DeleteIfExists(rawPath, headerPath, rawPath + ".aux.xml", headerPath + ".aux.xml");
        }
    }

    private static FarmCloudScore CountClasses(string rawPath)
    {
        var classCounts = new long[256];
        var buffer = new byte[1024 * 1024];
        long totalPixels = 0;

        using var stream = File.OpenRead(rawPath);
        while (true)
        {
            var bytesRead = stream.Read(buffer, 0, buffer.Length);
            if (bytesRead == 0)
            {
                break;
            }

            totalPixels += bytesRead;
            for (var i = 0; i < bytesRead; i++)
            {
                classCounts[buffer[i]]++;
            }
        }

        if (totalPixels == 0)
        {
            throw new InvalidOperationException("Farm SCL raster contained no pixels.");
        }

        var coveredPixels = 0L;
        for (var classValue = 1; classValue <= 11; classValue++)
        {
            coveredPixels += classCounts[classValue];
        }

        var clearPixels = classCounts[2] + classCounts[4] + classCounts[5] + classCounts[6];
        var cloudPixels = classCounts[8] + classCounts[9] + classCounts[10];
        var cloudShadowPixels = classCounts[3];
        var unclassifiedPixels = classCounts[7];
        var snowPixels = classCounts[11];
        var saturatedOrDefectivePixels = classCounts[1];

        return new FarmCloudScore(
            totalPixels,
            coveredPixels,
            clearPixels,
            cloudPixels,
            cloudShadowPixels,
            unclassifiedPixels,
            snowPixels,
            saturatedOrDefectivePixels,
            Percent(coveredPixels, totalPixels),
            Percent(cloudPixels, coveredPixels),
            Percent(cloudShadowPixels, coveredPixels),
            Percent(cloudPixels + cloudShadowPixels, coveredPixels),
            Percent(unclassifiedPixels, coveredPixels),
            Percent(snowPixels, coveredPixels),
            Percent(saturatedOrDefectivePixels, coveredPixels));
    }

    private static double Percent(long numerator, long denominator)
    {
        return denominator <= 0 ? 0d : numerator * 100d / denominator;
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
}
