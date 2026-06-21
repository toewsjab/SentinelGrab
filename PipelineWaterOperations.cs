using System.Globalization;
using System.Net;
using System.Data;
using Microsoft.Data.SqlClient;
using NetTopologySuite.IO;

public static class PipelineWaterOperations
{
    private const int StacPageLimit = 100;

    public static bool IsPipelineWaterProduct(string? productCode)
    {
        return string.Equals(
            productCode?.Trim(),
            SentinelGrabProductCodes.PipelineWater,
            StringComparison.OrdinalIgnoreCase);
    }

    public static async Task RunCliModeAsync(
        AppConfig config,
        string projectRoot,
        string[] args,
        CancellationToken cancellationToken = default)
    {
        var dateFrom = ParseRequiredDate(args, "--date-from");
        var dateTo = ParseRequiredDate(args, "--date-to");
        if (dateTo < dateFrom)
        {
            throw new InvalidOperationException("--date-to must be greater than or equal to --date-from.");
        }

        var saveToDb = HasArg(args, "--save-pipeline-water-db");
        var repo = BuildRepository(config, saveToDb);
        var pipelineConfig = BuildConfig(config.PipelineWater, null, args);
        var (path, importResult) = await ResolvePipelinePathAsync(
            repo,
            config,
            projectRoot,
            args,
            pipelineConfig,
            saveToDb,
            cancellationToken);

        var workRoot = ResolveRootPath(config.WorkRoot ?? "data", projectRoot);
        var outputDirectory = Path.Combine(
            workRoot,
            "pipeline-water",
            "cli",
            $"{SanitizePathSegment(path.PathName)}_{dateFrom:yyyy-MM-dd}_{dateTo:yyyy-MM-dd}");
        Directory.CreateDirectory(outputDirectory);

        var jobId = 0L;
        var jobProductId = 0L;
        if (saveToDb)
        {
            if (repo is null)
            {
                throw new InvalidOperationException("SqlConnectionString is required when --save-pipeline-water-db is supplied.");
            }

            var createdProduct = await CreateCliJobProductAsync(
                config.SqlConnectionString!,
                path,
                importResult,
                config,
                projectRoot,
                dateFrom,
                dateTo);
            jobId = createdProduct.JobId;
            jobProductId = createdProduct.JobProductId;
        }

        using var http = CreateHttpClient();
        var processor = new PipelineWaterProcessor(
            new StacClient(http, config.PlanetaryComputer.ToStacClientOptions()),
            PipelineWaterDetectorFactory.Create());
        var result = await processor.BuildAsync(new PipelineWaterProcessingRequest
        {
            JobId = jobId,
            JobProductId = jobProductId,
            PipelinePath = path,
            DateFrom = dateFrom,
            DateTo = dateTo,
            OutputDirectory = outputDirectory,
            PipelineWater = pipelineConfig,
            WaterDetection = config.WaterDetection,
            Sections = ToPipelineSections(importResult),
            CloudCoverMax = GetIntArg(args, "--cloud") ?? config.Cli.CloudCoverMax,
            StacPageLimit = StacPageLimit
        }, cancellationToken);

        var observations = GetObservations(result);
        var exportPaths = BuildExportPaths(outputDirectory);
        result = result with
        {
            ObservationsGeoJsonPath = exportPaths.ObservationsGeoJsonPath,
            ZonesGeoJsonPath = exportPaths.ZonesGeoJsonPath
        };

        if (saveToDb)
        {
            if (repo is null)
            {
                throw new InvalidOperationException("SqlConnectionString is required when --save-pipeline-water-db is supplied.");
            }

            var saved = await repo.ReplacePipelineWaterRunAsync(BuildSaveRequest(
                result,
                observations,
                dateFrom,
                dateTo,
                pipelineConfig,
                exportPaths.ObservationsGeoJsonPath,
                exportPaths.ZonesGeoJsonPath));
            result = result with { Zones = saved.Zones };
        }

        var exports = await new PipelineWaterExportWriter().WriteAsync(path, result, observations, outputDirectory, cancellationToken);
        Console.WriteLine(BuildProductLog(path, dateFrom, dateTo, pipelineConfig, result, exports));

        if (!HasEnoughClearData(observations, pipelineConfig))
        {
            Console.WriteLine("PIPELINE_WATER completed with insufficient clear observations; the result is low-data, not Dry.");
        }
    }

    public static async Task ProcessDbProductAsync(
        SentinelGrabJob job,
        SentinelGrabJobProduct product,
        JobRepository repo,
        AppConfig config,
        string projectRoot,
        StacClient stac,
        DateTime dateFrom,
        DateTime dateTo,
        string jobRoot,
        int cloudCoverMax,
        CancellationToken cancellationToken = default)
    {
        await repo.UpdateJobProductStatusAsync(
            product.JobProductId,
            "Running",
            null,
            "Starting PIPELINE_WATER analysis.");

        try
        {
            var request = await repo.GetPipelineWaterRequestAsync(product.JobProductId)
                ?? throw new InvalidOperationException($"No SentinelPipelineWaterRequests row exists for JobProductId {product.JobProductId}.");
            var path = await repo.GetPipelinePathAsync(request.PipelinePathId)
                ?? throw new InvalidOperationException($"Pipeline path {request.PipelinePathId} was not found.");
            if (!path.IsActive)
            {
                throw new InvalidOperationException($"Pipeline path {path.PipelinePathId} is inactive.");
            }

            var pipelineConfig = BuildConfig(config.PipelineWater, request, null);
            var importResult = ImportStoredPath(path, pipelineConfig);
            var outputDirectory = Path.Combine(
                jobRoot,
                "pipeline-water",
                product.JobProductId.ToString(CultureInfo.InvariantCulture));
            Directory.CreateDirectory(outputDirectory);

            var processor = new PipelineWaterProcessor(stac, PipelineWaterDetectorFactory.Create());
            var result = await processor.BuildAsync(new PipelineWaterProcessingRequest
            {
                JobId = job.JobId,
                JobProductId = product.JobProductId,
                PipelinePath = path,
                DateFrom = dateFrom,
                DateTo = dateTo,
                OutputDirectory = outputDirectory,
                PipelineWater = pipelineConfig,
                WaterDetection = config.WaterDetection,
                Sections = ToPipelineSections(importResult),
                CloudCoverMax = cloudCoverMax,
                StacPageLimit = StacPageLimit
            }, cancellationToken);

            var observations = GetObservations(result);
            var exportPaths = BuildExportPaths(outputDirectory);
            var saved = await repo.ReplacePipelineWaterRunAsync(BuildSaveRequest(
                result,
                observations,
                dateFrom,
                dateTo,
                pipelineConfig,
                exportPaths.ObservationsGeoJsonPath,
                exportPaths.ZonesGeoJsonPath));
            result = result with
            {
                ObservationsGeoJsonPath = exportPaths.ObservationsGeoJsonPath,
                ZonesGeoJsonPath = exportPaths.ZonesGeoJsonPath,
                Zones = saved.Zones
            };

            var exports = await new PipelineWaterExportWriter().WriteAsync(path, result, observations, outputDirectory, cancellationToken);
            var log = BuildProductLog(path, dateFrom, dateTo, pipelineConfig, result, exports);
            if (HasEnoughClearData(observations, pipelineConfig))
            {
                await repo.UpdateJobProductStatusAsync(product.JobProductId, "Succeeded", null, Truncate(log));
            }
            else
            {
                await repo.UpdateJobProductStatusAsync(
                    product.JobProductId,
                    "Failed",
                    "Insufficient clear observations for PIPELINE_WATER.",
                    Truncate(log + Environment.NewLine + "Low-data output is not reported as Dry."));
            }
        }
        catch (Exception ex)
        {
            await repo.UpdateJobProductStatusAsync(product.JobProductId, "Failed", Truncate(ex.Message), Truncate(ex.ToString()));
        }
    }

    private static PipelineWaterConfig BuildConfig(
        PipelineWaterConfig defaults,
        SentinelPipelineWaterRequestRecord? request,
        string[]? args)
    {
        var config = new PipelineWaterConfig
        {
            CorridorHalfWidthM = defaults.CorridorHalfWidthM,
            AnalysisBinLengthM = defaults.AnalysisBinLengthM,
            MaximumSectionLengthM = defaults.MaximumSectionLengthM,
            MinimumClearObservations = defaults.MinimumClearObservations,
            MinimumClearFractionPerBin = defaults.MinimumClearFractionPerBin,
            PersistentFrequencyThreshold = defaults.PersistentFrequencyThreshold,
            SeasonalFrequencyThreshold = defaults.SeasonalFrequencyThreshold,
            IncludedMonthsCsv = defaults.IncludedMonthsCsv,
            MergeGapM = defaults.MergeGapM,
            Method = defaults.Method,
            AlgorithmVersion = defaults.AlgorithmVersion,
            MaximumConcurrentSections = defaults.MaximumConcurrentSections,
            MaximumAcquisitions = defaults.MaximumAcquisitions,
            MaximumLocalDiskBytes = defaults.MaximumLocalDiskBytes,
            MaximumRasterPixels = defaults.MaximumRasterPixels,
            AllowBinLengthGreaterThanCorridorDiameter = defaults.AllowBinLengthGreaterThanCorridorDiameter
        };

        if (request is not null)
        {
            config.CorridorHalfWidthM = (double)request.CorridorHalfWidthM;
            config.AnalysisBinLengthM = (double)request.AnalysisBinLengthM;
            config.MinimumClearObservations = request.MinimumClearObservations;
            config.PersistentFrequencyThreshold = (double)request.PersistentFrequencyThreshold;
            config.SeasonalFrequencyThreshold = (double)request.SeasonalFrequencyThreshold;
            config.IncludedMonthsCsv = request.IncludedMonthsCsv ?? config.IncludedMonthsCsv;
        }

        if (args is not null)
        {
            config.CorridorHalfWidthM = GetDoubleArg(args, "--corridor-half-width-m") ?? config.CorridorHalfWidthM;
            config.AnalysisBinLengthM = GetDoubleArg(args, "--bin-length-m") ?? config.AnalysisBinLengthM;
            config.IncludedMonthsCsv = GetArgValue(args, "--included-months") ?? config.IncludedMonthsCsv;
        }

        config.Validate();
        return config;
    }

    private static async Task<(SentinelPipelinePathRecord Path, PipelinePathImportResult ImportResult)> ResolvePipelinePathAsync(
        JobRepository? repo,
        AppConfig config,
        string projectRoot,
        string[] args,
        PipelineWaterConfig pipelineConfig,
        bool saveToDb,
        CancellationToken cancellationToken)
    {
        if (GetLongArg(args, "--pipeline-path-id") is { } pipelinePathId)
        {
            if (repo is null)
            {
                throw new InvalidOperationException("SqlConnectionString is required for --pipeline-path-id.");
            }

            var storedPath = await repo.GetPipelinePathAsync(pipelinePathId)
                ?? throw new InvalidOperationException($"Pipeline path {pipelinePathId} was not found.");
            if (!storedPath.IsActive)
            {
                throw new InvalidOperationException($"Pipeline path {storedPath.PipelinePathId} is inactive.");
            }

            return (storedPath, ImportStoredPath(storedPath, pipelineConfig));
        }

        var fileArg = GetArgValue(args, "--pipeline-geojson")
            ?? throw new InvalidOperationException("PIPELINE_WATER CLI requires --pipeline-path-id or --pipeline-geojson.");
        var sourcePath = ResolveRootPath(fileArg, projectRoot);
        var sourceText = await File.ReadAllTextAsync(sourcePath, cancellationToken);
        var importResult = ImportSourcePath(
            sourceText,
            GetArgValue(args, "--pipeline-name") ?? Path.GetFileNameWithoutExtension(sourcePath),
            GetArgValue(args, "--direction-description"),
            sourcePath,
            GetDecimalArg(args, "--chainage-origin-m") ?? 0m,
            pipelineConfig);
        var path = importResult.Path;

        if (saveToDb)
        {
            if (repo is null)
            {
                throw new InvalidOperationException("SqlConnectionString is required when --save-pipeline-water-db is supplied.");
            }

            path = await repo.GetPipelinePathBySourceHashAsync(path.SourceHash)
                ?? await repo.InsertPipelinePathAsync(path);
            importResult = importResult with { Path = path };
        }

        return (path, importResult);
    }

    private static PipelinePathImportResult ImportStoredPath(
        SentinelPipelinePathRecord path,
        PipelineWaterConfig pipelineConfig)
    {
        return ImportSourcePath(
            path.RouteGeometry,
            path.PathName,
            path.DirectionDescription,
            path.SourceReference,
            path.ChainageOriginM,
            pipelineConfig) with { Path = path };
    }

    private static PipelinePathImportResult ImportSourcePath(
        string sourceText,
        string pathName,
        string? directionDescription,
        string? sourceReference,
        decimal chainageOriginM,
        PipelineWaterConfig pipelineConfig)
    {
        return new PipelinePathImporter().Import(new PipelinePathImportRequest
        {
            SourceText = sourceText,
            PathName = pathName,
            DirectionDescription = directionDescription,
            SourceReference = sourceReference,
            ChainageOriginM = chainageOriginM,
            MaxProjectedSectionLengthM = pipelineConfig.MaximumSectionLengthM
        });
    }

    private static IReadOnlyList<PipelineSection> ToPipelineSections(PipelinePathImportResult importResult)
    {
        return importResult.Sections
            .OrderBy(section => section.SectionOrdinal)
            .Select(section => new PipelineSection
            {
                SectionOrdinal = section.SectionOrdinal,
                UtmZone = section.UtmZone,
                NorthernHemisphere = section.NorthernHemisphere,
                StartChainageM = section.StartChainageM,
                EndChainageM = section.EndChainageM,
                RouteSectionWkt = section.SectionGeometryWkt
            })
            .ToList();
    }

    private static async Task<CreatedPipelineWaterProduct> CreateCliJobProductAsync(
        string connectionString,
        SentinelPipelinePathRecord path,
        PipelinePathImportResult importResult,
        AppConfig config,
        string projectRoot,
        DateTime dateFrom,
        DateTime dateTo)
    {
        var outputRoot = ResolveRootPath(config.OutputRootPath ?? config.WorkRoot ?? "data", projectRoot);
        var bbox = BuildRouteBbox(importResult.Path.RouteGeometry);

        const string jobSql = @"
INSERT INTO dbo.SentinelGrabJobs
(
    JobName,
    Layer,
    DateKey,
    DateFrom,
    DateTo,
    CloudCoverMax,
    MaxScenes,
    PreferMosaic,
    MinLon,
    MinLat,
    MaxLon,
    MaxLat,
    OutputRootPath,
    ZoomMin,
    ZoomMax,
    TileFormat,
    Priority,
    CreatedBy
)
OUTPUT INSERTED.JobId
VALUES
(
    @JobName,
    @Layer,
    @DateKey,
    @DateFrom,
    @DateTo,
    @CloudCoverMax,
    @MaxScenes,
    @PreferMosaic,
    @MinLon,
    @MinLat,
    @MaxLon,
    @MaxLat,
    @OutputRootPath,
    @ZoomMin,
    @ZoomMax,
    @TileFormat,
    @Priority,
    @CreatedBy
);";

        const string productSql = @"
INSERT INTO dbo.SentinelGrabJobProducts
(
    JobId,
    ProductCode,
    OutputSubPath
)
OUTPUT INSERTED.JobProductId
VALUES
(
    @JobId,
    @ProductCode,
    @OutputSubPath
);";

        await using var conn = new SqlConnection(connectionString);
        await conn.OpenAsync();
        await using var transaction = (SqlTransaction)await conn.BeginTransactionAsync();

        try
        {
            long jobId;
            await using (var jobCmd = new SqlCommand(jobSql, conn, transaction))
            {
                jobCmd.Parameters.Add("@JobName", SqlDbType.NVarChar, 200).Value = $"PIPELINE_WATER {path.PathName}";
                jobCmd.Parameters.Add("@Layer", SqlDbType.NVarChar, 50).Value = SentinelGrabProductCodes.PipelineWater;
                jobCmd.Parameters.Add("@DateKey", SqlDbType.NVarChar, 20).Value = $"{dateFrom:yyyy-MM-dd}_{dateTo:yyyy-MM-dd}";
                jobCmd.Parameters.Add("@DateFrom", SqlDbType.Date).Value = dateFrom.Date;
                jobCmd.Parameters.Add("@DateTo", SqlDbType.Date).Value = dateTo.Date;
                jobCmd.Parameters.Add("@CloudCoverMax", SqlDbType.Decimal).Value = 100;
                jobCmd.Parameters.Add("@MaxScenes", SqlDbType.Int).Value = 1;
                jobCmd.Parameters.Add("@PreferMosaic", SqlDbType.Bit).Value = false;
                jobCmd.Parameters.Add("@MinLon", SqlDbType.Float).Value = bbox.MinLon;
                jobCmd.Parameters.Add("@MinLat", SqlDbType.Float).Value = bbox.MinLat;
                jobCmd.Parameters.Add("@MaxLon", SqlDbType.Float).Value = bbox.MaxLon;
                jobCmd.Parameters.Add("@MaxLat", SqlDbType.Float).Value = bbox.MaxLat;
                jobCmd.Parameters.Add("@OutputRootPath", SqlDbType.NVarChar, 400).Value = outputRoot;
                jobCmd.Parameters.Add("@ZoomMin", SqlDbType.Int).Value = 8;
                jobCmd.Parameters.Add("@ZoomMax", SqlDbType.Int).Value = 14;
                jobCmd.Parameters.Add("@TileFormat", SqlDbType.NVarChar, 10).Value = "geojson";
                jobCmd.Parameters.Add("@Priority", SqlDbType.Int).Value = 50;
                jobCmd.Parameters.Add("@CreatedBy", SqlDbType.NVarChar, 100).Value = "SentinelGrabCli";
                jobId = Convert.ToInt64(await jobCmd.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
            }

            long jobProductId;
            await using (var productCmd = new SqlCommand(productSql, conn, transaction))
            {
                productCmd.Parameters.Add("@JobId", SqlDbType.BigInt).Value = jobId;
                productCmd.Parameters.Add("@ProductCode", SqlDbType.NVarChar, 20).Value = SentinelGrabProductCodes.PipelineWater;
                productCmd.Parameters.Add("@OutputSubPath", SqlDbType.NVarChar, 200).Value = DBNull.Value;
                jobProductId = Convert.ToInt64(await productCmd.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
            }

            await transaction.CommitAsync();
            return new CreatedPipelineWaterProduct(jobId, jobProductId);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }

    private static PipelineWaterRunSaveRequest BuildSaveRequest(
        PipelineWaterBuildResult result,
        IReadOnlyList<PipelineWaterBinObservation> observations,
        DateTime dateFrom,
        DateTime dateTo,
        PipelineWaterConfig config,
        string observationsGeoJsonPath,
        string zonesGeoJsonPath)
    {
        return new PipelineWaterRunSaveRequest
        {
            JobId = result.JobId,
            JobProductId = result.JobProductId,
            PipelinePathId = result.PipelinePathId,
            DateFrom = dateFrom,
            DateTo = dateTo,
            Method = result.Method,
            AlgorithmVersion = result.AlgorithmVersion,
            CorridorHalfWidthM = ToDecimal(config.CorridorHalfWidthM, 2),
            AnalysisBinLengthM = ToDecimal(config.AnalysisBinLengthM, 2),
            AcquisitionCount = result.AcquisitionCount,
            ClearAcquisitionCount = result.ClearAcquisitionCount,
            OutputDirectory = result.OutputDirectory,
            ObservationsGeoJsonPath = observationsGeoJsonPath,
            ZonesGeoJsonPath = zonesGeoJsonPath,
            Observations = observations,
            Zones = result.Zones
        };
    }

    private static IReadOnlyList<PipelineWaterBinObservation> GetObservations(PipelineWaterBuildResult result)
    {
        return result.Acquisitions
            .SelectMany(acquisition => acquisition.BinObservations)
            .OrderBy(observation => observation.BinIndex)
            .ThenBy(observation => observation.AcquiredAt)
            .ThenBy(observation => observation.AcquisitionKey, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }

    private static bool HasEnoughClearData(
        IReadOnlyList<PipelineWaterBinObservation> observations,
        PipelineWaterConfig config)
    {
        if (observations.Count == 0)
        {
            return false;
        }

        return observations
            .GroupBy(observation => observation.BinIndex)
            .All(group => group.Count(IsClearObservation) >= config.MinimumClearObservations);
    }

    private static bool IsClearObservation(PipelineWaterBinObservation observation)
    {
        return string.Equals(observation.ObservationState, "Water", StringComparison.Ordinal)
            || string.Equals(observation.ObservationState, "Dry", StringComparison.Ordinal);
    }

    private static string BuildProductLog(
        SentinelPipelinePathRecord path,
        DateTime dateFrom,
        DateTime dateTo,
        PipelineWaterConfig config,
        PipelineWaterBuildResult result,
        PipelineWaterExportResult exports)
    {
        return string.Join(Environment.NewLine, new[]
        {
            $"PIPELINE_WATER route {path.PipelinePathId} ({path.PathName})",
            $"Date range: {dateFrom:yyyy-MM-dd} through {dateTo:yyyy-MM-dd}",
            $"Corridor half-width m: {config.CorridorHalfWidthM.ToString("0.##", CultureInfo.InvariantCulture)}",
            $"Acquisitions: {result.AcquisitionCount}",
            $"Clear acquisitions: {result.ClearAcquisitionCount}",
            $"Zones: {result.Zones.Count}",
            $"Observations GeoJSON: {exports.ObservationsGeoJsonPath}",
            $"Zones GeoJSON: {exports.ZonesGeoJsonPath}",
            $"Route GeoJSON: {exports.RouteGeoJsonPath}",
            $"Summary JSON: {exports.SummaryJsonPath}",
            result.ProcessingLog
        });
    }

    private static (string ObservationsGeoJsonPath, string ZonesGeoJsonPath) BuildExportPaths(string outputDirectory)
    {
        return (
            Path.Combine(outputDirectory, "pipeline-water-observations.geojson"),
            Path.Combine(outputDirectory, "pipeline-water-zones.geojson"));
    }

    private static Bbox BuildRouteBbox(string routeWkt)
    {
        var geometry = new WKTReader(NetTopologySuite.NtsGeometryServices.Instance).Read(routeWkt);
        if (geometry is null || geometry.IsEmpty)
        {
            throw new InvalidOperationException("Pipeline route geometry is empty.");
        }

        var envelope = geometry.EnvelopeInternal;
        return new Bbox(envelope.MinX, envelope.MinY, envelope.MaxX, envelope.MaxY);
    }

    private static JobRepository? BuildRepository(AppConfig config, bool required)
    {
        if (string.IsNullOrWhiteSpace(config.SqlConnectionString))
        {
            if (required)
            {
                throw new InvalidOperationException("SqlConnectionString is required for PIPELINE_WATER database persistence.");
            }

            return null;
        }

        return new JobRepository(config.SqlConnectionString);
    }

    private static DateTime ParseRequiredDate(string[] args, string name)
    {
        var value = GetArgValue(args, name);
        if (!string.IsNullOrWhiteSpace(value)
            && DateTime.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var parsed))
        {
            return parsed.Date;
        }

        throw new InvalidOperationException($"PIPELINE_WATER CLI requires {name} yyyy-MM-dd.");
    }

    private static bool HasArg(string[] args, string name)
    {
        return args.Any(arg => string.Equals(arg, name, StringComparison.OrdinalIgnoreCase));
    }

    private static string? GetArgValue(string[] args, string name)
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

    private static int? GetIntArg(string[] args, string name)
    {
        return GetArgValue(args, name) is { } value && int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    private static long? GetLongArg(string[] args, string name)
    {
        return GetArgValue(args, name) is { } value && long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    private static double? GetDoubleArg(string[] args, string name)
    {
        return GetArgValue(args, name) is { } value && double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    private static decimal? GetDecimalArg(string[] args, string name)
    {
        return GetArgValue(args, name) is { } value && decimal.TryParse(value, NumberStyles.Number, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    private static decimal ToDecimal(double value, int decimals)
    {
        return Math.Round((decimal)value, decimals, MidpointRounding.AwayFromZero);
    }

    private static HttpClient CreateHttpClient()
    {
        return new HttpClient(new HttpClientHandler
        {
            AutomaticDecompression = DecompressionMethods.All
        })
        {
            Timeout = TimeSpan.FromMinutes(10)
        };
    }

    private static string ResolveRootPath(string path, string projectRoot)
    {
        return Path.IsPathRooted(path)
            ? Path.GetFullPath(path)
            : Path.GetFullPath(Path.Combine(projectRoot, path));
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

    private static string Truncate(string? value, int maxLen = 20000)
    {
        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        return value.Length <= maxLen ? value : value.Substring(0, maxLen);
    }

    private sealed record CreatedPipelineWaterProduct(long JobId, long JobProductId);
}
