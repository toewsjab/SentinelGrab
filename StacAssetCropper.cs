using System.Net;
using System.Text;
using System.Text.RegularExpressions;

public sealed class StacAssetCropper
{
    private readonly StacClient _stacClient;
    private readonly HttpClient _http;

    public StacAssetCropper(StacClient stacClient, HttpClient http)
    {
        _stacClient = stacClient;
        _http = http;
    }

    public async Task<StacAssetCropResult> CropAsync(
        StacAssetCropRequest request,
        CancellationToken cancellationToken = default)
    {
        ValidateRequest(request);
        Directory.CreateDirectory(request.OutputDirectory);

        var cutlinePath = GetCutlinePath(request);
        await File.WriteAllTextAsync(cutlinePath, request.SectionCorridorGeoJson, cancellationToken);

        var gdal = new GdalToolRunner(request.OsgeoRoot);
        var log = new StringBuilder();
        var signedHref = await _stacClient.SignHrefAsync(request.AssetHref, cancellationToken);
        var remoteCommand = CreateRemoteCropCommand(request, signedHref, cutlinePath);

        try
        {
            await RunCropWithRetryAsync(gdal, remoteCommand, log, cancellationToken);
            await ValidateRasterLimitsAsync(gdal, remoteCommand.OutputPath, request, cancellationToken);
            log.AppendLine("STAC asset crop used GDAL /vsicurl remote COG access.");
            return new StacAssetCropResult
            {
                OutputPath = remoteCommand.OutputPath,
                UsedRemoteCog = true,
                UsedFallbackDownload = false,
                Log = log.ToString()
            };
        }
        catch (Exception ex) when (IsLikelyRemoteCogFailure(ex))
        {
            log.AppendLine("GDAL /vsicurl remote COG access failed; retrying with temporary local asset.");
            log.AppendLine(StacClient.RedactSensitiveUrls(ex.Message));
        }

        var fullAssetPath = GetFullAssetDownloadPath(request);
        try
        {
            await DownloadSignedAssetAsync(signedHref, fullAssetPath, request.MaximumLocalDiskBytes, cancellationToken);
            var fallbackCommand = CreateLocalCropCommand(request, fullAssetPath, cutlinePath);
            await RunCropWithRetryAsync(gdal, fallbackCommand, log, cancellationToken);
            await ValidateRasterLimitsAsync(gdal, fallbackCommand.OutputPath, request, cancellationToken);
            log.AppendLine("STAC asset crop used temporary local COG fallback.");
            return new StacAssetCropResult
            {
                OutputPath = fallbackCommand.OutputPath,
                UsedRemoteCog = false,
                UsedFallbackDownload = true,
                Log = log.ToString()
            };
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                "STAC asset crop failed. " + StacClient.RedactSensitiveUrls(ex.Message),
                ex);
        }
        finally
        {
            if (File.Exists(fullAssetPath))
            {
                File.Delete(fullAssetPath);
            }
        }
    }

    public static StacAssetCropCommand CreateRemoteCropCommand(
        StacAssetCropRequest request,
        string signedHref,
        string? cutlinePath = null)
    {
        ValidateRequest(request);
        var source = "/vsicurl/" + signedHref;
        var redactedSource = "/vsicurl/" + StacClient.RedactSensitiveUrl(signedHref);
        return CreateCropCommand(request, source, redactedSource, cutlinePath ?? GetCutlinePath(request), usesRemoteCog: true);
    }

    public static StacAssetCropCommand CreateLocalCropCommand(
        StacAssetCropRequest request,
        string localAssetPath,
        string? cutlinePath = null)
    {
        ValidateRequest(request);
        return CreateCropCommand(request, localAssetPath, localAssetPath, cutlinePath ?? GetCutlinePath(request), usesRemoteCog: false);
    }

    public static string ResolveResampling(string assetKey)
    {
        return string.Equals(assetKey, "SCL", StringComparison.OrdinalIgnoreCase)
            ? "near"
            : "bilinear";
    }

    public static string SanitizeForFileName(string value)
    {
        var safe = Regex.Replace(value, @"[^A-Za-z0-9._-]+", "_");
        return string.IsNullOrWhiteSpace(safe) ? "asset" : safe;
    }

    private static StacAssetCropCommand CreateCropCommand(
        StacAssetCropRequest request,
        string source,
        string redactedSource,
        string cutlinePath,
        bool usesRemoteCog)
    {
        Directory.CreateDirectory(request.OutputDirectory);
        var outputPath = GetOutputPath(request);
        var args = BuildWarpArgs(request, source, cutlinePath, outputPath);
        var redactedArgs = BuildWarpArgs(request, redactedSource, cutlinePath, outputPath);
        return new StacAssetCropCommand
        {
            ToolName = "gdalwarp",
            Arguments = args,
            RedactedArguments = redactedArgs,
            WorkingDirectory = request.OutputDirectory,
            CutlinePath = cutlinePath,
            OutputPath = outputPath,
            UsesRemoteCog = usesRemoteCog
        };
    }

    private static IReadOnlyList<string> BuildWarpArgs(
        StacAssetCropRequest request,
        string source,
        string cutlinePath,
        string outputPath)
    {
        return new[]
        {
            "-overwrite",
            "-of", "GTiff",
            "-cutline", cutlinePath,
            "-crop_to_cutline",
            "-dstnodata", "0",
            "-r", ResolveResampling(request.AssetKey),
            "-co", "TILED=YES",
            "-co", "COMPRESS=DEFLATE",
            source,
            outputPath
        };
    }

    private static async Task RunCropWithRetryAsync(
        GdalToolRunner gdal,
        StacAssetCropCommand command,
        StringBuilder log,
        CancellationToken cancellationToken)
    {
        const int maxAttempts = 3;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                log.AppendLine($"Running {command.ToolName}: {string.Join(" ", command.RedactedArguments)}");
                await gdal.RunCheckedAsync(command.ToolName, command.Arguments, command.WorkingDirectory, log);
                return;
            }
            catch (Exception) when (attempt < maxAttempts)
            {
                var delay = TimeSpan.FromSeconds(10 * attempt);
                log.AppendLine($"Crop attempt {attempt} failed; retrying in {delay.TotalSeconds:0} seconds.");
                await Task.Delay(delay, cancellationToken);
            }
        }
    }

    private async Task DownloadSignedAssetAsync(
        string signedHref,
        string fullAssetPath,
        long maximumLocalDiskBytes,
        CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(fullAssetPath) ?? Environment.CurrentDirectory);
        using var response = await SendDownloadWithRetryAsync(signedHref, cancellationToken);
        response.EnsureSuccessStatusCode();

        if (response.Content.Headers.ContentLength is { } contentLength && contentLength > maximumLocalDiskBytes)
        {
            throw new InvalidOperationException($"STAC asset is larger than MaximumLocalDiskBytes={maximumLocalDiskBytes}.");
        }

        await using var source = await response.Content.ReadAsStreamAsync(cancellationToken);
        await using var destination = File.Create(fullAssetPath);
        var buffer = new byte[1024 * 1024];
        long total = 0;
        while (true)
        {
            var read = await source.ReadAsync(buffer, cancellationToken);
            if (read == 0)
            {
                break;
            }

            total += read;
            if (total > maximumLocalDiskBytes)
            {
                throw new InvalidOperationException($"STAC asset exceeded MaximumLocalDiskBytes={maximumLocalDiskBytes} while downloading.");
            }

            await destination.WriteAsync(buffer.AsMemory(0, read), cancellationToken);
        }
    }

    private async Task<HttpResponseMessage> SendDownloadWithRetryAsync(string signedHref, CancellationToken cancellationToken)
    {
        const int maxAttempts = 3;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            var response = await _http.GetAsync(signedHref, HttpCompletionOption.ResponseHeadersRead, cancellationToken);
            if (!ShouldRetry(response.StatusCode) || attempt == maxAttempts)
            {
                return response;
            }

            response.Dispose();
            await Task.Delay(TimeSpan.FromSeconds(10 * attempt), cancellationToken);
        }

        throw new InvalidOperationException("STAC asset download retry loop exited unexpectedly.");
    }

    private static async Task ValidateRasterLimitsAsync(
        GdalToolRunner gdal,
        string outputPath,
        StacAssetCropRequest request,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var outputInfo = await RasterInfo.ReadAsync(gdal, outputPath, request.OutputDirectory);
        var pixels = checked((long)outputInfo.Width * outputInfo.Height);
        if (pixels > request.MaximumRasterPixels)
        {
            throw new InvalidOperationException($"Cropped raster has {pixels} pixels, exceeding MaximumRasterPixels={request.MaximumRasterPixels}.");
        }
    }

    private static bool IsLikelyRemoteCogFailure(Exception ex)
    {
        var message = ex.ToString();
        return message.Contains("/vsicurl/", StringComparison.OrdinalIgnoreCase)
            || message.Contains("HTTP", StringComparison.OrdinalIgnoreCase)
            || message.Contains("CURL", StringComparison.OrdinalIgnoreCase)
            || message.Contains("network", StringComparison.OrdinalIgnoreCase);
    }

    private static bool ShouldRetry(HttpStatusCode statusCode)
    {
        return statusCode == HttpStatusCode.TooManyRequests
            || statusCode == HttpStatusCode.RequestTimeout
            || statusCode == HttpStatusCode.InternalServerError
            || statusCode == HttpStatusCode.BadGateway
            || statusCode == HttpStatusCode.ServiceUnavailable
            || statusCode == HttpStatusCode.GatewayTimeout;
    }

    private static void ValidateRequest(StacAssetCropRequest request)
    {
        if (string.IsNullOrWhiteSpace(request.AssetKey))
        {
            throw new InvalidOperationException("STAC crop asset key is required.");
        }

        if (string.IsNullOrWhiteSpace(request.AssetHref))
        {
            throw new InvalidOperationException("STAC crop asset href is required.");
        }

        if (string.IsNullOrWhiteSpace(request.AcquisitionKey))
        {
            throw new InvalidOperationException("STAC crop acquisition key is required.");
        }

        if (request.SectionOrdinal <= 0)
        {
            throw new InvalidOperationException("STAC crop section ordinal must be greater than zero.");
        }

        if (string.IsNullOrWhiteSpace(request.SectionCorridorGeoJson))
        {
            throw new InvalidOperationException("STAC crop section corridor GeoJSON is required.");
        }

        if (string.IsNullOrWhiteSpace(request.OutputDirectory))
        {
            throw new InvalidOperationException("STAC crop output directory is required.");
        }

        if (request.MaximumLocalDiskBytes <= 0)
        {
            throw new InvalidOperationException("STAC crop MaximumLocalDiskBytes must be greater than zero.");
        }

        if (request.MaximumRasterPixels <= 0)
        {
            throw new InvalidOperationException("STAC crop MaximumRasterPixels must be greater than zero.");
        }
    }

    private static string GetCutlinePath(StacAssetCropRequest request)
    {
        return Path.Combine(
            request.OutputDirectory,
            $"section-{request.SectionOrdinal.ToString("0000")}_{SanitizeForFileName(request.AcquisitionKey)}_corridor.geojson");
    }

    private static string GetOutputPath(StacAssetCropRequest request)
    {
        return Path.Combine(
            request.OutputDirectory,
            $"section-{request.SectionOrdinal.ToString("0000")}_{SanitizeForFileName(request.AcquisitionKey)}_{SanitizeForFileName(request.AssetKey)}.tif");
    }

    private static string GetFullAssetDownloadPath(StacAssetCropRequest request)
    {
        return Path.Combine(
            request.OutputDirectory,
            "_full-assets",
            $"section-{request.SectionOrdinal.ToString("0000")}_{SanitizeForFileName(request.AcquisitionKey)}_{SanitizeForFileName(request.AssetKey)}_full.tif");
    }
}
