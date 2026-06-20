using System.Globalization;

public static class PipelineWaterExportCli
{
    public static bool IsExportCommand(string[] args)
    {
        return HasArg(args, "--export-pipeline-water-zones")
            || HasArg(args, "--export-pipeline-water-observations")
            || HasArg(args, "--pipeline-water-csv");
    }

    public static async Task RunAsync(
        AppConfig config,
        string projectRoot,
        string[] args,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(config.SqlConnectionString))
        {
            throw new InvalidOperationException("SqlConnectionString is required for pipeline-water export commands.");
        }

        var runId = GetLongArg(args, "--run-id")
            ?? throw new InvalidOperationException("Pipeline-water export commands require --run-id <id>.");
        var outputArg = GetArgValue(args, "--output")
            ?? throw new InvalidOperationException("Pipeline-water export commands require --output <path>.");
        var outputPath = ResolveRootPath(outputArg, projectRoot);
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath) ?? projectRoot);

        var repository = new PipelineWaterQueryRepository(config.SqlConnectionString);
        var writer = new PipelineWaterMapExportWriter();

        if (HasArg(args, "--export-pipeline-water-zones"))
        {
            var zones = await repository.GetZonesByRunAsync(runId);
            await File.WriteAllTextAsync(outputPath, writer.BuildZonesGeoJson(zones), cancellationToken);
            Console.WriteLine($"Exported {zones.Count.ToString(CultureInfo.InvariantCulture)} pipeline-water zone feature(s) to {outputPath}.");
            return;
        }

        if (HasArg(args, "--export-pipeline-water-observations"))
        {
            var observations = await repository.GetObservationsByRunAsync(runId);
            await File.WriteAllTextAsync(outputPath, writer.BuildObservationsGeoJson(observations), cancellationToken);
            Console.WriteLine($"Exported {observations.Count.ToString(CultureInfo.InvariantCulture)} pipeline-water observation feature(s) to {outputPath}.");
            return;
        }

        if (HasArg(args, "--pipeline-water-csv"))
        {
            var run = await repository.GetRunAsync(runId)
                ?? throw new InvalidOperationException($"Pipeline-water run {runId} was not found.");
            var zones = await repository.GetZonesByRunAsync(runId);
            await File.WriteAllTextAsync(outputPath, writer.BuildZonesCsv(zones), cancellationToken);

            var summaryPath = BuildSummaryPath(outputPath);
            await File.WriteAllTextAsync(summaryPath, writer.BuildIntegrityScreeningSummaryJson(run, zones), cancellationToken);
            Console.WriteLine($"Exported {zones.Count.ToString(CultureInfo.InvariantCulture)} pipeline-water CSV row(s) to {outputPath}.");
            Console.WriteLine($"Wrote integrity-screening summary to {summaryPath}.");
        }
    }

    private static string BuildSummaryPath(string outputPath)
    {
        var directory = Path.GetDirectoryName(outputPath);
        var name = Path.GetFileNameWithoutExtension(outputPath);
        return Path.Combine(directory ?? string.Empty, $"{name}.integrity-screening-summary.json");
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

    private static long? GetLongArg(string[] args, string name)
    {
        return GetArgValue(args, name) is { } value && long.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : null;
    }

    private static string ResolveRootPath(string path, string projectRoot)
    {
        return Path.IsPathRooted(path)
            ? Path.GetFullPath(path)
            : Path.GetFullPath(Path.Combine(projectRoot, path));
    }
}
