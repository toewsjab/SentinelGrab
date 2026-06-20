using System.Globalization;

public sealed record PipelineWaterProcessingRequest
{
    public long JobId { get; init; }
    public long JobProductId { get; init; }
    public SentinelPipelinePathRecord PipelinePath { get; init; } = new();
    public DateTime DateFrom { get; init; }
    public DateTime DateTo { get; init; }
    public string OutputDirectory { get; init; } = "";
    public PipelineWaterConfig PipelineWater { get; init; } = new();
    public WaterDetectionConfig WaterDetection { get; init; } = new();
    public IReadOnlyList<PipelineSection> Sections { get; init; } = Array.Empty<PipelineSection>();
    public int CloudCoverMax { get; init; } = 100;
    public int StacPageLimit { get; init; } = 100;
}

public sealed class PipelineWaterProcessor
{
    private readonly StacClient _stacClient;
    private readonly PipelineWaterSectionDetector _sectionDetector;

    public PipelineWaterProcessor(StacClient stacClient, PipelineWaterSectionDetector sectionDetector)
    {
        _stacClient = stacClient;
        _sectionDetector = sectionDetector;
    }

    public async Task<PipelineWaterBuildResult> BuildAsync(
        PipelineWaterProcessingRequest request,
        CancellationToken cancellationToken = default)
    {
        request.PipelineWater.Validate();
        if (request.Sections.Count == 0)
        {
            throw new InvalidOperationException("Pipeline water processing requires at least one route section.");
        }

        Directory.CreateDirectory(request.OutputDirectory);
        var scenePlanner = new PipelineWaterScenePlanner(_stacClient);
        var sectionPlans = await scenePlanner.SearchSectionsAsync(
            request.Sections,
            request.DateFrom,
            request.DateTo,
            request.PipelineWater,
            request.CloudCoverMax,
            request.StacPageLimit,
            cancellationToken);

        var acquisitionWork = BuildAcquisitionWork(sectionPlans);
        if (acquisitionWork.Count == 0)
        {
            return new PipelineWaterBuildResult
            {
                JobId = request.JobId,
                JobProductId = request.JobProductId,
                PipelinePathId = request.PipelinePath.PipelinePathId,
                Method = request.PipelineWater.Method,
                AlgorithmVersion = request.PipelineWater.AlgorithmVersion,
                AcquisitionCount = 0,
                ClearAcquisitionCount = 0,
                OutputDirectory = request.OutputDirectory,
                ProcessingLog = "No Sentinel-2 acquisitions intersected the configured pipeline corridor and date/month filters."
            };
        }

        var bins = new PipelineChainageBinBuilder().Build(
            request.Sections,
            request.PipelineWater.AnalysisBinLengthM,
            request.PipelineWater.CorridorHalfWidthM);
        var observationBuilder = new PipelineWaterObservationBuilder();
        var acquisitionResults = new List<PipelineWaterAcquisitionResult>();

        foreach (var acquisition in acquisitionWork.OrderBy(item => item.Acquisition.AcquiredAtUtc).ThenBy(item => item.Acquisition.AcquisitionKey, StringComparer.OrdinalIgnoreCase))
        {
            var sectionDetections = await DetectAcquisitionSectionsAsync(
                acquisition,
                request,
                cancellationToken);
            var acquisitionDetection = new PipelineWaterAcquisitionDetection
            {
                AcquisitionKey = acquisition.Acquisition.AcquisitionKey,
                AcquiredAt = acquisition.Acquisition.AcquiredAtUtc,
                Sections = sectionDetections
            };
            acquisitionResults.Add(observationBuilder.Build(acquisitionDetection, bins, request.PipelineWater.MinimumClearFractionPerBin));
        }

        var observations = acquisitionResults
            .SelectMany(result => result.BinObservations)
            .ToList();
        var zones = new PipelineWaterZoneAggregator().BuildZones(observations, request.PipelineWater);
        var clearAcquisitionCount = acquisitionResults.Count(result =>
            result.BinObservations.Any(observation =>
                string.Equals(observation.ObservationState, "Water", StringComparison.Ordinal)
                || string.Equals(observation.ObservationState, "Dry", StringComparison.Ordinal)));

        return new PipelineWaterBuildResult
        {
            JobId = request.JobId,
            JobProductId = request.JobProductId,
            PipelinePathId = request.PipelinePath.PipelinePathId,
            Method = request.PipelineWater.Method,
            AlgorithmVersion = request.PipelineWater.AlgorithmVersion,
            AcquisitionCount = acquisitionResults.Count,
            ClearAcquisitionCount = clearAcquisitionCount,
            OutputDirectory = request.OutputDirectory,
            Acquisitions = acquisitionResults,
            Zones = zones,
            ProcessingLog = $"Processed {request.Sections.Count.ToString(CultureInfo.InvariantCulture)} pipeline section(s), {acquisitionResults.Count.ToString(CultureInfo.InvariantCulture)} acquisition(s), {zones.Count.ToString(CultureInfo.InvariantCulture)} zone(s)."
        };
    }

    private async Task<IReadOnlyList<PipelineWaterSectionDetection>> DetectAcquisitionSectionsAsync(
        AcquisitionWork acquisition,
        PipelineWaterProcessingRequest request,
        CancellationToken cancellationToken)
    {
        var detections = new List<PipelineWaterSectionDetection>();
        var failures = new List<Exception>();
        using var semaphore = new SemaphoreSlim(Math.Max(1, request.PipelineWater.MaximumConcurrentSections));

        var tasks = acquisition.SectionGroups.Select(async sectionGroup =>
        {
            await semaphore.WaitAsync(cancellationToken);
            try
            {
                var sectionDirectory = Path.Combine(
                    request.OutputDirectory,
                    "sections",
                    sectionGroup.Corridor.Section.SectionOrdinal.ToString("0000", CultureInfo.InvariantCulture),
                    SanitizePathSegment(acquisition.Acquisition.AcquisitionKey));
                Directory.CreateDirectory(sectionDirectory);
                var detection = await _sectionDetector(
                    new PipelineWaterSectionDetectionRequest
                    {
                        Acquisition = sectionGroup.Acquisition,
                        Corridor = sectionGroup.Corridor,
                        WorkDirectory = sectionDirectory,
                        WaterDetection = request.WaterDetection
                    },
                    cancellationToken);
                lock (detections)
                {
                    detections.Add(detection);
                }
            }
            catch (Exception ex)
            {
                lock (failures)
                {
                    failures.Add(ex);
                }
            }
            finally
            {
                semaphore.Release();
            }
        }).ToArray();

        await Task.WhenAll(tasks);
        if (failures.Count > 0)
        {
            throw new InvalidOperationException(
                $"Pipeline water processing failed for {failures.Count.ToString(CultureInfo.InvariantCulture)} section(s). No partial result was persisted.",
                failures[0]);
        }

        return detections
            .OrderBy(detection => detection.SectionOrdinal)
            .ToList();
    }

    private static IReadOnlyList<AcquisitionWork> BuildAcquisitionWork(IReadOnlyList<PipelineWaterSectionSearchPlan> sectionPlans)
    {
        return sectionPlans
            .SelectMany(plan => plan.AcquisitionGroups.Select(group => new SectionAcquisitionGroup(plan.Corridor, group)))
            .GroupBy(item => new { item.Acquisition.AcquisitionKey, item.Acquisition.AcquisitionDateUtc })
            .Select(group =>
            {
                var items = group
                    .SelectMany(item => item.Acquisition.Items)
                    .GroupBy(item => item.Id, StringComparer.OrdinalIgnoreCase)
                    .Select(itemGroup => itemGroup.First())
                    .OrderBy(item => item.MgrsTile, StringComparer.OrdinalIgnoreCase)
                    .ThenBy(item => item.Id, StringComparer.OrdinalIgnoreCase)
                    .ToList();
                var first = group.OrderBy(item => item.Acquisition.AcquiredAtUtc).First().Acquisition;
                var merged = new StacAcquisitionGroup(
                    first.AcquisitionKey,
                    first.AcquisitionDateUtc,
                    first.AcquiredAtUtc,
                    items);
                return new AcquisitionWork(
                    merged,
                    group
                        .OrderBy(item => item.Corridor.Section.SectionOrdinal)
                        .ToList());
            })
            .OrderBy(item => item.Acquisition.AcquiredAtUtc)
            .ThenBy(item => item.Acquisition.AcquisitionKey, StringComparer.OrdinalIgnoreCase)
            .ToList();
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

    private sealed record SectionAcquisitionGroup(
        PipelineSectionCorridor Corridor,
        StacAcquisitionGroup Acquisition);

    private sealed record AcquisitionWork(
        StacAcquisitionGroup Acquisition,
        IReadOnlyList<SectionAcquisitionGroup> SectionGroups);
}
