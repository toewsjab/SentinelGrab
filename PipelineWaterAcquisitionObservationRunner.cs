public sealed class PipelineWaterAcquisitionObservationRunner
{
    private readonly PipelineWaterSectionDetector _sectionDetector;
    private readonly PipelineWaterObservationBuilder _observationBuilder;

    public PipelineWaterAcquisitionObservationRunner(
        PipelineWaterSectionDetector sectionDetector,
        PipelineWaterObservationBuilder? observationBuilder = null)
    {
        _sectionDetector = sectionDetector;
        _observationBuilder = observationBuilder ?? new PipelineWaterObservationBuilder();
    }

    public async Task<PipelineWaterAcquisitionResult> BuildAsync(
        StacAcquisitionGroup acquisition,
        IReadOnlyList<PipelineSectionCorridor> sectionCorridors,
        IReadOnlyList<PipelineChainageBinGeometry> bins,
        string workDirectory,
        WaterDetectionConfig waterDetection,
        PipelineWaterConfig pipelineWater,
        CancellationToken cancellationToken = default)
    {
        if (sectionCorridors.Count == 0)
        {
            throw new InvalidOperationException("Pipeline water acquisition processing requires at least one section corridor.");
        }

        pipelineWater.Validate();
        Directory.CreateDirectory(workDirectory);

        var detections = new List<PipelineWaterSectionDetection>();
        foreach (var corridor in sectionCorridors.OrderBy(corridor => corridor.Section.SectionOrdinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var request = new PipelineWaterSectionDetectionRequest
            {
                Acquisition = acquisition,
                Corridor = corridor,
                WorkDirectory = Path.Combine(workDirectory, $"section-{corridor.Section.SectionOrdinal:0000}"),
                WaterDetection = waterDetection
            };
            Directory.CreateDirectory(request.WorkDirectory);
            detections.Add(await _sectionDetector(request, cancellationToken));
        }

        var detection = new PipelineWaterAcquisitionDetection
        {
            AcquisitionKey = acquisition.AcquisitionKey,
            AcquiredAt = acquisition.AcquiredAtUtc,
            Sections = detections
        };

        return _observationBuilder.Build(detection, bins, pipelineWater.MinimumClearFractionPerBin);
    }
}
