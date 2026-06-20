public sealed class PipelineWaterScenePlanner
{
    private readonly StacClient _stacClient;
    private readonly PipelineCorridorBuilder _corridorBuilder;

    public PipelineWaterScenePlanner(StacClient stacClient, PipelineCorridorBuilder? corridorBuilder = null)
    {
        _stacClient = stacClient;
        _corridorBuilder = corridorBuilder ?? new PipelineCorridorBuilder();
    }

    public async Task<IReadOnlyList<PipelineWaterSectionSearchPlan>> SearchSectionsAsync(
        IReadOnlyList<PipelineSection> sections,
        DateTime dateFrom,
        DateTime dateTo,
        PipelineWaterConfig config,
        int cloudCoverMax,
        int pageLimit,
        CancellationToken cancellationToken = default)
    {
        if (sections.Count == 0)
        {
            return Array.Empty<PipelineWaterSectionSearchPlan>();
        }

        config.Validate();
        var plans = new List<PipelineWaterSectionSearchPlan>();
        var acquisitionKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var section in sections.OrderBy(section => section.SectionOrdinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var corridor = _corridorBuilder.Build(section, config.CorridorHalfWidthM);
            var items = await _stacClient.SearchIntersectsAsync(
                corridor.CorridorWgs84GeoJson,
                dateFrom,
                dateTo,
                cloudCoverMax,
                pageLimit,
                cancellationToken);

            var filtered = FilterByIncludedMonthsUtc(items, config.IncludedMonths);
            foreach (var item in filtered)
            {
                acquisitionKeys.Add(item.AcquisitionKey);
            }

            if (acquisitionKeys.Count > config.MaximumAcquisitions)
            {
                throw new InvalidOperationException(
                    $"PipelineWater STAC search returned more than MaximumAcquisitions={config.MaximumAcquisitions}. Narrow the date range, route, or configured month filter.");
            }

            plans.Add(new PipelineWaterSectionSearchPlan
            {
                Corridor = corridor,
                AcquisitionGroups = StacClient.GroupByAcquisition(filtered)
            });
        }

        return plans;
    }

    private static IReadOnlyList<StacItem> FilterByIncludedMonthsUtc(
        IEnumerable<StacItem> items,
        IReadOnlyList<int> includedMonths)
    {
        if (includedMonths.Count == 0)
        {
            return items.ToList();
        }

        var monthSet = includedMonths.ToHashSet();
        return items
            .Where(item => item.AcquiredAt.HasValue && monthSet.Contains(item.AcquiredAt.Value.ToUniversalTime().Month))
            .ToList();
    }
}
