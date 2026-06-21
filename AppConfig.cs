using Microsoft.Extensions.Configuration;

public sealed class AppConfig
{
    public string? Mode { get; set; }
    public string? SqlConnectionString { get; set; }
    public string? OsgeoRoot { get; set; }
    public string? WorkRoot { get; set; }
    public string? OutputRootPath { get; set; }
    public int DefaultScaleMaxRGB { get; set; } = 4000;
    public double DefaultNdviMin { get; set; } = -0.2;
    public double DefaultNdviMax { get; set; } = 0.9;
    public int DefaultProcesses { get; set; } = 1;
    public CliConfig Cli { get; set; } = new();
    public AreaLimitConfig AreaLimit { get; set; } = new();
    public FarmCloudScreeningConfig FarmCloudScreening { get; set; } = new();
    public WaterDetectionConfig WaterDetection { get; set; } = new();
    public PipelineWaterConfig PipelineWater { get; set; } = new();
    public DailyCheckConfig DailyCheck { get; set; } = new();
    public PlanetaryComputerConfig PlanetaryComputer { get; set; } = new();

    public static AppConfig Load(string basePath)
    {
        var builder = new ConfigurationBuilder()
            .SetBasePath(basePath)
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables();

        var configRoot = builder.Build();
        var config = configRoot.Get<AppConfig>() ?? new AppConfig();

        var envConn = configRoot["FarmTrackerConn"];
        if (!string.IsNullOrWhiteSpace(envConn))
        {
            config.SqlConnectionString = envConn;
        }

        config.PlanetaryComputer.ApplyEnvironmentFallback(configRoot);
        config.WaterDetection.Validate();
        config.PipelineWater.Validate();

        return config;
    }
}

public sealed class PlanetaryComputerConfig
{
    public string? SubscriptionKey { get; set; }
    public string? StacSearchUrl { get; set; }
    public string? SasSignUrl { get; set; }

    public void ApplyEnvironmentFallback(IConfiguration configRoot)
    {
        if (!string.IsNullOrWhiteSpace(SubscriptionKey))
        {
            return;
        }

        var envKey = configRoot["PC_SDK_SUBSCRIPTION_KEY"];
        if (!string.IsNullOrWhiteSpace(envKey))
        {
            SubscriptionKey = envKey;
        }
    }

    public StacClientOptions ToStacClientOptions()
    {
        return new StacClientOptions(
            string.IsNullOrWhiteSpace(SubscriptionKey) ? null : SubscriptionKey,
            string.IsNullOrWhiteSpace(StacSearchUrl) ? null : StacSearchUrl,
            string.IsNullOrWhiteSpace(SasSignUrl) ? null : SasSignUrl);
    }
}

public sealed class CliConfig
{
    public string? Bbox { get; set; }
    public int Year { get; set; } = 2025;
    public int Month { get; set; } = 5;
    public int CloudCoverMax { get; set; } = 80;
}

public sealed class AreaLimitConfig
{
    public double CenterLat { get; set; } = 50.73766897400433d;
    public double CenterLon { get; set; } = -103.42551899991804d;
    public double RadiusMiles { get; set; } = 15d;
}

public sealed class FarmCloudScreeningConfig
{
    public bool Enabled { get; set; } = false;
    public double MaxCloudOrShadowPercent { get; set; } = 1d;
    public double MinDataCoveragePercent { get; set; } = 99d;

    // 0 means evaluate every acquisition returned by the STAC search.
    public int MaxAcquisitionsToCheck { get; set; } = 0;
}

public sealed class WaterDetectionConfig
{
    public string Method { get; set; } = "Scl";
    public string AlgorithmVersion { get; set; } = "scl-v1";
    public double MinAreaSquareMetres { get; set; } = 400d;
    public int Connectivity { get; set; } = 8;
    public double MinimumClearCoveragePercent { get; set; } = 95d;
    public double NdwiThreshold { get; set; } = 0.10d;
    public double MndwiThreshold { get; set; } = 0.00d;
    public bool UseSclWaterSeed { get; set; } = true;
    public bool KeepIntermediateFiles { get; set; } = false;

    public void Validate()
    {
        if (!string.Equals(Method, "Scl", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(Method, "Hybrid", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("WaterDetection Method must be either 'Scl' or 'Hybrid'.");
        }

        if (!double.IsFinite(MinAreaSquareMetres) || MinAreaSquareMetres <= 0)
        {
            throw new InvalidOperationException("WaterDetection MinAreaSquareMetres must be greater than zero.");
        }

        if (Connectivity is not 4 and not 8)
        {
            throw new InvalidOperationException("WaterDetection Connectivity must be 4 or 8.");
        }

        ValidatePercent(MinimumClearCoveragePercent, nameof(MinimumClearCoveragePercent));

        if (!double.IsFinite(NdwiThreshold))
        {
            throw new InvalidOperationException("WaterDetection NdwiThreshold must be a finite number.");
        }

        if (!double.IsFinite(MndwiThreshold))
        {
            throw new InvalidOperationException("WaterDetection MndwiThreshold must be a finite number.");
        }
    }

    private static void ValidatePercent(double value, string name)
    {
        if (!double.IsFinite(value) || value < 0 || value > 100)
        {
            throw new InvalidOperationException($"WaterDetection {name} must be a finite value between 0 and 100.");
        }
    }
}

public sealed class PipelineWaterConfig
{
    public double CorridorHalfWidthM { get; set; } = 50d;
    public double AnalysisBinLengthM { get; set; } = 20d;
    public double MaximumSectionLengthM { get; set; } = 25000d;
    public int MinimumClearObservations { get; set; } = 5;
    public double MinimumClearFractionPerBin { get; set; } = 0.80d;
    public double PersistentFrequencyThreshold { get; set; } = 0.80d;
    public double SeasonalFrequencyThreshold { get; set; } = 0.20d;
    public string? IncludedMonthsCsv { get; set; } = "4,5,6,7,8,9,10";
    public double MergeGapM { get; set; } = 20d;
    public string Method { get; set; } = "Sentinel2Water";
    public string AlgorithmVersion { get; set; } = "pipeline-water-s2-v1";
    public int MaximumConcurrentSections { get; set; } = 2;
    public int MaximumAcquisitions { get; set; } = 100;
    public long MaximumLocalDiskBytes { get; set; } = 2147483648L;
    public long MaximumRasterPixels { get; set; } = 500000000L;
    public bool AllowBinLengthGreaterThanCorridorDiameter { get; set; } = false;

    public IReadOnlyList<int> IncludedMonths { get; private set; } = Array.Empty<int>();

    public void Validate()
    {
        ValidatePositive(CorridorHalfWidthM, nameof(CorridorHalfWidthM));
        ValidatePositive(AnalysisBinLengthM, nameof(AnalysisBinLengthM));
        ValidatePositive(MaximumSectionLengthM, nameof(MaximumSectionLengthM));
        ValidatePositive(MergeGapM, nameof(MergeGapM));

        if (!AllowBinLengthGreaterThanCorridorDiameter && AnalysisBinLengthM > CorridorHalfWidthM * 2d)
        {
            throw new InvalidOperationException("PipelineWater AnalysisBinLengthM cannot be larger than twice CorridorHalfWidthM unless explicitly allowed.");
        }

        if (MinimumClearObservations <= 0)
        {
            throw new InvalidOperationException("PipelineWater MinimumClearObservations must be greater than zero.");
        }

        ValidateUnitInterval(MinimumClearFractionPerBin, nameof(MinimumClearFractionPerBin));
        ValidateUnitInterval(PersistentFrequencyThreshold, nameof(PersistentFrequencyThreshold));
        ValidateUnitInterval(SeasonalFrequencyThreshold, nameof(SeasonalFrequencyThreshold));

        if (PersistentFrequencyThreshold <= SeasonalFrequencyThreshold)
        {
            throw new InvalidOperationException("PipelineWater PersistentFrequencyThreshold must be greater than SeasonalFrequencyThreshold.");
        }

        if (string.IsNullOrWhiteSpace(Method))
        {
            throw new InvalidOperationException("PipelineWater Method is required.");
        }

        if (string.IsNullOrWhiteSpace(AlgorithmVersion))
        {
            throw new InvalidOperationException("PipelineWater AlgorithmVersion is required.");
        }

        if (MaximumConcurrentSections < 1 || MaximumConcurrentSections > 16)
        {
            throw new InvalidOperationException("PipelineWater MaximumConcurrentSections must be between 1 and 16.");
        }

        if (MaximumAcquisitions <= 0)
        {
            throw new InvalidOperationException("PipelineWater MaximumAcquisitions must be greater than zero.");
        }

        if (MaximumLocalDiskBytes <= 0)
        {
            throw new InvalidOperationException("PipelineWater MaximumLocalDiskBytes must be greater than zero.");
        }

        if (MaximumRasterPixels <= 0)
        {
            throw new InvalidOperationException("PipelineWater MaximumRasterPixels must be greater than zero.");
        }

        IncludedMonths = ParseIncludedMonths();
    }

    private IReadOnlyList<int> ParseIncludedMonths()
    {
        if (string.IsNullOrWhiteSpace(IncludedMonthsCsv))
        {
            return Array.Empty<int>();
        }

        var months = new List<int>();
        foreach (var part in IncludedMonthsCsv.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            if (!int.TryParse(part, out var month) || month < 1 || month > 12)
            {
                throw new InvalidOperationException("PipelineWater IncludedMonthsCsv must contain only month numbers 1 through 12.");
            }

            if (months.Contains(month))
            {
                throw new InvalidOperationException("PipelineWater IncludedMonthsCsv cannot contain duplicate months.");
            }

            months.Add(month);
        }

        return months;
    }

    private static void ValidatePositive(double value, string name)
    {
        if (!double.IsFinite(value) || value <= 0d)
        {
            throw new InvalidOperationException($"PipelineWater {name} must be a finite value greater than zero.");
        }
    }

    private static void ValidateUnitInterval(double value, string name)
    {
        if (!double.IsFinite(value) || value < 0d || value > 1d)
        {
            throw new InvalidOperationException($"PipelineWater {name} must be a finite value between 0 and 1.");
        }
    }
}

public sealed class DailyCheckConfig
{
    public int LookbackDays { get; set; } = 14;
    public int LagDays { get; set; } = 1;
    public string[] ProductCodes { get; set; } = new[] { "RGB", "NDVI", "NDMI", "NDRE" };
    public int ZoomMin { get; set; } = 14;
    public int ZoomMax { get; set; } = 14;
    public int Priority { get; set; } = 50;
    public string Layer { get; set; } = "Sentinel-2 L2A";
    public string CreatedBy { get; set; } = "SentinelGrabDaily";
}
