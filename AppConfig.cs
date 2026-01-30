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

    public static AppConfig Load(string basePath)
    {
        var builder = new ConfigurationBuilder()
            .SetBasePath(basePath)
            .AddJsonFile("appsettings.json", optional: true)
            .AddEnvironmentVariables();

        var configRoot = builder.Build();
        var config = configRoot.Get<AppConfig>() ?? new AppConfig();

        var connFromConnStrings = configRoot.GetConnectionString("SqlConnectionString")
            ?? configRoot.GetConnectionString("SentinelGrab");

        if (!string.IsNullOrWhiteSpace(connFromConnStrings))
        {
            config.SqlConnectionString = connFromConnStrings;
        }

        return config;
    }
}

public sealed class CliConfig
{
    public string? Bbox { get; set; }
    public int Year { get; set; } = 2025;
    public int Month { get; set; } = 5;
    public int CloudCoverMax { get; set; } = 80;
}
