using System.Data;
using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;

public sealed class PipelineWaterQueryRepository
{
    private readonly string _connectionString;

    public PipelineWaterQueryRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<PipelineWaterRunQueryRecord?> GetRunAsync(long pipelineWaterRunId)
    {
        const string sql = @"
SELECT
    r.PipelineWaterRunId,
    r.JobId,
    r.JobProductId,
    r.PipelinePathId,
    p.PathName,
    r.DateFrom,
    r.DateTo,
    r.Method,
    r.AlgorithmVersion,
    r.AcquisitionCount,
    r.ClearAcquisitionCount
FROM dbo.SentinelPipelineWaterRuns r
INNER JOIN dbo.SentinelPipelinePaths p
    ON p.PipelinePathId = r.PipelinePathId
WHERE r.PipelineWaterRunId = @PipelineWaterRunId;";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add("@PipelineWaterRunId", SqlDbType.BigInt).Value = pipelineWaterRunId;

        await using var reader = await cmd.ExecuteReaderAsync();
        return await reader.ReadAsync() ? ReadRun(reader) : null;
    }

    public async Task<IReadOnlyList<PipelineWaterZoneQueryRecord>> GetZonesByRunAsync(long pipelineWaterRunId)
    {
        const string sql = ZoneSelectSql + @"
WHERE z.PipelineWaterRunId = @PipelineWaterRunId
ORDER BY z.ZoneOrdinal;";

        return await QueryZonesAsync(sql, cmd =>
        {
            cmd.Parameters.Add("@PipelineWaterRunId", SqlDbType.BigInt).Value = pipelineWaterRunId;
        });
    }

    public async Task<IReadOnlyList<PipelineWaterObservationQueryRecord>> GetObservationsByRunAsync(long pipelineWaterRunId)
    {
        const string sql = ObservationSelectSql + @"
WHERE o.PipelineWaterRunId = @PipelineWaterRunId
ORDER BY o.AcquiredAt, o.BinIndex, o.AcquisitionKey;";

        return await QueryObservationsAsync(sql, cmd =>
        {
            cmd.Parameters.Add("@PipelineWaterRunId", SqlDbType.BigInt).Value = pipelineWaterRunId;
        });
    }

    public async Task<IReadOnlyList<PipelineWaterZoneQueryRecord>> GetZonesByPipelinePathAsync(
        long pipelinePathId,
        decimal? startChainageM = null,
        decimal? endChainageM = null)
    {
        var sql = new StringBuilder(ZoneSelectSql);
        sql.AppendLine("WHERE r.PipelinePathId = @PipelinePathId");
        if (startChainageM.HasValue)
        {
            sql.AppendLine("    AND z.EndChainageM > @StartChainageM");
        }

        if (endChainageM.HasValue)
        {
            sql.AppendLine("    AND z.StartChainageM < @EndChainageM");
        }

        sql.AppendLine("ORDER BY r.DateFrom, z.StartChainageM, z.ZoneOrdinal;");

        return await QueryZonesAsync(sql.ToString(), cmd =>
        {
            cmd.Parameters.Add("@PipelinePathId", SqlDbType.BigInt).Value = pipelinePathId;
            if (startChainageM.HasValue)
            {
                AddDecimal(cmd, "@StartChainageM", startChainageM.Value, 19, 3);
            }

            if (endChainageM.HasValue)
            {
                AddDecimal(cmd, "@EndChainageM", endChainageM.Value, 19, 3);
            }
        });
    }

    public async Task<IReadOnlyList<PipelineWaterZoneQueryRecord>> GetZonesByPersistenceClassAsync(string persistenceClass)
    {
        const string sql = ZoneSelectSql + @"
WHERE z.PersistenceClass = @PersistenceClass
ORDER BY p.PathName, r.DateFrom, z.StartChainageM, z.ZoneOrdinal;";

        return await QueryZonesAsync(sql, cmd =>
        {
            cmd.Parameters.Add("@PersistenceClass", SqlDbType.VarChar, 20).Value = persistenceClass;
        });
    }

    public async Task<IReadOnlyList<PipelineWaterObservationQueryRecord>> GetObservationsByZoneAsync(
        long pipelineWaterZoneId,
        DateTime? acquiredFrom = null,
        DateTime? acquiredTo = null)
    {
        var sql = new StringBuilder(ObservationSelectSql);
        sql.AppendLine("INNER JOIN dbo.SentinelPipelineWaterZones targetZone");
        sql.AppendLine("    ON targetZone.PipelineWaterZoneId = @PipelineWaterZoneId");
        sql.AppendLine("    AND targetZone.PipelineWaterRunId = o.PipelineWaterRunId");
        sql.AppendLine("    AND o.EndChainageM > targetZone.StartChainageM");
        sql.AppendLine("    AND o.StartChainageM < targetZone.EndChainageM");
        sql.AppendLine("WHERE 1 = 1");
        if (acquiredFrom.HasValue)
        {
            sql.AppendLine("    AND o.AcquiredAt >= @AcquiredFrom");
        }

        if (acquiredTo.HasValue)
        {
            sql.AppendLine("    AND o.AcquiredAt < @AcquiredToExclusive");
        }

        sql.AppendLine("ORDER BY o.AcquiredAt, o.BinIndex, o.AcquisitionKey;");

        return await QueryObservationsAsync(sql.ToString(), cmd =>
        {
            cmd.Parameters.Add("@PipelineWaterZoneId", SqlDbType.BigInt).Value = pipelineWaterZoneId;
            if (acquiredFrom.HasValue)
            {
                cmd.Parameters.Add("@AcquiredFrom", SqlDbType.DateTimeOffset).Value = new DateTimeOffset(acquiredFrom.Value.Date, TimeSpan.Zero);
            }

            if (acquiredTo.HasValue)
            {
                cmd.Parameters.Add("@AcquiredToExclusive", SqlDbType.DateTimeOffset).Value = new DateTimeOffset(acquiredTo.Value.Date.AddDays(1), TimeSpan.Zero);
            }
        });
    }

    public async Task<PipelineWaterObservationQueryRecord?> GetLatestObservedWaterCrossingAsync(long pipelinePathId)
    {
        const string sql = ObservationSelectSql + @"
WHERE r.PipelinePathId = @PipelinePathId
    AND o.ObservationState = 'Water'
    AND o.ExposureType = 'Crossing'
ORDER BY o.AcquiredAt DESC, o.StartChainageM, o.PipelineWaterObservationId DESC;";

        var observations = await QueryObservationsAsync(sql, cmd =>
        {
            cmd.Parameters.Add("@PipelinePathId", SqlDbType.BigInt).Value = pipelinePathId;
        }, top: 1);

        return observations.Count == 0 ? null : observations[0];
    }

    public async Task<IReadOnlyList<PipelineWaterInsufficientClearLocationRecord>> GetInsufficientClearLocationsAsync(long pipelinePathId)
    {
        const string sql = @"
SELECT
    o.PipelineWaterRunId,
    r.PipelinePathId,
    p.PathName,
    o.BinIndex,
    o.StartChainageM,
    o.EndChainageM,
    q.MinimumClearObservations,
    o.ObservationState,
    o.RouteBinGeometry.STAsText() AS RouteBinWkt
FROM dbo.SentinelPipelineWaterBinObservations o
INNER JOIN dbo.SentinelPipelineWaterRuns r
    ON r.PipelineWaterRunId = o.PipelineWaterRunId
INNER JOIN dbo.SentinelPipelineWaterRequests q
    ON q.JobProductId = r.JobProductId
INNER JOIN dbo.SentinelPipelinePaths p
    ON p.PipelinePathId = r.PipelinePathId
WHERE r.PipelinePathId = @PipelinePathId
ORDER BY o.PipelineWaterRunId, o.StartChainageM, o.BinIndex, o.AcquiredAt;";

        var rows = new List<InsufficientClearObservationRow>();
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add("@PipelinePathId", SqlDbType.BigInt).Value = pipelinePathId;

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new InsufficientClearObservationRow
            {
                PipelineWaterRunId = reader.GetInt64(0),
                PipelinePathId = reader.GetInt64(1),
                PathName = reader.GetString(2),
                BinIndex = reader.GetInt32(3),
                StartChainageM = reader.GetDecimal(4),
                EndChainageM = reader.GetDecimal(5),
                MinimumClearObservations = reader.GetInt32(6),
                ObservationState = reader.GetString(7),
                RouteBinWkt = reader.GetString(8)
            });
        }

        return rows
            .GroupBy(row => new
            {
                row.PipelineWaterRunId,
                row.PipelinePathId,
                row.PathName,
                row.BinIndex,
                row.StartChainageM,
                row.EndChainageM,
                row.MinimumClearObservations,
                row.RouteBinWkt
            })
            .Select(group => new PipelineWaterInsufficientClearLocationRecord
            {
                PipelineWaterRunId = group.Key.PipelineWaterRunId,
                PipelinePathId = group.Key.PipelinePathId,
                PathName = group.Key.PathName,
                BinIndex = group.Key.BinIndex,
                StartChainageM = group.Key.StartChainageM,
                EndChainageM = group.Key.EndChainageM,
                MinimumClearObservations = group.Key.MinimumClearObservations,
                ClearObservationCount = group.Count(row => row.ObservationState is "Water" or "Dry"),
                UnknownObservationCount = group.Count(row => row.ObservationState == "Unknown"),
                RouteBinWkt = group.Key.RouteBinWkt
            })
            .Where(location => location.ClearObservationCount < location.MinimumClearObservations)
            .OrderBy(location => location.PipelineWaterRunId)
            .ThenBy(location => location.StartChainageM)
            .ThenBy(location => location.BinIndex)
            .ToList();
    }

    private async Task<IReadOnlyList<PipelineWaterZoneQueryRecord>> QueryZonesAsync(
        string sql,
        Action<SqlCommand> configure)
    {
        var zones = new List<PipelineWaterZoneQueryRecord>();
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        configure(cmd);

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            zones.Add(ReadZone(reader));
        }

        return zones;
    }

    private async Task<IReadOnlyList<PipelineWaterObservationQueryRecord>> QueryObservationsAsync(
        string sql,
        Action<SqlCommand> configure,
        int? top = null)
    {
        if (top.HasValue)
        {
            sql = sql.Replace("SELECT", $"SELECT TOP ({top.Value.ToString(CultureInfo.InvariantCulture)})", StringComparison.Ordinal);
        }

        var observations = new List<PipelineWaterObservationQueryRecord>();
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        configure(cmd);

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            observations.Add(ReadObservation(reader));
        }

        return observations;
    }

    private static PipelineWaterRunQueryRecord ReadRun(SqlDataReader reader)
    {
        return new PipelineWaterRunQueryRecord
        {
            PipelineWaterRunId = reader.GetInt64(0),
            JobId = reader.GetInt64(1),
            JobProductId = reader.GetInt64(2),
            PipelinePathId = reader.GetInt64(3),
            PathName = reader.GetString(4),
            DateFrom = reader.GetDateTime(5),
            DateTo = reader.GetDateTime(6),
            Method = reader.GetString(7),
            AlgorithmVersion = reader.GetString(8),
            AcquisitionCount = reader.GetInt32(9),
            ClearAcquisitionCount = reader.GetInt32(10)
        };
    }

    private static PipelineWaterZoneQueryRecord ReadZone(SqlDataReader reader)
    {
        return new PipelineWaterZoneQueryRecord
        {
            PipelineWaterZoneId = reader.GetInt64(0),
            PipelineWaterRunId = reader.GetInt64(1),
            PipelinePathId = reader.GetInt64(2),
            PathName = reader.GetString(3),
            ZoneOrdinal = reader.GetInt32(4),
            StartChainageM = reader.GetDecimal(5),
            EndChainageM = reader.GetDecimal(6),
            LengthM = reader.GetDecimal(7),
            WaterObservationCount = reader.GetInt32(8),
            DryObservationCount = reader.GetInt32(9),
            UnknownObservationCount = reader.GetInt32(10),
            ClearObservationCount = reader.GetInt32(11),
            WaterFrequency = reader.IsDBNull(12) ? null : reader.GetDecimal(12),
            PersistenceClass = reader.GetString(13),
            FirstWaterObservedAt = reader.IsDBNull(14) ? null : reader.GetFieldValue<DateTimeOffset>(14),
            LastWaterObservedAt = reader.IsDBNull(15) ? null : reader.GetFieldValue<DateTimeOffset>(15),
            MaximumWaterAreaM2 = reader.IsDBNull(16) ? null : reader.GetDecimal(16),
            MinimumWaterDistanceM = reader.IsDBNull(17) ? null : reader.GetDecimal(17),
            HasCentrelineCrossing = reader.GetBoolean(18),
            RouteZoneWkt = reader.GetString(19)
        };
    }

    private static PipelineWaterObservationQueryRecord ReadObservation(SqlDataReader reader)
    {
        return new PipelineWaterObservationQueryRecord
        {
            PipelineWaterObservationId = reader.GetInt64(0),
            PipelineWaterRunId = reader.GetInt64(1),
            PipelinePathId = reader.GetInt64(2),
            PathName = reader.GetString(3),
            PipelineWaterZoneId = reader.IsDBNull(4) ? null : reader.GetInt64(4),
            AcquisitionKey = reader.GetString(5),
            AcquiredAt = reader.GetFieldValue<DateTimeOffset>(6),
            BinIndex = reader.GetInt32(7),
            StartChainageM = reader.GetDecimal(8),
            EndChainageM = reader.GetDecimal(9),
            ObservationState = reader.GetString(10),
            ExposureType = reader.IsDBNull(11) ? null : reader.GetString(11),
            WaterAreaInCorridorM2 = reader.IsDBNull(12) ? null : reader.GetDecimal(12),
            LengthOnWaterM = reader.IsDBNull(13) ? null : reader.GetDecimal(13),
            NearestWaterDistanceM = reader.IsDBNull(14) ? null : reader.GetDecimal(14),
            RouteBinWkt = reader.GetString(15),
            WaterIntersectionWkt = reader.IsDBNull(16) ? null : reader.GetString(16)
        };
    }

    private static void AddDecimal(SqlCommand cmd, string name, decimal value, byte precision, byte scale)
    {
        var parameter = cmd.Parameters.Add(name, SqlDbType.Decimal);
        parameter.Precision = precision;
        parameter.Scale = scale;
        parameter.Value = value;
    }

    private sealed record InsufficientClearObservationRow
    {
        public long PipelineWaterRunId { get; init; }
        public long PipelinePathId { get; init; }
        public string PathName { get; init; } = "";
        public int BinIndex { get; init; }
        public decimal StartChainageM { get; init; }
        public decimal EndChainageM { get; init; }
        public int MinimumClearObservations { get; init; }
        public string ObservationState { get; init; } = "";
        public string RouteBinWkt { get; init; } = "";
    }

    private const string ZoneSelectSql = @"
SELECT
    z.PipelineWaterZoneId,
    z.PipelineWaterRunId,
    r.PipelinePathId,
    p.PathName,
    z.ZoneOrdinal,
    z.StartChainageM,
    z.EndChainageM,
    z.LengthM,
    z.WaterObservationCount,
    z.DryObservationCount,
    z.UnknownObservationCount,
    z.ClearObservationCount,
    z.WaterFrequency,
    z.PersistenceClass,
    z.FirstWaterObservedAt,
    z.LastWaterObservedAt,
    z.MaximumWaterAreaM2,
    z.MinimumWaterDistanceM,
    CASE
        WHEN EXISTS
        (
            SELECT 1
            FROM dbo.SentinelPipelineWaterBinObservations crossing
            WHERE crossing.PipelineWaterRunId = z.PipelineWaterRunId
                AND crossing.EndChainageM > z.StartChainageM
                AND crossing.StartChainageM < z.EndChainageM
                AND crossing.ObservationState = 'Water'
                AND crossing.ExposureType = 'Crossing'
        )
        THEN CAST(1 AS bit)
        ELSE CAST(0 AS bit)
    END AS HasCentrelineCrossing,
    z.RouteZoneGeometry.STAsText() AS RouteZoneWkt
FROM dbo.SentinelPipelineWaterZones z
INNER JOIN dbo.SentinelPipelineWaterRuns r
    ON r.PipelineWaterRunId = z.PipelineWaterRunId
INNER JOIN dbo.SentinelPipelinePaths p
    ON p.PipelinePathId = r.PipelinePathId
";

    private const string ObservationSelectSql = @"
SELECT
    o.PipelineWaterObservationId,
    o.PipelineWaterRunId,
    r.PipelinePathId,
    p.PathName,
    z.PipelineWaterZoneId,
    o.AcquisitionKey,
    o.AcquiredAt,
    o.BinIndex,
    o.StartChainageM,
    o.EndChainageM,
    o.ObservationState,
    o.ExposureType,
    o.WaterAreaInCorridorM2,
    o.LengthOnWaterM,
    o.NearestWaterDistanceM,
    o.RouteBinGeometry.STAsText() AS RouteBinWkt,
    CASE
        WHEN o.WaterIntersectionGeometry IS NULL THEN NULL
        ELSE o.WaterIntersectionGeometry.STAsText()
    END AS WaterIntersectionWkt
FROM dbo.SentinelPipelineWaterBinObservations o
INNER JOIN dbo.SentinelPipelineWaterRuns r
    ON r.PipelineWaterRunId = o.PipelineWaterRunId
INNER JOIN dbo.SentinelPipelinePaths p
    ON p.PipelinePathId = r.PipelinePathId
LEFT JOIN dbo.SentinelPipelineWaterZones z
    ON z.PipelineWaterRunId = o.PipelineWaterRunId
    AND o.EndChainageM > z.StartChainageM
    AND o.StartChainageM < z.EndChainageM
";
}
