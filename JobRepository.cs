using Microsoft.Data.SqlClient;
using System.Data;
using System.Globalization;

public sealed record JobSchema(bool HasModifiedAt, bool HasStartedAt, bool HasFinishedAt);

public sealed class JobRepository
{
    private readonly string _connectionString;
    private JobSchema? _schema;

    public JobRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task<SentinelGrabJob?> ClaimNextJobAsync()
    {
        var schema = await GetSchemaAsync();
        var setClauses = new List<string> { "Status = 'Running'" };

        if (schema.HasModifiedAt)
        {
            setClauses.Add("ModifiedAt = SYSDATETIME()");
        }

        if (schema.HasStartedAt)
        {
            setClauses.Add("StartedAt = SYSDATETIME()");
        }

        var sql = $@"
;WITH cte AS (
    SELECT TOP (1) *
    FROM dbo.SentinelGrabJobs WITH (ROWLOCK, READPAST, UPDLOCK)
    WHERE Status IN ('Queued','Failed')
    ORDER BY Priority DESC, CreatedAt ASC
)
UPDATE cte
SET {string.Join(", ", setClauses)}
OUTPUT inserted.*;";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        return ReadJob(reader);
    }

    public async Task<List<SentinelGrabJobProduct>> GetPendingProductsAsync(long jobId)
    {
        const string sql = @"
SELECT JobProductId, JobId, ProductCode, OutputSubPath, Status
FROM dbo.SentinelGrabJobProducts
WHERE JobId = @JobId AND Status IN ('Queued','Failed');";

        var list = new List<SentinelGrabJobProduct>();
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@JobId", jobId);

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            list.Add(new SentinelGrabJobProduct
            {
                JobProductId = reader.GetInt64(0),
                JobId = reader.GetInt64(1),
                ProductCode = reader.GetString(2),
                OutputSubPath = reader.IsDBNull(3) ? null : reader.GetString(3),
                Status = reader.GetString(4)
            });
        }

        return list;
    }

    public async Task<SentinelGrabJobProduct> InsertDefaultProductAsync(long jobId)
    {
        const string sql = @"
INSERT INTO dbo.SentinelGrabJobProducts (JobId, ProductCode, OutputSubPath, Status)
OUTPUT inserted.JobProductId, inserted.JobId, inserted.ProductCode, inserted.OutputSubPath, inserted.Status
VALUES (@JobId, 'RGB', 'rgb', 'Queued');";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@JobId", jobId);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            throw new InvalidOperationException("Failed to insert default product row.");
        }

        return new SentinelGrabJobProduct
        {
            JobProductId = reader.GetInt64(0),
            JobId = reader.GetInt64(1),
            ProductCode = reader.GetString(2),
            OutputSubPath = reader.IsDBNull(3) ? null : reader.GetString(3),
            Status = reader.GetString(4)
        };
    }

    public async Task<SentinelPipelinePathRecord> InsertPipelinePathAsync(SentinelPipelinePathRecord path)
    {
        const string sql = @"
INSERT INTO dbo.SentinelPipelinePaths
(
    PathName,
    RouteGeometry,
    RouteLengthM,
    ChainageOriginM,
    DirectionDescription,
    SourceReference,
    SourceHash,
    IsActive
)
OUTPUT INSERTED.PipelinePathId
VALUES
(
    @PathName,
    geometry::STGeomFromText(@RouteGeometryWkt, 4326),
    @RouteLengthM,
    @ChainageOriginM,
    @DirectionDescription,
    @SourceReference,
    @SourceHash,
    @IsActive
);";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add("@PathName", SqlDbType.NVarChar, 200).Value = path.PathName;
        cmd.Parameters.Add("@RouteGeometryWkt", SqlDbType.NVarChar, -1).Value = path.RouteGeometry;
        cmd.Parameters.Add("@RouteLengthM", SqlDbType.Decimal).Value = path.RouteLengthM;
        cmd.Parameters.Add("@ChainageOriginM", SqlDbType.Decimal).Value = path.ChainageOriginM;
        cmd.Parameters.Add("@DirectionDescription", SqlDbType.NVarChar, 300).Value = (object?)path.DirectionDescription ?? DBNull.Value;
        cmd.Parameters.Add("@SourceReference", SqlDbType.NVarChar, 300).Value = (object?)path.SourceReference ?? DBNull.Value;
        cmd.Parameters.Add("@SourceHash", SqlDbType.Char, 64).Value = path.SourceHash;
        cmd.Parameters.Add("@IsActive", SqlDbType.Bit).Value = path.IsActive;

        var pipelinePathId = Convert.ToInt64(await cmd.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
        return path with { PipelinePathId = pipelinePathId };
    }

    public async Task<SentinelPipelinePathRecord?> GetPipelinePathAsync(long pipelinePathId)
    {
        const string sql = @"
SELECT
    PipelinePathId,
    PathName,
    RouteGeometry.STAsText() AS RouteGeometryWkt,
    RouteLengthM,
    ChainageOriginM,
    DirectionDescription,
    SourceReference,
    SourceHash,
    IsActive,
    CreatedAt,
    ModifiedAt
FROM dbo.SentinelPipelinePaths
WHERE PipelinePathId = @PipelinePathId;";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add("@PipelinePathId", SqlDbType.BigInt).Value = pipelinePathId;

        await using var reader = await cmd.ExecuteReaderAsync();
        return await reader.ReadAsync() ? ReadPipelinePath(reader) : null;
    }

    public async Task<SentinelPipelinePathRecord?> GetPipelinePathBySourceHashAsync(string sourceHash)
    {
        const string sql = @"
SELECT
    PipelinePathId,
    PathName,
    RouteGeometry.STAsText() AS RouteGeometryWkt,
    RouteLengthM,
    ChainageOriginM,
    DirectionDescription,
    SourceReference,
    SourceHash,
    IsActive,
    CreatedAt,
    ModifiedAt
FROM dbo.SentinelPipelinePaths
WHERE SourceHash = @SourceHash;";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add("@SourceHash", SqlDbType.Char, 64).Value = sourceHash;

        await using var reader = await cmd.ExecuteReaderAsync();
        return await reader.ReadAsync() ? ReadPipelinePath(reader) : null;
    }

    public async Task<IReadOnlyList<SentinelPipelinePathRecord>> GetActivePipelinePathsAsync()
    {
        const string sql = @"
SELECT
    PipelinePathId,
    PathName,
    RouteGeometry.STAsText() AS RouteGeometryWkt,
    RouteLengthM,
    ChainageOriginM,
    DirectionDescription,
    SourceReference,
    SourceHash,
    IsActive,
    CreatedAt,
    ModifiedAt
FROM dbo.SentinelPipelinePaths
WHERE IsActive = 1
ORDER BY PathName, PipelinePathId;";

        var paths = new List<SentinelPipelinePathRecord>();
        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);

        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            paths.Add(ReadPipelinePath(reader));
        }

        return paths;
    }

    public async Task<SentinelPipelineWaterRequestRecord?> GetPipelineWaterRequestAsync(long jobProductId)
    {
        const string sql = @"
SELECT
    JobProductId,
    PipelinePathId,
    CorridorHalfWidthM,
    AnalysisBinLengthM,
    MinimumClearObservations,
    PersistentFrequencyThreshold,
    SeasonalFrequencyThreshold,
    IncludedMonthsCsv,
    CreatedAt
FROM dbo.SentinelPipelineWaterRequests
WHERE JobProductId = @JobProductId;";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add("@JobProductId", SqlDbType.BigInt).Value = jobProductId;

        await using var reader = await cmd.ExecuteReaderAsync();
        return await reader.ReadAsync() ? ReadPipelineWaterRequest(reader) : null;
    }

    public async Task InsertPipelineWaterBinObservationsAsync(
        long pipelineWaterRunId,
        IReadOnlyCollection<PipelineWaterBinObservation> observations)
    {
        if (observations.Count == 0)
        {
            return;
        }

        const string sql = @"
INSERT INTO dbo.SentinelPipelineWaterBinObservations
(
    PipelineWaterRunId,
    AcquisitionKey,
    AcquiredAt,
    BinIndex,
    StartChainageM,
    EndChainageM,
    ObservationState,
    ExposureType,
    WaterAreaInCorridorM2,
    LengthOnWaterM,
    NearestWaterDistanceM,
    RouteBinGeometry,
    WaterIntersectionGeometry
)
VALUES
(
    @PipelineWaterRunId,
    @AcquisitionKey,
    @AcquiredAt,
    @BinIndex,
    @StartChainageM,
    @EndChainageM,
    @ObservationState,
    @ExposureType,
    @WaterAreaInCorridorM2,
    @LengthOnWaterM,
    @NearestWaterDistanceM,
    geometry::STGeomFromText(@RouteBinGeometryWkt, 4326),
    CASE
        WHEN @WaterIntersectionGeometryWkt IS NULL THEN NULL
        ELSE geometry::STGeomFromText(@WaterIntersectionGeometryWkt, 4326)
    END
);";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var transaction = (SqlTransaction)await conn.BeginTransactionAsync();

        try
        {
            foreach (var observation in observations)
            {
                await using var cmd = new SqlCommand(sql, conn, transaction);
                cmd.Parameters.Add("@PipelineWaterRunId", SqlDbType.BigInt).Value = pipelineWaterRunId;
                cmd.Parameters.Add("@AcquisitionKey", SqlDbType.NVarChar, 200).Value = observation.AcquisitionKey;
                cmd.Parameters.Add("@AcquiredAt", SqlDbType.DateTimeOffset).Value = observation.AcquiredAt;
                cmd.Parameters.Add("@BinIndex", SqlDbType.Int).Value = observation.BinIndex;
                cmd.Parameters.Add("@StartChainageM", SqlDbType.Decimal).Value = observation.StartChainageM;
                cmd.Parameters.Add("@EndChainageM", SqlDbType.Decimal).Value = observation.EndChainageM;
                cmd.Parameters.Add("@ObservationState", SqlDbType.VarChar, 12).Value = observation.ObservationState;
                cmd.Parameters.Add("@ExposureType", SqlDbType.VarChar, 12).Value = (object?)observation.ExposureType ?? DBNull.Value;
                cmd.Parameters.Add("@WaterAreaInCorridorM2", SqlDbType.Decimal).Value = (object?)observation.WaterAreaInCorridorM2 ?? DBNull.Value;
                cmd.Parameters.Add("@LengthOnWaterM", SqlDbType.Decimal).Value = (object?)observation.LengthOnWaterM ?? DBNull.Value;
                cmd.Parameters.Add("@NearestWaterDistanceM", SqlDbType.Decimal).Value = (object?)observation.NearestWaterDistanceM ?? DBNull.Value;
                cmd.Parameters.Add("@RouteBinGeometryWkt", SqlDbType.NVarChar, -1).Value = observation.RouteBinWkt;
                cmd.Parameters.Add("@WaterIntersectionGeometryWkt", SqlDbType.NVarChar, -1).Value = (object?)observation.WaterIntersectionWkt ?? DBNull.Value;
                await cmd.ExecuteNonQueryAsync();
            }

            await transaction.CommitAsync();
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }
    public async Task<PipelineWaterRunSaveResult> ReplacePipelineWaterRunAsync(PipelineWaterRunSaveRequest request)
    {
        const string deleteSql = @"
DELETE FROM dbo.SentinelPipelineWaterRuns
WHERE JobProductId = @JobProductId;";

        const string runSql = @"
INSERT INTO dbo.SentinelPipelineWaterRuns
(
    JobId,
    JobProductId,
    PipelinePathId,
    DateFrom,
    DateTo,
    Method,
    AlgorithmVersion,
    CorridorHalfWidthM,
    AnalysisBinLengthM,
    AcquisitionCount,
    ClearAcquisitionCount,
    OutputDirectory,
    ObservationsGeoJsonPath,
    ZonesGeoJsonPath
)
OUTPUT INSERTED.PipelineWaterRunId
VALUES
(
    @JobId,
    @JobProductId,
    @PipelinePathId,
    @DateFrom,
    @DateTo,
    @Method,
    @AlgorithmVersion,
    @CorridorHalfWidthM,
    @AnalysisBinLengthM,
    @AcquisitionCount,
    @ClearAcquisitionCount,
    @OutputDirectory,
    @ObservationsGeoJsonPath,
    @ZonesGeoJsonPath
);";

        const string observationSql = @"
INSERT INTO dbo.SentinelPipelineWaterBinObservations
(
    PipelineWaterRunId,
    AcquisitionKey,
    AcquiredAt,
    BinIndex,
    StartChainageM,
    EndChainageM,
    ObservationState,
    ExposureType,
    WaterAreaInCorridorM2,
    LengthOnWaterM,
    NearestWaterDistanceM,
    RouteBinGeometry,
    WaterIntersectionGeometry
)
VALUES
(
    @PipelineWaterRunId,
    @AcquisitionKey,
    @AcquiredAt,
    @BinIndex,
    @StartChainageM,
    @EndChainageM,
    @ObservationState,
    @ExposureType,
    @WaterAreaInCorridorM2,
    @LengthOnWaterM,
    @NearestWaterDistanceM,
    geometry::STGeomFromText(@RouteBinGeometryWkt, 4326),
    CASE
        WHEN @WaterIntersectionGeometryWkt IS NULL THEN NULL
        ELSE geometry::STGeomFromText(@WaterIntersectionGeometryWkt, 4326)
    END
);";

        const string zoneSql = @"
INSERT INTO dbo.SentinelPipelineWaterZones
(
    PipelineWaterRunId,
    ZoneOrdinal,
    StartChainageM,
    EndChainageM,
    LengthM,
    WaterObservationCount,
    DryObservationCount,
    UnknownObservationCount,
    WaterFrequency,
    PersistenceClass,
    FirstWaterObservedAt,
    LastWaterObservedAt,
    MaximumWaterAreaM2,
    MinimumWaterDistanceM,
    RouteZoneGeometry
)
OUTPUT INSERTED.PipelineWaterZoneId
VALUES
(
    @PipelineWaterRunId,
    @ZoneOrdinal,
    @StartChainageM,
    @EndChainageM,
    @LengthM,
    @WaterObservationCount,
    @DryObservationCount,
    @UnknownObservationCount,
    @WaterFrequency,
    @PersistenceClass,
    @FirstWaterObservedAt,
    @LastWaterObservedAt,
    @MaximumWaterAreaM2,
    @MinimumWaterDistanceM,
    geometry::STGeomFromText(@RouteZoneGeometryWkt, 4326)
);";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var transaction = (SqlTransaction)await conn.BeginTransactionAsync();

        try
        {
            await using (var deleteCmd = new SqlCommand(deleteSql, conn, transaction))
            {
                deleteCmd.Parameters.Add("@JobProductId", SqlDbType.BigInt).Value = request.JobProductId;
                await deleteCmd.ExecuteNonQueryAsync();
            }

            long runId;
            await using (var runCmd = new SqlCommand(runSql, conn, transaction))
            {
                runCmd.Parameters.Add("@JobId", SqlDbType.BigInt).Value = request.JobId;
                runCmd.Parameters.Add("@JobProductId", SqlDbType.BigInt).Value = request.JobProductId;
                runCmd.Parameters.Add("@PipelinePathId", SqlDbType.BigInt).Value = request.PipelinePathId;
                runCmd.Parameters.Add("@DateFrom", SqlDbType.Date).Value = request.DateFrom.Date;
                runCmd.Parameters.Add("@DateTo", SqlDbType.Date).Value = request.DateTo.Date;
                runCmd.Parameters.Add("@Method", SqlDbType.NVarChar, 30).Value = request.Method;
                runCmd.Parameters.Add("@AlgorithmVersion", SqlDbType.NVarChar, 40).Value = request.AlgorithmVersion;
                runCmd.Parameters.Add("@CorridorHalfWidthM", SqlDbType.Decimal).Value = request.CorridorHalfWidthM;
                runCmd.Parameters.Add("@AnalysisBinLengthM", SqlDbType.Decimal).Value = request.AnalysisBinLengthM;
                runCmd.Parameters.Add("@AcquisitionCount", SqlDbType.Int).Value = request.AcquisitionCount;
                runCmd.Parameters.Add("@ClearAcquisitionCount", SqlDbType.Int).Value = request.ClearAcquisitionCount;
                runCmd.Parameters.Add("@OutputDirectory", SqlDbType.NVarChar, 600).Value = request.OutputDirectory;
                runCmd.Parameters.Add("@ObservationsGeoJsonPath", SqlDbType.NVarChar, 600).Value = (object?)request.ObservationsGeoJsonPath ?? DBNull.Value;
                runCmd.Parameters.Add("@ZonesGeoJsonPath", SqlDbType.NVarChar, 600).Value = (object?)request.ZonesGeoJsonPath ?? DBNull.Value;
                runId = Convert.ToInt64(await runCmd.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
            }

            foreach (var observation in request.Observations)
            {
                await using var observationCmd = new SqlCommand(observationSql, conn, transaction);
                observationCmd.Parameters.Add("@PipelineWaterRunId", SqlDbType.BigInt).Value = runId;
                observationCmd.Parameters.Add("@AcquisitionKey", SqlDbType.NVarChar, 200).Value = observation.AcquisitionKey;
                observationCmd.Parameters.Add("@AcquiredAt", SqlDbType.DateTimeOffset).Value = observation.AcquiredAt;
                observationCmd.Parameters.Add("@BinIndex", SqlDbType.Int).Value = observation.BinIndex;
                observationCmd.Parameters.Add("@StartChainageM", SqlDbType.Decimal).Value = observation.StartChainageM;
                observationCmd.Parameters.Add("@EndChainageM", SqlDbType.Decimal).Value = observation.EndChainageM;
                observationCmd.Parameters.Add("@ObservationState", SqlDbType.VarChar, 12).Value = observation.ObservationState;
                observationCmd.Parameters.Add("@ExposureType", SqlDbType.VarChar, 12).Value = (object?)observation.ExposureType ?? DBNull.Value;
                observationCmd.Parameters.Add("@WaterAreaInCorridorM2", SqlDbType.Decimal).Value = (object?)observation.WaterAreaInCorridorM2 ?? DBNull.Value;
                observationCmd.Parameters.Add("@LengthOnWaterM", SqlDbType.Decimal).Value = (object?)observation.LengthOnWaterM ?? DBNull.Value;
                observationCmd.Parameters.Add("@NearestWaterDistanceM", SqlDbType.Decimal).Value = (object?)observation.NearestWaterDistanceM ?? DBNull.Value;
                observationCmd.Parameters.Add("@RouteBinGeometryWkt", SqlDbType.NVarChar, -1).Value = observation.RouteBinWkt;
                observationCmd.Parameters.Add("@WaterIntersectionGeometryWkt", SqlDbType.NVarChar, -1).Value = (object?)observation.WaterIntersectionWkt ?? DBNull.Value;
                await observationCmd.ExecuteNonQueryAsync();
            }

            var savedZones = new List<PipelineWaterZoneResult>();
            foreach (var zone in request.Zones.OrderBy(zone => zone.ZoneOrdinal))
            {
                await using var zoneCmd = new SqlCommand(zoneSql, conn, transaction);
                zoneCmd.Parameters.Add("@PipelineWaterRunId", SqlDbType.BigInt).Value = runId;
                zoneCmd.Parameters.Add("@ZoneOrdinal", SqlDbType.Int).Value = zone.ZoneOrdinal;
                zoneCmd.Parameters.Add("@StartChainageM", SqlDbType.Decimal).Value = zone.StartChainageM;
                zoneCmd.Parameters.Add("@EndChainageM", SqlDbType.Decimal).Value = zone.EndChainageM;
                zoneCmd.Parameters.Add("@LengthM", SqlDbType.Decimal).Value = zone.LengthM;
                zoneCmd.Parameters.Add("@WaterObservationCount", SqlDbType.Int).Value = zone.WaterObservationCount;
                zoneCmd.Parameters.Add("@DryObservationCount", SqlDbType.Int).Value = zone.DryObservationCount;
                zoneCmd.Parameters.Add("@UnknownObservationCount", SqlDbType.Int).Value = zone.UnknownObservationCount;
                zoneCmd.Parameters.Add("@WaterFrequency", SqlDbType.Decimal).Value = (object?)zone.WaterFrequency ?? DBNull.Value;
                zoneCmd.Parameters.Add("@PersistenceClass", SqlDbType.VarChar, 20).Value = zone.PersistenceClass;
                zoneCmd.Parameters.Add("@FirstWaterObservedAt", SqlDbType.DateTimeOffset).Value = (object?)zone.FirstWaterObservedAt ?? DBNull.Value;
                zoneCmd.Parameters.Add("@LastWaterObservedAt", SqlDbType.DateTimeOffset).Value = (object?)zone.LastWaterObservedAt ?? DBNull.Value;
                zoneCmd.Parameters.Add("@MaximumWaterAreaM2", SqlDbType.Decimal).Value = (object?)zone.MaximumWaterAreaM2 ?? DBNull.Value;
                zoneCmd.Parameters.Add("@MinimumWaterDistanceM", SqlDbType.Decimal).Value = (object?)zone.MinimumWaterDistanceM ?? DBNull.Value;
                zoneCmd.Parameters.Add("@RouteZoneGeometryWkt", SqlDbType.NVarChar, -1).Value = zone.RouteZoneWkt;
                var zoneId = Convert.ToInt64(await zoneCmd.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
                savedZones.Add(zone with { PipelineWaterZoneId = zoneId });
            }

            await transaction.CommitAsync();
            return new PipelineWaterRunSaveResult(runId, savedZones);
        }
        catch
        {
            await transaction.RollbackAsync();
            throw;
        }
    }
    public async Task DeactivatePipelinePathAsync(long pipelinePathId)
    {
        const string sql = @"
UPDATE dbo.SentinelPipelinePaths
SET IsActive = 0,
    ModifiedAt = SYSDATETIME()
WHERE PipelinePathId = @PipelinePathId;";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add("@PipelinePathId", SqlDbType.BigInt).Value = pipelinePathId;
        await cmd.ExecuteNonQueryAsync();
    }
    public async Task UpdateJobProductStatusAsync(long jobProductId, string status, string? lastError, string? lastLog)
    {
        const string sql = @"
UPDATE dbo.SentinelGrabJobProducts
SET Status = @Status,
    LastError = @LastError,
    LastLog = @LastLog,
    ModifiedAt = SYSDATETIME()
WHERE JobProductId = @JobProductId;";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@Status", status);
        cmd.Parameters.AddWithValue("@LastError", (object?)lastError ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@LastLog", (object?)lastLog ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@JobProductId", jobProductId);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task UpsertAvailableLayerAsync(AvailableLayer layer)
    {
        const string sql = @"
IF EXISTS (SELECT 1 FROM dbo.SentinelGrabAvailableLayers WHERE JobProductId = @JobProductId)
BEGIN
    UPDATE dbo.SentinelGrabAvailableLayers
    SET JobId = @JobId,
        ProductCode = @ProductCode,
        DateKey = @DateKey,
        DateFrom = @DateFrom,
        DateTo = @DateTo,
        BboxMinLon = @BboxMinLon,
        BboxMinLat = @BboxMinLat,
        BboxMaxLon = @BboxMaxLon,
        BboxMaxLat = @BboxMaxLat,
        OutputRootPath = @OutputRootPath,
        ProductSubPath = @ProductSubPath,
        OutputDir = @OutputDir,
        ModifiedAt = SYSDATETIME()
    WHERE JobProductId = @JobProductId;
END
ELSE
BEGIN
    INSERT INTO dbo.SentinelGrabAvailableLayers
    (
        JobId,
        JobProductId,
        ProductCode,
        DateKey,
        DateFrom,
        DateTo,
        BboxMinLon,
        BboxMinLat,
        BboxMaxLon,
        BboxMaxLat,
        OutputRootPath,
        ProductSubPath,
        OutputDir
    )
    VALUES
    (
        @JobId,
        @JobProductId,
        @ProductCode,
        @DateKey,
        @DateFrom,
        @DateTo,
        @BboxMinLon,
        @BboxMinLat,
        @BboxMaxLon,
        @BboxMaxLat,
        @OutputRootPath,
        @ProductSubPath,
        @OutputDir
    );
END;";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@JobId", layer.JobId);
        cmd.Parameters.AddWithValue("@JobProductId", layer.JobProductId);
        cmd.Parameters.AddWithValue("@ProductCode", layer.ProductCode);
        cmd.Parameters.AddWithValue("@DateKey", layer.DateKey);
        cmd.Parameters.AddWithValue("@DateFrom", (object?)layer.DateFrom ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@DateTo", (object?)layer.DateTo ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@BboxMinLon", (object?)layer.BboxMinLon ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@BboxMinLat", (object?)layer.BboxMinLat ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@BboxMaxLon", (object?)layer.BboxMaxLon ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@BboxMaxLat", (object?)layer.BboxMaxLat ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@OutputRootPath", layer.OutputRootPath);
        cmd.Parameters.AddWithValue("@ProductSubPath", layer.ProductSubPath);
        cmd.Parameters.AddWithValue("@OutputDir", layer.OutputDir);
        await cmd.ExecuteNonQueryAsync();
    }

    public async Task<(int Total, int Succeeded, int Failed)> GetJobProductStatusCountsAsync(long jobId)
    {
        const string sql = @"
SELECT
    COUNT(*) AS Total,
    SUM(CASE WHEN Status = 'Succeeded' THEN 1 ELSE 0 END) AS Succeeded,
    SUM(CASE WHEN Status = 'Failed' THEN 1 ELSE 0 END) AS Failed
FROM dbo.SentinelGrabJobProducts
WHERE JobId = @JobId;";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@JobId", jobId);

        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return (0, 0, 0);
        }

        return (
            reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
            reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
            reader.IsDBNull(2) ? 0 : reader.GetInt32(2)
        );
    }

    public async Task MarkJobSucceededAsync(long jobId)
    {
        await UpdateJobStatusAsync(jobId, "Succeeded");
    }

    public async Task MarkJobFailedAsync(long jobId)
    {
        await UpdateJobStatusAsync(jobId, "Failed");
    }

    private async Task UpdateJobStatusAsync(long jobId, string status)
    {
        var schema = await GetSchemaAsync();
        var setClauses = new List<string> { "Status = @Status" };

        if (schema.HasModifiedAt)
        {
            setClauses.Add("ModifiedAt = SYSDATETIME()");
        }

        if (schema.HasFinishedAt)
        {
            setClauses.Add("FinishedAt = SYSDATETIME()");
        }

        var sql = $@"
UPDATE dbo.SentinelGrabJobs
SET {string.Join(", ", setClauses)}
WHERE JobId = @JobId;";

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@Status", status);
        cmd.Parameters.AddWithValue("@JobId", jobId);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task<JobSchema> GetSchemaAsync()
    {
        if (_schema is not null)
        {
            return _schema;
        }

        await using var conn = new SqlConnection(_connectionString);
        await conn.OpenAsync();

        var hasModifiedAt = await ColumnExistsAsync(conn, "dbo", "SentinelGrabJobs", "ModifiedAt");
        var hasStartedAt = await ColumnExistsAsync(conn, "dbo", "SentinelGrabJobs", "StartedAt");
        var hasFinishedAt = await ColumnExistsAsync(conn, "dbo", "SentinelGrabJobs", "FinishedAt");

        _schema = new JobSchema(hasModifiedAt, hasStartedAt, hasFinishedAt);
        return _schema;
    }

    private static async Task<bool> ColumnExistsAsync(SqlConnection conn, string schema, string table, string column)
    {
        var fullName = $"{schema}.{table}";
        const string sql = "SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(@TableName) AND name = @ColumnName";
        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.AddWithValue("@TableName", fullName);
        cmd.Parameters.AddWithValue("@ColumnName", column);
        var result = await cmd.ExecuteScalarAsync();
        return result is not null;
    }

    private static SentinelPipelinePathRecord ReadPipelinePath(SqlDataReader reader)
    {
        return new SentinelPipelinePathRecord
        {
            PipelinePathId = reader.GetInt64(0),
            PathName = reader.GetString(1),
            RouteGeometry = reader.GetString(2),
            RouteLengthM = reader.GetDecimal(3),
            ChainageOriginM = reader.GetDecimal(4),
            DirectionDescription = reader.IsDBNull(5) ? null : reader.GetString(5),
            SourceReference = reader.IsDBNull(6) ? null : reader.GetString(6),
            SourceHash = reader.GetString(7),
            IsActive = reader.GetBoolean(8),
            CreatedAt = reader.IsDBNull(9) ? null : reader.GetDateTime(9),
            ModifiedAt = reader.IsDBNull(10) ? null : reader.GetDateTime(10)
        };
    }
    private static SentinelPipelineWaterRequestRecord ReadPipelineWaterRequest(SqlDataReader reader)
    {
        return new SentinelPipelineWaterRequestRecord
        {
            JobProductId = reader.GetInt64(0),
            PipelinePathId = reader.GetInt64(1),
            CorridorHalfWidthM = reader.GetDecimal(2),
            AnalysisBinLengthM = reader.GetDecimal(3),
            MinimumClearObservations = reader.GetInt32(4),
            PersistentFrequencyThreshold = reader.GetDecimal(5),
            SeasonalFrequencyThreshold = reader.GetDecimal(6),
            IncludedMonthsCsv = reader.IsDBNull(7) ? null : reader.GetString(7),
            CreatedAt = reader.IsDBNull(8) ? null : reader.GetDateTime(8)
        };
    }

    private static SentinelGrabJob ReadJob(SqlDataReader reader)
    {
        var ordinals = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < reader.FieldCount; i++)
        {
            ordinals[reader.GetName(i)] = i;
        }

        object? GetValue(string name)
        {
            if (!ordinals.TryGetValue(name, out var index))
            {
                return null;
            }

            return reader.IsDBNull(index) ? null : reader.GetValue(index);
        }

        int? GetNullableInt(string name)
        {
            var value = GetValue(name);
            return value is null ? null : Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        double? GetNullableDouble(string name)
        {
            var value = GetValue(name);
            return value is null ? null : Convert.ToDouble(value, CultureInfo.InvariantCulture);
        }

        DateTime? GetNullableDateTime(string name)
        {
            var value = GetValue(name);
            return value as DateTime?;
        }

        string? GetNullableString(string name)
        {
            var value = GetValue(name);
            return value is null ? null : Convert.ToString(value, CultureInfo.InvariantCulture);
        }

        var jobId = Convert.ToInt64(GetValue("JobId") ?? 0, CultureInfo.InvariantCulture);
        var status = Convert.ToString(GetValue("Status"), CultureInfo.InvariantCulture) ?? string.Empty;
        var priority = GetNullableInt("Priority");
        var createdAt = GetNullableDateTime("CreatedAt");
        var dateFrom = GetNullableDateTime("DateFrom");
        var dateTo = GetNullableDateTime("DateTo");
        var dateKey = GetNullableString("DateKey");

        var bboxMinLon = GetNullableDouble("BboxMinLon") ?? GetNullableDouble("MinLon");
        var bboxMinLat = GetNullableDouble("BboxMinLat") ?? GetNullableDouble("MinLat");
        var bboxMaxLon = GetNullableDouble("BboxMaxLon") ?? GetNullableDouble("MaxLon");
        var bboxMaxLat = GetNullableDouble("BboxMaxLat") ?? GetNullableDouble("MaxLat");
        var bbox = GetNullableString("Bbox");

        var cloudCoverMax = GetNullableInt("CloudCoverMax");

        var preferMosaicVal = GetValue("PreferMosaic");
        var preferMosaic = preferMosaicVal is not null && Convert.ToInt32(preferMosaicVal, CultureInfo.InvariantCulture) != 0;

        var maxScenes = GetNullableInt("MaxScenes");
        var zoomMin = GetNullableInt("ZoomMin");
        var zoomMax = GetNullableInt("ZoomMax");
        var outputRootPath = GetNullableString("OutputRootPath");
        var sceneId = GetNullableString("SceneId");

        return new SentinelGrabJob
        {
            JobId = jobId,
            Status = status,
            Priority = priority,
            CreatedAt = createdAt,
            DateFrom = dateFrom,
            DateTo = dateTo,
            DateKey = dateKey,
            BboxMinLon = bboxMinLon,
            BboxMinLat = bboxMinLat,
            BboxMaxLon = bboxMaxLon,
            BboxMaxLat = bboxMaxLat,
            Bbox = bbox,
            CloudCoverMax = cloudCoverMax,
            PreferMosaic = preferMosaic,
            MaxScenes = maxScenes,
            ZoomMin = zoomMin,
            ZoomMax = zoomMax,
            OutputRootPath = outputRootPath,
            SceneId = sceneId
        };
    }
}
