SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

IF OBJECT_ID('dbo.SentinelPipelinePaths', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SentinelPipelinePaths
    (
        PipelinePathId BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SentinelPipelinePaths PRIMARY KEY,
        PathName NVARCHAR(200) NOT NULL,
        RouteGeometry geometry NOT NULL,
        RouteLengthM DECIMAL(19,3) NOT NULL,
        ChainageOriginM DECIMAL(19,3) NOT NULL CONSTRAINT DF_SentinelPipelinePaths_ChainageOriginM DEFAULT(0),
        DirectionDescription NVARCHAR(300) NULL,
        SourceReference NVARCHAR(300) NULL,
        SourceHash CHAR(64) NOT NULL,
        IsActive BIT NOT NULL CONSTRAINT DF_SentinelPipelinePaths_IsActive DEFAULT(1),
        CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_SentinelPipelinePaths_CreatedAt DEFAULT(SYSDATETIME()),
        ModifiedAt DATETIME2(0) NOT NULL CONSTRAINT DF_SentinelPipelinePaths_ModifiedAt DEFAULT(SYSDATETIME())
    );
END;

IF OBJECT_ID('dbo.SentinelPipelineWaterRequests', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SentinelPipelineWaterRequests
    (
        JobProductId BIGINT NOT NULL CONSTRAINT PK_SentinelPipelineWaterRequests PRIMARY KEY,
        PipelinePathId BIGINT NOT NULL,
        CorridorHalfWidthM DECIMAL(10,2) NOT NULL,
        AnalysisBinLengthM DECIMAL(10,2) NOT NULL,
        MinimumClearObservations INT NOT NULL,
        PersistentFrequencyThreshold DECIMAL(6,5) NOT NULL,
        SeasonalFrequencyThreshold DECIMAL(6,5) NOT NULL,
        IncludedMonthsCsv VARCHAR(50) NULL,
        CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_SentinelPipelineWaterRequests_CreatedAt DEFAULT(SYSDATETIME())
    );
END;

IF OBJECT_ID('dbo.SentinelPipelineWaterRuns', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SentinelPipelineWaterRuns
    (
        PipelineWaterRunId BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SentinelPipelineWaterRuns PRIMARY KEY,
        JobId BIGINT NOT NULL,
        JobProductId BIGINT NOT NULL,
        PipelinePathId BIGINT NOT NULL,
        DateFrom DATE NOT NULL,
        DateTo DATE NOT NULL,
        Method NVARCHAR(30) NOT NULL,
        AlgorithmVersion NVARCHAR(40) NOT NULL,
        CorridorHalfWidthM DECIMAL(10,2) NOT NULL,
        AnalysisBinLengthM DECIMAL(10,2) NOT NULL,
        AcquisitionCount INT NOT NULL,
        ClearAcquisitionCount INT NOT NULL,
        OutputDirectory NVARCHAR(600) NOT NULL,
        ObservationsGeoJsonPath NVARCHAR(600) NULL,
        ZonesGeoJsonPath NVARCHAR(600) NULL,
        CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_SentinelPipelineWaterRuns_CreatedAt DEFAULT(SYSDATETIME()),
        ModifiedAt DATETIME2(0) NOT NULL CONSTRAINT DF_SentinelPipelineWaterRuns_ModifiedAt DEFAULT(SYSDATETIME())
    );
END;

IF OBJECT_ID('dbo.SentinelPipelineWaterBinObservations', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SentinelPipelineWaterBinObservations
    (
        PipelineWaterObservationId BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SentinelPipelineWaterBinObservations PRIMARY KEY,
        PipelineWaterRunId BIGINT NOT NULL,
        AcquisitionKey NVARCHAR(200) NOT NULL,
        AcquiredAt DATETIMEOFFSET(0) NOT NULL,
        BinIndex INT NOT NULL,
        StartChainageM DECIMAL(19,3) NOT NULL,
        EndChainageM DECIMAL(19,3) NOT NULL,
        ObservationState VARCHAR(12) NOT NULL,
        ExposureType VARCHAR(12) NULL,
        WaterAreaInCorridorM2 DECIMAL(19,2) NULL,
        LengthOnWaterM DECIMAL(19,3) NULL,
        NearestWaterDistanceM DECIMAL(19,3) NULL,
        RouteBinGeometry geometry NOT NULL,
        WaterIntersectionGeometry geometry NULL
    );
END;

IF OBJECT_ID('dbo.SentinelPipelineWaterZones', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SentinelPipelineWaterZones
    (
        PipelineWaterZoneId BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SentinelPipelineWaterZones PRIMARY KEY,
        PipelineWaterRunId BIGINT NOT NULL,
        ZoneOrdinal INT NOT NULL,
        StartChainageM DECIMAL(19,3) NOT NULL,
        EndChainageM DECIMAL(19,3) NOT NULL,
        LengthM DECIMAL(19,3) NOT NULL,
        WaterObservationCount INT NOT NULL,
        DryObservationCount INT NOT NULL,
        UnknownObservationCount INT NOT NULL,
        ClearObservationCount AS (WaterObservationCount + DryObservationCount) PERSISTED,
        WaterFrequency DECIMAL(8,6) NULL,
        PersistenceClass VARCHAR(20) NOT NULL,
        FirstWaterObservedAt DATETIMEOFFSET(0) NULL,
        LastWaterObservedAt DATETIMEOFFSET(0) NULL,
        MaximumWaterAreaM2 DECIMAL(19,2) NULL,
        MinimumWaterDistanceM DECIMAL(19,3) NULL,
        RouteZoneGeometry geometry NOT NULL
    );
END;

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_SentinelPipelineWaterRequests_SentinelGrabJobProducts' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterRequests'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterRequests
        ADD CONSTRAINT FK_SentinelPipelineWaterRequests_SentinelGrabJobProducts
            FOREIGN KEY (JobProductId) REFERENCES dbo.SentinelGrabJobProducts(JobProductId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_SentinelPipelineWaterRequests_SentinelPipelinePaths' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterRequests'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterRequests
        ADD CONSTRAINT FK_SentinelPipelineWaterRequests_SentinelPipelinePaths
            FOREIGN KEY (PipelinePathId) REFERENCES dbo.SentinelPipelinePaths(PipelinePathId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_SentinelPipelineWaterRuns_SentinelGrabJobs' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterRuns'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterRuns
        ADD CONSTRAINT FK_SentinelPipelineWaterRuns_SentinelGrabJobs
            FOREIGN KEY (JobId) REFERENCES dbo.SentinelGrabJobs(JobId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_SentinelPipelineWaterRuns_SentinelGrabJobProducts' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterRuns'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterRuns
        ADD CONSTRAINT FK_SentinelPipelineWaterRuns_SentinelGrabJobProducts
            FOREIGN KEY (JobProductId) REFERENCES dbo.SentinelGrabJobProducts(JobProductId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_SentinelPipelineWaterRuns_SentinelPipelinePaths' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterRuns'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterRuns
        ADD CONSTRAINT FK_SentinelPipelineWaterRuns_SentinelPipelinePaths
            FOREIGN KEY (PipelinePathId) REFERENCES dbo.SentinelPipelinePaths(PipelinePathId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_SentinelPipelineWaterBinObservations_SentinelPipelineWaterRuns' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterBinObservations
        ADD CONSTRAINT FK_SentinelPipelineWaterBinObservations_SentinelPipelineWaterRuns
            FOREIGN KEY (PipelineWaterRunId) REFERENCES dbo.SentinelPipelineWaterRuns(PipelineWaterRunId)
            ON DELETE CASCADE;
END;

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_SentinelPipelineWaterZones_SentinelPipelineWaterRuns' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterZones'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterZones
        ADD CONSTRAINT FK_SentinelPipelineWaterZones_SentinelPipelineWaterRuns
            FOREIGN KEY (PipelineWaterRunId) REFERENCES dbo.SentinelPipelineWaterRuns(PipelineWaterRunId)
            ON DELETE CASCADE;
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelinePaths_RouteGeometry' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelinePaths'))
BEGIN
    ALTER TABLE dbo.SentinelPipelinePaths WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelinePaths_RouteGeometry
            CHECK
            (
                RouteGeometry.STSrid = 4326
                AND RouteGeometry.STIsValid() = 1
                AND RouteGeometry.STIsEmpty() = 0
                AND RouteGeometry.STGeometryType() IN (N'LineString', N'MultiLineString')
            );
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelinePaths_RouteLengthM' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelinePaths'))
BEGIN
    ALTER TABLE dbo.SentinelPipelinePaths WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelinePaths_RouteLengthM
            CHECK (RouteLengthM > 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelinePaths_ChainageOriginM' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelinePaths'))
BEGIN
    ALTER TABLE dbo.SentinelPipelinePaths WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelinePaths_ChainageOriginM
            CHECK (ChainageOriginM >= 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterRequests_Distances' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterRequests'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterRequests WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterRequests_Distances
            CHECK (CorridorHalfWidthM > 0 AND AnalysisBinLengthM > 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterRequests_MinimumClearObservations' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterRequests'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterRequests WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterRequests_MinimumClearObservations
            CHECK (MinimumClearObservations > 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterRequests_Thresholds' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterRequests'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterRequests WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterRequests_Thresholds
            CHECK
            (
                PersistentFrequencyThreshold >= 0
                AND PersistentFrequencyThreshold <= 1
                AND SeasonalFrequencyThreshold >= 0
                AND SeasonalFrequencyThreshold <= 1
                AND PersistentFrequencyThreshold > SeasonalFrequencyThreshold
            );
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterRuns_DateRange' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterRuns'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterRuns WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterRuns_DateRange
            CHECK (DateTo >= DateFrom);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterRuns_Distances' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterRuns'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterRuns WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterRuns_Distances
            CHECK (CorridorHalfWidthM > 0 AND AnalysisBinLengthM > 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterRuns_Counts' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterRuns'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterRuns WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterRuns_Counts
            CHECK (AcquisitionCount >= 0 AND ClearAcquisitionCount >= 0 AND ClearAcquisitionCount <= AcquisitionCount);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterBinObservations_BinIndex' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterBinObservations WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterBinObservations_BinIndex
            CHECK (BinIndex >= 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterBinObservations_Chainage' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterBinObservations WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterBinObservations_Chainage
            CHECK (StartChainageM >= 0 AND EndChainageM > StartChainageM);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterBinObservations_ObservationState' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterBinObservations WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterBinObservations_ObservationState
            CHECK (ObservationState IN ('Water', 'Dry', 'Unknown'));
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterBinObservations_ExposureType' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterBinObservations WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterBinObservations_ExposureType
            CHECK (ExposureType IS NULL OR ExposureType IN ('Crossing', 'Proximity'));
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterBinObservations_Measures' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterBinObservations WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterBinObservations_Measures
            CHECK
            (
                (WaterAreaInCorridorM2 IS NULL OR WaterAreaInCorridorM2 >= 0)
                AND (LengthOnWaterM IS NULL OR LengthOnWaterM >= 0)
                AND (NearestWaterDistanceM IS NULL OR NearestWaterDistanceM >= 0)
            );
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterBinObservations_RouteBinGeometry' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterBinObservations WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterBinObservations_RouteBinGeometry
            CHECK
            (
                RouteBinGeometry.STSrid = 4326
                AND RouteBinGeometry.STIsValid() = 1
                AND RouteBinGeometry.STIsEmpty() = 0
                AND RouteBinGeometry.STGeometryType() IN (N'LineString', N'MultiLineString')
            );
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterBinObservations_WaterIntersectionGeometry' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterBinObservations WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterBinObservations_WaterIntersectionGeometry
            CHECK
            (
                WaterIntersectionGeometry IS NULL
                OR
                (
                    WaterIntersectionGeometry.STSrid = 4326
                    AND WaterIntersectionGeometry.STIsValid() = 1
                    AND WaterIntersectionGeometry.STIsEmpty() = 0
                )
            );
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterZones_ZoneOrdinal' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterZones'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterZones WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterZones_ZoneOrdinal
            CHECK (ZoneOrdinal > 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterZones_Chainage' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterZones'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterZones WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterZones_Chainage
            CHECK (StartChainageM >= 0 AND EndChainageM > StartChainageM AND LengthM > 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterZones_Counts' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterZones'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterZones WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterZones_Counts
            CHECK (WaterObservationCount >= 0 AND DryObservationCount >= 0 AND UnknownObservationCount >= 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterZones_WaterFrequency' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterZones'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterZones WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterZones_WaterFrequency
            CHECK (WaterFrequency IS NULL OR (WaterFrequency >= 0 AND WaterFrequency <= 1));
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterZones_PersistenceClass' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterZones'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterZones WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterZones_PersistenceClass
            CHECK (PersistenceClass IN ('Persistent', 'Seasonal', 'Intermittent', 'InsufficientData'));
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterZones_Measures' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterZones'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterZones WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterZones_Measures
            CHECK
            (
                (MaximumWaterAreaM2 IS NULL OR MaximumWaterAreaM2 >= 0)
                AND (MinimumWaterDistanceM IS NULL OR MinimumWaterDistanceM >= 0)
            );
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelPipelineWaterZones_RouteZoneGeometry' AND parent_object_id = OBJECT_ID('dbo.SentinelPipelineWaterZones'))
BEGIN
    ALTER TABLE dbo.SentinelPipelineWaterZones WITH CHECK
        ADD CONSTRAINT CK_SentinelPipelineWaterZones_RouteZoneGeometry
            CHECK
            (
                RouteZoneGeometry.STSrid = 4326
                AND RouteZoneGeometry.STIsValid() = 1
                AND RouteZoneGeometry.STIsEmpty() = 0
                AND RouteZoneGeometry.STGeometryType() IN (N'LineString', N'MultiLineString')
            );
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_SentinelPipelinePaths_SourceHash' AND object_id = OBJECT_ID('dbo.SentinelPipelinePaths'))
BEGIN
    CREATE UNIQUE INDEX UX_SentinelPipelinePaths_SourceHash
        ON dbo.SentinelPipelinePaths(SourceHash);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'SIX_SentinelPipelinePaths_RouteGeometry' AND object_id = OBJECT_ID('dbo.SentinelPipelinePaths'))
BEGIN
    CREATE SPATIAL INDEX SIX_SentinelPipelinePaths_RouteGeometry
        ON dbo.SentinelPipelinePaths(RouteGeometry)
        USING GEOMETRY_GRID
        WITH
        (
            BOUNDING_BOX = (xmin = -180, ymin = -90, xmax = 180, ymax = 90),
            GRIDS = (LEVEL_1 = MEDIUM, LEVEL_2 = MEDIUM, LEVEL_3 = MEDIUM, LEVEL_4 = MEDIUM),
            CELLS_PER_OBJECT = 16
        );
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_SentinelPipelineWaterRuns_JobProductId' AND object_id = OBJECT_ID('dbo.SentinelPipelineWaterRuns'))
BEGIN
    CREATE UNIQUE INDEX UX_SentinelPipelineWaterRuns_JobProductId
        ON dbo.SentinelPipelineWaterRuns(JobProductId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_SentinelPipelineWaterBinObservations_RunAcquisitionBin' AND object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    CREATE UNIQUE INDEX UX_SentinelPipelineWaterBinObservations_RunAcquisitionBin
        ON dbo.SentinelPipelineWaterBinObservations(PipelineWaterRunId, AcquisitionKey, BinIndex);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SentinelPipelineWaterBinObservations_RunBin' AND object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    CREATE INDEX IX_SentinelPipelineWaterBinObservations_RunBin
        ON dbo.SentinelPipelineWaterBinObservations(PipelineWaterRunId, BinIndex);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SentinelPipelineWaterBinObservations_AcquiredAt' AND object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    CREATE INDEX IX_SentinelPipelineWaterBinObservations_AcquiredAt
        ON dbo.SentinelPipelineWaterBinObservations(AcquiredAt);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SentinelPipelineWaterBinObservations_ObservationState' AND object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    CREATE INDEX IX_SentinelPipelineWaterBinObservations_ObservationState
        ON dbo.SentinelPipelineWaterBinObservations(ObservationState);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'SIX_SentinelPipelineWaterBinObservations_RouteBinGeometry' AND object_id = OBJECT_ID('dbo.SentinelPipelineWaterBinObservations'))
BEGIN
    CREATE SPATIAL INDEX SIX_SentinelPipelineWaterBinObservations_RouteBinGeometry
        ON dbo.SentinelPipelineWaterBinObservations(RouteBinGeometry)
        USING GEOMETRY_GRID
        WITH
        (
            BOUNDING_BOX = (xmin = -180, ymin = -90, xmax = 180, ymax = 90),
            GRIDS = (LEVEL_1 = MEDIUM, LEVEL_2 = MEDIUM, LEVEL_3 = MEDIUM, LEVEL_4 = MEDIUM),
            CELLS_PER_OBJECT = 16
        );
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_SentinelPipelineWaterZones_RunZoneOrdinal' AND object_id = OBJECT_ID('dbo.SentinelPipelineWaterZones'))
BEGIN
    CREATE UNIQUE INDEX UX_SentinelPipelineWaterZones_RunZoneOrdinal
        ON dbo.SentinelPipelineWaterZones(PipelineWaterRunId, ZoneOrdinal);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SentinelPipelineWaterZones_RunChainageClass' AND object_id = OBJECT_ID('dbo.SentinelPipelineWaterZones'))
BEGIN
    CREATE INDEX IX_SentinelPipelineWaterZones_RunChainageClass
        ON dbo.SentinelPipelineWaterZones(PipelineWaterRunId, StartChainageM, EndChainageM, PersistenceClass);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'SIX_SentinelPipelineWaterZones_RouteZoneGeometry' AND object_id = OBJECT_ID('dbo.SentinelPipelineWaterZones'))
BEGIN
    CREATE SPATIAL INDEX SIX_SentinelPipelineWaterZones_RouteZoneGeometry
        ON dbo.SentinelPipelineWaterZones(RouteZoneGeometry)
        USING GEOMETRY_GRID
        WITH
        (
            BOUNDING_BOX = (xmin = -180, ymin = -90, xmax = 180, ymax = 90),
            GRIDS = (LEVEL_1 = MEDIUM, LEVEL_2 = MEDIUM, LEVEL_3 = MEDIUM, LEVEL_4 = MEDIUM),
            CELLS_PER_OBJECT = 16
        );
END;
