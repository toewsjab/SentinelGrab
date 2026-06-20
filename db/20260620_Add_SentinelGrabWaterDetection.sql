SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;

IF OBJECT_ID('dbo.SentinelGrabWaterDetections', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SentinelGrabWaterDetections
    (
        WaterDetectionId BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SentinelGrabWaterDetections PRIMARY KEY,
        JobId BIGINT NOT NULL,
        JobProductId BIGINT NOT NULL,
        DateKey NVARCHAR(20) NOT NULL,
        AcquisitionKey NVARCHAR(200) NOT NULL,
        AcquiredAt DATETIMEOFFSET(0) NULL,
        SourceItemIdsJson NVARCHAR(MAX) NOT NULL,
        DetectionMethod NVARCHAR(20) NOT NULL,
        AlgorithmVersion NVARCHAR(40) NOT NULL,
        ThresholdsJson NVARCHAR(2000) NOT NULL,
        ProcessingSrid INT NOT NULL,
        ResolutionMetres DECIMAL(9,3) NOT NULL,
        MinAreaSquareMetres DECIMAL(18,2) NOT NULL,
        ClearCoveragePercent DECIMAL(7,3) NOT NULL,
        UnknownCoveragePercent DECIMAL(7,3) NOT NULL,
        PolygonCount INT NOT NULL,
        TotalWaterAreaSquareMetres DECIMAL(19,2) NOT NULL,
        GeoJsonPath NVARCHAR(600) NOT NULL,
        CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_SentinelGrabWaterDetections_CreatedAt DEFAULT(SYSDATETIME()),
        ModifiedAt DATETIME2(0) NOT NULL CONSTRAINT DF_SentinelGrabWaterDetections_ModifiedAt DEFAULT(SYSDATETIME())
    );
END;

IF OBJECT_ID('dbo.SentinelGrabWaterPolygons', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SentinelGrabWaterPolygons
    (
        WaterPolygonId BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SentinelGrabWaterPolygons PRIMARY KEY,
        WaterDetectionId BIGINT NOT NULL,
        PolygonOrdinal INT NOT NULL,
        Shape geometry NOT NULL,
        AreaSquareMetres DECIMAL(19,2) NOT NULL,
        CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_SentinelGrabWaterPolygons_CreatedAt DEFAULT(SYSDATETIME())
    );
END;

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_SentinelGrabWaterDetections_SentinelGrabJobs' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterDetections'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterDetections
        ADD CONSTRAINT FK_SentinelGrabWaterDetections_SentinelGrabJobs
            FOREIGN KEY (JobId) REFERENCES dbo.SentinelGrabJobs(JobId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_SentinelGrabWaterDetections_SentinelGrabJobProducts' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterDetections'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterDetections
        ADD CONSTRAINT FK_SentinelGrabWaterDetections_SentinelGrabJobProducts
            FOREIGN KEY (JobProductId) REFERENCES dbo.SentinelGrabJobProducts(JobProductId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'FK_SentinelGrabWaterPolygons_SentinelGrabWaterDetections' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterPolygons'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterPolygons
        ADD CONSTRAINT FK_SentinelGrabWaterPolygons_SentinelGrabWaterDetections
            FOREIGN KEY (WaterDetectionId) REFERENCES dbo.SentinelGrabWaterDetections(WaterDetectionId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterDetections_Method' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterDetections'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterDetections WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterDetections_Method
            CHECK (DetectionMethod IN (N'Scl', N'Hybrid'));
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterDetections_ProcessingSrid' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterDetections'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterDetections WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterDetections_ProcessingSrid
            CHECK (ProcessingSrid > 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterDetections_ResolutionMetres' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterDetections'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterDetections WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterDetections_ResolutionMetres
            CHECK (ResolutionMetres > 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterDetections_MinAreaSquareMetres' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterDetections'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterDetections WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterDetections_MinAreaSquareMetres
            CHECK (MinAreaSquareMetres > 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterDetections_ClearCoveragePercent' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterDetections'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterDetections WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterDetections_ClearCoveragePercent
            CHECK (ClearCoveragePercent >= 0 AND ClearCoveragePercent <= 100);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterDetections_UnknownCoveragePercent' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterDetections'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterDetections WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterDetections_UnknownCoveragePercent
            CHECK (UnknownCoveragePercent >= 0 AND UnknownCoveragePercent <= 100);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterDetections_PolygonCount' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterDetections'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterDetections WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterDetections_PolygonCount
            CHECK (PolygonCount >= 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterDetections_TotalWaterAreaSquareMetres' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterDetections'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterDetections WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterDetections_TotalWaterAreaSquareMetres
            CHECK (TotalWaterAreaSquareMetres >= 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterPolygons_PolygonOrdinal' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterPolygons'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterPolygons WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterPolygons_PolygonOrdinal
            CHECK (PolygonOrdinal > 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterPolygons_AreaSquareMetres' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterPolygons'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterPolygons WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterPolygons_AreaSquareMetres
            CHECK (AreaSquareMetres >= 0);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterPolygons_ShapeSrid' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterPolygons'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterPolygons WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterPolygons_ShapeSrid
            CHECK (Shape.STSrid = 4326);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterPolygons_ShapeValid' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterPolygons'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterPolygons WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterPolygons_ShapeValid
            CHECK (Shape.STIsValid() = 1);
END;

IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name = 'CK_SentinelGrabWaterPolygons_ShapeType' AND parent_object_id = OBJECT_ID('dbo.SentinelGrabWaterPolygons'))
BEGIN
    ALTER TABLE dbo.SentinelGrabWaterPolygons WITH CHECK
        ADD CONSTRAINT CK_SentinelGrabWaterPolygons_ShapeType
            CHECK (Shape.STGeometryType() IN (N'Polygon', N'MultiPolygon'));
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_SentinelGrabWaterDetections_JobProductId' AND object_id = OBJECT_ID('dbo.SentinelGrabWaterDetections'))
BEGIN
    CREATE UNIQUE INDEX UX_SentinelGrabWaterDetections_JobProductId
        ON dbo.SentinelGrabWaterDetections(JobProductId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_SentinelGrabWaterPolygons_DetectionOrdinal' AND object_id = OBJECT_ID('dbo.SentinelGrabWaterPolygons'))
BEGIN
    CREATE UNIQUE INDEX UX_SentinelGrabWaterPolygons_DetectionOrdinal
        ON dbo.SentinelGrabWaterPolygons(WaterDetectionId, PolygonOrdinal);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SentinelGrabWaterPolygons_WaterDetectionId' AND object_id = OBJECT_ID('dbo.SentinelGrabWaterPolygons'))
BEGIN
    CREATE INDEX IX_SentinelGrabWaterPolygons_WaterDetectionId
        ON dbo.SentinelGrabWaterPolygons(WaterDetectionId)
        INCLUDE (AreaSquareMetres);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'SIX_SentinelGrabWaterPolygons_Shape' AND object_id = OBJECT_ID('dbo.SentinelGrabWaterPolygons'))
BEGIN
    CREATE SPATIAL INDEX SIX_SentinelGrabWaterPolygons_Shape
        ON dbo.SentinelGrabWaterPolygons(Shape)
        USING GEOMETRY_GRID
        WITH
        (
            BOUNDING_BOX = (xmin = -180, ymin = -90, xmax = 180, ymax = 90),
            GRIDS = (LEVEL_1 = MEDIUM, LEVEL_2 = MEDIUM, LEVEL_3 = MEDIUM, LEVEL_4 = MEDIUM),
            CELLS_PER_OBJECT = 16
        );
END;
