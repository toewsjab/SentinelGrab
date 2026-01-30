IF OBJECT_ID('dbo.SentinelGrabJobProducts', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SentinelGrabJobProducts
    (
        JobProductId BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SentinelGrabJobProducts PRIMARY KEY,
        JobId BIGINT NOT NULL,
        ProductCode NVARCHAR(20) NOT NULL,
        OutputSubPath NVARCHAR(200) NULL,
        Status NVARCHAR(20) NOT NULL CONSTRAINT DF_SentinelGrabJobProducts_Status DEFAULT('Queued'),
        LastError NVARCHAR(MAX) NULL,
        LastLog NVARCHAR(MAX) NULL,
        CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_SentinelGrabJobProducts_CreatedAt DEFAULT(SYSDATETIME()),
        ModifiedAt DATETIME2(0) NOT NULL CONSTRAINT DF_SentinelGrabJobProducts_ModifiedAt DEFAULT(SYSDATETIME())
    );

    ALTER TABLE dbo.SentinelGrabJobProducts
        ADD CONSTRAINT FK_SentinelGrabJobProducts_SentinelGrabJobs
            FOREIGN KEY (JobId) REFERENCES dbo.SentinelGrabJobs(JobId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_JobId' AND object_id = OBJECT_ID('dbo.SentinelGrabJobProducts'))
BEGIN
    CREATE INDEX IX_JobId ON dbo.SentinelGrabJobProducts(JobId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_JobId_ProductCode' AND object_id = OBJECT_ID('dbo.SentinelGrabJobProducts'))
BEGIN
    CREATE UNIQUE INDEX UX_JobId_ProductCode ON dbo.SentinelGrabJobProducts(JobId, ProductCode);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SentinelGrabJobs_StatusPriority' AND object_id = OBJECT_ID('dbo.SentinelGrabJobs'))
BEGIN
    DECLARE @include NVARCHAR(MAX) = '';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'JobId') IS NOT NULL SET @include = @include + ',JobId';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'DateKey') IS NOT NULL SET @include = @include + ',DateKey';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'DateFrom') IS NOT NULL SET @include = @include + ',DateFrom';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'DateTo') IS NOT NULL SET @include = @include + ',DateTo';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'CloudCoverMax') IS NOT NULL SET @include = @include + ',CloudCoverMax';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'PreferMosaic') IS NOT NULL SET @include = @include + ',PreferMosaic';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'MaxScenes') IS NOT NULL SET @include = @include + ',MaxScenes';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'Bbox') IS NOT NULL SET @include = @include + ',Bbox';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'BboxMinLon') IS NOT NULL SET @include = @include + ',BboxMinLon';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'BboxMinLat') IS NOT NULL SET @include = @include + ',BboxMinLat';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'BboxMaxLon') IS NOT NULL SET @include = @include + ',BboxMaxLon';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'BboxMaxLat') IS NOT NULL SET @include = @include + ',BboxMaxLat';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'OutputRootPath') IS NOT NULL SET @include = @include + ',OutputRootPath';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'ZoomMin') IS NOT NULL SET @include = @include + ',ZoomMin';
    IF COL_LENGTH('dbo.SentinelGrabJobs', 'ZoomMax') IS NOT NULL SET @include = @include + ',ZoomMax';

    IF LEN(@include) > 0
        SET @include = ' INCLUDE (' + STUFF(@include, 1, 1, '') + ')';

    DECLARE @sql NVARCHAR(MAX) = 'CREATE INDEX IX_SentinelGrabJobs_StatusPriority ON dbo.SentinelGrabJobs (Status, Priority, CreatedAt)' + @include;
    EXEC(@sql);
END;
