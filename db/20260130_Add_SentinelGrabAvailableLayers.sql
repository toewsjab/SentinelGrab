IF OBJECT_ID('dbo.SentinelGrabAvailableLayers', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.SentinelGrabAvailableLayers
    (
        AvailableLayerId BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SentinelGrabAvailableLayers PRIMARY KEY,
        JobId BIGINT NOT NULL,
        JobProductId BIGINT NOT NULL,
        ProductCode NVARCHAR(20) NOT NULL,
        DateKey NVARCHAR(20) NOT NULL,
        DateFrom DATE NULL,
        DateTo DATE NULL,
        BboxMinLon FLOAT NULL,
        BboxMinLat FLOAT NULL,
        BboxMaxLon FLOAT NULL,
        BboxMaxLat FLOAT NULL,
        OutputRootPath NVARCHAR(400) NOT NULL,
        ProductSubPath NVARCHAR(200) NOT NULL,
        OutputDir NVARCHAR(600) NOT NULL,
        CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_SentinelGrabAvailableLayers_CreatedAt DEFAULT(SYSDATETIME()),
        ModifiedAt DATETIME2(0) NOT NULL CONSTRAINT DF_SentinelGrabAvailableLayers_ModifiedAt DEFAULT(SYSDATETIME())
    );

    ALTER TABLE dbo.SentinelGrabAvailableLayers
        ADD CONSTRAINT FK_SentinelGrabAvailableLayers_SentinelGrabJobs
            FOREIGN KEY (JobId) REFERENCES dbo.SentinelGrabJobs(JobId);

    ALTER TABLE dbo.SentinelGrabAvailableLayers
        ADD CONSTRAINT FK_SentinelGrabAvailableLayers_SentinelGrabJobProducts
            FOREIGN KEY (JobProductId) REFERENCES dbo.SentinelGrabJobProducts(JobProductId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_SentinelGrabAvailableLayers_JobProductId' AND object_id = OBJECT_ID('dbo.SentinelGrabAvailableLayers'))
BEGIN
    CREATE UNIQUE INDEX UX_SentinelGrabAvailableLayers_JobProductId ON dbo.SentinelGrabAvailableLayers(JobProductId);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SentinelGrabAvailableLayers_ProductDate' AND object_id = OBJECT_ID('dbo.SentinelGrabAvailableLayers'))
BEGIN
    CREATE INDEX IX_SentinelGrabAvailableLayers_ProductDate
        ON dbo.SentinelGrabAvailableLayers(ProductCode, DateKey)
        INCLUDE (OutputDir, OutputRootPath, ProductSubPath, DateFrom, DateTo, BboxMinLon, BboxMinLat, BboxMaxLon, BboxMaxLat);
END;

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_SentinelGrabAvailableLayers_DateKey' AND object_id = OBJECT_ID('dbo.SentinelGrabAvailableLayers'))
BEGIN
    CREATE INDEX IX_SentinelGrabAvailableLayers_DateKey
        ON dbo.SentinelGrabAvailableLayers(DateKey)
        INCLUDE (ProductCode, OutputDir);
END;
