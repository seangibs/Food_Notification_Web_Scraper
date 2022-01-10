/********
SQL used within python script for creating tables
*********/

--notification staging table
BEGIN TRY
            
    BEGIN TRANSACTION
        --If this table does not exist then create it
        IF OBJECT_ID(N'stg_RASFF_Notifications', N'U') IS NULL
            BEGIN
                CREATE TABLE [stg_RASFF_Notifications](
                [Reference] VARCHAR(100) NOT NULL,
                [Subject] NVARCHAR(500) NULL,
                [NotificationDate] NVARCHAR(100) NULL,
                [LastUpdate] NVARCHAR(100) NULL,
                [NotificationType] NVARCHAR(255) NULL,
                [NotificationCategory] NVARCHAR(255) NULL,
                [NotificationSource] NVARCHAR(255) NULL,
                [NotificationStatus] NVARCHAR(255) NULL,
                [ActionTaken] NVARCHAR(255) NULL,
                [NotificationFrom] NVARCHAR(255) NULL,
                [DistributionStatus] NVARCHAR(1000) NULL,
                [Product] NVARCHAR(255) NULL,
                [ProductCategory] NVARCHAR(255) NULL,
                [RiskDecision] NVARCHAR(255) NULL)
            END
            
    COMMIT TRAN
            
END TRY
            
BEGIN CATCH
            
    IF @@TRANCOUNT > 0
        ROLLBACK TRAN
            
END CATCH


--hazard staging
BEGIN TRY
    
    BEGIN TRANSACTION
        
        --If this table does not exist then create it
        IF OBJECT_ID(N'stg_RASFF_Notifications_SubstanceHazard', N'U') IS NULL
            BEGIN
                CREATE TABLE [stg_RASFF_Notifications_SubstanceHazard](
                    [Reference] VARCHAR(100) NOT NULL,
                    [SubstanceHazard] NVARCHAR(255) NULL,
                    [Category] NVARCHAR(255) NULL,
                    [AnalyticalResult] NVARCHAR(255) NULL,
                    [Units] NVARCHAR(255) NULL,
                    [SamplingDate] NVARCHAR(10) NULL) 
            END
        
    COMMIT TRAN
        
END TRY
        
BEGIN CATCH
        
    IF @@TRANCOUNT > 0
        ROLLBACK TRAN
        
END CATCH

--country staging
BEGIN TRY
    
    BEGIN TRANSACTION
        
        --If this table does not exist then create it
        IF OBJECT_ID(N'stg_RASFF_Notifications_Country', N'U') IS NULL
            BEGIN
                        
                CREATE TABLE [stg_RASFF_Notifications_Country](
                    [Reference] VARCHAR(255) NOT NULL,
                    [Country] NVARCHAR(255) NULL,
                    [Type] BIT NOT NULL
                )
            END
        
    COMMIT TRAN
        
END TRY
        
BEGIN CATCH
        
    IF @@TRANCOUNT > 0
        ROLLBACK TRAN
        
END CATCH

/***************
Interface Tables
***************/
--Notifications
BEGIN TRY
        
    BEGIN TRANSACTION
        
        --If this table does not exist then create it
        IF OBJECT_ID(N'tbl_RASFF_Notifications', N'U') IS NULL
            BEGIN
                        
                CREATE TABLE [tbl_RASFF_Notifications](
                    [Reference] VARCHAR(100) NOT NULL,
                    [Subject] NVARCHAR(500) NULL,
                    [NotificationDate] DATE NULL,
                    [LastUpdate] DATE NULL,
                    [NotificationType] NVARCHAR(255) NULL,
                    [NotificationCategory] NVARCHAR(255) NULL,
                    [NotificationSource] NVARCHAR(255) NULL,
                    [NotificationStatus] NVARCHAR(255) NULL,
                    [ActionTaken] NVARCHAR(255) NULL,
                    [NotificationFrom] NVARCHAR(255) NULL,
                    [DistributionStatus] NVARCHAR(1000) NULL,
                    [Product] NVARCHAR(255) NULL,
                    [ProductCategory] NVARCHAR(255) NULL,
                    [RiskDecision] NVARCHAR(255) NULL
        
                PRIMARY KEY CLUSTERED 
                (
                    [Reference] ASC
                ))
                        
            END
        
    COMMIT TRAN
        
END TRY
        
BEGIN CATCH
        
    IF @@TRANCOUNT > 0
        ROLLBACK TRAN
        
END CATCH

--Hazards
BEGIN TRY
        
    BEGIN TRANSACTION
        
        --If this table does not exist then create it
        IF OBJECT_ID(N'tbl_RASFF_Notifications_SubstanceHazard', N'U') IS NULL
            BEGIN
                        
                        
                CREATE TABLE [tbl_RASFF_Notifications_SubstanceHazard](
                    [Id] BIGINT NOT NULL,
                    [Reference] VARCHAR(100) NOT NULL,
                    [SubstanceHazard] NVARCHAR(255) NULL,
                    [Category] NVARCHAR(255) NULL,
                    [AnalyticalResult] NVARCHAR(255) NULL,
                    [Units] NVARCHAR(255) NULL,
                    [SamplingDate] DATE NULL,
                    PRIMARY KEY([Id]),
                    FOREIGN KEY ([Reference]) REFERENCES [tbl_RASFF_Notifications]([Reference])
                ) 
                        
            END
        
    COMMIT TRAN
        
END TRY
        
BEGIN CATCH
        
    IF @@TRANCOUNT > 0
        ROLLBACK TRAN
        
END CATCH 

--Country
BEGIN TRY
    
BEGIN TRANSACTION
    
        --If this table does not exist then create it
        IF OBJECT_ID(N'tbl_RASFF_Notifications_Country', N'U') IS NULL
            BEGIN
                        
                CREATE TABLE [tbl_RASFF_Notifications_Country](
                    [Id] BIGINT NOT NULL,
                    [Reference] VARCHAR(100) NOT NULL,
                    [Country] NVARCHAR(255) NULL,
                    [Type] BIT NOT NULL
                    PRIMARY KEY([Id]),
                    FOREIGN KEY ([Reference]) REFERENCES [tbl_RASFF_Notifications]([Reference])
                ) 
                        
            END
        
    COMMIT TRAN
        
END TRY
        
BEGIN CATCH
        
    IF @@TRANCOUNT > 0
        ROLLBACK TRAN
        
END CATCH