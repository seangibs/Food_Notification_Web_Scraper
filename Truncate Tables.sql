/****************************
Script to truncate all tables
****************************/

--Truncate all tables
BEGIN TRY
        
    BEGIN TRANSACTION
        
        --Truncate main notification table is exists. Cannot truncate as it has a key reference
        IF OBJECT_ID(N'tbl_RASFF_Notifications', N'U') IS NOT NULL
            BEGIN
                       
                DELETE FROM [tbl_RASFF_Notifications]
                        
            END
                    
        --Truncate main country table is exists
        IF OBJECT_ID(N'tbl_RASFF_Notifications_Country', N'U') IS NOT NULL
            BEGIN
                        
                TRUNCATE TABLE [tbl_RASFF_Notifications_Country]
                        
            END
                    
        --Truncate main hazard table is exists
        IF OBJECT_ID(N'tbl_RASFF_Notifications_SubstanceHazard', N'U') IS NOT NULL
            BEGIN
                            
                TRUNCATE TABLE [tbl_RASFF_Notifications_SubstanceHazard]
                        
            END
        
        
        --Truncate staging notification table is exists
        IF OBJECT_ID(N'stg_RASFF_Notifications', N'U') IS NOT NULL
            BEGIN
                        
                TRUNCATE TABLE [stg_RASFF_Notifications]
                        
            END
                    
        --Truncate staging country table is exists
        IF OBJECT_ID(N'stg_RASFF_Notifications_Country', N'U') IS NOT NULL
            BEGIN
                        
                TRUNCATE TABLE [stg_RASFF_Notifications_Country]
                        
            END
                    
        --Truncate staging hazard table is exists
        IF OBJECT_ID(N'stg_RASFF_Notifications_SubstanceHazard', N'U') IS NOT NULL
            BEGIN
                            
                TRUNCATE TABLE [stg_RASFF_Notifications_SubstanceHazard]
                        
            END
        
    COMMIT TRAN
        
END TRY
        
BEGIN CATCH
        
    IF @@TRANCOUNT > 0
        ROLLBACK TRAN
        
END CATCH